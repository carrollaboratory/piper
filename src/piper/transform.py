#!/usr/bin/env python

import logging
import os
import sys
from argparse import ArgumentParser, FileType
from collections import defaultdict
from importlib.metadata import version
from pathlib import Path

from yaml import safe_load

from piper.datamodel import LinkMLModelLoader
from piper.datamodel.linkml import get_model_pk
from piper.fhir_consumers import (
    DewrangleJSON,
    ResourceSummary,
    ValidateAgainstIG,
    ValidateResourceBasic,
)
from piper.fhir_consumers.utils import Progress
from piper.template_projector import TemplateProjector

from . import setup_logging

# This will load the configuration from the piper.yaml file
from .config import load_piper_config

try:
    from rich import print
except:
    pass


def build_host_config():
    """If we aren't using ~/.fhir_hosts file, then we can just use the same
    values but pull them from the environment. We'll build out a simple object
    that will look exactly the same."""
    auth_type = os.getenv("AUTH_TYPE", None)
    if auth_type is None:
        logging.warn("No FHIR Host auth type. Assuming local HAPI with no auth")
        return {
            "auth_type": "auth_basic",
            "target_service_url": "http://localhost:8080/fhir",
            "username": "test",
            "password": "nopass",
        }
    elif auth_type == "auth_basic":
        return {
            "auth_type": "auth_basic",
            "target_service_url": os.getenv("FHIR_HOST", "http://localhost:8080/fhir"),
            "username": os.getenv("FHIR_USER", "test"),
            "password": os.getenv("FHIR_PWD", "nopass"),
        }
    # For now, we'll assume a KF openid based auth
    assert os.getenv("FHIR_CLIENT_ID")
    assert os.getenv("FHIR_SECRET")
    return {
        "auth_type": os.getenv("AUTH_TYPE", "auth_kf_openid"),
        "target_service_url": os.getenv(
            "TARGET_SERVICE_URL", "http://localhost:8080/fhir"
        ),
        "client_id": os.getenv("FHIR_CLIENT_ID"),
        "password": os.getenv("FHIR_SECRET"),
    }


def play(cfg, resource_consumers, resource_summary, harmony_file=None, dburi=None, dbenv='dev'):
    config = safe_load(cfg)
    print(config)

    database_uri = dburi
    if not database_uri:
        database_uri = config["db"][dbenv]["uri"]

    db_schema_name = config["db"][dbenv]["db_schema_name"]
    tbl_prefix = config["db"]["table_name_template"]

    harmony_file = harmony_file or Path(config.get("harmony"))

    logging.info(f"Projection Configuration:\t'{cfg.name}'")
    model_config = config["data_model"]
    if "as_file" in model_config:
        logging.info(
            f"Loading model:\t'{model_config['as_file']['model_filename']}'"
        )

    import pdb
    pdb.set_trace()
    datamodel = LinkMLModelLoader(
        database_url=database_uri,
        model_as_file=model_config.get("as_file"),
        model_import_path=model_config.get("import_path"),
        table_prefix=tbl_prefix,
        schema_name=db_schema_name,
    ).load()
    session = datamodel.create_session()

    projection_template_dir = config["projection"]["templates"]
    model_helpers = config["data_model"]["model_helpers"]
    TemplateProjector.curie_map = config["projection"]["curie_mappings"]
    projector = TemplateProjector(
        model_helpers=model_helpers,
        template_dir=projection_template_dir,
        resource_consumers=resource_consumers,
        harmony_file=harmony_file,
        data_model=datamodel
    )
    projections = defaultdict(list)

    # Studies
    study_classname = model_helpers["study"]["classname"]
    study_model = datamodel.get_model(study_classname)
    study_pk = get_model_pk(study_model)
    # For now, let's not allow our primary keys to be complex
    assert len(study_pk) == 1
    study_pk = study_pk[0]
    studies = session.query(study_model).all()
    logging.info(f"{len(studies)} studies found")

    # These are the ids we will recognize as being relevant
    study_ids = set()
    with Progress() as progress:
        task = progress.add_task("Processing Studies ", total=len(studies))

        for study in studies:
            if study:
                local_projections = defaultdict(list)
                projector.process_study(study)
                projections.update(local_projections)
                progress.update(task, advance=1)

                study_id = getattr(study, study_pk)

                # Now, let's grab all of the data associated with that
                # study

                innersess = datamodel.create_session()
                for cls in projector.templates.keys():
                    # No point in redoing the study again
                    if cls != study_classname:
                        # Extract the appropriate SQLA Class
                        dataclass = datamodel.get_model(cls)
                        # Get the study_id attribute so that we can
                        # perform the necessary query
                        key_attr = getattr(dataclass, study_pk)

                        # Grab the data!
                        content = innersess.query(dataclass).filter(key_attr == study_id).all()
                        for entry in content:
                            projector.process_entry(entry=entry, study=study, model_helpers=model_helpers)

                resource_summary.reset(f"Study '{study_id}' Data")
    """
    # Subjects
    local_projections = defaultdict(list)

    # Get Subject count for progress bar
    subject_model = datamodel.get_model(model_helpers["subject"]["classname"])
    subject_count = session.query(subject_model).count()
    with Progress() as progress:
        task = progress.add_task("Processing Subjects ", total=subject_count)

        # for subject in subjects:
        for subject in datamodel.stream(model_helpers["subject"]["classname"]):
            # TODO: if we have more than one study, how are we handling
            # participants? Are they found inside the study or do they point
            # to their primary study? Is there a mechanism we need to use to
            # single out the primary study resource?
            projector.process_subject(subject, study=studies[0])
            progress.update(task, advance=1)

    resource_summary.reset("Patient Data")
    # with Path("output/data_resources.json").open('rt') as f:
    """
    resource_summary.report_totals("All Data")


def run():
    hosts_file = Path("~/.fhir_hosts").expanduser()
    host_config = None
    if hosts_file.exists():
        host_config = safe_load(hosts_file.open("rt"))

    parser = ArgumentParser(
        description="Transform data from RDB into FHIR resources for one or more studies"
    )
    parser.add_argument(
        "-c",
        "--class-name",
        type=str,
        nargs="+",
        help="For selectively processing data, provide the LinkML class name associated with the data to be transformed",
    )
    parser.add_argument(
        "-o",
        "--output-directory",
        default="output",
        type=str,
        help="Directory where output files are written.",
    )
    if host_config:
        parser.add_argument(
            "--host",
            choices=host_config.keys(),
            default=None,
            help="Optional host configuration if '~/.fhir_hosts' exists",
        )
    parser.add_argument(
        "-e",
        "--dbenv",
        default="local",
        type=str,
        help="Which DB Connection details from the config should we use",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=["NOTSET", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
        help="Log level",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate FHIR resources as they are produced",
    )
    parser.add_argument(
        "--max-validation-count",
        type=int,
        default=0,
        help="When greater than 0, only validate that many of any given resource type",
    )
    parser.add_argument(
        "config",
        type=FileType("rt"),
        nargs="+",
        help="YAML configuration for data extraction",
    )
    parser.add_argument(
        "--harmony",
        type=FileType("rt"),
        help="File to be used for lookups",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {version('piper')}"
    )
    args = parser.parse_args()
    setup_logging(args.log_level)

    logging.info(f"Piper Transform: '{','.join([f.name for f in args.config])}'")

    if "host" in args and args.host is not None:
        hostcfg = host_config["args.host"]
    else:
        # Extract relevant stuff from the environment
        hostcfg = build_host_config()

    resource_summary = ResourceSummary()
    resource_consumers = [
        ValidateResourceBasic(),
        resource_summary,
        DewrangleJSON(filename="output/fhir/dewrangle.json", buffersize=1000),
    ]
    if args.validate:
        resource_consumers.append(ValidateAgainstIG(hostcfg, args.max_validation_count))

    for cfg in args.config:
        play(cfg=cfg,
            resource_consumers=resource_consumers,
            resource_summary=resource_summary,
            harmony_file=args.harmony,
            dbconn=None,
            dbenv=args.dbenv)
