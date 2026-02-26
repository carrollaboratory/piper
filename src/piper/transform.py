#!/usr/bin/env python

import logging
import sys
import os
from argparse import ArgumentParser, FileType
from collections import defaultdict
from pathlib import Path

from yaml import safe_load

from piper.datamodel import LinkMLModelLoader
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
    that will look exactly the same. """
    auth_type = os.getenv("AUTH_TYPE", None)
    if auth_type is None:
        logging.warn("No FHIR Host auth type. Assuming local HAPI with no auth")
        return {
            "auth_type": "auth_basic",
            "target_service": "FHIR_HOST", "http://localhost:8080:fhir",
            "username": "test",
            "password": "nopass"
        }
    elif auth_type == "auth_basic":
        return {
            "auth_type": "auth_basic",
            "target_service": os.getenv("FHIR_HOST", "http://localhost:8080:fhir"),
            "username": os.getenv("FHIR_USER", "test"),
            "password": os.getenv("FHIR_PWD", "nopass")
        }
    # For now, we'll assume a KF openid based auth
    assert(os.getenv("FHIR_CLIENT_ID"))
    assert(os.getenv("FHIR_SECRET"))
    return {
        "auth_type": os.getenv("AUTH_TYPE", "auth_kf_openid"),
        "target_service_url": os.getenv("TARGET_SERVICE_URL", "http://localhost:8080/fhir"),
        "client_id": os.getenv("FHIR_CLIENT_ID"),
        "password": os.getenv("FHIR_SECRET")
    }
def run():
    hosts_file = Path("~/.fhir_hosts").expanduser()
    host_config = None
    if hosts_file.exists():
        host_config = safe_load(hosts_file)

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
        action='store_true',
        help="Validate FHIR resources as they are produced"
    )
    parser.add_argument(
        "--max-validation-count",
        type=int,
        default=0,
        help="When greater than 0, only validate that many of any given resource type"
    )
    parser.add_argument(
        "config",
        type=FileType("rt"),
        nargs="+",
        help="YAML configuration for data extraction",
    )
    args = parser.parse_args()
    setup_logging(args.log_level)

    logging.info(f"Piper Transform: '{','.join([f.name for f in args.config])}'")

    if "host"in args and args.host is not None:
        hostcfg = host_config['args.host']
    else:
        # Extract relevant stuff from the environment
        hostcfg = build_host_config()

    for cfg in args.config:
        config = safe_load(cfg)
        database_uri = config["db"][args.dbenv]["uri"]
        db_schema_name = config["db"][args.dbenv]["db_schema_name"]
        tbl_prefix = config["db"]["table_name_template"]

        logging.info(f"Projection Configuration:\t'{cfg.name}'")
        model_config = config["data_model"]
        logging.info(f"Loading model:\t'{model_config['model_source']}'")

        datamodel = LinkMLModelLoader(
            model_source=model_config["model_source"],
            model_filename=model_config["model_filename"],
            database_url=database_uri,
            table_prefix=tbl_prefix,
            schema_name=db_schema_name,
            source_ref=model_config["source_ref"],
        ).load()
        session = datamodel.create_session()

        projection_template_dir = config["projection"]["templates"]
        model_helpers = config["data_model"]["model_helpers"]

        projector = TemplateProjector(
            model_helpers=model_helpers, template_dir=projection_template_dir
        )
        projections = defaultdict(list)

        # Studies
        study_model = datamodel.get_model(model_helpers["study"]["classname"])
        studies = session.query(study_model).all()
        logging.info(f"{len(studies)} studies found")

        for study in studies:
            if study:
                local_projections = defaultdict(list)
                logging.info(f"{study.id}")
                projector.process_study(study, resources=local_projections)
                for key, value in local_projections.items():
                    logging.info(f"{key}: {len(value)} resources")
                projections.update(local_projections)

        # Subjects
        local_projections = defaultdict(list)
        # for subject in subjects:
        for subject in datamodel.stream(model_helpers["subject"]["classname"]):
            # TODO: if we have more than one study, how are we handling
            # participants? Are they found inside the study or do they point
            # to their primary study? Is there a mechanism we need to use to
            # single out the primary study resource?
            projector.process_subject(
                subject, study=studies[0], resources=local_projections
            )
        logging.info("Participant resources created: ")
        for key, value in local_projections.items():
            logging.info(f"{key}: {len(value)} resources")
        projections.update(local_projections)

        logging.info("Total resources created: ")
        for key, value in projections.items():
            logging.info(f"{key}: {len(value)} resources")

        # with Path("output/data_resources.json").open('rt') as f:

    logging.warn("This is a warning")
    logging.info(f"Hello world\n{args}! TBD")
