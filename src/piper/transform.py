#!/usr/bin/env python

import logging
import sys
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


def run():
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
        "config",
        type=FileType("rt"),
        nargs="+",
        help="YAML configuration for data extraction",
    )
    args = parser.parse_args()
    setup_logging(args.log_level)

    logging.info(f"Piper Transform: '{','.join([f.name for f in args.config])}'")

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
