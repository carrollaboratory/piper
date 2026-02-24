#!/usr/bin/env python

import logging
import sys
from argparse import ArgumentParser, FileType
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

        study_model = datamodel.get_model(model_helpers["study"]["classname"])
        studies = session.query(study_model).all()
        logging.info(f"{len(studies)} studies found")

        subject_model = datamodel.get_model(model_helpers["subject"]["classname"])
        subjects = session.query(subject_model).all()
        logging.info(f"{len(subjects)} subjects found")

    logging.warn("This is a warning")
    logging.info(f"Hello world\n{args}! TBD")
