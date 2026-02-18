#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from pathlib import Path

from . import setup_logging

# This will load the configuration from the piper.yaml file
from .config import load_piper_config


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
        "config",
        type=FileType("rt"),
        required=True,
        help="YAML configuration for data extraction",
    )
    args = parser.parse_args()

    print(f"Hello world\n{args}! TBD")
