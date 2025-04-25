#!/usr/bin/env python

from pathlib import Path

from argparse import ArgumentParser, FileType

# This will load the configuration from the piper.yaml file
from .config import load_piper_config

import sys

import pdb

from subprocess import run
## Configuration Logic:
## Command line arguments always take priority over values from the config
##   ie the default version of the projection should be overridden by 
##   whatever is passed as an argument, if such an argument exists. 
## 
def buildfhir(config, dataset, 
         module_path=None,          # Path to the module root dir (i.e. projector/harmonized)
         projection_version=None,   # The version to be run (i.e. current)
         harmony_dir=None,          # directory where harmony file lives
         whistle_src=None,          # Path to what we are currently calling the _entry.wstle file
         outdir=None,               # Where is the output going to be written
         whistle_path="whistle"): 
    
    projection_directory = Path(module_path) / projection_version 

    command = [
        whistle_path, 
        "-harmonize_code_dir_spec", harmony_dir, 
        "-input_file_spec", dataset, 
        "-mapping_file_spect", whistle_src, 
        "-lib_dir_spec", projection_directory,
        "-verbose",
        "-output_dir", outdir
    ]
    result = run(command)

    if result.returncode != 0:
        print(f"Std out    : {result.stdout.decode()}")
        print(f"Std Err    : {result.stderr.decode()}")
        print("\n🤦An error was encountered.🙉 Something was out of tune.")
        print(f"The command was {' '.join(command)}")
        sys.exit(1)
    else:
        final_result = f"{outdir}/{str(dataset.name).split('/')[-1].replace('.json', '.output.json')}"
        print(f"🎶 Beautifully played.🎵 \nResulting File: {final_result}")

    return f"{outdir}/{Path(dataset.name).stem}.output.json"


def run(args=None):
    """
    This is the entrypoint for CLI runs where we are calling play directly. 
    
    Attributes:
        _args (stdin array): Generally, this can be left empty and the parser 
             will pull the values from stdin. 
    """
    # Use the stdin's args if they weren't provided.
    if args is None:
        # Python's stdin includes the script's name as argv[0], and we want
        # to ignore that one. 
        args = sys.argv[1:]

    parser = ArgumentParser(
        description="Call whistle with appropriate details and move output to "\
          "appropriate place."
    )
    parser.add_argument(
        "-i", 
        "--dataset-input", 
        required=True, 
        type=FileType("rt"),
        help="Dataset in JSON format organized according the projection "\
          "expectations."
    )
    parser.add_argument(
        "-c",
        "--config",
        default="piper.yaml",
        type=FileType("rt"),
        help="YAML Configuration with information about the projection library"\
          ", versioning and defaults for harmony files, etc. "
    )
    parser.add_argument(
        "--harmony", 
        type=str, 
        help="Directory where the harmony files lives."
    )

    # TODO: Add all of the necessary arguments

    # Parse the arguments
    args = parser.parse_args(args)

    # Build the config 
    config = load_piper_config(args.config)

    if (args.harmony is None):
        args.harmony = config['harmony']['path']

    pdb.set_trace()

    # Call play with the appropriate parameters
    buildfhir(
        config, 
        dataset=args.dataset_input.name, 
        harmony_dir=args.harmony
    )

if __name__ == "__main__":
    run()