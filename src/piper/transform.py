#!/usr/bin/env python

from pathlib import Path

from argparse import ArgumentParser, FileType

# This will load the configuration from the piper.yaml file
from .config import load_piper_config

import sys

import pdb
from rich import print
import subprocess
## Configuration Logic:
## Command line arguments always take priority over values from the config
##   ie the default version of the projection should be overridden by 
##   whatever is passed as an argument, if such an argument exists. 
## 
def buildfhir(config, dataset, 
         module_path,          # Path to the module root dir (i.e. projector/harmonized)
         projection_version,   # The version to be run (i.e. current)
         harmony_dir,          # directory where harmony file lives
         whistle_src,          # Path to what we are currently calling the _entry.wstle file
         outdir,               # Where is the output going to be written
         whistle_path="whistle"): 
    
    projection_directory = Path(module_path) / projection_version 

    command = [
        whistle_path, 
        "-harmonize_code_dir_spec", harmony_dir, 
        "-input_file_spec", dataset, 
        "-mapping_file_spec", whistle_src, 
        "-lib_dir_spec", str(projection_directory),
        "-verbose",
        "-output_dir", outdir
    ]
    result = subprocess.run(command)
    if result.returncode != 0:
        print(f"Std out    : {result.stdout.decode()}")
        print(f"Std Err    : {result.stderr.decode()}")
        print("\n🤦An error was encountered.🙉 Something was out of tune.")
        print(f"The command was {' '.join(command)}")
        sys.exit(1)
    else:
        final_result = f"{outdir}/{Path(dataset).stem}.output.json"
        print(dataset)
        # final_result = f"{outdir}/{str(dataset.name).split('/')[-1].replace('.json', '.output.json')}"
        print(f"🎶 Beautifully played.🎵 \nResulting File: {final_result}")

    # return f"{outdir}/{Path(dataset.name).stem}.output.json"
    return final_result




def run(args=None):
    print("I'M RUNNING!!!!")
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
        default=None,
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
        default=None,
        type=str, 
        help="Directory where the harmony files lives."
    )

    # TODO: Add all of the necessary arguments
    parser.add_argument(
        "--output-dir",
        default="output/whistle-output/study",
        type=str
    )

    parser.add_argument(
        "--output",
        default="piper-output.json",
        type=str,
        help="The FHIR output."
    )

    parser.add_argument(
        "-m",
        "--module",
        default=None,
        type=str,
    )

    parser.add_argument(
        "-p",
        "-projection-dir",
        default="projector/harmonized",
        # type=FileType("rt"),
        help="Directory containing projection modules."
    )    

    parser.add_argument(
        "-v",
        "--projection-version",
        default=None,
        type=str
    )

    parser.add_argument(
        "--whistle-path",
        default="whistle",
    )

    # Parse the arguments
    args = parser.parse_args(args) 
    # pdb.set_trace()
    

    # Build the config 
    config = load_piper_config(args.config)

    # If DATASET_INPUT is None, pull harmony out of config
    if (args.dataset_input is None):
        args.dataset_input = config['harmony']['filename']
    else:
        args.dataset_input = args.dataset_input.name

    if (args.module is None):
        args.module = config['projections']['default_module']

    if (args.harmony is None):
        args.harmony = config['harmony']['path']

    if (args.projection_version is None):
        args.projection_version = config['projections']['default_version']


    proj_module = config['projections']['modules'][args.module]


    # use Path object to make output directory that's the same as the output directory.
    # if it doesn't exist, create it. 
    if (args.output_dir is None):
        output_dir = Path(proj_module['output_dir'])
    else:    
        output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    #rename outdir file to what the user specified, IF they specified something (see line 51 for how outdir works)
    # check if user-specified inputs exist (module, version), and throw error if it doesn't (maybe Path(path).exists() ?)
    
    # Call play with the appropriate parameters
    buildfhir(
        config=config, 
        dataset=args.dataset_input, 
        harmony_dir=args.harmony,
        module_path=proj_module['path'],
        whistle_src=proj_module['whistle_src'],
        outdir=output_dir,
        projection_version=args.projection_version
    )
