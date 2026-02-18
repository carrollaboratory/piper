from argparse import ArgumentParser, FileType
from pathlib import Path

from chanter import is_interactive
from sqlalchemy import inspect

"""

For now, this isn't deemed to be necessary. However, it should be a relatively
easy lift using a bit of python introspection.



# Iterate over each class of interest and capture their relationship properties
# Once you are realizing that data, you can use getattr(instance, attribute_name)
# and add it to the resulting dictionary.
#
# If the extracted property is of a type that has relationships inside it,
# recuse into that one.
for varname, rel in inspect(Subject).relationships.items():
    related_class = rel.mapper.class_
    relationships['Subject'][varname] = str(related_class)



"""


def exec():
    parser = ArgumentParser(
        description="Extract data from RDB with relational data inlined"
    )
    parser.add_argument(
        "-c",
        "--root-class",
        default="Subject",
        type=str,
        help="Root class for export (can be provided multiple times)",
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
        nargs="+",
        type=FileType("rt"),
        help="YAML configuration for data extraction",
    )
    args = parser.parse_args()

    print(f"Hello world\n{args}. This will be built if we decide we really need it!")
