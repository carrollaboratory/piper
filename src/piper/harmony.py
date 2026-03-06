from collections import defaultdict
from csv import DictReader
from dataclasses import dataclass, field
from pathlib import Path
from typing import DefaultDict, List, NamedTuple, Optional


@dataclass
class Coding:
    code: str
    system: str
    display: Optional[str] = field(default=None, compare=False)
    version: Optional[str] = field(default=None, compare=False)

    # Relationship to the code is mapped to
    relationship: Optional[str] = field(default=None, compare=False)

    mapped_codes: List = field(default_factory=list, compare=False)


class Harmony:
    """Provide mechanism to harmonize local enums to public ontologies"""

    def __init__(self, harmony_file: str):
        self.filename = harmony_file
        self.harmony_content = defaultdict(dict)

        self.load()

    def harmonize(self, code, local_system):
        """Local system can be local_system from harmony or table_name or
        parent_varname"""

        if (
            local_system in self.harmony_content
            and code in self.harmony_content[local_system]
        ):
            return self.harmony_content[local_system][code].mapped_codes
        else:
            return None

    def load(self, harmony_file: str | None = None):
        if harmony_file is None:
            harmony_file = self.filename

        with open(harmony_file, "rt") as f:
            reader = DictReader(f, delimiter=",", quotechar='"')

            for line in reader:
                local_coding = Coding(
                    code=line["local_code"],
                    display=line["text"],
                    system=line["local_code_system"],
                )
                mapping = Coding(
                    code=line["code"],
                    system=line["code_system"],
                    display=line["display"],
                )
                self.add_mapping(local_coding.system, local_coding, mapping)
                self.add_mapping(line["table_name"], local_coding, mapping)
                self.add_mapping(line["parent_varname"], local_coding, mapping)

    def add_mapping(self, system, local_coding, mapping):
        if (
            system not in self.harmony_content
            or local_coding.code not in self.harmony_content[system]
        ):
            self.harmony_content[system][local_coding.code] = local_coding

        if mapping not in self.harmony_content[system][local_coding.code].mapped_codes:
            self.harmony_content[system][local_coding.code].mapped_codes.append(mapping)
