import json
import logging
from collections import UserDict, defaultdict
from csv import DictReader
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import DefaultDict, Dict, List, NamedTuple, Optional


class JinjaJSONEncoder(json.JSONEncoder):
    def default(self, o):
        # We'll be sure to define a to_dict function if we want to stream
        # these using | tojson in the templates
        if hasattr(o, "to_dict"):
            return o.to_dict()
        return super().default(o)


@dataclass
class Coding:
    code: str
    system: str
    display: Optional[str] = field(default=None, compare=False)
    version: Optional[str] = field(default=None, compare=False)

    # Relationship to the code is mapped to
    relationship: Optional[str] = field(default=None, compare=False)
    _SILENT_FIELDS = {"version", "relationship", "mapped_codes"}

    mapped_codes: List = field(default_factory=list, compare=False)

    def keys(self, ignore_cols=None):
        if ignore_cols is None:
            ignore_cols = Coding._SILENT_FIELDS
        return [f.name for f in fields(self) if f.name not in ignore_cols]

    def __iter__(self):
        """Enables dict(coding_instance) — used by Jinja2's tojson."""
        for key in self.keys():
            yield key, getattr(self, key)

    def __getitem__(self, key):
        if key not in self.keys():
            raise KeyError(key)
        return getattr(self, key)

    def to_json(self, ignore_cols=None):
        data = self.to_dict(ignore_cols)
        return json.dumps(data)

    def to_dict(self, ignore_cols=None):
        """Explicit dict conversion respecting _SILENT_FIELDS."""
        return {k: getattr(self, k) for k in self.keys(ignore_cols)}


class CurieHelper:
    """Provides helper for working with curies inside jinja templates"""

    def __init__(self, system_lookup: Dict[str, str]):
        self.systems = system_lookup

    def system(self, curie: str, system: str | None = None) -> str:
        if system is None:
            prefix = curie.split(":")[0]
            return self.systems.get(prefix, prefix)
        return system

    def coding(
        self, curie: str, display: str | None = None, system: str | None = None
    ) -> Dict[str, str]:
        coding = {"code": curie}
        if display:
            coding["display"] = display

        coding["system"] = self.system(curie, system)
        return coding

    def __call__(self, curie: str, system: str | None = None) -> str:
        return self.system(curie, system)


class Harmony:
    """Provide mechanism to harmonize local enums to public ontologies"""

    def __init__(self, harmony_file: str):
        self.filename = harmony_file
        self.harmony_content = defaultdict(dict)

        self.load()

    def __call__(
        self, code: str, local_system: str, first_only: bool = False
    ) -> list[Coding] | None:
        return self.harmonize(code, local_system, first_only)

    def harmonize(
        self, code: str, local_system: str, first_only: bool = False
    ) -> list[Coding] | None:
        """Local system can be local_system from harmony or table_name or
        parent_varname"""

        if (
            local_system in self.harmony_content
            and code in self.harmony_content[local_system]
        ):
            if first_only:
                return self.harmony_content[local_system][code].mapped_codes[0]
            else:
                return self.harmony_content[local_system][code].mapped_codes
        else:
            logging.error(f"No mapping found for {code} in {local_system}")
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
        logging.info("Harmony loaded")

    def add_mapping(self, system: str, local_coding: Coding, mapping: Coding):
        if (
            system not in self.harmony_content
            or local_coding.code not in self.harmony_content[system]
        ):
            self.harmony_content[system][local_coding.code] = local_coding

        if mapping not in self.harmony_content[system][local_coding.code].mapped_codes:
            self.harmony_content[system][local_coding.code].mapped_codes.append(mapping)


class ProjectionHelpers:
    def __init__(self, curie_map: Dict[str, str], harmony: Harmony | str):
        self.curie = CurieHelper(curie_map)
        if type(harmony) is Harmony:
            self.harmony = harmony
        elif type(harmony) is str:
            self.harmony = Harmony(harmony)
        else:
            logging.error(f"Unrecognized harmony type {type(harmony)}")
            self.harmony = None
