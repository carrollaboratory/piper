"""
Main templates will be loose inside the project's template directory where each
template's filename is an exact match to the relevant Class name from the
project's SQL Alchemy model (which should just be the linkml class name).

At this time, I imagine we'll have 3 facets of information that can be passed
to a given template: Study, Participant and [Target Instance]

Obviously, Study specific resources won't need a specific participant, but some
may require "all participants", such as for the study group. This should be
able to be derived from the study itself.

Subject Assertions, for instance, will probably require the Subject and the
instance of type "SubjectAssertion". But it will also get the study instance
as well in case it is necessary for meta or something.

Since the majority of the templates will require a participant and another
thing, that will be the assumption. However, for anything that requires less
than all three, we'll need to flag that inside the project's configuration.

----

Template authors can rely on incoming data named according to the LinkML
conventions. The script is agnostic to all model design details except:
    * There must be a Study class whose name is provided as part of the config
    * There must be a Subject class whose name is provided as part of the
      config.
    * All template files should generate a single FHIR resource and must match
      an existing class within the LinkML class.
    * These classes must be found as part of one of the anchor concepts:
        subject & study
      as there is explicit logic to traverse the model based on those anchor
      concepts. If there is a need for additional anchors or deeper traversal
      approaches, those will require some effort to support.

These are the two key requirements as of Feb 2026.
"""

import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Callable, DefaultDict, Dict, List, TypeVar

import jinja2.exceptions
from camel_converter import to_snake
from jinja2 import Environment, FileSystemLoader, Template, TemplateSyntaxError
from json_repair import repair_json
from pydantic import ValidationError

# from sqlalchemy import inspect
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase

from . import (
    debug_print,
    extract_jinja_linenumber,
    get_real_template_error,
    get_template_error_info,
)
from .exceptions import ProjectionError
from .harmony import Harmony

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class TypingBase(DeclarativeBase):
    pass


# Used for typing the SQL Alchemy classes
T = TypeVar("T", bound=TypingBase)

""" If we want to pull the harmony content directly from the model, this
should work:


from importlib.resources import files

harmony_csv = files("acr_harmonized_data_model").joinpath("harmony.csv")
with harmony_csv.open("r") as f:
    content = f.read()

"""


class TemplateProjector:
    curie_map = {}

    def __init__(
        self,
        model_helpers: Dict,
        template_dir: Path,
        resource_consumers: List[Callable],
        harmony_file: Path | None = None,
        data_model=None,
    ):
        """
        Manage the linking between the data model and it's projection.

        The model_keys maps the anchor concepts study and subject to model
        specific classes. This necessary for basic traversal logic.
            * key_classes (study and subject class names)
        """
        self.model_helpers = model_helpers
        self.template_dir = Path(template_dir)
        # Initialize Jinja environment
        logging.info(f"Projection Directory: '{template_dir}")
        self.env = Environment(loader=FileSystemLoader(self.template_dir))

        self.datamodel = data_model

        self.harmony = None
        if harmony_file and harmony_file.exists():
            self.harmony = Harmony(str(harmony_file))

        self.env.globals["HARMONIZE"] = self.harmony
        self.env.globals["SYSTEMS"] = TemplateProjector.curie_map
        self.templates = {}
        self.resource_consumers = resource_consumers

        self._load_class_templates()
        logging.info(f"{len(self.templates)} template files found.")

    def _load_class_templates(self):
        """
        Scans the directory for .j2 files starting with an uppercase letter
        and maps the class name to the loaded Jinja template.
        """
        if not self.template_dir.exists():
            raise FileNotFoundError(f"Directory {self.template_dir} does not exist.")

        for filename in self.template_dir.glob("[A-Z]*.j2"):
            try:
                self.templates[filename.stem] = self.env.get_template(filename.name)
            except TemplateSyntaxError as e:
                if e.source:
                    debug_print(e.filename, e.source, error_line=e.lineno)

                logging.error(
                    f"Malformed template file, '{e.filename}' at line {e.lineno}:"
                )
                logging.error(f" - {e.message})")

                logging.error(f"Error loading template {filename}: {e}")

    def add_backref(
        self, entry: T, cname: str, ref: dict, context: dict, sess: "Session"
    ):
        """Add a backreference variable to a template's context

        For relationships that aren't directly supported by the linkml SQLA class,
        we can add the references into the configuration. This will build the
        right list of data from the database and assign it to the suggested var

        Args:
            cname (str): The class name from the config file
            ref (dict): The details required to pull the data

        Returns:
            This will add the new variable to the context
        """

        varname = ref["varname"]
        cls = self.datamodel.get_model(cname)
        fk = getattr(cls, ref["fk"])
        id = getattr(entry, ref["fk"])
        records = sess.query(cls).filter(fk == id).all()
        context[varname] = records

    def render_object(
        self,
        obj: T,
        study: T,
        study_subject: T | None = None,
        class_name: str | None = None,
    ) -> str:
        """
        Identifies the class of the provided object, finds the matching
        template, and renders it using the object instance.
        """
        if class_name is None:
            class_name = obj.__class__.__name__

        if class_name not in self.templates:
            logging.debug(self.templates)

            raise ValueError(f"No template found for class: '{class_name}';")

        template = self.templates[class_name]

        varname = to_snake(class_name)

        # We don't really care what model calls the study, only that it is provided
        study_varname = to_snake(study.__class__.__name__)

        session = None
        if self.datamodel:
            session = self.datamodel.create_session()
        try:
            # Passes the instance to the template as 'obj'
            # We probably should define what we are calling study and subject's
            # here in the config
            if study_subject:
                # We don't really care what model calls the subject, only that it
                # is provided
                subject_varname = to_snake(study_subject.__class__.__name__)

                return template.render(
                    **{
                        varname: obj,
                        study_varname: study,
                        subject_varname: study_subject,
                        session: session,
                    }
                )
            else:
                return template.render(**{varname: obj, study_varname: study})
        except jinja2.exceptions.UndefinedError as e:
            err_details = get_real_template_error(template)
            source, filename, uptodate = self.env.loader.get_source(
                self.env, template.name
            )
            debug_print(
                filename,
                resource=source,
                error_line=err_details["python_line"],
            )
            logging.error(e.message)
            logging.error(err_details)
            sys.exit(1)

    def black_list(self, model_component: str):
        """Return the list of fields that a particular model component should
        ignore while traversing it's children (ie these will be expected to be
        handled elsewhere.

        NOTE - This is probably no longer relevant based on the newer model
        where we are just iterating over the templates directly
        """
        blacklist = set(self.model_helpers.keys())
        if model_component in self.model_helpers:
            blacklist.update(self.model_helpers[model_component]["blacklist"])
        return blacklist

    def consume(self, resource: str, template_name="Unknown"):
        """Feed output of a template projection to each of of the consumers.

        Args:
            resource (str): This is the projected resource
            template_name (str): Helpful for debugging

        """
        try:
            payload = json.loads(resource)
        except json.JSONDecodeError as e:
            err = ProjectionError(
                decode_err=e, body=resource, template_name=template_name
            )
            debug_print(template_name, resource=resource, error_line=err.lnum)
            logging.warning(e)

            try:
                logging.debug("Trying to repair...")
                resource = repair_json(resource)
                payload = json.loads(resource)
                logging.debug("Success!")
            except ValueError:
                logging.error(
                    f"The template, {template_name}, is not a valid JSON object."
                )
                sys.exit(1)

        resource_type = payload.get("resourceType")
        if not resource_type:
            debug_print(template_name=template_name, resource=resource)
            logging.error(
                f"No resourceType found in projected resource of type: {template_name}"
            )

        # We'll provide the raw JSON to our consumers
        for consumer in self.resource_consumers:
            consumer(template_name, resource, payload)

    def process_entry(self, entry: T, study: T, model_helpers: dict):
        """Process any type of template"""
        study_classname = study.__class__.__name__
        study_varname = to_snake(study_classname)
        classname = entry.__class__.__name__

        innersess = self.datamodel.create_session()

        varname = to_snake(classname)
        template = self.templates[classname]

        tcontext = {varname: entry, study_varname: study}

        # Check to see if we need to add any other data into the template
        # context.
        if classname in model_helpers:
            backrefs = model_helpers[classname].get("backrefs")
            if backrefs:
                for cname, ref in backrefs.items():
                    self.add_backref(
                        entry=entry,
                        cname=cname,
                        ref=ref,
                        context=tcontext,
                        sess=innersess,
                    )

        self.consume(template.render(**tcontext), template_name=classname)

    def process_study(self, study: T):
        """
        Accept a single study instance for all non-participant level resources

        TODO: If there are situations where resources are nested deeper than
        directly inside the study, we'll need to consider a recursive
        approach. I can't think of a situation like that OTOH.
        """
        # We don't really care what model calls the study, only that it is provided
        study_classname = study.__class__.__name__
        study_varname = to_snake(study_classname)
        template = self.templates[study_classname]

        self.consume(
            template.render(**{study_varname: study}), template_name=study_classname
        )

    def process_subject(self, subject: T, study: T):
        """
        Accept a single subject instance and generate all of the related FHIR
        resources associated with that particular subject.

        WARNING - This is no longer relevant, but I'm leaving the code for
        reference for the time being, since it uses some SQLA stuff that
        could be useful later one.

        TODO: If there are situations where resources are nested deeper than
        directly inside the patient, we'll need to consider a recursive
        approach. I can't think of a situation like that OTOH.
        """
        subject_varname = to_snake(subject.__class__.__name__)
        self.consume(
            self.render_object(subject, study=study), template_name=subject_varname
        )
        # Iterate on all classes that are a part of the subject:
        for varname, rel in inspect(subject.__class__).relationships.items():
            related_class = rel.mapper.class_
            # If this doesn't match any of our templates, we don't really have any interest in it
            if related_class.__name__ in self.templates:
                # I'm not sure we need to differentiate these: [ONETOMANY, MANYTOMANY, MANYTOONE]
                # direction = rel.direction.name
                # The variable is a list
                if rel.uselist:
                    for item in getattr(subject, varname):
                        self.consume(
                            resource=self.render_object(
                                item,
                                study_subject=subject,
                                study=study,
                                class_name=related_class.__name__,
                            ),
                            template_name=related_class.__name__,
                        )
                else:
                    item = getattr(subject, varname)
                    self.consume(
                        self.render_object(
                            item,
                            study_subject=subject,
                            study=study,
                            class_name=related_class.__name__,
                        ),
                        template_name=related_class.__name__,
                    )
