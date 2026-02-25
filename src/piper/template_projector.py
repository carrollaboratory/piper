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

import logging
from pathlib import Path
from typing import DefaultDict, Dict, TypeVar

from camel_converter import to_snake
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy import inspect
from sqlalchemy.orm import DeclarativeBase


class TypingBase(DeclarativeBase):
    pass


# Used for typing the SQL Alchemy classes
T = TypeVar("T", bound=TypingBase)


class TemplateProjector:
    def __init__(self, model_helpers: Dict, template_dir: Path):
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
        self.templates = {}

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
            except Exception as e:
                logging.error(f"Error loading template {filename}: {e}")

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
            print(self.templates)
            raise ValueError(f"No template found for class: '{class_name}';")

        template = self.templates[class_name]

        varname = to_snake(class_name)

        # We don't really care what model calls the study, only that it is provided
        study_varname = to_snake(study.__class__.__name__)

        # Passes the instance to the template as 'obj'
        # We probably should define what we are calling study and subject's
        # here in the config
        if study_subject:
            # We don't really care what model calls the subject, only that it
            # is provided
            subject_varname = to_snake(study_subject.__class__.__name__)

            logging.info(f"varname: {obj}")
            logging.info(f"study_varname: {study}")
            logging.info(f"study_subject: {study_subject}")
            return template.render(
                **{varname: obj, study_varname: study, subject_varname: study_subject}
            )
        else:
            logging.info(f"varname: {obj}")
            logging.info(f"study_varname: {study}")

            return template.render(**{varname: obj, study_varname: study})

    def black_list(self, model_component: str):
        """Return the list of fields that a particular model component should
        ignore while traversing it's children (ie these will be expected to be
        handled elsewhere."""
        blacklist = set(self.model_helpers.keys())
        if model_component in self.model_helpers:
            blacklist.update(self.model_helpers[model_component]["blacklist"])
        return blacklist

    def process_study(self, study: T, resources: DefaultDict[str, list[str]]):
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
        resources[study_varname].append(template.render(**{study_varname: study}))

        # Unless we decide we want to fully recurse the tree and track our
        # progress to prevent dupes, we can just handle "subjects" differently
        # than the rest of the stuff found inside a study.
        #
        # TODO: What about study specific stuff that is also tagged by a
        # subject, like the Access Control Record. That should be instantiated
        # at the same level as the study members, but will need to be
        # referenced by the subject's members.

        # For now, we'll just grab the classes found inside the model_keys map

        for varname, rel in inspect(study.__class__).relationships.items():
            related_class = rel.mapper.class_
            blacklist = self.black_list("study")

            # We assume that any projections will exist and match our
            # classnames
            if related_class in self.templates:
                # Skip special vars
                if varname not in blacklist:
                    if not rel.uselist:
                        items = [getattr(study, varname)]
                    else:
                        items = getattr(study, varname)

                        for item in items:
                            resources[varname].append(
                                template.render_object(
                                    item, study=study, class_name=related_class
                                )
                            )

    def process_subject(
        self, subject: T, study: T, resources: DefaultDict[str, list[str]]
    ):
        """
        Accept a single subject instance and generate all of the related FHIR
        resources associated with that particular subject.

        TODO: If there are situations where resources are nested deeper than
        directly inside the patient, we'll need to consider a recursive
        approach. I can't think of a situation like that OTOH.
        """
        subject_varname = to_snake(subject.__class__.__name__)
        resources[subject_varname].append(self.render_object(subject, study=study))
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
                        resources[to_snake(related_class.__name__)].append(
                            self.render_object(
                                item,
                                study_subject=subject,
                                study=study,
                                class_name=related_class.__name__,
                            )
                        )
                else:
                    item = getattr(subject, varname)
                    resources[to_snake(related_class.__name__)].append(
                        self.render_object(
                            item,
                            study_subject=subject,
                            study=study,
                            class_name=related_class.__name__,
                        )
                    )
