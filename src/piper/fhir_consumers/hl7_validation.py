import json
import logging
import sys
from typing import Callable, DefaultDict, Dict, List, TypeVar

from fhir.resources import get_fhir_model_class
from pydantic import ValidationError

from .. import debug_print
from ..exceptions import ProjectionError
from .utils import scrub_empty


class ValidateResourceBasic:
    """This just validates the basic conformance to FHIR resources. This won't
    catch IG errors, so that will be handled elsewhere. This should be fast
    enough to act as a basic sanity check for basic structure and general
    conformance before submitting to an external server for IG compliance."""

    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __call__(self, template_name, resource, payload):
        """Validate resource using fhir.resource"""
        # payload = json.loads(resource)
        resource_type = payload.get("resourceType")
        if not resource_type:
            debug_print(template_name, resource=resource)
            logging.error(
                f"No resourceType found in projected resource of type: {template_name}"
            )

        try:
            fhir_class = get_fhir_model_class(resource_type)
            dropped_keys = []

            cleaned_payload = scrub_empty(payload, dropped_keys=dropped_keys)

            # print(cleaned_payload)
            fhir_data = fhir_class(**cleaned_payload)
        except ValidationError as e:
            # import pdb

            # pdb.set_trace()
            if len(dropped_keys) > 0:
                logging.warning(
                    f"""The properties keys had been assigned '' and were dropped before validation: \n\t\t'{"'\n\t\t'".join(dropped_keys)}'"""
                )
                debug_print(
                    f"{template_name} Original", resource=json.dumps(payload, indent=2)
                )
            fhir_errors = e.errors()
            error_locs = [err.get("loc")[-1] for err in fhir_errors]
            debug_print(
                template_name,
                resource=json.dumps(cleaned_payload, indent=2),
                error_line=0,
                errorfields=error_locs,
            )
            logging.error(
                f"Basic FHIR validation of {template_name} resulted in {len(fhir_errors)} errors"
            )
            for error in fhir_errors:
                # print(error)
                message = [
                    f"field '{error['loc'][-1]}' had value '{error['input']}' Message: '{error['msg'].replace('\n', ' ')}'"
                ]

                if "ctx" in error:
                    message.append(f"Context: {error['ctx']}")
                if "url" in error:
                    message.append(f"URL: {error['url']}")
                logging.error("\n\t\t".join(message))
            sys.exit(1)
