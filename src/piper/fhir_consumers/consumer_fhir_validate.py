"""
Validate FHIR resources upon consume
"""

import logging
from collections import defaultdict
from typing import Dict

from ncpi_fhir_client.fhir_client import FhirClient


# Write to a JSON file suitable for dewrangle
#
class ValidateFHIR:
    """Submit the resources to a FHIR server for validation.

    If max_validation_count is greater than 0, it will only validate that many
    resources of any given type.
    """

    def __init__(self, fhir_config: Dict, max_validation_count: int = 0):
        self.max_validation_count = max_validation_count
        self.observed_resource_types = defaultdict(int)
        self.fhir_config = fhir_config
        self.fhir_client = None

    def __enter__(self):
        """Initialize the FHIR client based on the config details."""
        if self.fhir_client is None:
            self.fhir_client = FhirClient(self.fhir_config)
        return self

    def __call__(self, resource):
        """Feed in the resources one at a time from our iteration"""
        resource_type = resource["resourceType"]

        if (
            self.max_validation_count < 1
            or self.observed_resource_types[resource_type] < self.max_validation_count
        ):
            response = self.fhir_client.load(resource_type, resource, True)
            if response["status_code"] < 300:
                self.observed_resource_types[resource_type] += 1
            else:
                logging.error(response)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Nothing really required here, but leaving it as an example for other
        similar classes."""
        pass
