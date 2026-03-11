"""
Validate FHIR resources upon consume
"""

import json
import logging
import sys
from collections import defaultdict
from typing import Dict

from ncpi_fhir_client.fhir_client import FhirClient

from .utils import scrub_empty


def format_operation_outcome(oo_dict):
    """
    Formats a FHIR OperationOutcome dictionary into a clean, human-readable report.
    """
    issues = oo_dict.get("issue", [])
    if not issues:
        return "No issues found in OperationOutcome."

    # Header
    report = [f"{'=' * 60}", f"{'FHIR API PROFILE ERRORS':^60}", f"{'=' * 60}"]

    # Group issues by severity to reduce noise
    for severity in ["fatal", "error", "warning", "information"]:
        relevant_issues = [i for i in issues if i.get("severity") == severity]
        if not relevant_issues:
            continue

        report.append(f"\n[{severity.upper()}]")

        for issue in relevant_issues:
            # 1. Get the Location (Prefer expression/FHIRPath over XPath)
            loc = "General"
            if "expression" in issue:
                loc = " -> ".join(issue["expression"])
            elif "location" in issue:
                loc = " -> ".join(issue["location"])

            # 2. Get the Message
            details = issue.get("details", {}).get("text")
            diagnostics = issue.get("diagnostics")
            message = details or diagnostics or "Unknown error"

            report.append(f"  • Loc: {loc}")
            report.append(f"    Msg: {message}")

    report.append(f"\n{'=' * 60}")
    return "\n".join(report)


# Write to a JSON file suitable for dewrangle
#
class ValidateAgainstIG:
    """Submit the resources to a FHIR server for validation.

    If max_validation_count is greater than 0, it will only validate that many
    resources of any given type.
    """

    def __init__(self, fhir_config: Dict, max_validation_count: int = 0):
        self.max_validation_count = max_validation_count
        self.observed_resource_types = defaultdict(int)
        self.fhir_config = fhir_config
        self.fhir_client = None
        # Initialize the FHIR client based on the config details.
        if self.fhir_client is None:
            print(self.fhir_config)

            self.fhir_client = FhirClient(self.fhir_config)

    def __enter__(self):
        """For use in with statements"""
        return self

    def __call__(self, template_name, resource, payload):
        """Feed in the resources one at a time from our iteration"""
        resource_type = payload["resourceType"]

        dropped_keys = []
        cleaned_payload = scrub_empty(payload, dropped_keys=dropped_keys)
        if (
            self.max_validation_count < 1
            or self.observed_resource_types[resource_type] < self.max_validation_count
        ):
            response = self.fhir_client.load(resource_type, cleaned_payload, True)
            if response["status_code"] < 300:
                self.observed_resource_types[resource_type] += 1
            else:
                logging.info(json.dumps(cleaned_payload, indent=2))
                logging.info(response["request_url"])
                logging.error(response["status_code"])
                print(response.keys())
                logging.debug(format_operation_outcome(response["response"]))
                sys.exit(1)
                # logging.error(response)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Nothing really required here, but leaving it as an example for other
        similar classes."""
        pass
