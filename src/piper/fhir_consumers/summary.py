import logging
from collections import defaultdict


class ResourceSummary:
    def __init__(self):
        self.local_counts = defaultdict(int)
        self.total_counts = defaultdict(int)

    def __call__(self, template_name, resource, payload):
        self.local_counts[payload["resourceType"]] += 1
        self.total_counts[payload["resourceType"]] += 1

    def reset(self, report_locals=True):
        """reset the local counts back to an empty dict"""
        old_counts = dict(self.local_counts)

        if report_locals:
            for resource, count in self.local_counts.items():
                print(f"{resource}: {count}")
        self.local_counts = defaultdict(int)
        return old_counts
