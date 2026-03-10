import logging
from collections import defaultdict

from .utils import Console, Table, print_table


class ResourceSummary:
    def __init__(self):
        self.local_counts = defaultdict(int)
        self.total_counts = defaultdict(int)

    def __call__(self, template_name, resource, payload):
        self.local_counts[payload["resourceType"]] += 1
        self.total_counts[payload["resourceType"]] += 1

    def reset(self, title, report_locals=True):
        """reset the local counts back to an empty dict"""
        old_counts = dict(self.local_counts)

        table = Table(
            title=title, row_styles=["green", ""], title_style="black on white"
        )
        table.add_column("Resource", justify="right")
        table.add_column("Count", justify="right")
        if report_locals:
            for resource, count in self.local_counts.items():
                table.add_row(str(resource), str(count))

        self.local_counts = defaultdict(int)
        print_table(table)
        return old_counts

    def report_totals(self, title):
        table = Table(
            title=title, row_styles=["cyan", ""], title_style="black on white"
        )
        table.add_column("Resource", justify="right")
        table.add_column("Count", justify="right")

        for resource, count in self.total_counts.items():
            table.add_row(str(resource), str(count))

        print_table(table)
