import logging
import sys

try:
    from rich.console import Console
    from rich.progress import Progress as RichProgress
    from rich.table import Table as RichTable

    # Only use Rich if we are in an interactive terminal
    _USE_RICH = sys.stdout.isatty()
except ImportError:
    _USE_RICH = False

"""
----------- Optional Progress bars
While testing, Rich makes life a lot easier, but we definitely don't want it
running on automated processes. So, we'll stub out a mock Progress to enable
this to keep our relevant code blocks more readable.

In order to use this, however, we must reference this Process class and not
try to import it directly from Rich.
"""


if _USE_RICH:
    Progress = RichProgress
else:
    # Minimal stub that mimics the Rich Progress interface
    class Progress:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        def add_task(self, description, total=None, **kwargs):
            return None

        def update(self, task_id, advance=0, completed=None, **kwargs):
            print(".", end="", flush=True)
            if completed:
                print(" done!")


"""
---------- Optional Rich Table

"""
if _USE_RICH:
    Table = RichTable

    def print_table(table):
        Console().print(table)
else:
    """Stub out a text friendly table for reporting"""

    class Table:
        def __init__(self, title=None, **kwargs):
            self.title = title
            self.columns = []
            self.rows = []

        def add_column(self, header, **kwargs):
            self.columns.append(header)

        def add_row(self, *args, **kwargs):
            self.rows.append([str(x) for x in args])

    def print_table(table):
        if table.title:
            logging.info(f"\n--- {table.title} ---")

        # Print headers
        logging.info(" | ".join(table.columns))
        logging.info("-" * (len(" | ".join(table.columns))))
        # Print rows
        for row in table.rows:
            logging.info(" | ".join(row))


"""
----------- Strip Empty Strings

For many FHIR properties, the min cardinality is 0. For these cases, an empty
string fails validation even if the FHIR server ignores them upon load.

To avoid this issue, we'll drop any property that results in an empty string,
at least before validating.
"""


def do_drop(value):
    if type(value) is str:
        return value == ""
    if type(value) is list:
        return "" in value

    return False


def scrub_empty(obj, parent_value="", dropped_keys=None):
    """Recursively remove empty strings and empty containers."""

    def property_name(key):
        return key if parent_value == "" else f"{parent_value}.{key}"

    if dropped_keys is None:
        dropped_keys = []

    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if do_drop(v):
                dropped_keys.append(property_name(k))
                continue
            res = scrub_empty(
                v, parent_value=property_name(k), dropped_keys=dropped_keys
            )
            if res is not None:
                cleaned[k] = res
        return cleaned if cleaned else None
    elif isinstance(obj, list):
        cleaned = [
            scrub_empty(item, parent_value=parent_value, dropped_keys=dropped_keys)
            for item in obj
        ]
        # Filter out None results from the list
        cleaned = [i for i in cleaned if i is not None]
        return cleaned if cleaned else None
    return obj
