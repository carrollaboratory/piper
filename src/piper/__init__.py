import logging
import logging.config
import os
import sys
from asyncio.events import Handle
from pathlib import Path

try:
    from rich.logging import RichHandler

    IS_RICH = True
except ImportError:
    IS_RICH = False

from typing import Callable, DefaultDict, Dict, List, TypeVar


def is_interactive():
    if hasattr(sys, "ps1"):  # ps1 should be present for interactive shells
        return True

    if os.isatty(sys.stdin.fileno()):  # TTY
        return True
    return False


def setup_logging(level="INFO", log_file="output/log.txt"):
    """If rich is installed and we are running inside a tty, we'll use rich's
    built in handler for logging and will simplify the format since otherwise,
    there is duplicated information.

    Log information is streamed and written to file, unless log_file is None"""
    use_rich = is_interactive() and IS_RICH

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "rich": {"datefmt": "%H:%M:%S"},
            "detailed": {"format": "%(asctime)s %(levelname)s [%(name)s] %(message)s"},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "detailed",
                "level": level,
            },
        },
        "root": {
            "handlers": ["console"],
            "level": level,
        },
    }
    if use_rich:
        config["handlers"]["console"]["class"] = "rich.logging.RichHandler"
        config["handlers"]["console"]["formatter"] = "rich"
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "filename": log_file,
            "formatter": "detailed",
            "level": level,
        }
        config["root"]["handlers"].append("file")

    if use_rich:
        config["handlers"]["console"]["rich_tracebacks"] = True
        config["handlers"]["console"]["markup"] = True
    else:
        config["handlers"]["console"]["formatter"] = "detailed"

    logging.config.dictConfig(config)


def get_template_error_info():
    """Extracts lineno and filename from an active UndefinedError."""
    exc_type, exc_value, tb = sys.exc_info()
    # Iterate through frames to find the one associated with the template
    while tb:
        # Jinja uses '<template>' or the actual filename if loaded via FileSystemLoader
        if tb.tb_frame.f_code.co_filename != __file__:
            return {
                "filename": tb.tb_frame.f_code.co_filename,
                "lineno": tb.tb_lineno,
                "message": str(exc_value),
            }
        tb = tb.tb_next
    return None

def get_accurate_template_line(tb):
    """
    Checks if Jinja has already mapped the line or if manual
    translation is required via the template object.
    """
    # Look for the hidden Jinja template reference in the execution frame
    template = tb.tb_frame.f_globals.get("__jinja_template__")
    if template is not None:
        # If this is present, it's safer to use Jinja's mapping
        return template.get_corresponding_lineno(tb.tb_lineno)

    # Fallback to the raw line number if no mapping exists
    return tb.tb_lineno

def get_real_template_error(template_obj):
    """Translates the Python-bytecode line number back to the Jinja source line."""
    exc_type, exc_value, tb = sys.exc_info()

    # Iterate back through the traceback to find the template execution frame
    while tb:
        # Check if this frame is where Jinja executed the template
        # Usually marked as '<template>' or the actual filename if loaded via Loader
        if tb.tb_frame.f_code.co_filename in ["<template>", template_obj.filename]:
            # Use Jinja's internal mapper to find the correct line
            real_lineno = template_obj.get_corresponding_lineno(tb.tb_lineno)
            return {
                "filename": template_obj.filename or "Memory/String",
                "template_line": real_lineno,
                "python_line": tb.tb_lineno,  # This is your 179
                "message": str(exc_value),
            }
        tb = tb.tb_next
    return None


def extract_jinja_linenumber():
    exc_type, exc_value, tb = sys.exc_info()
    while tb:
        # Jinja often uses '<template>' as the filename in the stack frame
        if tb.tb_frame.f_code.co_filename == "<template>":
            return tb.tb_lineno
        tb = tb.tb_next
    return None


def debug_print(
    template_name: str, resource: str, error_line: int = 0, errorfields: List[str] = []
):
    if template_name == "Unknown":
        import pdb

        pdb.set_trace()
    lines = resource.split("\n")
    padding = len(str(len(lines)))
    logging.info(f"\n--------- {template_name} ------")
    for i, line in enumerate(lines, start=1):
        # Format: "  1 | {line_content}"
        msg = f"{i:>{padding}} | {line}"
        if i == error_line or (
            errorfields != [] and any(f'"{field}"' in line for field in errorfields)
        ):
            logging.error(f"[bold red]{msg}[/bold red]")
        elif error_line > 0 and i > error_line - 3 and i < error_line + 3:
            logging.info(msg)
        else:
            logging.debug(msg)
    logging.info(f"--------- {template_name} ------\n")
