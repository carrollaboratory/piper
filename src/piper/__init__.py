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
    IS_RICH = True


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
