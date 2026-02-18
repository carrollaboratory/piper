import logging
import logging.config
import os
import sys
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
    use_rich = is_interactive() and IS_RICH

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        },
        "handlers": {
            "console": {
                "class": "rich.logging.RichHandler"
                if use_rich
                else "logging.StreamHandler",
                "formatter": "standard",
                "level": level,
            },
        },
        "root": {
            "handlers": ["console"],
            "level": level,
        },
    }
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
        config["handlers"]["console"]["formatter"] = "standard"

    logging.config.dictConfig(config)
