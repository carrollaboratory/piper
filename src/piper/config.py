from yaml import safe_load
from pathlib import Path

__version__ = "0.0.1"

def load_piper_config(piper_config):
    return safe_load(piper_config)