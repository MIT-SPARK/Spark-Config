import importlib
import logging
import pkgutil

from spark_config.config import *

def discover_plugins(plugin_prefix):
    def _try_load(name):
        try:
            return importlib.import_module(name)
        except ImportError as e:
            logging.getLogger("spark_config").warning(
                f"Unable to load plugin '{name}': {e}"
            )
            return None

    discovered_plugins = {
        name: _try_load(name)
        for finder, name, ispkg in pkgutil.iter_modules()
        if name.startswith(plugin_prefix)
    }
    discovered_plugins = {k: v for k, v in discovered_plugins.items() if v is not None}
    return discovered_plugins


# TOOD(nathan) register discovered
