import importlib
import pkgutil
import warnings

from spark_config.config import *


def discover_plugins(plugin_prefix):
    def _try_load(name):
        try:
            return importlib.import_module(name)
        except ImportError as e:
            warnings.warn(f"Unable to load plugin '{name}': {e}")
            return None

    discovered_plugins = {
        name: _try_load(name)
        for finder, name, ispkg in pkgutil.iter_modules()
        if name.startswith(plugin_prefix)
    }
    discovered_plugins = {k: v for k, v in discovered_plugins.items() if v is not None}
    return discovered_plugins
