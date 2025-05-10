import importlib
import pkgutil
import re
import warnings

from spark_config.config import *


def discover_plugins(plugin_prefix, skiplist=None):
    """
    Import all modules with that match the provided prefix.

    Args:
        plugin_prefix (str): Plugin prefix to match against
        skiplist (Optional[List[str]]): List of plugins (regex supported) to skip

    Returns:
        All imported modules that match the prefix
    """

    def _try_load(name):
        try:
            return importlib.import_module(name)
        except ImportError as e:
            warnings.warn(f"Unable to load plugin '{name}': {e}")
            return None

    names = [x for _, x, _ in pkgutil.iter_modules() if x.startswith(plugin_prefix)]
    if skiplist is not None:
        matcher = re.compile("|".join(skiplist))
        names = [x for x in names if matcher.match(x) is None]

    discovered_plugins = {name: _try_load(name) for name in names}
    discovered_plugins = {k: v for k, v in discovered_plugins.items() if v is not None}
    return discovered_plugins
