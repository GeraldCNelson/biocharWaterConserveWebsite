"""
Compatibility shim.

Old imports expected:
    from biochar_app.scripts.config import YEARS, DEFAULT_YEAR, ...

New config lives in:
    biochar_app.config.*

This module re-exports the new config names to preserve legacy imports.
"""
# noinspection PyUnusedLocal,PyWildcardImport,PyUnresolvedReferences
from __future__ import annotations

# Re-export modules (keeps code navigation nicer)
from biochar_app.config import core, paths, units, table_specs, pakbus  # noqa: F401

# Re-export names (legacy behavior)
from biochar_app.config.core import *        # noqa: F403
from biochar_app.config.paths import *       # noqa: F403
from biochar_app.config.units import *       # noqa: F403
from biochar_app.config.table_specs import * # noqa: F403
from biochar_app.config.pakbus import *      # noqa: F403


def _exported_names(mod):
    """Collect public names from a module for __all__."""
    names = getattr(mod, "__all__", None)
    if names:
        return list(names)
    return [k for k in vars(mod).keys() if not k.startswith("_")]


__all__ = (
    _exported_names(core)
    + _exported_names(paths)
    + _exported_names(units)
    + _exported_names(table_specs)
    + _exported_names(pakbus)
)