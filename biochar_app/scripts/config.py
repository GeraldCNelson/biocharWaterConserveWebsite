"""
Compatibility shim.

Old imports expected:
    from biochar_app.scripts.config import YEARS, DEFAULT_YEAR, ...

New config lives in: biochar_app.config

This module re-exports the curated package-level config API to preserve
legacy imports during migration.
"""

from __future__ import annotations

from biochar_app.config import *  # noqa: F403
from biochar_app.config import __all__  # noqa: F401