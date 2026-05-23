from __future__ import annotations

from typing import Any, Optional, cast

import pandas as pd


def coerce_optional_timestamp(value: object) -> Optional[pd.Timestamp]:
    """
    Convert a scalar-like value to pd.Timestamp.

    Returns None for missing, invalid, or unparseable values.
    Keeps pandas/mypy casting noise isolated in one place.
    """
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value

    ts = pd.to_datetime(cast(Any, value), errors="coerce")
    if pd.isna(ts):
        return None

    return pd.Timestamp(ts)


def coerce_optional_float(value: object) -> Optional[float]:
    """
    Convert a scalar-like value to float.

    Returns None for missing, invalid, or unparseable values.
    """
    if value is None or pd.isna(value):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_optional_int(value: object) -> Optional[int]:
    """
    Convert a scalar-like value to int.

    Returns None for missing, invalid, or unparseable values.
    """
    if value is None or pd.isna(value):
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None