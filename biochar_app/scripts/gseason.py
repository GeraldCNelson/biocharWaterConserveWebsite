"""
Growing-season core utilities.

Provides:
  • compute_seasons(...) – build per-period rows from a time-indexed DataFrame
      - MEANS for non-precip variables
      - SUM for precip increments (precip_in / precip_mm)
  • assign_gseason_periods(...) – tag a timestamp with a season code
"""

from typing import Mapping, Any
import pandas as pd
from biochar_app.scripts.config import DEFAULT_GSEASON_PERIODS


def _slice_and_mean(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    """Column-wise mean of df[start:end] (inclusive). Assumes DatetimeIndex."""
    mask = (df.index >= start) & (df.index <= end)
    return df.loc[mask].mean(numeric_only=True)


def compute_seasons(
    df: pd.DataFrame,
    year: int,
    periods: Mapping[str, Mapping[str, Any]] = DEFAULT_GSEASON_PERIODS,
    *,
    include_precip: bool = True,
) -> pd.DataFrame:
    """
    Compute one row per custom period.

    Parameters
    ----------
    df : DataFrame
        Must have a DatetimeIndex in local (naive) America/Denver, and contain
        either 'precip_in' or 'precip_mm' if include_precip=True.
        Precip values must be **increments** (e.g., 5-min), already cleaned (-999→NaN→0, negatives→0).
        If a 'timestamp' column exists, it will be set as index.
    year : int
        Calendar year to which seasonal windows are anchored.
    periods : mapping
        DEFAULT_GSEASON_PERIODS-like dict: { code: {"label": str, "start": "MM-DD", "end": "MM-DD"} }
    include_precip : bool
        If True, sum the precip increments for each window.

    Returns
    -------
    DataFrame with one row per period (code, label, start, end, precip?, plus MEANs of other columns).
    """
    if "timestamp" in df.columns:
        df = df.set_index(pd.to_datetime(df["timestamp"], errors="coerce")).drop(columns=["timestamp"])
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("compute_seasons: df must be indexed by DatetimeIndex or contain a 'timestamp' column.")

    have_precip_in = "precip_in" in df.columns
    have_precip_mm = "precip_mm" in df.columns
    precip_col = "precip_in" if have_precip_in else ("precip_mm" if have_precip_mm else None)

    out_rows = []
    for code, spec in periods.items():
        sm, sd = map(int, spec["start"].split("-"))
        em, ed = map(int, spec["end"].split("-"))

        # Resolve window for this calendar year (wrap-aware, e.g., Nov–Feb)
        start_year = year - 1 if sm > em else year
        end_year   = year
        start = pd.Timestamp(f"{start_year}-{spec['start']}")
        end   = pd.Timestamp(f"{end_year}-{spec['end']}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        # MEAN of all non-precip columns
        means = _slice_and_mean(df.drop(columns=[precip_col], errors="ignore"), start, end).to_dict()

        row = {
            "code":  code,
            "label": spec.get("label", code.replace("_", " ")),
            "start": start,
            "end":   end,
        }
        row.update(means)

        # SUM of precip increments over the window
        if include_precip and precip_col is not None:
            ser = pd.to_numeric(df[precip_col], errors="coerce").fillna(0.0).clip(lower=0.0)
            mask = (df.index >= start) & (df.index <= end)
            row[precip_col] = float(ser.loc[mask].sum())

        out_rows.append(row)

    return pd.DataFrame(out_rows)


def assign_gseason_periods(ts: pd.Timestamp, year: int) -> str | None:
    """
    Return the period code in DEFAULT_GSEASON_PERIODS that contains timestamp `ts`.
    Handles wrap-around windows (e.g., Nov–Feb maps to the given `year`).
    """
    ts = pd.to_datetime(ts)
    for code, period in DEFAULT_GSEASON_PERIODS.items():
        sm, sd = map(int, period["start"].split("-"))
        em, ed = map(int, period["end"].split("-"))

        start_year = year - 1 if sm > em else year
        end_year   = year

        start = pd.Timestamp(f"{start_year}-{period['start']}")
        end   = pd.Timestamp(f"{end_year}-{period['end']}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        if start <= ts <= end:
            return code
    return None