# biochar_app/scripts/gseason.py

from typing import Sequence, Mapping, Any
from biochar_app.scripts.config import DEFAULT_GSEASON_PERIODS
import pandas as pd

def slice_and_mean(
    df: pd.DataFrame,
    start: pd.Timestamp,
    end:   pd.Timestamp
) -> pd.Series:
    """
    Return the column‐wise mean of df between start and end (inclusive).
    Assumes df is indexed by a DatetimeIndex.
    """
    mask = (df.index >= start) & (df.index <= end)
    return df.loc[mask].mean()


def compute_seasons(
    df: pd.DataFrame,
    periods: Sequence[Mapping[str, Any]],
    *,
    precip_col:       str  = "precip_in",
    include_precip:   bool = True,
    unit_system:      str  = "us",
) -> pd.DataFrame:
    """
    Compute one row per custom period.

    Parameters
    ----------
    df : DataFrame
        Must have a DatetimeIndex and, if include_precip is True,
        a column named precip_col or its fallback ("precip_mm"/"precip_in").
    periods : list of dict
        Each dict must contain:
          - "code"  : unique identifier for the period
          - "label" : human‐readable name
          - "start" : ISO date string e.g. "2024-03-01"
          - "end"   : ISO date string e.g. "2024-05-31"
    precip_col : str
        Which precipitation column to sum (will fall back & convert
        if missing—see below).
    include_precip : bool
        Whether to sum precipitation in each period.
    unit_system : str
        "us" or "metric"—only used if we need to convert from the fallback.
    """
    rows = []
    # pre‐compute fallback column & conversion if needed
    fallback = "precip_mm" if precip_col == "precip_in" else "precip_in"
    for p in periods:
        start = pd.to_datetime(p["start"])
        end   = pd.to_datetime(p["end"])
        # 1) mean of all sensor columns
        means = slice_and_mean(df.drop(columns=["timestamp"], errors="ignore"), start, end)
        data = means.to_dict()

        # 2) metadata
        data["period_code"]  = p["code"]
        data["period_label"] = p.get("label", p["code"])

        # 3) optional precip sum
        if include_precip:
            if precip_col in df.columns:
                ser = df[precip_col]
            elif fallback in df.columns:
                ser = df[fallback]
                # convert fallback → precip_col
                if precip_col=="precip_in":
                    ser = ser / 25.4
                else:
                    ser = ser * 25.4
            else:
                ser = pd.Series([], dtype=float)

            mask = (df.index >= start) & (df.index <= end)
            data["precip"] = float(ser.loc[mask].sum())

        rows.append(data)

    return pd.DataFrame(rows)

def assign_gseason_periods(ts: pd.Timestamp, year: int) -> str | None:
    for label, period in DEFAULT_GSEASON_PERIODS.items():
        start_str = period["start"]
        end_str = period["end"]
        sm, sd = map(int, start_str.split("-"))
        em, ed = map(int, end_str.split("-"))

        start_year = year - 1 if sm > em else year
        end_year = year

        start = pd.Timestamp(f"{start_year}-{start_str}")
        end = pd.Timestamp(f"{end_year}-{end_str}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        if start <= ts <= end:
            return label
    return None