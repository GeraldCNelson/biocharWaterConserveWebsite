#!/usr/bin/env python3
"""
biochar_app/scripts/logger_toa5.py

Purpose
-------
Small, side-effect-free utilities for reading Campbell Scientific TOA5 logger exports
(e.g., CR206X Table1) and parsing their timestamp column into a clean pandas
datetime64[ns] Series.

Why this module exists
----------------------
We want ONE canonical implementation for:
  - reading TOA5 `.dat` files (Table1) safely
  - parsing TIMESTAMP strings to pandas datetimes
  - returning a tidy timestamp Series suitable for diagnostics (clock shifts, gaps)
    and for ETL (merging/aggregation)

This avoids duplicating logic across:
  - biochar_app/scripts/etl.py
  - biochar_app/diagnostics/* (clock shift scanners, battery analysis, etc.)

Design principles
-----------------
* No orchestration, no I/O side effects beyond reading the specified file.
* Does NOT modify any files in data-raw.
* Keeps timestamps timezone-naive. They represent the logger's "wall time"
  as written in the TOA5 export (typically Mountain local time, but may be
  manually set to MST/MDT depending on field procedures).
* Returns datetime64[ns] with NaT rows dropped to make downstream `.diff()`
  and windowing operations deterministic.

Inputs / assumptions
--------------------
* TOA5 Table1 exports follow the typical structure:
    Row 1: metadata
    Row 2: column names (includes TIMESTAMP)
    Row 3: units
    Row 4: aggregation / sample info
    Row 5+: data
  We therefore read with skiprows=4 and use the row-2 names.

* Timestamp string format expected:
    YYYY-MM-DD HH:MM:SS
  If a row fails to parse, it becomes NaT and is dropped.

Typical usage
-------------
ETL:
  from biochar_app.scripts.logger_toa5 import _read_toa5_table1_dat, normalize_logger_timestamp_series

Diagnostics:
  from biochar_app.scripts.logger_toa5 import read_dat_timestamps
  ts = read_dat_timestamps(Path(".../S3M_Table1.dat"))
  diffs = ts.diff()

Notes
-----
If you ever change timestamp format in the logger program or export settings,
update normalize_logger_timestamp_series() and keep ALL callers consistent.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
from pandas import Series


def normalize_logger_timestamp_series(ts: Series) -> Series:
    """
    Parse TOA5 TIMESTAMP strings into pandas datetimes (timezone-naive).
    Expected format: 'YYYY-MM-DD HH:MM:SS'
    """
    s = ts.astype("string").str.strip()
    return pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")


def _read_toa5_table1_dat(datfile: Path) -> pd.DataFrame:
    """
    Read a Campbell Scientific TOA5 Table1 .dat file and return a DataFrame with
    column names derived from the TOA5 header row.

    The returned frame still contains raw strings; callers typically parse
    'TIMESTAMP'/'timestamp' using normalize_logger_timestamp_series().
    """
    with datfile.open("r", newline="") as f:
        r = csv.reader(f)
        _meta = next(r, None)
        colnames = next(r, None)
        _units = next(r, None)
        _aggs = next(r, None)

    if not colnames:
        raise ValueError(f"{datfile.name}: missing TOA5 column-name row.")
    if "TIMESTAMP" not in colnames and "timestamp" not in colnames:
        raise ValueError(f"{datfile.name}: TOA5 column-name row does not include TIMESTAMP.")

    return pd.read_csv(
        datfile,
        skiprows=4,
        header=None,
        names=colnames,
        na_values=["", "NA", "NAN"],
        engine="python",
    )


def read_dat_timestamps(datfile: Path) -> pd.Series:
    """
    Convenience helper:
      - reads TOA5 Table1 .dat
      - normalizes TIMESTAMP -> 'timestamp' if needed
      - parses timestamps using normalize_logger_timestamp_series()
      - drops NaT
      - returns Series[datetime64[ns]] suitable for diff/gap analysis
    """
    df = _read_toa5_table1_dat(datfile)

    if "TIMESTAMP" in df.columns and "timestamp" not in df.columns:
        df = df.rename(columns={"TIMESTAMP": "timestamp"})
    if "timestamp" not in df.columns:
        raise ValueError(f"{datfile.name}: missing timestamp column after normalization.")

    raw_ts = df["timestamp"].copy()
    ts = normalize_logger_timestamp_series(raw_ts)
    ts = pd.to_datetime(ts, errors="coerce")
    ts = ts.dropna()

    # Force dtype for consistent diff math / comparisons.
    return ts.astype("datetime64[ns]")

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Read TOA5 .dat timestamps and report gaps.")
    ap.add_argument("datfile", type=str, help="Path to TOA5 Table1 .dat file")
    args = ap.parse_args()

    from pathlib import Path
    ts = read_dat_timestamps(Path(args.datfile)).sort_values()
    diffs = ts.diff()

    print(f"\nFile: {args.datfile}")
    print(f"Rows: {len(ts)}")

    fwd = diffs[diffs > pd.Timedelta(minutes=45)]
    bwd = diffs[diffs < -pd.Timedelta(minutes=30)]

    print("\nForward jumps:")
    print(fwd if not fwd.empty else "None")

    print("\nBackward jumps:")
    print(bwd if not bwd.empty else "None")