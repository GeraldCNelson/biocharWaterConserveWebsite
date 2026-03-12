#!/usr/bin/env python3
"""
check_dst_transitions_parquet.py

Inspect ETL-corrected parquet timestamps around expected DST transition dates.

Purpose
-------
Validate that final parquet timestamps reflect the intended local wall-time
behavior around spring-forward and fall-back transitions.

This checks the OUTPUT of ETL, not the raw .dat files.

Input
-----
biochar_app/data-processed/parquet/summary/15min/{year}_15min.parquet

Output
------
For each requested transition window:
- prints the timestamps in the window
- prints timestamp diffs in minutes
- flags any negative diffs
- optionally writes a CSV per window for inspection
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


PARQUET_DIR = Path("biochar_app/data-processed/parquet/summary/15min")
OUT_DIR = Path("biochar_app/diagnostics/reports/dst_transition_checks")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# These are the transition dates you want to inspect.
# We use a broad window around each date rather than trying to encode DST rules.
TRANSITIONS: dict[int, list[tuple[str, str]]] = {
    2023: [("fall_back", "2023-11-05")],
    2024: [("spring_forward", "2024-03-10"), ("fall_back", "2024-11-03")],
    2025: [("spring_forward", "2025-03-09"), ("fall_back", "2025-11-02")],
}


def load_year_parquet(year: int) -> pd.DataFrame:
    path = PARQUET_DIR / f"{year}_15min.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet file: {path}")

    df = pd.read_parquet(path, columns=["timestamp"]).copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df


def inspect_window(
    df: pd.DataFrame,
    *,
    year: int,
    label: str,
    center_date: str,
    hours_before: int = 12,
    hours_after: int = 12,
    write_csv: bool = True,
) -> None:
    center = pd.Timestamp(center_date)
    start = center - pd.Timedelta(hours=hours_before)
    end = center + pd.Timedelta(hours=hours_after)

    win = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)].copy()
    if win.empty:
        print(f"\n[{year} {label}] No rows found in window {start} .. {end}")
        return

    win["diff_min"] = win["timestamp"].diff().dt.total_seconds() / 60.0

    print(f"\n=== {year} {label} ===")
    print(f"Window: {start} .. {end}")
    print(win.to_string(index=False))

    neg = win[win["diff_min"] < 0]
    if not neg.empty:
        print("\n!!! Negative diffs found !!!")
        print(neg[["timestamp", "diff_min"]].to_string(index=False))

    unusual = win[(win["diff_min"].notna()) & (win["diff_min"] != 15.0)]
    if not unusual.empty:
        print("\nUnusual diffs:")
        print(unusual[["timestamp", "diff_min"]].to_string(index=False))

    if write_csv:
        out_path = OUT_DIR / f"{year}_{label}.csv"
        win.to_csv(out_path, index=False)
        print(f"\nWrote: {out_path}")


def main(years: Iterable[int] = (2023, 2024, 2025)) -> None:
    for year in years:
        df = load_year_parquet(year)
        for label, center_date in TRANSITIONS.get(year, []):
            inspect_window(df, year=year, label=label, center_date=center_date)


if __name__ == "__main__":
    main()