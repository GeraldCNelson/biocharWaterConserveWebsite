#!/usr/bin/env python3
"""
biochar_app/diagnostics/scan_clock_shifts.py

Scan raw logger parquet timestamps for likely manual "Set Clock" events.

We detect:
- forward jumps: large positive timestamp gaps (e.g., 45/60/75 minutes)
- backward jumps: negative timestamp jumps (e.g., -45/-60 minutes)

This is *not* changing any data. It only reports likely manual clock-set events.

Example:
  python -m biochar_app.diagnostics.scan_clock_shifts --years 2024 2025
  python -m biochar_app.diagnostics.scan_clock_shifts --years 2024 --start 2024-01-01 --end 2024-06-30
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from biochar_app.scripts.config import PARQUET_DIR, STRIPS, LOGGER_LOCATIONS  # type: ignore


# ----------------------------- IO helpers ----------------------------- #

def _read_raw_logger_parquet(year: int) -> pd.DataFrame:
    path = Path(PARQUET_DIR) / str(year) / f"{year}_raw_logger.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Raw logger parquet not found for {year}: {path}")
    df = pd.read_parquet(path)

    if "timestamp" not in df.columns:
        raise ValueError(f"{path.name}: expected a 'timestamp' column")

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df.reset_index(drop=True)


def _parse_dt(s: Optional[str]) -> Optional[pd.Timestamp]:
    if not s:
        return None
    ts = pd.to_datetime(s, errors="coerce")
    if ts is pd.NaT or pd.isna(ts):
        raise ValueError(f"Could not parse datetime: {s!r}")
    assert isinstance(ts, pd.Timestamp)
    return ts


# ----------------------------- detection ----------------------------- #

@dataclass(frozen=True)
class ClockEvent:
    logger: str
    direction: str  # "forward" or "backward"
    prev_ts: pd.Timestamp
    ts: pd.Timestamp
    gap_min: float


def _logger_suffix_cols(df: pd.DataFrame, strip: str, loc: str) -> List[str]:
    """
    Your raw columns look like: VWC_1_raw_S1_T, T_2_raw_S3_M, BattV_Min_S1_T, etc.
    The reliable way to isolate a logger is suffix: _<strip>_<loc>.
    """
    suffix = f"_{strip}_{loc}"
    cols = [c for c in df.columns if c != "timestamp" and str(c).endswith(suffix)]
    # also include battery column pattern if present
    batt = f"BattV_Min_{strip}_{loc}"
    if batt in df.columns and batt not in cols:
        cols.append(batt)
    return cols


def find_events(
    sub: pd.DataFrame,
    logger_name: str,
    *,
    min_forward_minutes: float = 30.0,
    max_forward_minutes: float = 24 * 60.0,  # ignore multi-day outages here; those are "missing data" not clock sets
    min_backward_minutes: float = -24 * 60.0,
    max_backward_minutes: float = -30.0,
) -> List[ClockEvent]:
    """
    sub must contain 'timestamp' and already be sorted + reset_index(drop=True).
    """
    if sub.shape[0] < 2:
        return []

    diffs = sub["timestamp"].diff().dropna()
    gap_min = diffs.dt.total_seconds() / 60.0

    events: List[ClockEvent] = []

    # forward: big positive gaps (e.g. 45/60/75)
    forward_mask = (gap_min >= min_forward_minutes) & (gap_min <= max_forward_minutes)
    forward_pos = np.flatnonzero(forward_mask.to_numpy())
    for j in forward_pos:
        # gap_min[j] corresponds to transition from row j -> j+1 in the *sub* after dropna
        # Because diffs is aligned to sub rows starting at index 1, we map:
        # diffs index k corresponds to sub row k (current) and sub row k-1 (prev).
        k = int(gap_min.index[j])  # label in sub because sub is reset-indexed, this is safe (0..n-1)
        prev_ts = sub["timestamp"].iloc[k - 1]
        ts = sub["timestamp"].iloc[k]
        events.append(
            ClockEvent(
                logger=logger_name,
                direction="forward",
                prev_ts=prev_ts,
                ts=ts,
                gap_min=float((ts - prev_ts).total_seconds() / 60.0),
            )
        )

    # backward: negative diffs around -45/-60 etc.
    backward_mask = (gap_min >= min_backward_minutes) & (gap_min <= max_backward_minutes)
    backward_pos = np.flatnonzero(backward_mask.to_numpy())
    for j in backward_pos:
        k = int(gap_min.index[j])
        prev_ts = sub["timestamp"].iloc[k - 1]
        ts = sub["timestamp"].iloc[k]
        events.append(
            ClockEvent(
                logger=logger_name,
                direction="backward",
                prev_ts=prev_ts,
                ts=ts,
                gap_min=float((ts - prev_ts).total_seconds() / 60.0),
            )
        )

    # sort chronologically within logger
    events.sort(key=lambda e: (e.prev_ts, e.ts))
    return events


def _format_ts(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%d %H:%M")


# ----------------------------- main ----------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description="Scan raw logger timestamps for manual Set Clock events.")
    ap.add_argument("--years", type=int, nargs="+", required=True, help="Years to scan (e.g., 2024 2025).")
    ap.add_argument("--start", type=str, default=None, help="Optional start datetime (e.g., 2024-01-01).")
    ap.add_argument("--end", type=str, default=None, help="Optional end datetime (e.g., 2024-12-31).")
    ap.add_argument("--min-forward", type=float, default=30.0, help="Min minutes for forward clock-set event.")
    ap.add_argument("--max-forward", type=float, default=24 * 60.0, help="Max minutes for forward clock-set event.")
    ap.add_argument("--max-backward", type=float, default=-30.0, help="Max minutes (negative) for backward event.")
    ap.add_argument("--min-backward", type=float, default=-24 * 60.0, help="Min minutes (negative) for backward event.")
    args = ap.parse_args()

    start_ts = _parse_dt(args.start)
    end_ts = _parse_dt(args.end)

    for year in args.years:
        df = _read_raw_logger_parquet(int(year))

        if start_ts is not None:
            df = df[df["timestamp"] >= start_ts]
        if end_ts is not None:
            df = df[df["timestamp"] <= end_ts]
        df = df.reset_index(drop=True)

        window_desc = "full year"
        if start_ts is not None or end_ts is not None:
            window_desc = f"{_format_ts(start_ts) if start_ts is not None else '…'} → {_format_ts(end_ts) if end_ts is not None else '…'}"

        print(f"\n=== YEAR {year} ({window_desc}) ===")

        any_events = False

        for strip in STRIPS:
            for loc in LOGGER_LOCATIONS:
                logger_name = f"{strip}{loc}"
                cols = _logger_suffix_cols(df, strip, loc)
                if not cols:
                    continue

                sub = df[["timestamp"] + cols].dropna(how="all", subset=cols).copy()
                if sub.empty:
                    continue

                sub = sub.sort_values("timestamp").reset_index(drop=True)

                ev = find_events(
                    sub,
                    logger_name=logger_name,
                    min_forward_minutes=float(args.min_forward),
                    max_forward_minutes=float(args.max_forward),
                    min_backward_minutes=float(args.min_backward),
                    max_backward_minutes=float(args.max_backward),
                )

                if not ev:
                    continue

                any_events = True
                print(f"\n--- {logger_name} ---")
                for e in ev:
                    sign = "+" if e.gap_min >= 0 else ""
                    print(
                        f"  {e.direction:8s}  gap={sign}{e.gap_min:.1f} min"
                        f"   { _format_ts(e.prev_ts) } -> { _format_ts(e.ts) }"
                    )

        if not any_events:
            print("No obvious clock-set events found in this window.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())