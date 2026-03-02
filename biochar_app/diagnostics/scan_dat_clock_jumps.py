#!/usr/bin/env python3
"""
biochar_app/diagnostics/scan_dat_clock_jumps.py

Purpose
-------
Scan raw Campbell TOA5 logger .dat files (Table1) for likely manual clock resets
(“Set Clock”) by looking at anomalous timestamp differences.

Outputs
-------
1) CSV summary (authoritative clock-mode inventory seed):
   logger,year,forward_jumps,backward_jumps,
   first_forward_prev_time,first_forward_time,first_forward_gap_min,
   first_backward_prev_time,first_backward_time,first_backward_gap_min,
   largest_forward_gap_min,largest_backward_gap_min,
   nonmonotonic_events,duplicate_timestamps,
   status

2) Optional zoom-in report for a specific logger/year:
   - candidate "Set Clock" forward events (gap approx 75 minutes)
   - downtime gaps (e.g., >= 2 hours)

Notes
-----
- Reads from DATA_RAW_DIR/datfiles_<year>/<logger>_Table1.dat
- Does NOT modify any raw data.
- Forward "Set Clock" on a 15-min cadence typically shows as ~75 minutes (15 + 60).
- Backward "Set Clock" often shows as a negative jump (~ -45 minutes) or duplicates/overlaps.
- Some TOA5 exports include rows from the previous/next year; we filter to the year window
  BEFORE counting jumps to avoid false positives at year boundaries.
- CRITICAL: We DO NOT sort timestamps before diff detection. Sorting would erase backward jumps.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from biochar_app.scripts.config import DATA_RAW_DIR, STRIPS, LOGGER_LOCATIONS  # type: ignore


# ----------------------------- year filtering ----------------------------- #

def filter_to_year_window_ts(ts: pd.Series, year: int) -> pd.Series:
    """
    Keep only timestamps within [Jan 1 year, Jan 1 year+1).

    IMPORTANT: Preserve original file order (do NOT sort), because sorting would
    erase backward jumps / overlaps created by manual clock changes.
    """
    if ts is None or ts.empty:
        return ts

    start = pd.Timestamp(year=year, month=1, day=1)
    end = pd.Timestamp(year=year + 1, month=1, day=1)

    vals = ts.to_numpy(dtype="datetime64[ns]")
    mask = (vals >= start.to_datetime64()) & (vals < end.to_datetime64())

    out = ts[mask].copy()
    return out.reset_index(drop=True)


# ----------------------------- TOA5 reading ----------------------------- #

def _read_toa5_timestamps(datfile: Path) -> pd.Series:
    """
    Read only the TIMESTAMP column from a TOA5 .dat file.
    Handles:
      - 4 header rows (meta, colnames, units, aggs)
      - TIMESTAMP or timestamp naming

    Returns a Series of datetime64[ns] timestamps in ORIGINAL FILE ORDER.
    """
    with datfile.open("r", newline="") as f:
        r = csv.reader(f)
        _meta = next(r, None)
        colnames = next(r, None)
        _units = next(r, None)
        _aggs = next(r, None)

    if not colnames:
        raise ValueError(f"{datfile.name}: missing TOA5 column-name row.")

    ts_name: Optional[str] = None
    for c in colnames:
        if c in ("TIMESTAMP", "timestamp"):
            ts_name = c
            break
    if ts_name is None:
        raise ValueError(f"{datfile.name}: TOA5 column-name row missing TIMESTAMP.")

    df = pd.read_csv(
        datfile,
        skiprows=4,
        header=None,
        names=colnames,
        usecols=[ts_name],
        na_values=["", "NA", "NAN"],
        engine="python",
    )

    # Parse; KEEP FILE ORDER (no sort)
    ts = pd.to_datetime(
        df[ts_name].astype("string").str.strip(),
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )

    ts = ts.dropna().astype("datetime64[ns]").reset_index(drop=True)
    return ts


# ----------------------------- jump detection ----------------------------- #

@dataclass(frozen=True)
class JumpSummary:
    forward_count: int
    backward_count: int

    first_forward_prev_time: Optional[pd.Timestamp]
    first_forward_time: Optional[pd.Timestamp]
    first_forward_gap_min: Optional[float]

    first_backward_prev_time: Optional[pd.Timestamp]
    first_backward_time: Optional[pd.Timestamp]
    first_backward_gap_min: Optional[float]

    largest_forward_gap_min: Optional[float]
    largest_backward_gap_min: Optional[float]

    # Extra diagnostics that often reveal "set back" behavior
    nonmonotonic_events: int         # diffs <= 0 minutes (includes duplicates and true backward)
    duplicate_timestamps: int        # diffs == 0 minutes


def summarize_clock_jumps(
    ts: pd.Series,
    *,
    # Forward "Set Clock +60" on 15-min cadence -> ~75 minutes (but can vary)
    fwd_min_minutes: float = 65.0,
    fwd_max_minutes: float = 95.0,
    # Backward "Set Clock -60" on 15-min cadence -> ~ -45 minutes (but can vary)
    bwd_min_minutes: float = -95.0,
    bwd_max_minutes: float = -35.0,
) -> JumpSummary:
    if ts is None or ts.shape[0] < 3:
        return JumpSummary(
            0, 0,
            None, None, None,
            None, None, None,
            None, None,
            0, 0
        )

    # diffs are in file order (critical)
    diffs = ts.diff().dropna()
    mins = diffs.dt.total_seconds() / 60.0

    fwd_mask = (mins >= fwd_min_minutes) & (mins <= fwd_max_minutes)
    bwd_mask = (mins >= bwd_min_minutes) & (mins <= bwd_max_minutes)

    nonmono_mask = mins <= 0
    dup_mask = mins == 0

    # mins index corresponds to the "current" row position in ts (because ts is 0..N-1)
    first_fwd_prev = first_fwd = None
    first_fwd_gap = None
    if bool(fwd_mask.any()):
        i = int(mins[fwd_mask].index[0])
        first_fwd_prev = ts.iloc[i - 1]
        first_fwd = ts.iloc[i]
        first_fwd_gap = float(mins.loc[i])

    first_bwd_prev = first_bwd = None
    first_bwd_gap = None
    if bool(bwd_mask.any()):
        i = int(mins[bwd_mask].index[0])
        first_bwd_prev = ts.iloc[i - 1]
        first_bwd = ts.iloc[i]
        first_bwd_gap = float(mins.loc[i])

    largest_fwd = float(mins[fwd_mask].max()) if bool(fwd_mask.any()) else None
    largest_bwd = float(mins[bwd_mask].min()) if bool(bwd_mask.any()) else None  # most negative

    return JumpSummary(
        forward_count=int(fwd_mask.sum()),
        backward_count=int(bwd_mask.sum()),
        first_forward_prev_time=first_fwd_prev,
        first_forward_time=first_fwd,
        first_forward_gap_min=first_fwd_gap,
        first_backward_prev_time=first_bwd_prev,
        first_backward_time=first_bwd,
        first_backward_gap_min=first_bwd_gap,
        largest_forward_gap_min=largest_fwd,
        largest_backward_gap_min=largest_bwd,
        nonmonotonic_events=int(nonmono_mask.sum()),
        duplicate_timestamps=int(dup_mask.sum()),
    )


# ----------------------------- scan helpers ----------------------------- #

def _logger_tags() -> List[str]:
    return [f"{s}{l}" for s in STRIPS for l in LOGGER_LOCATIONS]


def _dat_path(year: int, logger_tag: str) -> Path:
    return Path(DATA_RAW_DIR) / f"datfiles_{year}" / f"{logger_tag}_Table1.dat"


def scan_year(
    year: int,
    *,
    fwd_min_minutes: float,
    fwd_max_minutes: float,
    bwd_min_minutes: float,
    bwd_max_minutes: float,
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []

    for tag in _logger_tags():
        p = _dat_path(year, tag)
        if not p.exists():
            rows.append(
                {
                    "logger": tag,
                    "year": year,
                    "forward_jumps": 0,
                    "backward_jumps": 0,
                    "first_forward_prev_time": None,
                    "first_forward_time": None,
                    "first_forward_gap_min": None,
                    "first_backward_prev_time": None,
                    "first_backward_time": None,
                    "first_backward_gap_min": None,
                    "largest_forward_gap_min": None,
                    "largest_backward_gap_min": None,
                    "nonmonotonic_events": 0,
                    "duplicate_timestamps": 0,
                    "status": "missing",
                }
            )
            continue

        try:
            ts = _read_toa5_timestamps(p)

            # Filter to this year BEFORE computing diffs/jumps
            ts = filter_to_year_window_ts(ts, year)

            js = summarize_clock_jumps(
                ts,
                fwd_min_minutes=fwd_min_minutes,
                fwd_max_minutes=fwd_max_minutes,
                bwd_min_minutes=bwd_min_minutes,
                bwd_max_minutes=bwd_max_minutes,
            )

            rows.append(
                {
                    "logger": tag,
                    "year": year,
                    "forward_jumps": js.forward_count,
                    "backward_jumps": js.backward_count,
                    "first_forward_prev_time": js.first_forward_prev_time,
                    "first_forward_time": js.first_forward_time,
                    "first_forward_gap_min": js.first_forward_gap_min,
                    "first_backward_prev_time": js.first_backward_prev_time,
                    "first_backward_time": js.first_backward_time,
                    "first_backward_gap_min": js.first_backward_gap_min,
                    "largest_forward_gap_min": js.largest_forward_gap_min,
                    "largest_backward_gap_min": js.largest_backward_gap_min,
                    "nonmonotonic_events": js.nonmonotonic_events,
                    "duplicate_timestamps": js.duplicate_timestamps,
                    "status": "ok",
                }
            )
        except Exception as e:
            rows.append(
                {
                    "logger": tag,
                    "year": year,
                    "forward_jumps": 0,
                    "backward_jumps": 0,
                    "first_forward_prev_time": None,
                    "first_forward_time": None,
                    "first_forward_gap_min": None,
                    "first_backward_prev_time": None,
                    "first_backward_time": None,
                    "first_backward_gap_min": None,
                    "largest_forward_gap_min": None,
                    "largest_backward_gap_min": None,
                    "nonmonotonic_events": 0,
                    "duplicate_timestamps": 0,
                    "status": f"error: {e}",
                }
            )

    df = pd.DataFrame(rows)

    ordered_cols = [
        "logger",
        "year",
        "forward_jumps",
        "backward_jumps",
        "first_forward_prev_time",
        "first_forward_time",
        "first_forward_gap_min",
        "first_backward_prev_time",
        "first_backward_time",
        "first_backward_gap_min",
        "largest_forward_gap_min",
        "largest_backward_gap_min",
        "nonmonotonic_events",
        "duplicate_timestamps",
        "status",
    ]
    for c in ordered_cols:
        if c not in df.columns:
            df[c] = None
    return df[ordered_cols]


# ----------------------------- zoom report ----------------------------- #

def zoom_logger(
    year: int,
    logger_tag: str,
    *,
    downtime_hours: float = 2.0,
    fwd_min_minutes: float = 65.0,
    fwd_max_minutes: float = 95.0,
    out_dir: Path,
) -> Tuple[Path, Path]:
    """
    Write two CSVs:
      1) likely_set_clock_events_<logger>_<year>.csv
      2) downtime_gaps_<logger>_<year>.csv
    """
    p = _dat_path(year, logger_tag)
    if not p.exists():
        raise FileNotFoundError(f"Missing .dat file: {p}")

    ts = _read_toa5_timestamps(p)
    ts = filter_to_year_window_ts(ts, year)

    diffs = ts.diff().dropna()
    mins = diffs.dt.total_seconds() / 60.0

    # likely "Set Clock forward" (~75 min)
    setclock_mask = (mins >= fwd_min_minutes) & (mins <= fwd_max_minutes)
    idx_sc = mins[setclock_mask].index

    setclock_rows: List[Dict[str, object]] = []
    for i in idx_sc:
        ii = int(i)
        setclock_rows.append(
            {
                "logger": logger_tag,
                "year": year,
                "prev_time": ts.iloc[ii - 1],
                "time": ts.iloc[ii],
                "gap_minutes": float(mins.loc[ii]),
            }
        )

    # downtime gaps
    downtime_mask = mins >= (downtime_hours * 60.0)
    idx_dt = mins[downtime_mask].index

    downtime_rows: List[Dict[str, object]] = []
    for i in idx_dt:
        ii = int(i)
        gap_min = float(mins.loc[ii])
        downtime_rows.append(
            {
                "logger": logger_tag,
                "year": year,
                "prev_time": ts.iloc[ii - 1],
                "time": ts.iloc[ii],
                "gap_minutes": gap_min,
                "gap_hours": float(gap_min / 60.0),
            }
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    p_sc = out_dir / f"likely_set_clock_events_{logger_tag}_{year}.csv"
    p_dt = out_dir / f"downtime_gaps_{logger_tag}_{year}.csv"

    pd.DataFrame(setclock_rows).to_csv(p_sc, index=False)
    pd.DataFrame(downtime_rows).to_csv(p_dt, index=False)

    return p_sc, p_dt


# ----------------------------- CLI ----------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description="Scan raw TOA5 .dat files for likely manual clock resets.")
    ap.add_argument("--years", type=int, nargs="+", required=True, help="Years to scan (e.g., 2023 2024 2025).")

    ap.add_argument("--fwd-min", type=float, default=65.0, help="Min minutes for forward Set Clock detection (default 65).")
    ap.add_argument("--fwd-max", type=float, default=95.0, help="Max minutes for forward Set Clock detection (default 95).")

    ap.add_argument("--bwd-min", type=float, default=-95.0, help="Min minutes for backward Set Clock detection (default -95).")
    ap.add_argument("--bwd-max", type=float, default=-35.0, help="Max minutes for backward Set Clock detection (default -35).")

    ap.add_argument(
        "--write-csv",
        action="store_true",
        help="Write the summary CSV into biochar_app/diagnostics/reports/ (recommended).",
    )

    ap.add_argument("--zoom-logger", type=str, default="", help="Optional: logger tag to zoom (e.g., S3M).")
    ap.add_argument("--zoom-year", type=int, default=0, help="Optional: year for zoom logger (e.g., 2024).")
    ap.add_argument("--downtime-hours", type=float, default=2.0, help="Downtime gap threshold for zoom report (default 2h).")

    args = ap.parse_args()

    years = [int(y) for y in args.years]
    reports_dir = Path(__file__).resolve().parent / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    frames: List[pd.DataFrame] = []
    for y in years:
        frames.append(
            scan_year(
                y,
                fwd_min_minutes=float(args.fwd_min),
                fwd_max_minutes=float(args.fwd_max),
                bwd_min_minutes=float(args.bwd_min),
                bwd_max_minutes=float(args.bwd_max),
            )
        )

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    print("\n=== Clock jump scan summary (raw .dat) ===")
    show_cols = [
        "logger",
        "year",
        "forward_jumps",
        "backward_jumps",
        "nonmonotonic_events",
        "duplicate_timestamps",
        "first_forward_time",
        "first_backward_time",
        "status",
    ]
    present = [c for c in show_cols if c in out.columns]
    print(out[present].to_string(index=False))

    if args.write_csv:
        stamp = date.today().isoformat()
        p_out = reports_dir / f"clock_jump_summary_{stamp}.csv"
        out.to_csv(p_out, index=False)
        print(f"\n✅ Wrote: {p_out}")

    zl = args.zoom_logger.strip().upper()
    zy = int(args.zoom_year)
    if zl and zy:
        p_sc, p_dt = zoom_logger(
            zy,
            zl,
            downtime_hours=float(args.downtime_hours),
            fwd_min_minutes=float(args.fwd_min),
            fwd_max_minutes=float(args.fwd_max),
            out_dir=reports_dir,
        )
        print("\n=== Zoom report written ===")
        print(f"  - {p_sc}")
        print(f"  - {p_dt}")
    elif zl or zy:
        print("\n⚠️ Zoom requested but missing --zoom-logger or --zoom-year; skipping zoom output.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())