#!/usr/bin/env python3
"""
biochar_app/diagnostics/scan_dat_clock_events.py

Scan raw Campbell TOA5 logger .dat files (Table1) for clock anomalies by analyzing
timestamp deltas in ORIGINAL FILE ORDER (no sorting).

Outputs (when --write-csv):
  1) dat_clock_summary.csv        : per logger/year summary + classification
  2) dat_clock_events.csv         : one row per detected event (timeline)
  3) dat_clock_out_of_order.csv   : subset of events (small backward steps only)

Scope (by design):
  - ONLY scans: <DATA_RAW_DIR>/datfiles_{year}/  for year in 2023..2026
  - ONLY scans files whose names contain "Table1" and end with ".dat"
  - NOT recursive (ignores archive and subdirectories)

Why:
  - These checks must operate on RAW .dat “truth” (not parquet).
  - We keep file order to preserve backward jumps / overlaps caused by manual clock sets.

Event definitions (defaults; adjustable via CLI):
  - setclock_forward:  gap in [fwd_min, fwd_max] minutes
  - setclock_backward: gap in [bwd_min, bwd_max] minutes
  - duplicate_timestamp: gap == 0 minutes
  - out_of_order: negative gap, but NOT big enough to be a setclock_backward event
                  (e.g., -15 minutes or -30 minutes from file re-ordering / duplication)
  - downtime_gap: large positive gap >= downtime_hours

Notes:
  - We filter to the [Jan 1 year, Jan 1 year+1) window BEFORE diff detection,
    while preserving original file order within that window.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, cast

import pandas as pd
from pandas import Series

from biochar_app.scripts.config import DATA_RAW_DIR  # type: ignore


# ----------------------------- models ----------------------------- #

@dataclass(frozen=True)
class EventRow:
    logger_tag: str
    year: int
    file: str                 # relative path for readability
    file_row_idx: int         # position in filtered-year series (>=1 because it references diff)
    prev_time: str
    time: str
    gap_min: float
    event_type: str           # setclock_forward/backward/duplicate/out_of_order/nonmonotonic/downtime_gap
    event_class: str          # "clock" | "order" | "downtime" | "other"


@dataclass(frozen=True)
class SummaryRow:
    logger_tag: str
    year: int
    file: str
    n_rows: int

    forward_setclock: int
    backward_setclock: int
    duplicate_timestamps: int
    out_of_order: int
    nonmonotonic_events: int
    downtime_gaps: int

    first_forward_time: str
    first_backward_time: str
    first_out_of_order_time: str

    has_FF: bool
    has_BB: bool

    classification: str       # likely_explained | needs_research
    reasons: str              # human-readable reasons
    status: str               # ok | missing | read_error


# ----------------------------- helpers ----------------------------- #

TS_FORMATS: List[str] = [
    "%Y-%m-%d %H:%M:%S",   # common TOA5 export
    "%m/%d/%y %H:%M",      # your example: 1/13/26 1:15
    "%m/%d/%Y %H:%M",
]


def iter_table1_files(years: List[int]) -> Iterable[Tuple[int, Path]]:
    """
    Yield (year, datfile) for *non-recursive* datfiles_{year} directories.
    Only files containing "Table1" and ending in ".dat".
    """
    base = Path(DATA_RAW_DIR)
    for year in years:
        d = base / f"datfiles_{year}"
        if not d.exists() or not d.is_dir():
            continue
        for f in sorted(d.iterdir()):
            if not f.is_file():
                continue
            name = f.name
            if name.endswith(".dat") and ("Table1" in name):
                yield year, f


def logger_tag_from_filename(datfile: Path) -> str:
    """
    Expect names like 'S3M_Table1.dat' -> 'S3M'
    If it doesn't match, return stem up to first underscore.
    """
    stem = datfile.name
    if "_" in stem:
        return stem.split("_", 1)[0]
    return datfile.stem


def _read_toa5_timestamp_column(datfile: Path) -> pd.Series:
    """
    Read TIMESTAMP column in ORIGINAL FILE ORDER (no sorting).
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

    raw = df[ts_name].astype("string").str.strip()

    # Default parse (fallback)
    parsed: Series = cast(Series, pd.to_datetime(raw, errors="coerce"))

    # Try known formats first; if any work, keep that format’s parse
    for fmt in TS_FORMATS:
        ts = cast(Series, pd.to_datetime(raw, format=fmt, errors="coerce"))
        if ts.notna().any():
            parsed = ts
            break

    parsed = cast(Series, parsed.dropna().astype("datetime64[ns]").reset_index(drop=True))
    return parsed


def filter_to_year_window(ts: pd.Series, year: int) -> pd.Series:
    """
    Keep timestamps within [Jan 1 year, Jan 1 year+1), preserving file order.
    """
    if ts is None or ts.empty:
        return ts

    start = pd.Timestamp(year=year, month=1, day=1)
    end = pd.Timestamp(year=year + 1, month=1, day=1)

    vals = ts.to_numpy(dtype="datetime64[ns]")
    mask = (vals >= start.to_datetime64()) & (vals < end.to_datetime64())
    out = ts[mask].copy()
    return out.reset_index(drop=True)


def _has_consecutive(seq: List[str], target: str) -> bool:
    if len(seq) < 2:
        return False
    return any((a == target and b == target) for a, b in zip(seq[:-1], seq[1:]))


def _fmt(ts: Optional[pd.Timestamp]) -> str:
    if ts is None or pd.isna(ts):
        return ""
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


# ----------------------------- core detection ----------------------------- #

def detect_events(
    ts: pd.Series,
    *,
    logger_tag: str,
    year: int,
    file_rel: str,
    fwd_min_minutes: float,
    fwd_max_minutes: float,
    bwd_min_minutes: float,
    bwd_max_minutes: float,
    out_of_order_max_abs_minutes: float,
    downtime_hours: float,
) -> Tuple[List[EventRow], SummaryRow, List[EventRow]]:
    """
    Returns:
      events: all detected events
      summary: per file/year summary
      ooo: out-of-order subset (small backward steps)
    """
    if ts is None or ts.empty or ts.shape[0] < 2:
        summary = SummaryRow(
            logger_tag=logger_tag,
            year=year,
            file=file_rel,
            n_rows=int(ts.shape[0]) if ts is not None else 0,
            forward_setclock=0,
            backward_setclock=0,
            duplicate_timestamps=0,
            out_of_order=0,
            nonmonotonic_events=0,
            downtime_gaps=0,
            first_forward_time="",
            first_backward_time="",
            first_out_of_order_time="",
            has_FF=False,
            has_BB=False,
            classification="likely_explained",
            reasons="",
            status="ok",
        )
        return [], summary, []

    # Compute deltas in FILE ORDER
    gaps = ts.diff().dt.total_seconds().div(60.0)

    events: List[EventRow] = []
    ooo: List[EventRow] = []
    seq_FB: List[str] = []

    fwd_ct = bwd_ct = dup_ct = ooo_ct = nonmono_ct = down_ct = 0
    first_fwd: Optional[pd.Timestamp] = None
    first_bwd: Optional[pd.Timestamp] = None
    first_ooo: Optional[pd.Timestamp] = None

    downtime_min = float(downtime_hours) * 60.0

    for i in range(1, len(ts)):
        gap = float(gaps.iloc[i]) if pd.notna(gaps.iloc[i]) else float("nan")
        if pd.isna(gap):
            continue

        prev_t = pd.Timestamp(ts.iloc[i - 1])
        cur_t = pd.Timestamp(ts.iloc[i])

        event_type = ""
        event_class = ""

        # Classification priority: explicit clock set windows first
        if fwd_min_minutes <= gap <= fwd_max_minutes:
            event_type = "setclock_forward"
            event_class = "clock"
            fwd_ct += 1
            seq_FB.append("F")
            if first_fwd is None:
                first_fwd = cur_t

        elif bwd_min_minutes <= gap <= bwd_max_minutes:
            event_type = "setclock_backward"
            event_class = "clock"
            bwd_ct += 1
            seq_FB.append("B")
            if first_bwd is None:
                first_bwd = cur_t

        elif gap == 0.0:
            event_type = "duplicate_timestamp"
            event_class = "order"
            dup_ct += 1
            nonmono_ct += 1

        elif gap < 0.0:
            # Negative gap, but NOT in the "setclock_backward" window.
            # Treat small backward steps as "out_of_order" (likely export/merge issues),
            # and larger ones as generic nonmonotonic.
            nonmono_ct += 1
            if abs(gap) <= float(out_of_order_max_abs_minutes):
                event_type = "out_of_order"
                event_class = "order"
                ooo_ct += 1
                if first_ooo is None:
                    first_ooo = cur_t
            else:
                event_type = "nonmonotonic"
                event_class = "other"

        elif gap >= downtime_min:
            event_type = "downtime_gap"
            event_class = "downtime"
            down_ct += 1

        if event_type:
            er = EventRow(
                logger_tag=logger_tag,
                year=year,
                file=file_rel,
                file_row_idx=i,
                prev_time=_fmt(prev_t),
                time=_fmt(cur_t),
                gap_min=round(gap, 3),
                event_type=event_type,
                event_class=event_class,
            )
            events.append(er)
            if event_type == "out_of_order":
                ooo.append(er)

    has_FF = _has_consecutive(seq_FB, "F")
    has_BB = _has_consecutive(seq_FB, "B")

    reasons: List[str] = []
    needs_research = False

    # Your preferred posture: keep ETL diagnostics minimal; this script is the truth-check.
    # Still, classify likely trouble patterns to prioritize review.
    if year == 2023 and fwd_ct > 0:
        needs_research = True
        reasons.append("2023 has forward setclock event(s)")
    if has_FF:
        needs_research = True
        reasons.append("consecutive forward setclock events (FF)")
    if has_BB:
        needs_research = True
        reasons.append("consecutive backward setclock events (BB)")
    if ooo_ct > 0:
        # Small backward steps are often export ordering / file mixing issues worth reviewing.
        needs_research = True
        reasons.append(f"out_of_order events={ooo_ct}")

    classification = "needs_research" if needs_research else "likely_explained"

    summary = SummaryRow(
        logger_tag=logger_tag,
        year=year,
        file=file_rel,
        n_rows=int(ts.shape[0]),
        forward_setclock=fwd_ct,
        backward_setclock=bwd_ct,
        duplicate_timestamps=dup_ct,
        out_of_order=ooo_ct,
        nonmonotonic_events=nonmono_ct,
        downtime_gaps=down_ct,
        first_forward_time=_fmt(first_fwd),
        first_backward_time=_fmt(first_bwd),
        first_out_of_order_time=_fmt(first_ooo),
        has_FF=has_FF,
        has_BB=has_BB,
        classification=classification,
        reasons="; ".join(reasons),
        status="ok",
    )

    return events, summary, ooo


# ----------------------------- CLI / orchestration ----------------------------- #

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", nargs="+", type=int, default=[2023, 2024, 2025, 2026])

    ap.add_argument(
        "--write-csv",
        action="store_true",
        help="Write CSV outputs to biochar_app/diagnostics/ (or --out-dir).",
    )
    ap.add_argument(
        "--out-dir",
        default="biochar_app/diagnostics",
        help="Directory for output CSVs (default: biochar_app/diagnostics).",
    )

    # Thresholds (minutes)
    ap.add_argument("--fwd-min", type=float, default=65.0)
    ap.add_argument("--fwd-max", type=float, default=95.0)
    ap.add_argument("--bwd-min", type=float, default=-95.0)
    ap.add_argument("--bwd-max", type=float, default=-35.0)

    # Out-of-order definition (minutes)
    ap.add_argument(
        "--out-of-order-max-abs-min",
        type=float,
        default=30.0,
        help="Max absolute minutes for a small backward step to be classified as out_of_order (default: 30).",
    )

    ap.add_argument("--downtime-hours", type=float, default=2.0)

    # Optional focus
    ap.add_argument("--zoom-logger", default="", help="Only analyze this logger tag (e.g., S3M).")
    ap.add_argument("--zoom-year", type=int, default=0, help="Only analyze this year (e.g., 2024).")

    args = ap.parse_args()

    years = [int(y) for y in args.years]
    years = [y for y in years if 2023 <= y <= 2026]  # enforce intended scope
    years = sorted(set(years))

    if args.zoom_year:
        years = [int(args.zoom_year)]

    out_dir = Path(args.out_dir).expanduser()
    if args.write_csv:
        out_dir.mkdir(parents=True, exist_ok=True)

    all_summaries: List[SummaryRow] = []
    all_events: List[EventRow] = []
    all_ooo: List[EventRow] = []

    scanned = 0
    missing = 0
    errors = 0

    for year, datfile in iter_table1_files(years):
        tag = logger_tag_from_filename(datfile)
        if args.zoom_logger and tag != args.zoom_logger:
            continue

        scanned += 1
        file_rel = str(datfile.relative_to(Path.cwd())) if datfile.is_absolute() else str(datfile)

        try:
            ts_raw = _read_toa5_timestamp_column(datfile)
            ts = filter_to_year_window(ts_raw, year)
            events, summary, ooo = detect_events(
                ts,
                logger_tag=tag,
                year=year,
                file_rel=file_rel,
                fwd_min_minutes=float(args.fwd_min),
                fwd_max_minutes=float(args.fwd_max),
                bwd_min_minutes=float(args.bwd_min),
                bwd_max_minutes=float(args.bwd_max),
                out_of_order_max_abs_minutes=float(args.out_of_order_max_abs_min),
                downtime_hours=float(args.downtime_hours),
            )
            all_summaries.append(summary)
            all_events.extend(events)
            all_ooo.extend(ooo)

        except FileNotFoundError:
            missing += 1
            all_summaries.append(
                SummaryRow(
                    logger_tag=tag,
                    year=year,
                    file=file_rel,
                    n_rows=0,
                    forward_setclock=0,
                    backward_setclock=0,
                    duplicate_timestamps=0,
                    out_of_order=0,
                    nonmonotonic_events=0,
                    downtime_gaps=0,
                    first_forward_time="",
                    first_backward_time="",
                    first_out_of_order_time="",
                    has_FF=False,
                    has_BB=False,
                    classification="needs_research",
                    reasons="missing_file",
                    status="missing",
                )
            )

        except Exception as e:
            errors += 1
            all_summaries.append(
                SummaryRow(
                    logger_tag=tag,
                    year=year,
                    file=file_rel,
                    n_rows=0,
                    forward_setclock=0,
                    backward_setclock=0,
                    duplicate_timestamps=0,
                    out_of_order=0,
                    nonmonotonic_events=0,
                    downtime_gaps=0,
                    first_forward_time="",
                    first_backward_time="",
                    first_out_of_order_time="",
                    has_FF=False,
                    has_BB=False,
                    classification="needs_research",
                    reasons=f"read_error: {e}",
                    status="read_error",
                )
            )

    # Print summary to console
    df_sum = pd.DataFrame([s.__dict__ for s in all_summaries])
    df_evt = pd.DataFrame([e.__dict__ for e in all_events])
    df_ooo = pd.DataFrame([e.__dict__ for e in all_ooo])

    print(f"Scanned Table1 .dat files: {scanned}")
    if missing:
        print(f"Missing files: {missing}")
    if errors:
        print(f"Read errors: {errors}")
    print(f"Event rows: {len(df_evt)}")
    print(f"Out-of-order rows: {len(df_ooo)}")

    if args.write_csv:
        # Stable filenames (easy to diff/commit)
        p_sum = out_dir / "dat_clock_summary.csv"
        p_evt = out_dir / "dat_clock_events.csv"
        p_ooo = out_dir / "dat_clock_out_of_order.csv"

        df_sum.to_csv(p_sum, index=False)
        df_evt.to_csv(p_evt, index=False)
        df_ooo.to_csv(p_ooo, index=False)

        print(f"Wrote: {p_sum}")
        print(f"Wrote: {p_evt}")
        print(f"Wrote: {p_ooo}")


if __name__ == "__main__":
    main()