# Revised scan_dat_state_tracker.py (with S3T deduplication)
#!/usr/bin/env python3
"""
biochar_app/diagnostics/scan_dat_state_tracker.py

Purpose
-------
Build a chronological clock-state tracker per logger by scanning TOA5 .dat files
in original file order, detecting manual "Set Clock" events via anomalous diffs.

S3T REMEDIATION
---------------
For logger S3T (2024), drop later duplicate (TIMESTAMP, RECORD) rows before detection.
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


def _logger_tags() -> List[str]:
    return [f"{s}{l}" for s in STRIPS for l in LOGGER_LOCATIONS]


def _dat_path(year: int, logger_tag: str) -> Path:
    return Path(DATA_RAW_DIR) / f"datfiles_{year}" / f"{logger_tag}_Table1.dat"


def _mode_from_offset_minutes(offset_min: int) -> str:
    m = offset_min % 120
    if m < 0:
        m += 120
    if m == 0:
        return "MDT"
    if m == 60:
        return "MST"
    return f"SHIFTED({offset_min}min)"


def _find_colname_ci(cols: List[str], name: str) -> Optional[str]:
    target = name.strip().lower()
    for c in cols:
        if str(c).strip().lower() == target:
            return c
    return None


def filter_to_year_window_ts(ts: pd.Series, year: int) -> pd.Series:
    if ts is None or ts.empty:
        return ts
    start = pd.Timestamp(year=year, month=1, day=1)
    end = pd.Timestamp(year=year+1, month=1, day=1)
    vals = ts.to_numpy(dtype="datetime64[ns]")
    mask = (vals >= start.to_datetime64()) & (vals < end.to_datetime64())
    return ts[mask].copy().reset_index(drop=True)


def _filter_to_year_window_df(df: pd.DataFrame, year: int, ts_col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    start = pd.Timestamp(year=year, month=1, day=1)
    end = pd.Timestamp(year=year+1, month=1, day=1)
    vals = df[ts_col].to_numpy(dtype="datetime64[ns]")
    mask = (vals >= start.to_datetime64()) & (vals < end.to_datetime64())
    return df[mask].copy().reset_index(drop=True)


def _read_toa5_timestamps(datfile: Path) -> pd.Series:
    with datfile.open("r", newline="") as f:
        r = csv.reader(f)
        _ = next(r, None); cols = next(r, None)
        _ = next(r, None); _ = next(r, None)
    if not cols:
        raise ValueError(f"{datfile.name}: missing columns")
    ts_col = _find_colname_ci(cols, "TIMESTAMP")
    if ts_col is None:
        raise ValueError(f"{datfile.name}: missing TIMESTAMP")
    df = pd.read_csv(datfile, skiprows=4, header=None, names=cols,
                     usecols=[ts_col], na_values=["", "NA", "NAN"], engine="python")
    ts = pd.to_datetime(df[ts_col].astype("string").str.strip(),
                        format="%Y-%m-%d %H:%M:%S", errors="coerce")
    return ts.dropna().astype("datetime64[ns]").reset_index(drop=True)


def _read_toa5_time_record(datfile: Path) -> Tuple[pd.DataFrame, str, str]:
    with datfile.open("r", newline="") as f:
        r = csv.reader(f)
        _ = next(r, None); cols = next(r, None)
        _ = next(r, None); _ = next(r, None)
    if not cols:
        raise ValueError(f"{datfile.name}: missing columns")
    ts_col = _find_colname_ci(cols, "TIMESTAMP")
    rec_col = _find_colname_ci(cols, "RECORD")
    if ts_col is None or rec_col is None:
        raise ValueError(f"{datfile.name}: missing TIMESTAMP or RECORD")
    df = pd.read_csv(datfile, skiprows=4, header=None, names=cols,
                     usecols=[ts_col, rec_col], na_values=["", "NA", "NAN"], engine="python")
    ts = pd.to_datetime(df[ts_col].astype("string").str.strip(),
                        format="%Y-%m-%d %H:%M:%S", errors="coerce")
    rec = pd.to_numeric(df[rec_col].astype("string").str.strip(), errors="coerce")
    out = pd.DataFrame({"TIMESTAMP": ts, "RECORD": rec})
    out = out.dropna(subset=["TIMESTAMP", "RECORD"]).reset_index(drop=True)
    out["RECORD"] = out["RECORD"].astype("int64")
    return out, "TIMESTAMP", "RECORD"


@dataclass(frozen=True)
class DetectedEvent:
    logger: str
    year: int
    event_type: str
    event_time: pd.Timestamp
    gap_min: float
    normalized_delta_min: int
    cumulative_offset_min: int


def detect_events_for_year(
    logger_tag: str,
    year: int,
    ts_year: pd.Series,
    *, fwd_min_minutes: float, fwd_max_minutes: float,
       bwd_min_minutes: float, bwd_max_minutes: float,
       downtime_minutes: float, start_offset_min: int
) -> Tuple[List[DetectedEvent], Dict[str,int]]:
    events: List[DetectedEvent] = []
    stats = {"forward_setclock": 0, "backward_setclock": 0,
             "duplicate_timestamps": 0, "nonmonotonic_events": 0, "downtime_gaps": 0}
    if ts_year is None or ts_year.shape[0] < 3:
        return events, stats
    diffs = ts_year.diff().dropna()
    mins = diffs.dt.total_seconds() / 60.0
    offset = int(start_offset_min)
    for i, gap in mins.items():
        ii = int(i)
        cur_t = ts_year.iloc[ii]
        gap_min = float(gap)
        if gap_min == 0:
            stats["duplicate_timestamps"] += 1
            stats["nonmonotonic_events"] += 1
            events.append(DetectedEvent(logger=logger_tag, year=year,
                                        event_type="DUPLICATE", event_time=cur_t,
                                        gap_min=gap_min, normalized_delta_min=0,
                                        cumulative_offset_min=offset))
            continue
        if gap_min < 0:
            stats["nonmonotonic_events"] += 1
        if gap_min >= downtime_minutes:
            stats["downtime_gaps"] += 1
            events.append(DetectedEvent(logger=logger_tag, year=year,
                                        event_type="DOWNTIME", event_time=cur_t,
                                        gap_min=gap_min, normalized_delta_min=0,
                                        cumulative_offset_min=offset))
        if fwd_min_minutes <= gap_min <= fwd_max_minutes:
            stats["forward_setclock"] += 1
            offset += 60
            events.append(DetectedEvent(logger=logger_tag, year=year,
                                        event_type="FORWARD_SET_CLOCK", event_time=cur_t,
                                        gap_min=gap_min, normalized_delta_min=60,
                                        cumulative_offset_min=offset))
            continue
        if bwd_min_minutes <= gap_min <= bwd_max_minutes:
            stats["backward_setclock"] += 1
            offset -= 60
            events.append(DetectedEvent(logger=logger_tag, year=year,
                                        event_type="BACKWARD_SET_CLOCK", event_time=cur_t,
                                        gap_min=gap_min, normalized_delta_min=-60,
                                        cumulative_offset_min=offset))
            continue
        if gap_min < 0:
            events.append(DetectedEvent(logger=logger_tag, year=year,
                                        event_type="NONMONOTONIC", event_time=cur_t,
                                        gap_min=gap_min, normalized_delta_min=0,
                                        cumulative_offset_min=offset))
    return events, stats


def classify_logger_year(
    *, year: int, forward_setclock: int, backward_setclock: int,
       has_ff: bool, has_bb: bool
) -> Tuple[str, bool, str]:
    needs = False
    reasons: List[str] = []
    if year == 2023 and forward_setclock > 0:
        needs = True; reasons.append("forward_in_2023")
    if has_ff:
        needs = True; reasons.append("two_forwards_no_intervening_backward")
    if has_bb:
        needs = True; reasons.append("two_backwards_no_intervening_forward")
    cls = "needs_research" if needs else "likely_explained"
    return cls, needs, ";".join(reasons) if reasons else "ok"


def compute_has_ff_bb(events: List[DetectedEvent]) -> Tuple[bool,bool]:
    last_fwd = last_bwd = None
    has_ff = has_bb = False
    for idx, ev in enumerate(events):
        if ev.event_type == "FORWARD_SET_CLOCK":
            if last_fwd is not None:
                if not any(e.event_type=="BACKWARD_SET_CLOCK"
                           for e in events[last_fwd+1:idx]):
                    has_ff = True
            last_fwd = idx
        if ev.event_type == "BACKWARD_SET_CLOCK":
            if last_bwd is not None:
                if not any(e.event_type=="FORWARD_SET_CLOCK"
                           for e in events[last_bwd+1:idx]):
                    has_bb = True
            last_bwd = idx
    return has_ff, has_bb


def scan(
    years: List[int],
    *, fwd_min_minutes: float, fwd_max_minutes: float,
         bwd_min_minutes: float, bwd_max_minutes: float,
         downtime_minutes: float
) -> Tuple[pd.DataFrame,pd.DataFrame]:
    summary_rows: List[Dict[str,object]] = []
    timeline_rows: List[Dict[str,object]] = []

    for logger_tag in _logger_tags():
        offset = 0
        setclock_events: List[DetectedEvent] = []
        per_year_cache: Dict[int,Dict[str,object]] = {}

        for year in years:
            p = _dat_path(year, logger_tag)
            if not p.exists():
                per_year_cache[year] = {
                    "logger": logger_tag, "year": year,
                    "forward_setclock": 0, "backward_setclock": 0,
                    "nonmonotonic_events": 0, "duplicate_timestamps": 0, "downtime_gaps": 0,
                    "start_offset_min": offset, "end_offset_min": offset,
                    "start_mode": _mode_from_offset_minutes(offset),
                    "end_mode": _mode_from_offset_minutes(offset),
                    "first_forward_time": pd.NaT, "first_backward_time": pd.NaT,
                    "status": "missing"
                }
                continue
            try:
                # S3T deduplication
                if logger_tag == "S3T":
                    try:
                        df_tr, ts_col, rec_col = _read_toa5_time_record(p)
                        df_tr = _filter_to_year_window_df(df_tr, year, ts_col)
                        before = len(df_tr)
                        df_tr = df_tr.drop_duplicates(subset=[ts_col, rec_col], keep="first")
                        dropped = before - len(df_tr)
                        print(f"🧹 S3T {year}: dropped {dropped} duplicate (TIMESTAMP, RECORD) rows before event detection")
                        ts = df_tr[ts_col].reset_index(drop=True)
                    except Exception as e:
                        print(f"⚠️ S3T {year}: dedup skipped ({e}); using TIMESTAMP-only")
                        ts = _read_toa5_timestamps(p)
                        ts = filter_to_year_window_ts(ts, year)
                else:
                    ts = _read_toa5_timestamps(p)
                    ts = filter_to_year_window_ts(ts, year)

                events, stats = detect_events_for_year(
                    logger_tag, year, ts,
                    fwd_min_minutes=fwd_min_minutes,
                    fwd_max_minutes=fwd_max_minutes,
                    bwd_min_minutes=bwd_min_minutes,
                    bwd_max_minutes=bwd_max_minutes,
                    downtime_minutes=downtime_minutes,
                    start_offset_min=offset
                )

                for ev in events:
                    timeline_rows.append({
                        "logger": ev.logger, "year": ev.year,
                        "event_time": ev.event_time, "event_type": ev.event_type,
                        "gap_min": ev.gap_min,
                        "normalized_delta_min": ev.normalized_delta_min,
                        "cumulative_offset_min": ev.cumulative_offset_min,
                        "implied_mode": _mode_from_offset_minutes(ev.cumulative_offset_min)
                    })

                if events:
                    offset = int(events[-1].cumulative_offset_min)

                setclock_events.extend(
                    e for e in events
                    if e.event_type in ("FORWARD_SET_CLOCK", "BACKWARD_SET_CLOCK")
                )

                first_fwd = next((e.event_time for e in events if e.event_type=="FORWARD_SET_CLOCK"), pd.NaT)
                first_bwd = next((e.event_time for e in events if e.event_type=="BACKWARD_SET_CLOCK"), pd.NaT)

                per_year_cache[year] = {
                    "logger": logger_tag, "year": year,
                    "forward_setclock": stats["forward_setclock"],
                    "backward_setclock": stats["backward_setclock"],
                    "nonmonotonic_events": stats["nonmonotonic_events"],
                    "duplicate_timestamps": stats["duplicate_timestamps"],
                    "downtime_gaps": stats["downtime_gaps"],
                    "start_offset_min": None,
                    "end_offset_min": offset,
                    "start_mode": None,
                    "end_mode": _mode_from_offset_minutes(offset),
                    "first_forward_time": first_fwd,
                    "first_backward_time": first_bwd,
                    "status": "ok"
                }
            except Exception as e:
                per_year_cache[year] = {
                    "logger": logger_tag, "year": year,
                    "forward_setclock": 0, "backward_setclock": 0,
                    "nonmonotonic_events": 0, "duplicate_timestamps": 0, "downtime_gaps": 0,
                    "start_offset_min": offset, "end_offset_min": offset,
                    "start_mode": _mode_from_offset_minutes(offset),
                    "end_mode": _mode_from_offset_minutes(offset),
                    "first_forward_time": pd.NaT,
                    "first_backward_time": pd.NaT,
                    "status": f"error: {e}"
                }

        has_ff, has_bb = compute_has_ff_bb(setclock_events)
        running_offset = 0
        for year in years:
            row = per_year_cache[year].copy()
            row["start_offset_min"] = running_offset
            row["start_mode"] = _mode_from_offset_minutes(running_offset)
            end_offset = int(row["end_offset_min"])
            running_offset = end_offset
            classification, needs_research, reasons = classify_logger_year(
                year=year,
                forward_setclock=int(row["forward_setclock"]),
                backward_setclock=int(row["backward_setclock"]),
                has_ff=has_ff, has_bb=has_bb
            )
            row.update({
                "has_FF": has_ff,
                "has_BB": has_bb,
                "classification": classification,
                "needs_research": needs_research,
                "reasons": reasons
            })
            summary_rows.append(row)

        summary_rows.append({
            "logger": logger_tag, "year": "ALL",
            "forward_setclock": sum(int(per_year_cache[y]["forward_setclock"]) for y in years),
            "backward_setclock": sum(int(per_year_cache[y]["backward_setclock"]) for y in years),
            "nonmonotonic_events": sum(int(per_year_cache[y]["nonmonotonic_events"]) for y in years),
            "duplicate_timestamps": sum(int(per_year_cache[y]["duplicate_timestamps"]) for y in years),
            "downtime_gaps": sum(int(per_year_cache[y]["downtime_gaps"]) for y in years),
            "start_offset_min": 0,
            "end_offset_min": running_offset,
            "start_mode": "MDT",
            "end_mode": _mode_from_offset_minutes(running_offset),
            "first_forward_time": pd.NaT, "first_backward_time": pd.NaT,
            "has_FF": has_ff, "has_BB": has_bb,
            "classification": "needs_research" if (has_ff or has_bb) else "likely_explained",
            "needs_research": bool(has_ff or has_bb),
            "reasons": ";".join([r for r in ["has_FF","has_BB"] if r]),
            "status": "ok"
        })

    summary_df = pd.DataFrame(summary_rows)
    timeline_df = pd.DataFrame(timeline_rows)

    summary_cols = [
        "logger","year","start_offset_min","start_mode",
        "forward_setclock","backward_setclock",
        "end_offset_min","end_mode",
        "first_forward_time","first_backward_time",
        "nonmonotonic_events","duplicate_timestamps","downtime_gaps",
        "has_FF","has_BB","classification","needs_research","reasons","status"
    ]
    for c in summary_cols:
        if c not in summary_df.columns:
            summary_df[c] = None
    summary_df = summary_df[summary_cols]

    if not timeline_df.empty:
        timeline_df = timeline_df.sort_values(
            ["logger","year","event_time","event_type"]
        ).reset_index(drop=True)
        timeline_cols = [
            "logger","year","event_time","event_type","gap_min",
            "normalized_delta_min","cumulative_offset_min","implied_mode"
        ]
        for c in timeline_cols:
            if c not in timeline_df.columns:
                timeline_df[c] = None
        timeline_df = timeline_df[timeline_cols]

    return summary_df, timeline_df


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Clock-state tracker for TOA5 .dat files (chronological)."
    )
    ap.add_argument("--years", type=int, nargs="+", required=True,
                    help="Years to scan, e.g. 2023 2024 2025 2026.")
    ap.add_argument("--fwd-min", type=float, default=65.0,
                    help="Min minutes for forward setclock (default 65).")
    ap.add_argument("--fwd-max", type=float, default=95.0,
                    help="Max minutes for forward setclock (default 95).")
    ap.add_argument("--bwd-min", type=float, default=-95.0,
                    help="Min minutes for backward setclock (default -95).")
    ap.add_argument("--bwd-max", type=float, default=-35.0,
                    help="Max minutes for backward setclock (default -35).")
    ap.add_argument("--downtime-min", type=float, default=120.0,
                    help="Downtime gap (minutes) threshold (default 120).")
    ap.add_argument("--write-csv", action="store_true",
                    help="Write summary and timeline CSVs to diagnostics/reports/.")

    args = ap.parse_args()
    years = [int(y) for y in args.years]

    summary_df, timeline_df = scan(
        years,
        fwd_min_minutes=args.fwd_min, fwd_max_minutes=args.fwd_max,
        bwd_min_minutes=args.bwd_min, bwd_max_minutes=args.bwd_max,
        downtime_minutes=args.downtime_min
    )

    print("\n=== Clock STATE summary (raw .dat) ===")
    show = summary_df[summary_df["year"].isin(years)]
    cols = ["logger","year","start_mode","end_mode",
            "forward_setclock","backward_setclock","has_FF","has_BB",
            "classification","reasons","status"]
    print(show[cols].to_string(index=False))

    if args.write_csv:
        reports_dir = Path(__file__).parent / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        stamp = date.today().isoformat()
        summary_df.to_csv(reports_dir / f"clock_state_summary_{stamp}.csv", index=False)
        timeline_df.to_csv(reports_dir / f"clock_state_timeline_{stamp}.csv", index=False)
        print(f"\n✅ Wrote summary : {reports_dir/f'clock_state_summary_{stamp}.csv'}")
        print(f"✅ Wrote timeline: {reports_dir/f'clock_state_timeline_{stamp}.csv'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
