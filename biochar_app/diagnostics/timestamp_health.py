#!/usr/bin/env python3
"""
timestamp_health.py

Timestamp continuity diagnostics for Biochar parquet datasets.

What it checks
--------------
For each {year}_15min.parquet in:
  biochar_app/data-processed/parquet/summary/15min/

It computes:
- duplicate timestamps (delta == 0)
- gaps bigger than expected cadence (delta > 15 minutes)
- "odd" deltas that are NOT a multiple of 15 minutes
- max gap
- expected vs actual row count on a perfect 15-min grid (for the ALL-series)

It also computes the same continuity metrics PER LOGGER using the battery columns
(e.g., BattV_Min_S4_B) by looking at timestamps where that battery column is non-NA.
That makes it easy to spot collection/telemetry gaps per logger.

Outputs
-------
1) Console summary (worst offenders)
2) CSV written to:
   biochar_app/data-processed/parquet/summary/timestamp_health_report.csv
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from biochar_app.config.paths import PARQUET_SUMMARY_15MIN_DIR, DATA_PROCESSED_DIR


EXPECTED_CADENCE = pd.Timedelta(minutes=15)
GAP_WARN = pd.Timedelta(hours=2)  # "big gap" threshold for highlighting (still report all >15m)
REPORT_CSV = DATA_PROCESSED_DIR / "parquet" / "summary" / "timestamp_health_report.csv"


# ----------------------------
# Helpers
# ----------------------------

def _infer_timestamp_column(df: pd.DataFrame) -> Optional[str]:
    """
    Returns the name of a timestamp column if present; else if DatetimeIndex exists,
    returns "__index__" to indicate index should be used.
    """
    if "timestamp" in df.columns:
        return "timestamp"
    if "TIMESTAMP" in df.columns:
        return "TIMESTAMP"
    if isinstance(df.index, pd.DatetimeIndex):
        return "__index__"
    return None


def _load_with_timestamp(path: Path) -> Optional[pd.DataFrame]:
    df = pd.read_parquet(path)

    ts_col = _infer_timestamp_column(df)
    if ts_col is None:
        return None

    if ts_col == "__index__":
        df = df.reset_index().rename(columns={"index": "timestamp"})
        ts_col = "timestamp"

    # normalize to df["timestamp"]
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    return df


def _battery_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if "batt" in c.lower()]


def _logger_from_batt_col(col: str) -> str:
    """
    Extract logger id like S4_B from a column like BattV_Min_S4_B.
    Falls back to the full column name if no match.
    """
    m = re.search(r"(S[1-4]_[BMT])", col)
    return m.group(1) if m else col


@dataclass(frozen=True)
class ContinuityStats:
    n_points: int
    start: Optional[pd.Timestamp]
    end: Optional[pd.Timestamp]
    n_duplicates: int
    n_gaps_gt_15m: int
    n_odd_deltas: int
    max_gap: Optional[pd.Timedelta]
    expected_grid_points: Optional[int]
    missing_from_grid: Optional[int]
    pct_missing_from_grid: Optional[float]


def _continuity_from_timestamps(ts: pd.Series, compute_grid: bool) -> ContinuityStats:
    """
    ts: timestamps (may contain duplicates). Must be datetime-like.
    compute_grid: if True, compute expected count on a perfect 15-min grid
                  between start and end (inclusive), and compare to actual.
    """
    if ts is None or len(ts) == 0:
        return ContinuityStats(
            n_points=0,
            start=None,
            end=None,
            n_duplicates=0,
            n_gaps_gt_15m=0,
            n_odd_deltas=0,
            max_gap=None,
            expected_grid_points=None,
            missing_from_grid=None,
            pct_missing_from_grid=None,
        )

    t = pd.to_datetime(ts, errors="coerce").dropna().sort_values()
    if t.empty:
        return ContinuityStats(
            n_points=0,
            start=None,
            end=None,
            n_duplicates=0,
            n_gaps_gt_15m=0,
            n_odd_deltas=0,
            max_gap=None,
            expected_grid_points=None,
            missing_from_grid=None,
            pct_missing_from_grid=None,
        )

    deltas = t.diff().dropna()

    # duplicates show up as delta == 0
    n_dup = int((deltas == pd.Timedelta(0)).sum())

    # gaps beyond expected cadence
    n_gaps = int((deltas > EXPECTED_CADENCE).sum())

    # odd deltas: not a clean multiple of 15 minutes (ignore zero)
    # Use integer minutes to be robust.
    delta_mins = (deltas / pd.Timedelta(minutes=1)).astype("float64")
    odd_mask = (deltas != pd.Timedelta(0)) & ((delta_mins % 15) != 0)
    n_odd = int(odd_mask.sum())

    max_gap = deltas.max() if not deltas.empty else None

    start = t.iloc[0]
    end = t.iloc[-1]

    expected_grid_points = None
    missing_from_grid = None
    pct_missing_from_grid = None

    if compute_grid and start is not None and end is not None and start <= end:
        # inclusive grid: start..end stepping 15 minutes
        expected_grid_points = int(((end - start) / EXPECTED_CADENCE)) + 1
        actual_points = int(len(t))
        missing_from_grid = max(0, expected_grid_points - actual_points)
        pct_missing_from_grid = (
            100.0 * (missing_from_grid / expected_grid_points)
            if expected_grid_points > 0
            else None
        )

    return ContinuityStats(
        n_points=int(len(t)),
        start=start,
        end=end,
        n_duplicates=n_dup,
        n_gaps_gt_15m=n_gaps,
        n_odd_deltas=n_odd,
        max_gap=max_gap,
        expected_grid_points=expected_grid_points,
        missing_from_grid=missing_from_grid,
        pct_missing_from_grid=pct_missing_from_grid,
    )


def main() -> int:
    if not PARQUET_SUMMARY_15MIN_DIR.exists():
        print(f"[ERROR] 15-min parquet directory not found: {PARQUET_SUMMARY_15MIN_DIR}")
        return 2

    year_files = sorted(PARQUET_SUMMARY_15MIN_DIR.glob("*_15min.parquet"))
    if not year_files:
        print(f"[ERROR] No *_15min.parquet files found in: {PARQUET_SUMMARY_15MIN_DIR}")
        return 2

    rows: list[dict] = []

    for f in year_files:
        year = int(f.stem.split("_")[0])
        print(f"[INFO] Processing {f.name}...")

        df = _load_with_timestamp(f)
        if df is None or df.empty:
            print(f"  [WARN] No usable timestamp column in {f.name}")
            continue

        # --- ALL timestamps (whole dataframe) ---
        all_stats = _continuity_from_timestamps(df["timestamp"], compute_grid=True)
        rows.append(
            dict(
                year=year,
                scope="ALL",
                logger="ALL",
                source_column="timestamp",
                n_points=all_stats.n_points,
                start=str(all_stats.start) if all_stats.start is not None else None,
                end=str(all_stats.end) if all_stats.end is not None else None,
                n_duplicates=all_stats.n_duplicates,
                n_gaps_gt_15m=all_stats.n_gaps_gt_15m,
                n_odd_deltas=all_stats.n_odd_deltas,
                max_gap=str(all_stats.max_gap) if all_stats.max_gap is not None else None,
                expected_grid_points=all_stats.expected_grid_points,
                missing_from_grid=all_stats.missing_from_grid,
                pct_missing_from_grid=round(all_stats.pct_missing_from_grid, 3)
                if all_stats.pct_missing_from_grid is not None
                else None,
            )
        )

        # --- Per logger, using battery columns as the "data present" mask ---
        batt_cols = _battery_cols(df)
        if not batt_cols:
            print("  [WARN] No battery columns found; skipping per-logger continuity.")
            continue

        for bc in batt_cols:
            s = pd.to_numeric(df[bc], errors="coerce")
            ts_non_na = df.loc[s.notna(), "timestamp"]
            st = _continuity_from_timestamps(ts_non_na, compute_grid=False)

            rows.append(
                dict(
                    year=year,
                    scope="LOGGER",
                    logger=_logger_from_batt_col(bc),
                    source_column=bc,
                    n_points=st.n_points,
                    start=str(st.start) if st.start is not None else None,
                    end=str(st.end) if st.end is not None else None,
                    n_duplicates=st.n_duplicates,
                    n_gaps_gt_15m=st.n_gaps_gt_15m,
                    n_odd_deltas=st.n_odd_deltas,
                    max_gap=str(st.max_gap) if st.max_gap is not None else None,
                    expected_grid_points=None,
                    missing_from_grid=None,
                    pct_missing_from_grid=None,
                )
            )

    report = pd.DataFrame(rows)
    if report.empty:
        print("[WARN] No results produced.")
        return 0

    # Write CSV
    REPORT_CSV.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(REPORT_CSV, index=False)
    print(f"\n[INFO] Full report written to: {REPORT_CSV}")

    # Console summary
    print("\n================ TIMESTAMP HEALTH (ALL) ================\n")
    all_df = report[report["scope"] == "ALL"].copy()
    if all_df.empty:
        print("None.")
    else:
        # Show key columns
        show_cols = [
            "year",
            "n_points",
            "n_duplicates",
            "n_gaps_gt_15m",
            "n_odd_deltas",
            "max_gap",
            "pct_missing_from_grid",
        ]
        print(all_df.sort_values(["year"]).loc[:, show_cols].to_string(index=False))

    print("\n================ WORST LOGGER OFFENDERS ================\n")
    log_df = report[report["scope"] == "LOGGER"].copy()
    if log_df.empty:
        print("None.")
        return 0

    # Flag "big gap" offenders for quick visibility
    def _to_timedelta(x):
        try:
            return pd.to_timedelta(x)
        except Exception:
            return pd.NaT

    log_df["max_gap_td"] = log_df["max_gap"].apply(_to_timedelta)

    worst_gap = log_df.sort_values(["max_gap_td"], ascending=False).head(12)
    print("=== Largest max_gap (per logger) ===")
    print(
        worst_gap.loc[:, ["year", "logger", "source_column", "n_points", "n_gaps_gt_15m", "n_odd_deltas", "max_gap"]]
        .to_string(index=False)
    )

    big_gap = log_df[log_df["max_gap_td"].notna() & (log_df["max_gap_td"] >= GAP_WARN)].copy()
    print("\n=== Loggers with max_gap >= 2 hours ===")
    if big_gap.empty:
        print("None.")
    else:
        print(
            big_gap.sort_values(["year", "logger"]).loc[
                :, ["year", "logger", "source_column", "max_gap", "n_gaps_gt_15m", "n_odd_deltas"]
            ].to_string(index=False)
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())