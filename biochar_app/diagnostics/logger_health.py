#!/usr/bin/env python3
"""
logger_health.py

Reusable health diagnostics for Biochar logger parquet files.
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from biochar_app.config.paths import PARQUET_SUMMARY_15MIN_DIR


DEFAULT_START_DATE = pd.Timestamp("2023-10-10")
DEFAULT_MAX_GAP = pd.Timedelta(hours=2)
DEFAULT_VOLTAGE_THRESHOLD = 11.5


def _load_parquet(path: Path) -> pd.DataFrame | None:
    df = pd.read_parquet(path)

    if "timestamp" not in df.columns and isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index().rename(columns={"index": "timestamp"})

    if "timestamp" not in df.columns:
        return None

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    return df


def _battery_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if "batt" in c.lower()]


def _continuity(ts: pd.Series, max_gap: pd.Timedelta):
    if len(ts) < 2:
        return False, None
    deltas = ts.diff().dropna()
    if deltas.empty:
        return False, None
    max_observed_gap = deltas.max()
    return bool(max_observed_gap <= max_gap), max_observed_gap


def evaluate_logger_health(
    start_date: pd.Timestamp = DEFAULT_START_DATE,
    voltage_threshold: float = DEFAULT_VOLTAGE_THRESHOLD,
    max_gap: pd.Timedelta = DEFAULT_MAX_GAP,
) -> pd.DataFrame:

    if not PARQUET_SUMMARY_15MIN_DIR.exists():
        raise FileNotFoundError(f"Missing directory: {PARQUET_SUMMARY_15MIN_DIR}")

    results = []

    for year_file in sorted(PARQUET_SUMMARY_15MIN_DIR.glob("*_15min.parquet")):
        year = int(year_file.stem.split("_")[0])
        df = _load_parquet(year_file)

        if df is None or df.empty:
            continue

        df = df[df["timestamp"] >= start_date]
        if df.empty:
            continue

        for col in _battery_columns(df):
            s = pd.to_numeric(df[col], errors="coerce")
            n_total = len(s)
            n_non_na = int(s.notna().sum())

            if n_non_na == 0:
                continue

            min_v = float(s.min(skipna=True))
            any_below = bool((s < voltage_threshold).any())
            pct_missing = float(100 * (1 - n_non_na / n_total))

            ts_non_na = df.loc[s.notna(), "timestamp"].sort_values()
            continuous, max_observed_gap = _continuity(ts_non_na, max_gap)

            results.append(
                dict(
                    year=year,
                    logger=col.replace("BattV_Min_", ""),
                    battery_column=col,
                    min_voltage=round(min_v, 3),
                    any_below_threshold=any_below,
                    pct_missing=round(pct_missing, 2),
                    continuous_record=continuous,
                    max_gap=str(max_observed_gap),
                )
            )

    health = pd.DataFrame(results)

    if health.empty:
        return health

    health["status"] = "GOOD"
    health.loc[health["any_below_threshold"] == True, "status"] = "LOW_VOLTAGE"
    health.loc[health["continuous_record"] == False, "status"] = "GAP"

    return health.sort_values(["year", "logger"])


def print_health_summary(df: pd.DataFrame):
    if df.empty:
        print("[WARN] No health data produced.")
        return

    print("\n================ LOGGER HEALTH SUMMARY ================\n")
    print(df.to_string(index=False))

    failures = df[df["status"] != "GOOD"]

    print("\n============ LOGGERS WITH WARNINGS / FAILURES ============\n")

    if failures.empty:
        print("All loggers classified as GOOD.\n")
        return

    print(failures.to_string(index=False))
    print(f"\nTotal failing/warning loggers: {len(failures)} / {len(df)}\n")


def run_health_check(write_csv: bool = True):
    df = evaluate_logger_health()
    print_health_summary(df)

    if write_csv:
        out_path = PARQUET_SUMMARY_15MIN_DIR.parent / "logger_health_report.csv"
        df.to_csv(out_path, index=False)
        print(f"[INFO] Full report written to: {out_path.resolve()}")

if __name__ == "__main__":
    # Run defaults; write a CSV by default so you always get an artifact.
    # Flip to False if you only want console output.
    run_health_check(write_csv=True)