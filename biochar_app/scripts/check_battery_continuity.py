#!/usr/bin/env python3
"""
check_battery_continuity.py

Scan 15-min logger parquet files and evaluate whether each logger's
battery-voltage column has a continuous record since START_DATE.

Folder layout expected:
  biochar_app/data-processed/parquet/summary/15min/{year}_15min.parquet
"""

from __future__ import annotations

import pandas as pd
from biochar_app.config.paths import PARQUET_SUMMARY_15MIN_DIR


START_DATE = pd.Timestamp("2023-10-10")
MAX_GAP = pd.Timedelta(hours=2)
BATT_THRESHOLD_V = 11.5   # ← updated threshold


def _load_year(path) -> pd.DataFrame | None:
    df = pd.read_parquet(path)

    # Allow timestamp stored in the index
    if "timestamp" not in df.columns and "TIMESTAMP" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index": "timestamp"})

    # Normalize to 'timestamp'
    if "timestamp" in df.columns:
        ts_col = "timestamp"
    elif "TIMESTAMP" in df.columns:
        ts_col = "TIMESTAMP"
    else:
        return None

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    return df


def _battery_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if "batt" in c.lower()]


def _continuity(ts: pd.Series) -> tuple[bool, pd.Timedelta | None]:
    if len(ts) < 2:
        return False, None
    deltas = ts.diff().dropna()
    if deltas.empty:
        return False, None
    max_gap = deltas.max()
    return bool(max_gap <= MAX_GAP), max_gap


def main() -> int:
    if not PARQUET_SUMMARY_15MIN_DIR.exists():
        print(f"[ERROR] 15-min parquet directory not found: {PARQUET_SUMMARY_15MIN_DIR}")
        return 2

    year_files = sorted(PARQUET_SUMMARY_15MIN_DIR.glob("*_15min.parquet"))
    if not year_files:
        print(f"[ERROR] No *_15min.parquet files found in: {PARQUET_SUMMARY_15MIN_DIR}")
        return 2

    results: list[dict] = []

    for year_file in year_files:
        year = year_file.stem.split("_")[0]
        print(f"[INFO] Processing {year_file.name}...")

        df = _load_year(year_file)
        if df is None or df.empty:
            print(f"  [WARN] No usable timestamp column in {year_file.name}")
            continue

        df = df[df["timestamp"] >= START_DATE].copy()
        if df.empty:
            print(f"  [WARN] No rows on/after {START_DATE.date()} in {year_file.name}")
            continue

        batt_cols = _battery_cols(df)
        if not batt_cols:
            print("  [WARN] No battery columns found.")
            continue

        for batt_col in batt_cols:
            s = pd.to_numeric(df[batt_col], errors="coerce")
            n_total = len(s)
            n_non_na = int(s.notna().sum())

            if n_non_na == 0:
                results.append(
                    dict(
                        year=int(year),
                        battery_column=batt_col,
                        n_rows_since_start=int(n_total),
                        n_non_na=0,
                        pct_missing=100.0,
                        min_batt_since_start=None,
                        any_below_11_5V=None,
                        continuous_record=False,
                        max_gap_non_na=None,
                    )
                )
                continue

            min_batt = float(s.min(skipna=True))
            any_below_11_5 = bool((s < BATT_THRESHOLD_V).any())
            pct_missing = float(100.0 * (1.0 - (n_non_na / n_total)))

            ts_non_na = df.loc[s.notna(), "timestamp"].sort_values()
            continuous, max_gap = _continuity(ts_non_na)

            results.append(
                dict(
                    year=int(year),
                    battery_column=batt_col,
                    n_rows_since_start=int(n_total),
                    n_non_na=n_non_na,
                    pct_missing=round(pct_missing, 2),
                    min_batt_since_start=round(min_batt, 3),
                    any_below_11_5V=any_below_11_5,
                    continuous_record=bool(continuous),
                    max_gap_non_na=str(max_gap) if max_gap is not None else None,
                )
            )

    summary = pd.DataFrame(results)
    if summary.empty:
        print("[WARN] No results produced.")
        return 0

    print("\n================ SUMMARY ================\n")
    print(summary.sort_values(["year", "battery_column"]).to_string(index=False))

    print("\n============ LOGGERS MEETING REQUIREMENT ============\n")
    good = summary[
        (summary["any_below_11_5V"] == False) &
        (summary["continuous_record"] == True)
    ]
    if good.empty:
        print("None.")
    else:
        print(good.sort_values(["year", "battery_column"]).to_string(index=False))

    print("\n============ LOGGERS FAILING REQUIREMENT ============\n")
    bad = summary[
        (summary["any_below_11_5V"] == True) |
        (summary["continuous_record"] == False)
    ]
    if bad.empty:
        print("None.")
    else:
        print(f"Count failing: {len(bad)} / {len(summary)}")
        print(bad.sort_values(["year", "battery_column"]).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())