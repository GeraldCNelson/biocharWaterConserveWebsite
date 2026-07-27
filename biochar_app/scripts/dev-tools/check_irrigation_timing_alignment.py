#!/usr/bin/env python3
"""
Check logger VWC response timing relative to irrigation start times.

Run:
    python biochar_app/scripts/dev-tools/check_irrigation_timing_alignment.py
"""

from pathlib import Path
import pandas as pd

from biochar_app.scripts.data_loading import load_irrigation_data

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = PROJECT_ROOT / "data-processed" / "parquet"
OUTPUT_DIR = PROJECT_ROOT / "data-processed" / "irrigation-analysis"

YEARS = [2023, 2024, 2025, 2026]
STRIPS = ["S1", "S2", "S3", "S4"]
LOGGER_POSITIONS = ["T", "M", "B"]
DEPTHS = [1, 2, 3]

WINDOW_HOURS_BEFORE = 36
WINDOW_HOURS_AFTER = 72
MIN_POSITIVE_JUMP_PCT = 2.0

def load_logger_year(year: int) -> pd.DataFrame:
    path = PARQUET_DIR / str(year) / f"{year}_raw_logger.parquet"

    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_parquet(path)

    if "timestamp" not in df.columns:
        raise KeyError(f"{path} has no timestamp column")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()
    df = df.sort_values("timestamp").set_index("timestamp")

    return df

def first_positive_jump(
    series: pd.Series,
    irrigation_start: pd.Timestamp,
) -> dict[str, object]:
    s = pd.to_numeric(series, errors="coerce").dropna()

    if s.empty:
        return {
            "first_jump_time": pd.NaT,
            "first_jump_value": pd.NA,
            "hours_after_irrigation_start": pd.NA,
            "max_jump_time": pd.NaT,
            "max_jump_value": pd.NA,
            "baseline_vwc_at_start": pd.NA,
        }

    diffs = s.diff()

    positive_jumps = diffs[diffs >= MIN_POSITIVE_JUMP_PCT]

    first_jump_time = pd.NaT
    first_jump_value = pd.NA
    hours_after_start = pd.NA

    if not positive_jumps.empty:
        first_jump_time = positive_jumps.index[0]
        first_jump_value = float(positive_jumps.iloc[0])
        hours_after_start = (
            first_jump_time - irrigation_start
        ).total_seconds() / 3600.0

    max_jump_time = pd.NaT
    max_jump_value = pd.NA

    if not diffs.dropna().empty:
        max_jump_time = diffs.idxmax()
        max_jump_value = float(diffs.loc[max_jump_time])

    baseline_vwc_at_start = pd.NA
    before_start = s.loc[s.index <= irrigation_start]
    if not before_start.empty:
        baseline_vwc_at_start = float(before_start.iloc[-1])

    return {
        "first_jump_time": first_jump_time,
        "first_jump_value": first_jump_value,
        "hours_after_irrigation_start": hours_after_start,
        "max_jump_time": max_jump_time,
        "max_jump_value": max_jump_value,
        "baseline_vwc_at_start": baseline_vwc_at_start,
    }

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    irrigation = load_irrigation_data()

    required_cols = {
        "year",
        "strip",
        "start_timestamp",
        "end_timestamp",
    }

    missing = required_cols - set(irrigation.columns)
    if missing:
        raise KeyError(
            f"Irrigation data missing required columns: {sorted(missing)}"
        )

    irrigation["start_timestamp"] = pd.to_datetime(
        irrigation["start_timestamp"],
        errors="coerce",
    )
    irrigation["end_timestamp"] = pd.to_datetime(
        irrigation["end_timestamp"],
        errors="coerce",
    )

    rows: list[dict[str, object]] = []

    for year in YEARS:
        print(f"Checking {year}...")

        df = load_logger_year(year)

        events = irrigation[
            (irrigation["year"] == year)
            & irrigation["strip"].isin(STRIPS)
        ].copy()

        if events.empty:
            continue

        for _, event in events.iterrows():
            irrigation_start = event["start_timestamp"]

            if pd.isna(irrigation_start):
                continue

            irrigation_start = pd.Timestamp(irrigation_start)

            window_start = irrigation_start - pd.Timedelta(
                hours=WINDOW_HOURS_BEFORE
            )
            window_end = irrigation_start + pd.Timedelta(
                hours=WINDOW_HOURS_AFTER
            )

            event_id = event.get("event_id", pd.NA)
            strip = str(event["strip"])

            for logger_position in LOGGER_POSITIONS:
                for depth in DEPTHS:
                    col = f"VWC_{depth}_raw_{strip}_{logger_position}"

                    if col not in df.columns:
                        continue

                    window = df.loc[window_start:window_end, col]

                    stats = first_positive_jump(
                        series=window,
                        irrigation_start=irrigation_start,
                    )

                    rows.append(
                        {
                            "year": year,
                            "strip": strip,
                            "logger_position": logger_position,
                            "depth": depth,
                            "sensor_col": col,
                            "event_id": event_id,
                            "irrigation_start": irrigation_start,
                            "irrigation_end": event.get("end_timestamp", pd.NaT),
                            "gallons_strip": event.get("gallons_strip", pd.NA),
                            **stats,
                        }
                    )

    out = pd.DataFrame(rows)

    if out.empty:
        print("No diagnostic rows created.")
        return

    out["early_response_flag"] = (
        pd.to_numeric(
            out["hours_after_irrigation_start"],
            errors="coerce",
        )
        < -1.0
    )

    out["very_late_response_flag"] = (
        pd.to_numeric(
            out["hours_after_irrigation_start"],
            errors="coerce",
        )
        > 24.0
    )

    out_path = OUTPUT_DIR / "irrigation_logger_timing_alignment_check.csv"
    out.to_csv(out_path, index=False)

    print(f"\nWrote timing diagnostic file:")
    print(out_path)

    print("\nEarly response counts by year / strip / logger:")
    summary = (
        out.groupby(["year", "strip", "logger_position"], dropna=False)
        .agg(
            n_rows=("sensor_col", "size"),
            n_early=("early_response_flag", "sum"),
            median_hours_after_start=(
                "hours_after_irrigation_start",
                "median",
            ),
        )
        .reset_index()
    )

    print(summary.to_string(index=False))

if __name__ == "__main__":
    main()