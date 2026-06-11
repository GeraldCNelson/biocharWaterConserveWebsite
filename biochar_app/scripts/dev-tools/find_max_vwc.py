#!/usr/bin/env python3
"""
find_max_vwc.py

Search all logger parquet files and report maximum VWC values plus
basic QA indicators for suspicious VWC behavior.

Run:
    python biochar_app/scripts/dev-tools/find_max_vwc.py
"""

from pathlib import Path
from typing import Any, cast

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = PROJECT_ROOT / "data-processed" / "parquet"

YEARS = [2023, 2024, 2025, 2026]

HIGH_VWC_1 = 50.0
HIGH_VWC_2 = 55.0
LOW_VWC = 1.0
LARGE_JUMP = 10.0
OBS_PER_HOUR = 4.0  # 15-minute data


def get_vwc_columns(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c.startswith("VWC_") and "_raw_" in c
    ]


def count_bad_episodes(mask: pd.Series) -> int:
    if mask.empty:
        return 0

    starts = mask & ~mask.shift(fill_value=False)
    return int(starts.sum())


def episode_stats(
    values: pd.Series,
    threshold: float,
) -> tuple[int, float]:
    """
    Return the longest continuous episode above threshold.

    Returns:
        longest_episode_points,
        longest_episode_hours
    """
    mask = values > threshold

    if not mask.any():
        return 0, 0.0

    group_id = (mask != mask.shift(fill_value=False)).cumsum()
    longest_points = 0

    for _, group in mask.groupby(group_id):
        if bool(group.iloc[0]):
            longest_points = max(longest_points, len(group))

    longest_hours = longest_points / OBS_PER_HOUR
    return longest_points, longest_hours


def get_episode_details(
    sub: pd.DataFrame,
    threshold: float,
) -> list[dict[str, Any]]:
    """
    Return detailed contiguous episodes where value > threshold.

    Expected columns:
        timestamp, value
    """
    if sub.empty:
        return []

    work = sub.sort_values("timestamp").copy()
    mask = work["value"] > threshold

    if not mask.any():
        return []

    group_id = (mask != mask.shift(fill_value=False)).cumsum()
    episodes: list[dict[str, Any]] = []

    for _, group in work.groupby(group_id):
        if group.empty:
            continue

        first_value = bool(cast(Any, (group["value"] > threshold).iloc[0]))
        if not first_value:
            continue

        max_idx = group["value"].idxmax()
        max_row = group.loc[max_idx]

        start_ts = pd.Timestamp(cast(Any, group["timestamp"].iloc[0]))
        end_ts = pd.Timestamp(cast(Any, group["timestamp"].iloc[-1]))
        max_ts = pd.Timestamp(cast(Any, max_row["timestamp"]))
        max_value = float(cast(Any, max_row["value"]))
        n_points = int(len(group))
        duration_hours = n_points / OBS_PER_HOUR

        episodes.append(
            {
                "start": start_ts,
                "end": end_ts,
                "duration_hours": duration_hours,
                "n_points": n_points,
                "max_timestamp": max_ts,
                "max_value": max_value,
            }
        )

    return episodes


def summarize_sensor_qa(all_values: pd.DataFrame) -> None:
    print("\nVWC QA summary by sensor")
    print("-" * 160)

    qa_rows = []

    for column, sub in all_values.groupby("column"):
        sub = sub.sort_values("timestamp").copy()
        sub["delta"] = sub["value"].diff().abs()

        high_50 = sub["value"] > HIGH_VWC_1
        high_55 = sub["value"] > HIGH_VWC_2
        low_1 = sub["value"] < LOW_VWC
        large_jump = sub["delta"] > LARGE_JUMP

        longest_pts_50, longest_hrs_50 = episode_stats(
            sub["value"],
            HIGH_VWC_1,
        )
        longest_pts_55, longest_hrs_55 = episode_stats(
            sub["value"],
            HIGH_VWC_2,
        )

        max_idx = sub["value"].idxmax()
        max_row = sub.loc[max_idx]

        qa_rows.append(
            {
                "column": column,
                "n_obs": len(sub),
                "max_vwc": round(float(cast(Any, max_row["value"])), 2),
                "max_timestamp": pd.Timestamp(
                    cast(Any, max_row["timestamp"])
                ).strftime("%Y-%m-%d %H:%M"),
                "count_gt_50": int(high_50.sum()),
                "count_gt_55": int(high_55.sum()),
                "count_lt_1": int(low_1.sum()),
                "count_large_jump": int(large_jump.sum()),
                "episodes_gt_50": count_bad_episodes(high_50),
                "episodes_gt_55": count_bad_episodes(high_55),
                "episodes_lt_1": count_bad_episodes(low_1),
                "longest_pts_gt_50": longest_pts_50,
                "longest_hrs_gt_50": round(longest_hrs_50, 2),
                "longest_pts_gt_55": longest_pts_55,
                "longest_hrs_gt_55": round(longest_hrs_55, 2),
            }
        )

    qa_df = pd.DataFrame(qa_rows)

    qa_df = qa_df.sort_values(
        by=[
            "count_gt_55",
            "count_gt_50",
            "longest_hrs_gt_50",
            "count_lt_1",
            "count_large_jump",
            "max_vwc",
        ],
        ascending=False,
    )

    print(qa_df.to_string(index=False))


def summarize_high_values_by_hour(all_values: pd.DataFrame) -> None:
    print("\nHigh VWC observations by hour of day")
    print("-" * 80)

    high = all_values[all_values["value"] > HIGH_VWC_1].copy()

    if high.empty:
        print(f"No VWC values > {HIGH_VWC_1:.0f}% found.")
        return

    high["hour"] = high["timestamp"].dt.hour

    hourly = (
        high.groupby("hour")
        .size()
        .reset_index(name="count_gt_50")
        .sort_values("hour")
    )

    print(hourly.to_string(index=False))


def summarize_high_vwc_episodes(all_values: pd.DataFrame) -> None:
    print("\nDetailed VWC episodes > 50%")
    print("-" * 120)

    rows = []

    for column, sub in all_values.groupby("column"):
        episodes = get_episode_details(sub, HIGH_VWC_1)

        for episode in episodes:
            rows.append(
                {
                    "column": column,
                    "start": episode["start"].strftime("%Y-%m-%d %H:%M"),
                    "end": episode["end"].strftime("%Y-%m-%d %H:%M"),
                    "duration_hr": round(float(episode["duration_hours"]), 2),
                    "n_points": int(episode["n_points"]),
                    "max_vwc": round(float(episode["max_value"]), 2),
                    "max_timestamp": episode["max_timestamp"].strftime(
                        "%Y-%m-%d %H:%M"
                    ),
                }
            )

    if not rows:
        print(f"No VWC episodes > {HIGH_VWC_1:.0f}% found.")
        return

    episode_df = pd.DataFrame(rows)

    episode_df = episode_df.sort_values(
        by=["duration_hr", "max_vwc"],
        ascending=False,
    )

    print(episode_df.to_string(index=False))


def main() -> None:
    all_records: list[pd.DataFrame] = []

    print("\nMaximum VWC by year")
    print("-" * 80)

    for year in YEARS:
        parquet_file = PARQUET_DIR / str(year) / f"{year}_raw_logger.parquet"

        if not parquet_file.exists():
            print(f"{year}: file not found")
            continue

        df = pd.read_parquet(parquet_file)

        if "timestamp" not in df.columns:
            raise KeyError(f"{parquet_file} has no timestamp column")

        vwc_cols = get_vwc_columns(df)

        if not vwc_cols:
            print(f"{year}: no VWC columns found")
            continue

        long = df[["timestamp", *vwc_cols]].melt(
            id_vars="timestamp",
            value_vars=vwc_cols,
            var_name="column",
            value_name="value",
        )

        long["year"] = int(year)
        long["timestamp"] = pd.to_datetime(long["timestamp"], errors="coerce")
        long["value"] = pd.to_numeric(long["value"], errors="coerce")
        long = long.dropna(subset=["timestamp", "value"])

        if long.empty:
            print(f"{year}: no valid VWC values found")
            continue

        idx = long["value"].idxmax()
        max_row = long.loc[idx]

        max_value = float(cast(Any, max_row["value"]))
        max_timestamp = pd.Timestamp(cast(Any, max_row["timestamp"]))
        max_column = str(cast(Any, max_row["column"]))

        print(
            f"{year}: "
            f"{max_value:.2f}% "
            f"on {max_timestamp:%Y-%m-%d %H:%M} "
            f"({max_column})"
        )

        all_records.append(long[["year", "timestamp", "column", "value"]])

    print("\nOverall maximum")
    print("-" * 80)

    if not all_records:
        print("No VWC records found.")
        return

    all_values = pd.concat(all_records, ignore_index=True)

    overall_idx = all_values["value"].idxmax()
    overall = all_values.loc[overall_idx]

    overall_value = float(cast(Any, overall["value"]))
    overall_timestamp = pd.Timestamp(cast(Any, overall["timestamp"]))
    overall_year = int(cast(Any, overall["year"]))
    overall_column = str(cast(Any, overall["column"]))

    print(
        f"{overall_value:.2f}% "
        f"on {overall_timestamp:%Y-%m-%d %H:%M} "
        f"in {overall_year} "
        f"({overall_column})"
    )

    print("\nTop 20 highest VWC values")
    print("-" * 80)

    top_values = (
        all_values
        .sort_values("value", ascending=False)
        .head(20)
        .copy()
    )

    top_values["timestamp"] = top_values["timestamp"].dt.strftime(
        "%Y-%m-%d %H:%M"
    )
    top_values["value"] = top_values["value"].round(2)

    print(top_values.to_string(index=False))

    summarize_sensor_qa(all_values)
    summarize_high_values_by_hour(all_values)
    summarize_high_vwc_episodes(all_values)


if __name__ == "__main__":
    main()