import os
import json
import logging
import pandas as pd
from typing import TextIO
from biochar_app.config import (
    DATA_PROCESSED_DIR,
    GSEASON_PERIODS,
    YEARS
)
from biochar_app.utils import load_logger_data, compute_summary_statistics


def generate_gseason_summary(year, gseason_periods=None, overwrite=False):
    """
    Generate summary statistics (min, mean, max, std) for each growing season period.
    """
    gseason_periods = gseason_periods or GSEASON_PERIODS
    logging.info(f"🌱 Generating growing season summary for {year}...")

    output_path = os.path.join(DATA_PROCESSED_DIR, f"gseason_summary_{year}.json")
    if os.path.exists(output_path) and not overwrite:
        logging.info(f"✅ Already exists: {output_path} — Skipping.")
        return

    df_15min = load_logger_data(year, "15min")
    if df_15min is None or df_15min.empty:
        raise RuntimeError("❌ No 15-minute logger data available.")

    df_15min["timestamp"] = pd.to_datetime(df_15min["timestamp"], errors="coerce")

    summary = {}

    for label, info in gseason_periods.items():
        start_str = info["start"]
        end_str = info["end"]
        start_year = year - 1 if start_str > end_str else year
        start = pd.Timestamp(f"{start_year}-{start_str}")
        end = pd.Timestamp(f"{year}-{end_str}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        mask = (df_15min["timestamp"] >= start) & (df_15min["timestamp"] <= end)
        df_season = df_15min[mask]
        logging.info(f"📅 {label}: {len(df_season)} records from {start.date()} to {end.date()}")

        season_stats = {}
        for variable in ["VWC", "T", "EC", "SWC"]:
            season_stats[variable] = {}
            for strip in ["S1", "S2", "S3", "S4"]:
                for depth in ["1", "2", "3"]:
                    raw, ratio = compute_summary_statistics(df_season, variable, strip, depth)
                    is_temp = variable in ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"]
                    if is_temp:
                        ratio = {}
                    season_stats[variable][f"{strip}_D{depth}"] = {
                        "raw_statistics": raw,
                        "ratio_statistics": ratio
                    }

        summary[label] = season_stats

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logging.info(f"✅ Saved growing season summary: {output_path}")


if __name__ == "__main__":
    import sys
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    for y in YEARS:
        if y >= 2024:
            generate_gseason_summary(y, overwrite=True)
