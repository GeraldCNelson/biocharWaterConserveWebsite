# biochar_app/utils.py

import pandas as pd
import logging
import os
import zipfile
import json
from dataclasses import dataclass
from biochar_app.config import DATA_PROCESSED_DIR, GSEASON_PERIODS

@dataclass
class LoggerFileInfo:
    filename: str
    start_date: str
    end_date: str
    granularity: str

loaded_datasets = {}

def parse_filenames(data_dir, prefix="dataloggerData_", suffix=".zip"):
    filenames = [f for f in os.listdir(data_dir) if f.startswith(prefix) and f.endswith(suffix)]
    parsed_files = []

    for filename in filenames:
        try:
            core = filename[len(prefix):-len(suffix)]
            parts = core.split("_")
            if len(parts) != 3:
                logging.warning(f"Skipping invalid filename format: {filename}")
                continue

            start_date, end_date, granularity = parts
            parsed_files.append(LoggerFileInfo(
                filename=filename,
                start_date=start_date,
                end_date=end_date,
                granularity=granularity
            ))

        except Exception as e:
            logging.error(f"Error parsing filename {filename}: {e}")
            continue

    logging.info(f"🔍 Parsed Files: {parsed_files}")
    return parsed_files


def load_logger_data(year: int, granularity: str):
    key = f"{year}-{granularity}"  # ✅ Use both year and granularity for caching
    if key in loaded_datasets:
        return loaded_datasets[key]

    # ✅ Use centralized file parser
    parsed_files = parse_filenames(DATA_PROCESSED_DIR)

    matching_file = next(
        (f for f in parsed_files if f.granularity == granularity and f.start_date.startswith(str(year))),
        None
    )

    if matching_file is None:
        raise FileNotFoundError(f"No file found for {year} - {granularity}")

    file_path = os.path.join(DATA_PROCESSED_DIR, matching_file.filename)
    logging.info(f"📄 Loading data from {file_path}")

    with zipfile.ZipFile(file_path, 'r') as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            df = pd.read_csv(f)

    # ✅ Parse timestamp only if present and granularity isn't 'gseason'
    if granularity != "gseason" and "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # ✅ Cache and return
    loaded_datasets[key] = df
    return df


def compute_summary_statistics(df, variable: str, strip: str, depth: str):
    """
    Compute summary statistics for raw and ratio values filtered by variable, strip, and depth.
    Returns two dictionaries: raw_stats and ratio_stats.
    """
    if not variable or not strip or not depth or df is None or df.empty:
        return {}, {}

    df = df.copy()
    raw_stats = {}
    ratio_stats = {}

    # Build expected prefixes
    raw_prefix = f"{variable}_{depth}_raw_{strip}_"
    ratio_prefixes = [
        f"{variable}_{depth}_ratio_S1_S2_",
        f"{variable}_{depth}_ratio_S3_S4_"
    ]

    # ✅ Compute RAW stats
    raw_cols = [col for col in df.columns if col.startswith(raw_prefix)]
    for col in raw_cols:
        series = df[col].dropna()
        if not series.empty:
            raw_stats[col] = {
                "min": round(series.min(), 4),
                "mean": round(series.mean(), 4),
                "max": round(series.max(), 4),
                "std": round(series.std(), 4),
            }

    # ✅ Compute RATIO stats
    for prefix in ratio_prefixes:
        for col in df.columns:
            if col.startswith(prefix):
                series = df[col].dropna()
                if not series.empty:
                    ratio_stats[col] = {
                        "min": round(series.min(), 4),
                        "mean": round(series.mean(), 4),
                        "max": round(series.max(), 4),
                        "std": round(series.std(), 4),
                    }

    return raw_stats, ratio_stats


def assign_gseason_periods(ts: pd.Timestamp, year: int) -> str | None:
    for label, (start_str, end_str) in GSEASON_PERIODS.items():
        sm, sd = map(int, start_str.split("-"))
        em, ed = map(int, end_str.split("-"))

        start_year = year - 1 if sm > em else year
        end_year = year

        start = pd.Timestamp(f"{start_year}-{start_str}")
        end = pd.Timestamp(f"{end_year}-{end_str}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        if start <= ts <= end:
            return label
    return None

def load_or_generate_gseason_summary(year, overwrite=False):
    summary_path = os.path.join(DATA_PROCESSED_DIR, f"gseason_summary_{year}.json")
    if not os.path.exists(summary_path) or overwrite:
        from biochar_app.generate_gseason_summary import generate_gseason_summary
        generate_gseason_summary(year, overwrite=overwrite)

    with open(summary_path, "r", encoding="utf-8") as f:
        return json.load(f)

