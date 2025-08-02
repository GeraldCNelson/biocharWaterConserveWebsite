# biochar_app/utils.py

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
import json
from dataclasses import dataclass
from biochar_app.scripts.config import DATA_PROCESSED_DIR, DEFAULT_GSEASON_PERIODS, UNIT_CONVERSIONS, LOGGER_LOCATIONS, DEPTHS
from datetime import datetime
import plotly.graph_objects as go
from typing import List

@dataclass
class LoggerFileInfo:
    filename: str
    start_date: str
    end_date: str
    granularity: str

loaded_datasets = {}

def parse_filenames(parquet_root: Path) -> List[LoggerFileInfo]:
    """
    Discover all Parquet files under:

      <parquet_root>/
        ├── 2023/
        ├── 2024/
        └── 2025/
        summary/
          ├── daily/
          ├── monthly/
          └── gseason/
        weather/

    and return a LoggerFileInfo for each, with:
      - filename    : full path to the .parquet
      - start_date  : YYYY-MM-DD (always Jan 1 of that year)
      - end_date    : YYYY-MM-DD (always Dec 31 of that year)
      - granularity : one of "15min", "hourly", "daily", "monthly", "gseason", "weather"
    """
    parsed: List[LoggerFileInfo] = []
    # 1) Per-year raw and sensor data
    for year_dir in sorted(parquet_root.iterdir()):
        if not year_dir.is_dir() or year_dir.name == "summary" or year_dir.name == "weather":
            continue
        year = year_dir.name
        # assume all year dirs are numeric
        try:
            int(year)
        except ValueError:
            continue

        start_iso = f"{year}-01-01"
        end_iso   = f"{year}-12-31"

        # 15-minute / 1-hour?  We only see sensor files: S1B.parquet etc.
        # treat every S* file as 15min
        for sensor_file in year_dir.glob("S*.parquet"):
            parsed.append(
                LoggerFileInfo(
                    filename=str(sensor_file.resolve()),
                    start_date=start_iso,
                    end_date=end_iso,
                    granularity="15min"
                )
            )
        # if you have a one-hour aggregate under a different name, e.g. "1hour.parquet", add similar glob

        # also catch any top-level daily/monthly if present
        if (year_dir / "daily.parquet").is_file():
            parsed.append(
                LoggerFileInfo(
                    filename=str((year_dir / "daily.parquet").resolve()),
                    start_date=start_iso,
                    end_date=end_iso,
                    granularity="daily"
                )
            )
        if (year_dir / "monthly.parquet").is_file():
            parsed.append(
                LoggerFileInfo(
                    filename=str((year_dir / "monthly.parquet").resolve()),
                    start_date=start_iso,
                    end_date=end_iso,
                    granularity="monthly"
                )
            )

    # 2) Pre-computed summaries
    summary_root = parquet_root / "summary"
    for gran in ("daily", "monthly", "gseason"):
        folder = summary_root / gran
        if not folder.is_dir():
            continue
        for f in folder.glob(f"*_{gran}.parquet"):
            year = f.stem.split("_")[0]
            try:
                int(year)
            except ValueError:
                continue
            parsed.append(
                LoggerFileInfo(
                    filename=str(f.resolve()),
                    start_date=f"{year}-01-01",
                    end_date=f"{year}-12-31",
                    granularity=gran
                )
            )

    # 3) Optional weather data
    weather_root = parquet_root / "weather"
    if weather_root.is_dir():
        for f in weather_root.glob("*_weather.parquet"):
            year = f.stem.split("_")[0]
            try:
                int(year)
            except ValueError:
                continue
            parsed.append(
                LoggerFileInfo(
                    filename=str(f.resolve()),
                    start_date=f"{year}-01-01",
                    end_date=f"{year}-12-31",
                    granularity="weather"
                )
            )

    return parsed


def calculate_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute pairwise strip ratios for each variable/depth/location,
    replacing any infinite results with NA.
    """
    df = df.copy()
    for var in ["VWC", "T", "EC", "SWC"]:
        for s1, s2 in [("S1", "S2"), ("S3", "S4")]:
            for loc in LOGGER_LOCATIONS:
                for d in DEPTHS:
                    # SWC only at depth 1
                    if var == "SWC" and d != "1":
                        continue

                    c1 = f"{var}_{d}_raw_{s1}_{loc}"
                    c2 = f"{var}_{d}_raw_{s2}_{loc}"
                    out = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"

                    if c1 in df.columns and c2 in df.columns:
                        # suppress divide-by-zero warnings, compute ratio
                        with np.errstate(divide='ignore', invalid='ignore'):
                            ratio = df[c1] / df[c2]
                        # replace any ±Inf with NA
                        ratio = ratio.replace([np.inf, -np.inf], pd.NA)
                        df[out] = ratio
                    else:
                        # if either column missing, fill with NA
                        df[out] = pd.NA

    return df

