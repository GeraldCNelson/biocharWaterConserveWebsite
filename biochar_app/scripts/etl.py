#!/usr/bin/env python3
"""
Full ETL:
  - Read all .dat logger files per year (in data-raw/datfiles_{year})
  - Merge into a single 15-min DataFrame (dropping duplicate timestamps)
  - Resample/aggregate to 15-min, hourly, daily, monthly
  - Pivot wide, write raw‐logger parquet + ratios parquet
  - Compute & write growing‐season summaries + ratios
  - Fetch CoAgMet weather and write aggregated weather Parquets
"""

import os
from pathlib import Path
from io import StringIO
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from biochar_app.scripts.get_weather_data import (
    fetch_weather_data,
    get_weather_column_labels,
)
from utils import calculate_ratios
from config import (
    DATA_RAW_DIR,
    PARQUET_DIR,
    GRANULARITIES,
    YEARS,
    STRIPS,
    VALUE_COLS_STANDARD,
    VALUE_COLS_2024_PLUS,
    LOGGER_LOCATIONS,
    COLLECT_PERIOD,
    COAG_STATION,
    COAGMET_VARIABLE_MAP,
    DEFAULT_UNITS,
    DEFAULT_TIMEZONE,
)

from biochar_app.scripts.routes_utils import merge_all_loggers

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def rename_logger_columns(df: pd.DataFrame, logger_name: str) -> pd.DataFrame:
    strip = logger_name[:2]
    loc   = logger_name[2:]
    rename_map: dict[str,str] = {}
    for col in df.columns:
        if col == "timestamp":
            continue
        if col in VALUE_COLS_STANDARD:
            var, depth, _agg = col.split("_")
            rename_map[col] = f"{var}_{depth}_raw_{strip}_{loc}"
        elif col == "BattV_Min":
            rename_map[col] = f"BattV_Min_{strip}_{loc}"
    return df.rename(columns=rename_map)


def read_logger_data(dat_path: Path, year: int) -> pd.DataFrame:
    value_cols = VALUE_COLS_STANDARD if year == 2023 else VALUE_COLS_2024_PLUS
    names_new  = ["timestamp", "RECORD"] + value_cols
    df = pd.read_csv(
        dat_path,
        skiprows=4,
        names=names_new,
        na_values=["", "NA", "NAN"],
        dtype={"timestamp": str},
    ).drop(columns=["RECORD"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S")
    df = df.loc[~df["timestamp"].duplicated(keep="first")]
    logger_name = dat_path.stem.split("_", 1)[0]
    df = rename_logger_columns(df, logger_name)
    df = df.set_index("timestamp").sort_index()
    return df



def _write_fixed(
    df_raw: pd.DataFrame,
    freq_name: str,
    resample_code: str,
    out_dir: Path,
    year: int,
) -> None:
    agg_map = {col: "mean" for col in df_raw.columns}
    rc      = resample_code.lower()
    df_agg  = df_raw.resample(rc).agg(agg_map).round(3)

    df_long = (
        df_agg
        .reset_index()
        .melt(id_vars="timestamp", var_name="col", value_name="value")
    )
    df_long[["variable", "strip", "logger_name"]] = (
        df_long["col"].str.rsplit("_", n=2, expand=True)
    )
    df_long = df_long.drop(columns="col")

    df_pivot = df_long.pivot_table(
        index="timestamp",
        columns=["variable", "strip", "logger_name"],
        values="value",
    )
    df_wide = df_pivot.copy()
    df_wide.columns = [
        f"{var}_{strip}_{logger_name}"
        for (var, strip, logger_name) in df_pivot.columns
    ]

    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / f"{year}_{freq_name}.parquet"
    df_wide.reset_index().to_parquet(raw_path, index=False)
    logger.info(f"✅ Raw‐logger parquet written: {raw_path}")

    df_ratio = calculate_ratios(df_wide)
    ratio_path = out_dir / f"{year}_{freq_name}_ratios.parquet"
    df_ratio.reset_index().to_parquet(ratio_path, index=False)
    logger.info(f"✅ Ratio parquet written:      {ratio_path}")


def generate_summaries(years: list[int]) -> None:
    summary_base = Path(PARQUET_DIR) / "summary"

    for year in years:
        logger.info(f"🌦️  Generating summaries for {year}")

        # 1) sensor raw
        df_raw = merge_all_loggers(year)

        # 2) write raw‐logger parquet
        raw_outdir = Path(PARQUET_DIR) / str(year)
        raw_outdir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_outdir / f"{year}_raw_logger.parquet"
        df_raw.reset_index().to_parquet(raw_path, index=False)
        logger.info(f"✅ Raw logger data written: {raw_path}")

        # 3) sensor fixed‐frequency summaries
        for freq_name, code in GRANULARITIES:
            if code is None:
                continue
            out_dir = summary_base / freq_name
            _write_fixed(
                df_raw=df_raw,
                freq_name=freq_name,
                resample_code=code,
                out_dir=out_dir,
                year=year,
            )


        # 4) weather summaries
        weather_df = fetch_weather_data(year)
        weather_base = summary_base / "weather"
        weather_df["timestamp"] = pd.to_datetime(weather_df["timestamp"])
        for freq_name, code in GRANULARITIES:
            if code is None:
                continue
            out_dir = weather_base / freq_name
            out_dir.mkdir(parents=True, exist_ok=True)
            if freq_name == "15min":
                df_weather = weather_df.copy()
            else:
                agg_map = {
                    col: ("sum" if col.startswith("precip") else "mean")
                    for col in weather_df.columns if col != "timestamp"
                }
                df_weather = (
                    weather_df
                    .set_index("timestamp")
                    .resample(code)
                    .agg(agg_map)
                    .round(3)
                    .reset_index()
                )
            path = out_dir / f"{year}_{freq_name}.parquet"
            df_weather.to_parquet(path, index=False)
            logger.info(f"✅ Weather {freq_name} parquet written: {path}")


def main():
    generate_summaries(YEARS)


if __name__ == "__main__":
    main()