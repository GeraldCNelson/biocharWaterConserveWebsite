#!/usr/bin/env python3
"""
Full ETL:
  - Read all .dat logger files per year (in data-raw/datfiles_{year})
  - Normalize timestamps, drop DST‐gap NaT
  - Mask extreme placeholders → NaN
  - Mask VWC > 150% → NaN
  - Compute SWC cylinder volumes & logger‐ratios
  - Resample to 15min/hourly/daily/monthly; write Parquet + Parquet_ratios
  - Compute growing‐season summaries; write Parquet + Parquet_ratios
  - Fetch CoAgMet weather; write resampled Parquet
"""

import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from biochar_app.scripts.get_weather_data import fetch_weather_data
from biochar_app.scripts.config import (
    DATA_RAW_DIR,
    PARQUET_DIR,
    YEARS,
    STRIPS,
    LOGGER_LOCATIONS,
    VALUE_COLS_STANDARD,
    VALUE_COLS_2024_PLUS,
    GRANULARITIES,
    DEFAULT_GSEASON_PERIODS,
    UNIT_CONVERSIONS,
    cylinder_volume_m3,
)
from utils import calculate_ratios  # wherever you actually define it

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def normalize_timestamp_series(ts: pd.Series, tz: str = "America/Denver") -> pd.Series:
    """Localize to tz (dropping DST gaps), then drop tz info."""
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward")
    else:
        ts = ts.dt.tz_convert(tz)
    return ts.dt.tz_localize(None)


def rename_logger_columns(df: pd.DataFrame, logger_name: str) -> pd.DataFrame:
    """Standardize raw‐logger column names."""
    mapping: Dict[str, str] = {}
    for col in df.columns:
        if col == "timestamp":
            continue
        if col == "BattV_Min":
            mapping[col] = f"BattV_Min_{logger_name[:2]}_{logger_name[2:]}"
        elif col in VALUE_COLS_STANDARD:
            var, depth, _agg = col.split("_")
            mapping[col] = f"{var}_{depth}_raw_{logger_name[:2]}_{logger_name[2:]}"
    return df.rename(columns=mapping)


def read_logger_data(name: str, year: int) -> Optional[pd.DataFrame]:
    """Read one strip+loc .dat, normalize & filter to year, rename."""
    datfile = Path(DATA_RAW_DIR) / f"datfiles_{year}" / f"{name}_Table1.dat"
    if not datfile.exists():
        logger.warning(f"⚠️ Not found: {datfile}")
        return None

    df = pd.read_csv(   # type: ignore[arg-type]
        str(datfile),        header=None,
        skiprows=4,
        names=["timestamp", "RECORD"] + VALUE_COLS_2024_PLUS,
        na_values=["", "NA", "NAN"],
        parse_dates=["timestamp"],
    ).drop(columns=["RECORD"])

    df["timestamp"] = normalize_timestamp_series(df["timestamp"])
    n_nat = df["timestamp"].isna().sum()
    if n_nat:
        logger.warning(f"⚠️ {n_nat} NaT timestamps in {name}")

    df = df[df["timestamp"] >= pd.Timestamp(f"{year}-01-01")]
    if df.empty:
        return None

    return rename_logger_columns(df, name)


def merge_all_loggers(year: int) -> Optional[pd.DataFrame]:
    """Outer‐join all strip/logger .dat into one wide DataFrame."""
    frames: List[pd.DataFrame] = []
    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            tag = f"{strip}{loc}"
            df = read_logger_data(tag, year)
            if df is None:
                continue
            df = df.set_index("timestamp")
            df = df[~df.index.duplicated(keep="first")]
            frames.append(df)

    if not frames:
        return None

    merged = pd.concat(frames, axis=1)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    return merged.reset_index()


def replace_bad_values(df: pd.DataFrame, threshold: float = 999_999.0) -> pd.DataFrame:
    """Mask any |value| ≥ threshold as NaN in numeric columns."""
    for col in df.select_dtypes(include=["float", "int"]):
        df[col] = df[col].mask(df[col].abs() >= threshold, np.nan)
    logger.info("🧹 Replaced extreme placeholders with NaN")
    return df


def add_swc_cylinder_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """Compute SWC cylinder volumes in L & gallons for each VWC sensor."""
    df = df.copy()
    cyl_cm3 = cylinder_volume_m3()
    cyl_L   = cyl_cm3 / 1000.0
    cyl_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](cyl_L)

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col = f"VWC_{depth}_raw_{strip}_{loc}"
                if col not in df:
                    continue
                frac = df[col].astype(float) / 100.0
                df[f"SWC_vol_L_{strip}_{loc}_{depth}"]  = frac * cyl_L
                df[f"SWC_vol_gal_{strip}_{loc}_{depth}"] = frac * cyl_gal

    return df


def aggregate_and_write(year: int, df: pd.DataFrame) -> None:
    """
    Given cleaned df (indexed on timestamp), write:
      - raw‐logger + raw‐logger_ratios
      - fixed‐frequency summaries + *_ratios (dropping empty periods)
      - growing‐season summary + _ratios
    """
    year_dir = Path(PARQUET_DIR) / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    # 1) Raw + Ratios
    raw_path   = year_dir / f"{year}_raw_logger.parquet"
    ratio_path = year_dir / f"{year}_raw_logger_ratios.parquet"
    df.reset_index().to_parquet(raw_path,   index=False, compression="snappy")
    calculate_ratios(df).reset_index().to_parquet(ratio_path, index=False, compression="snappy")
    logger.info(f"✅ Wrote raw & ratio: {raw_path.name}, {ratio_path.name}")

    # identify actual sensor columns
    sensor_cols = [
        c for c in df.columns
        if any(c.startswith(prefix) for prefix in ("VWC_", "T_", "EC_", "SWC_"))
    ]

    # 2) Fixed‐frequency summaries
    summary_base = Path(PARQUET_DIR) / "summary"
    for freq, code in GRANULARITIES:
        if code is None:
            continue
        out_dir = summary_base / freq
        out_dir.mkdir(parents=True, exist_ok=True)

        agg_map = {col: ("sum" if col.startswith("precip") else "mean")
                   for col in df.columns}

        # a) raw summary
        df_s = df.resample(code).agg(agg_map).round(3)
        df_s = df_s.dropna(subset=sensor_cols, how="all").reset_index()

        fn_raw = f"{year}_{freq}.parquet"
        df_s.to_parquet(out_dir / fn_raw, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq}: {fn_raw}")

        # b) summary‐ratios
        df_s_ratio = calculate_ratios(df_s.set_index("timestamp", drop=True))
        fn_ratio    = f"{year}_{freq}_ratios.parquet"
        df_s_ratio.reset_index().to_parquet(out_dir / fn_ratio, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq} ratios: {fn_ratio}")

    # 3) Growing‐season summary
    gs_rows: List[pd.Series] = []
    for period_code, info in DEFAULT_GSEASON_PERIODS.items():
        sm, _ = info["start"].split("-")
        em, _ = info["end"].split("-")
        start_m, end_m = int(sm), int(em)
        months = (
            list(range(start_m, end_m+1))
            if start_m <= end_m
            else list(range(start_m,13)) + list(range(1, end_m+1))
        )

        slice_df = df[df.index.month.isin(months)]
        if not slice_df[sensor_cols].dropna(how="all").empty:
            means = slice_df.mean(skipna=True)

            # override the precip columns to be the *sum* over the whole period
            for pc in ("precip_in", "precip_mm"):
                if pc in slice_df.columns:
                    means[pc] = slice_df[pc].sum(skipna=True)

            means = means.round(3)
            means.name = period_code
            gs_rows.append(means)

    if gs_rows:
        df_gs = pd.DataFrame(gs_rows).reset_index().rename(columns={"index":"period_code"})

        # Note: we do *not* reconvert VWC again—leave them in the same percent units
        out_dir = summary_base / "gseason"
        out_dir.mkdir(parents=True, exist_ok=True)

        # a) raw growing‐season
        fn_gs = f"{year}_gseason.parquet"
        df_gs.to_parquet(out_dir / fn_gs, index=False, compression="snappy")
        logger.info(f"✅ Growing‐season summary: {fn_gs}")

        # b) growing‐season ratios
        df_gs_ratio = calculate_ratios(df_gs.set_index("period_code", drop=True))
        fn_gs_ratio = f"{year}_gseason_ratios.parquet"
        df_gs_ratio.reset_index().to_parquet(out_dir / fn_gs_ratio, index=False, compression="snappy")
        logger.info(f"✅ Growing‐season ratios: {fn_gs_ratio}")

def generate_summaries(years: List[int]) -> None:
    for year in years:
        logger.info(f"🌱 Starting ETL for {year}")
        df = merge_all_loggers(year)
        if df is None or df.empty:
            logger.error(f"❌ No logger .dat data for {year}, skipping.")
            continue

        # 1) Normalize & drop DST‐gap NaTs
        df["timestamp"] = normalize_timestamp_series(df["timestamp"])
        df = df.dropna(subset=["timestamp"])

        # 2) Mask placeholder extremes
        df = replace_bad_values(df)

        # 3) Mask VWC >150%
        vwc_cols = [c for c in df.columns if c.startswith("VWC_") and "_raw_" in c]
        outliers = df[vwc_cols].gt(150.0).sum().sum()
        if outliers:
            logger.warning(f"⚠️ {outliers} VWC>150% → NaN")
            df[vwc_cols] = df[vwc_cols].mask(df[vwc_cols] > 150.0)

        # 4) SWC volumes & logger‐ratios
        df = add_swc_cylinder_volumes(df)
        df = calculate_ratios(df)

        # 5) Index & aggregate/write everything
        df = df.set_index("timestamp", drop=True).sort_index()
        aggregate_and_write(year, df)

        # 6) Weather summaries
        dfw = fetch_weather_data(year)
        dfw["timestamp"] = pd.to_datetime(dfw["timestamp"], errors="coerce")
        dfw = dfw.dropna(subset=["timestamp"]).set_index("timestamp")
        dfw["precip_mm"]     = dfw["precip_in"].apply( UNIT_CONVERSIONS["us_to_metric"]["precip"] )
        dfw["temp_air_degC"] = dfw["temp_air_degF"].apply( UNIT_CONVERSIONS["us_to_metric"]["temp"] )

        weather_base = Path(PARQUET_DIR) / "summary" / "weather"
        for freq, code in GRANULARITIES:
            if code is None:
                continue
            out_dir = weather_base / freq
            out_dir.mkdir(parents=True, exist_ok=True)

            agg_map = {col: ("sum" if col.startswith("precip") else "mean") for col in dfw.columns}
            dfr = dfw.resample(code).agg(agg_map).round(3).reset_index()
            fn  = f"{year}_{freq}.parquet"
            dfr.to_parquet(out_dir / fn, index=False, compression="snappy")
            logger.info(f"✅ Weather {freq} for {year}")

    logger.info("🎉 ETL complete.")


if __name__ == "__main__":
    os.makedirs(PARQUET_DIR, exist_ok=True)
    generate_summaries(YEARS)