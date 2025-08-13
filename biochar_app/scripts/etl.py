#!/usr/bin/env python3
"""
Full ETL (no growing-season calculations here):
  - Read all .dat logger files per year (in data-raw/datfiles_{year})
  - Normalize timestamps to naive America/Denver (drop tzinfo; shift DST gaps forward)
  - Mask extreme placeholders → NaN
  - Convert VWC fractions → percent (×100) deterministically
  - Mask VWC > 150% → NaN
  - Compute SWC cylinder volumes & logger-ratios
  - Resample to 15min/hourly/daily/monthly; write Parquet + Parquet_ratios
  - Fetch CoAgMet 5-min weather; clean precip increments; write resampled Parquet
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional

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
    UNIT_CONVERSIONS,
    cylinder_volume_m3,
)
from utils import calculate_ratios  # ensure this exists in your project

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ============================= Common helpers ============================= #

def normalize_timestamp_series(ts: pd.Series, tz: str = "America/Denver") -> pd.Series:
    """
    Localize naive timestamps to `tz` (shifting DST gaps forward),
    convert any tz-aware to `tz`, then drop tz info (naive).
    This matches the logger convention used throughout the app.
    """
    s = pd.to_datetime(ts, errors="coerce")
    # If already tz-aware, convert; else localize.
    if getattr(s.dt, "tz", None) is None:
        s = s.dt.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward")
    else:
        s = s.dt.tz_convert(tz)
    return s.dt.tz_localize(None)


def rename_logger_columns(df: pd.DataFrame, logger_name: str) -> pd.DataFrame:
    """Standardize raw-logger column names (e.g., VWC_1_raw_S1_T)."""
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

    df = pd.read_csv(  # type: ignore[arg-type]
        str(datfile),
        header=None,
        skiprows=4,
        names=["timestamp", "RECORD"] + VALUE_COLS_2024_PLUS,
        na_values=["", "NA", "NAN"],
        parse_dates=["timestamp"],
    ).drop(columns=["RECORD"], errors="ignore")

    df["timestamp"] = normalize_timestamp_series(df["timestamp"])
    n_nat = df["timestamp"].isna().sum()
    if n_nat:
        logger.warning(f"⚠️ {n_nat} NaT timestamps in {name}")

    df = df[df["timestamp"] >= pd.Timestamp(f"{year}-01-01")]
    if df.empty:
        return None

    return rename_logger_columns(df, name)


def merge_all_loggers(year: int) -> Optional[pd.DataFrame]:
    """Outer-join all strip/logger .dat into one wide DataFrame."""
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
    """Mask any |value| ≥ threshold as NaN in numeric columns (logger data)."""
    for col in df.select_dtypes(include=["float", "int"]).columns:
        df[col] = df[col].mask(df[col].abs() >= threshold, np.nan)
    logger.info("🧹 Replaced extreme placeholders with NaN")
    return df


def scale_vwc_to_percent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deterministically convert ALL raw VWC columns from fractions (0–1)
    to percent (0–100) by multiplying by 100. Use this exactly once in ETL.
    """
    vwc_cols = [c for c in df.columns if c.startswith("VWC_") and "_raw_" in c]
    if not vwc_cols:
        return df
    for c in vwc_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float) * 100.0
    logger.info(f"📏 Scaled {len(vwc_cols)} VWC columns ×100 to percent")
    return df


def add_swc_cylinder_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """Compute SWC cylinder volumes in L & gallons for each VWC sensor."""
    df = df.copy()
    cyl_m3 = cylinder_volume_m3()
    cyl_L = cyl_m3 * 1000.0  # m^3 → L
    cyl_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](cyl_L)

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col = f"VWC_{depth}_raw_{strip}_{loc}"
                if col not in df:
                    continue
                # VWC now in percent; convert to fraction for volume math
                frac = pd.to_numeric(df[col], errors="coerce") / 100.0
                df[f"SWC_vol_L_{strip}_{loc}_{depth}"] = frac * cyl_L
                df[f"SWC_vol_gal_{strip}_{loc}_{depth}"] = frac * cyl_gal

    return df


# ============================= Aggregation (loggers) ============================= #

def aggregate_and_write(year: int, df: pd.DataFrame) -> None:
    """
    Given cleaned df (indexed on timestamp), write:
      - raw-logger + raw-logger_ratios
      - fixed-frequency summaries + *_ratios (no growing-season here)
    """
    year_dir = Path(PARQUET_DIR) / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    # 1) Raw + Ratios
    raw_path = year_dir / f"{year}_raw_logger.parquet"
    ratio_path = year_dir / f"{year}_raw_logger_ratios.parquet"
    df.reset_index().to_parquet(raw_path, index=False, compression="snappy")
    calculate_ratios(df).reset_index().to_parquet(ratio_path, index=False, compression="snappy")
    logger.info(f"✅ Wrote raw & ratio: {raw_path.name}, {ratio_path.name}")

    # Identify “real” sensor columns to use for all-null row dropping
    sensor_cols = [
        c for c in df.columns
        if any(c.startswith(prefix) for prefix in ("VWC_", "T_", "EC_", "SWC_"))
    ]

    # 2) Fixed-frequency summaries (15min/hourly/daily/monthly)
    summary_base = Path(PARQUET_DIR) / "summary"
    for freq, code in GRANULARITIES:
        if code is None:
            continue
        out_dir = summary_base / freq
        out_dir.mkdir(parents=True, exist_ok=True)

        # sum precip columns, mean otherwise
        agg_map = {col: ("sum" if col.startswith("precip") else "mean") for col in df.columns}

        # a) raw summary
        df_s = df.resample(code).agg(agg_map).round(3)
        df_s = df_s.dropna(subset=sensor_cols, how="all").reset_index()

        fn_raw = f"{year}_{freq}.parquet"
        df_s.to_parquet(out_dir / fn_raw, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq}: {fn_raw}")

        # b) summary-ratios
        df_s_ratio = calculate_ratios(df_s.set_index("timestamp", drop=True))
        fn_ratio = f"{year}_{freq}_ratios.parquet"
        df_s_ratio.reset_index().to_parquet(out_dir / fn_ratio, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq} ratios: {fn_ratio}")


# ============================= Weather (CoAgMet) ============================= #

def clean_weather_frame(dfw: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure CoAgMet weather is ready for resampling:
      - parse timestamp -> naive America/Denver (to match logger convention)
      - coerce precip_in to numeric, treat -999 as NA, fill NA with 0
      - clip negative increments to 0
      - optional: warn on implausible 5-min spikes
    Expects columns: ['timestamp', 'precip_in', 'temp_air_degF', ...]
    """
    dfw = dfw.copy()

    # Timestamp normalization (match logger convention)
    dfw["timestamp"] = normalize_timestamp_series(dfw["timestamp"])

    # Precip cleanup (5-min increments, inches)
    dfw["precip_in"] = pd.to_numeric(dfw["precip_in"], errors="coerce")
    # If caller didn't convert -999 to NaN yet, do it now
    dfw.loc[dfw["precip_in"] == -999, "precip_in"] = np.nan
    dfw["precip_in"] = dfw["precip_in"].fillna(0.0).clip(lower=0.0)

    # Sanity check for extreme 5-min spikes (~18 in/hr equiv is very high)
    spike = dfw["precip_in"].max()
    if pd.notna(spike) and spike > 1.5:
        logger.warning(f"⚠️ CoAgMet 5-min precip spike detected: {spike:.2f} in")

    return dfw


# ============================= Orchestration ============================= #

def generate_summaries(years: List[int]) -> None:
    for year in years:
        logger.info(f"🌱 Starting ETL for {year}")

        # ----- Logger data -----
        df = merge_all_loggers(year)
        if df is None or df.empty:
            logger.error(f"❌ No logger .dat data for {year}, skipping logger summaries.")
        else:
            # 1) Normalize & drop invalid timestamps
            df["timestamp"] = normalize_timestamp_series(df["timestamp"])
            df = df.dropna(subset=["timestamp"])

            # 2) Mask placeholder extremes
            df = replace_bad_values(df)

            # 3) Deterministically convert VWC to percent
            df = scale_vwc_to_percent(df)

            # 4) Mask VWC >150%
            vwc_cols = [c for c in df.columns if c.startswith("VWC_") and "_raw_" in c]
            outliers = (
                pd.concat([pd.to_numeric(df[c], errors="coerce") for c in vwc_cols], axis=1)
                .gt(150.0)
                .sum()
                .sum()
            )
            if outliers:
                logger.warning(f"⚠️ {outliers} VWC>150% → NaN")
            for c in vwc_cols:
                series = pd.to_numeric(df[c], errors="coerce")
                df[c] = series.mask(series > 150.0)

            # 5) SWC volumes & logger ratios (volumes use VWC fraction internally)
            df = add_swc_cylinder_volumes(df)

            # Compute point-in-time logger ratios
            df = calculate_ratios(df)

            # 6) Index & aggregate/write everything (no gseason here)
            df = df.set_index("timestamp", drop=True).sort_index()
            aggregate_and_write(year, df)

        # ----- Weather (CoAgMet) -----
        try:
            dfw = fetch_weather_data(year)
        except Exception as e:
            logger.error(f"❌ fetch_weather_data({year}) failed: {e}")
            continue

        # Validate expected columns
        required = {"timestamp", "precip_in", "temp_air_degF"}
        missing = [c for c in required if c not in dfw.columns]
        if missing:
            logger.error(f"❌ fetch_weather_data({year}) missing columns: {missing}")
            continue

        dfw = clean_weather_frame(dfw).set_index("timestamp").sort_index()

        # Derived metric fields
        dfw["precip_mm"] = dfw["precip_in"].apply(UNIT_CONVERSIONS["us_to_metric"]["precip"])
        dfw["temp_air_degC"] = dfw["temp_air_degF"].apply(UNIT_CONVERSIONS["us_to_metric"]["temp"])

        weather_base = Path(PARQUET_DIR) / "summary" / "weather"
        for freq, code in GRANULARITIES:
            if code is None:
                continue
            out_dir = weather_base / freq
            out_dir.mkdir(parents=True, exist_ok=True)

            agg_map = {col: ("sum" if col.startswith("precip") else "mean") for col in dfw.columns}
            dfr = dfw.resample(code).agg(agg_map).round(3).reset_index()
            fn = f"{year}_{freq}.parquet"
            dfr.to_parquet(out_dir / fn, index=False, compression="snappy")
            logger.info(f"✅ Weather {freq} for {year}")

    logger.info("🎉 ETL complete.")


if __name__ == "__main__":
    os.makedirs(PARQUET_DIR, exist_ok=True)
    generate_summaries(YEARS)