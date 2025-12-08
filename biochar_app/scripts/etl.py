#!/usr/bin/env python3
"""
Full ETL including growing-season (gseason) summaries:
  - Read all .dat logger files per year (in data-raw/datfiles_{year})
  - Normalize timestamps to naive America/Denver (drop tzinfo; shift DST gaps forward)
  - Mask extreme placeholders → NaN
  - Convert VWC fractions → percent (×100) deterministically
  - Mask VWC > 150% → NaN
  - Compute SWC cylinder volumes & logger‐ratios
  - Resample to 15 min / hourly / daily / monthly; write Parquet + Parquet_ratios
  - Build DEFAULT gseason summaries from daily data (with cross-year support)
  - Fetch CoAgMet 5 min weather; clean precip increments; write resampled Parquet
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
    DEFAULT_GSEASON_PERIODS,
)
from utils import calculate_ratios  # ensure this exists in your project

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ============================= Common helpers ============================= #

def convert_soil_t_to_fahrenheit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all logger soil-temperature columns T_*_raw_* from °C → °F.
    Run once in ETL, right after VWC scaling.
    """
    t_cols = [c for c in df.columns if c.startswith("T_") and "_raw_" in c]
    if not t_cols:
        return df

    to_f = UNIT_CONVERSIONS["metric_to_us"]["temp"]  # λ x: (x * 9/5) + 32

    for col in t_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").apply(to_f)

    logger.info(f"🌡 Converted {len(t_cols)} soil-temp columns from °C to °F")
    return df


def normalize_timestamp_series(
    ts: pd.Series,
    tz: str = "America/Denver",
) -> pd.Series:
    """
    Localize naive timestamps to `tz` (shifting DST gaps forward),
    convert any tz-aware to `tz`, then drop tz info (naive).
    """
    s = pd.to_datetime(ts, errors="coerce")
    tzinfo = s.dt.tz
    if tzinfo is None:
        s = s.dt.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward")
    else:
        s = s.dt.tz_convert(tz)
    return s.dt.tz_localize(None)


def rename_logger_columns(df: pd.DataFrame, logger_name: str) -> pd.DataFrame:
    """
    Standardize raw‐logger column names to a common pattern:
      VWC_1_Avg  → VWC_1_raw_S1_T  (if logger_name == "S1T")
      T_2_Avg    → T_2_raw_S1_T
      EC_3_Avg   → EC_3_raw_S1_T
    """
    mapping: Dict[str, str] = {}
    prefix = logger_name[:2]
    loc = logger_name[2:]

    for col in df.columns:
        if col == "timestamp":
            continue

        if col == "BattV_Min":
            mapping[col] = f"BattV_Min_{prefix}_{loc}"
            continue

        if col.startswith(("VWC_", "T_", "EC_")):
            parts = col.split("_", maxsplit=2)
            if len(parts) == 3:
                var, depth, _agg = parts
                mapping[col] = f"{var}_{depth}_raw_{prefix}_{loc}"

    return df.rename(columns=mapping)


def read_logger_data(name: str, year: int) -> Optional[pd.DataFrame]:
    """Read one strip+loc .dat, normalize & filter to year, rename."""
    datfile = Path(DATA_RAW_DIR) / f"datfiles_{year}" / f"{name}_Table1.dat"
    if not datfile.exists():
        logger.warning(f"⚠️ Not found: {datfile}")
        return None

    df = (
        pd.read_csv(
            datfile,
            header=None,
            skiprows=4,
            names=["timestamp", "RECORD"] + VALUE_COLS_2024_PLUS,
            na_values=["", "NA", "NAN"],
            parse_dates=["timestamp"],
        )
        .drop(columns=["RECORD"], errors="ignore")
    )

    df["timestamp"] = normalize_timestamp_series(df["timestamp"])
    n_nat = int(df["timestamp"].isna().sum())
    if n_nat > 0:
        logger.warning(f"⚠️ {n_nat} NaT timestamps in {name}")

    start = pd.Timestamp(f"{year}-01-01")
    end = pd.Timestamp(f"{year + 1}-01-01")
    df = df.loc[(df["timestamp"] >= start) & (df["timestamp"] < end)]
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
            if df is None or df.empty:
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
    numeric_cols = df.select_dtypes(include=["float", "int"]).columns
    for col in numeric_cols:
        df[col] = df[col].mask(df[col].abs() >= threshold, np.nan)
    logger.info("🧹 Replaced extreme placeholders with NaN")
    return df


def scale_vwc_to_percent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert ALL raw VWC columns from fractions (0–1) to percent (0–100)
    by multiplying by 100. Use this exactly once in ETL.
    """
    vwc_cols = [c for c in df.columns if c.startswith("VWC_") and "_raw_" in c]
    if not vwc_cols:
        return df

    for c in vwc_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce") * 100.0

    logger.info(f"📏 Scaled {len(vwc_cols)} VWC columns ×100 to percent")
    return df


def add_swc_cylinder_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """Compute SWC cylinder volumes in L & gallons for each VWC sensor."""
    df_copy = df.copy()
    cyl_m3 = cylinder_volume_m3()
    cyl_l = cyl_m3 * 1000.0  # m³ → L
    cyl_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](cyl_l)

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col = f"VWC_{depth}_raw_{strip}_{loc}"
                if col not in df_copy.columns:
                    continue
                frac = pd.to_numeric(df_copy[col], errors="coerce") / 100.0
                df_copy[f"SWC_vol_L_{strip}_{loc}_{depth}"] = frac * cyl_l
                df_copy[f"SWC_vol_gal_{strip}_{loc}_{depth}"] = frac * cyl_gal

    return df_copy


# ============================= Growing-season summary ============================= #

def write_gseason_summary(year: int, df_daily: pd.DataFrame) -> None:
    """
    Build a 3-row growing-season summary from DAILY logger data and
    write it as:

        PARQUET_DIR / "summary" / "gseason" / f"{year}_gseason.parquet"

    Uses DEFAULT_GSEASON_PERIODS from config.py. Each row corresponds
    to one period and contains:
      - period_code
      - period_label (e.g. "Winter", "Early Growing")
      - period_start, period_end (ISO dates)
      - aggregated sensor columns
        * precip_* columns summed over the period
        * all other columns averaged over the period

    Cross-year periods (e.g. "11-01" → "04-30") pull the earlier part
    from the previous year's daily summary file if it exists.
    """
    if "timestamp" not in df_daily.columns:
        logger.warning(
            f"⚠️ write_gseason_summary({year}) skipped: no 'timestamp' column in daily frame"
        )
        return

    df_daily = df_daily.copy()
    df_daily["timestamp"] = pd.to_datetime(df_daily["timestamp"], errors="coerce")
    df_daily = df_daily.dropna(subset=["timestamp"])

    if df_daily.empty:
        logger.warning(f"⚠️ write_gseason_summary({year}) skipped: empty daily frame")
        return

    value_cols = [c for c in df_daily.columns if c != "timestamp"]
    agg_map = {
        col: ("sum" if col.startswith("precip") else "mean")
        for col in value_cols
    }

    daily_dir = Path(PARQUET_DIR) / "summary" / "daily"
    prev_daily: Optional[pd.DataFrame] = None
    prev_loaded_for_year: Optional[int] = None

    rows = []

    for period_code, meta in DEFAULT_GSEASON_PERIODS.items():
        label = meta.get("label", period_code)
        start_mmdd = meta["start"]
        end_mmdd = meta["end"]

        start_month = int(start_mmdd.split("-")[0])
        end_month = int(end_mmdd.split("-")[0])
        wraps_year = start_month > end_month  # e.g. 11-01 → 04-30

        if wraps_year:
            start_year = year - 1
            end_year = year
        else:
            start_year = year
            end_year = year

        start_ts = pd.Timestamp(f"{start_year}-{start_mmdd}")
        end_ts = (
            pd.Timestamp(f"{end_year}-{end_mmdd}")
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1)
        )

        window_parts: List[pd.DataFrame] = []

        # Previous-year component (for wrap-around periods)
        if wraps_year and start_year < year:
            prev_path = daily_dir / f"{start_year}_daily.parquet"
            if prev_path.exists():
                if prev_daily is None or prev_loaded_for_year != start_year:
                    prev_daily = pd.read_parquet(prev_path)
                    prev_daily["timestamp"] = pd.to_datetime(
                        prev_daily["timestamp"], errors="coerce"
                    )
                    prev_daily = prev_daily.dropna(subset=["timestamp"])
                    prev_loaded_for_year = start_year

                mask_prev = (prev_daily["timestamp"] >= start_ts) & (
                    prev_daily["timestamp"] <= end_ts
                )
                window_parts.append(prev_daily.loc[mask_prev])
            else:
                logger.warning(
                    f"⚠️ No previous-year daily summary found at {prev_path} "
                    f"for gseason period {period_code} in {year}; "
                    f"using only {year} data."
                )

        # Current-year component
        mask_cur = (df_daily["timestamp"] >= start_ts) & (
            df_daily["timestamp"] <= end_ts
        )
        window_parts.append(df_daily.loc[mask_cur])

        if window_parts:
            window = pd.concat(window_parts, ignore_index=True)
        else:
            window = pd.DataFrame(columns=df_daily.columns)

        if window.empty:
            logger.warning(
                f"⚠️ No daily rows for gseason period {period_code} in {year} "
                f"[{start_ts.date()} → {end_ts.date()}]; filling with NaN."
            )
            stats = {col: np.nan for col in value_cols}
        else:
            # Aggregate and round to 3 decimals so gseason matches other summaries
            stats_series = window[value_cols].agg(agg_map)
            stats_series = stats_series.round(3)
            stats = stats_series.to_dict()

        rows.append(
            {
                "period_code": period_code,
                "period_label": label,
                "period_start": start_ts.date().isoformat(),
                "period_end": end_ts.date().isoformat(),
                **stats,
            }
        )

    out_df = pd.DataFrame(rows)

    # Final safety: ensure all numeric columns are rounded to 3 decimals
    num_cols = out_df.select_dtypes(include=["float", "int"]).columns
    if len(num_cols) > 0:
        out_df[num_cols] = out_df[num_cols].round(3)

    out_dir = Path(PARQUET_DIR) / "summary" / "gseason"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{year}_gseason.parquet"
    out_df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"✅ Summary gseason (DEFAULT periods): {out_path.name}")


# ============================= Aggregation (loggers) ============================= #

def aggregate_and_write(year: int, df: pd.DataFrame) -> None:
    """
    Given cleaned df (indexed on timestamp), write:
      - raw‐logger + raw‐logger_ratios
      - fixed‐frequency summaries + *_ratios
      - gseason summary built from daily data (using DEFAULT_GSEASON_PERIODS)
    """
    year_dir = Path(PARQUET_DIR) / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    # 1) Raw + ratios
    raw_path = year_dir / f"{year}_raw_logger.parquet"
    ratio_path = year_dir / f"{year}_raw_logger_ratios.parquet"
    df.reset_index().to_parquet(raw_path, index=False, compression="snappy")
    calculate_ratios(df).reset_index().to_parquet(
        ratio_path, index=False, compression="snappy"
    )
    logger.info(f"✅ Wrote raw & ratio: {raw_path.name}, {ratio_path.name}")

    # 2) Fixed‐frequency summaries
    sensor_prefixes = ("VWC_", "T_", "EC_", "SWC_")
    sensor_cols = [
        c for c in df.columns if any(c.startswith(pref) for pref in sensor_prefixes)
    ]
    summary_base = Path(PARQUET_DIR) / "summary"

    for freq, code in GRANULARITIES:
        if code is None:
            continue

        out_dir = summary_base / freq
        out_dir.mkdir(parents=True, exist_ok=True)

        agg_map = {
            col: "sum" if col.startswith("precip") else "mean"
            for col in df.columns
        }

        # a) raw summary
        df_s = df.resample(code).agg(agg_map).round(3)
        df_s = df_s.dropna(subset=sensor_cols, how="all").reset_index()

        fn_raw = f"{year}_{freq}.parquet"
        df_s.to_parquet(out_dir / fn_raw, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq}: {fn_raw}")

        # 👉 build DEFAULT gseason summary off the daily frame
        if freq == "daily":
            write_gseason_summary(year, df_s)

        # b) summary ratios
        df_s_ratio = calculate_ratios(df_s.set_index("timestamp"))
        fn_ratio = f"{year}_{freq}_ratios.parquet"
        df_s_ratio.reset_index().to_parquet(
            out_dir / fn_ratio, index=False, compression="snappy"
        )
        logger.info(f"✅ Summary {freq} ratios: {fn_ratio}")


# ============================= Weather (CoAgMet) ============================= #

def clean_weather_frame(dfw: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare CoAgMet 5 min weather for resampling:
      - parse timestamp → naive America/Denver
      - coerce precip_in, treat -999 as NA, fill NA with 0
      - clip negative increments to 0
      - warn on implausible spikes
    """
    df_copy = dfw.copy()
    df_copy["timestamp"] = normalize_timestamp_series(df_copy["timestamp"])

    df_copy["precip_in"] = pd.to_numeric(df_copy["precip_in"], errors="coerce")
    df_copy.loc[df_copy["precip_in"] == -999, "precip_in"] = np.nan
    df_copy["precip_in"] = df_copy["precip_in"].fillna(0.0).clip(lower=0.0)

    spike = df_copy["precip_in"].max()
    if pd.notna(spike) and spike > 1.5:
        logger.warning(f"⚠️ CoAgMet 5 min precip spike detected: {spike:.2f} in")

    return df_copy


# ============================= Orchestration ============================= #

def generate_summaries(years: List[int]) -> None:
    """
    Run the full ETL for each year in `years`:
      - logger data → merge, clean, aggregate (+ DEFAULT gseason)
      - weather data → fetch, clean, aggregate
    """
    for year in years:
        logger.info(f"🌱 Starting ETL for {year}")
        df = merge_all_loggers(year)
        if df is None or df.empty:
            logger.error(
                f"❌ No logger .dat data for {year}, skipping logger summaries."
            )
        else:
            # normalize and drop invalid timestamps
            df["timestamp"] = normalize_timestamp_series(df["timestamp"])
            df = df.dropna(subset=["timestamp"])

            df = replace_bad_values(df)
            df = scale_vwc_to_percent(df)

            # mask VWC > 150%
            vwc_cols = [c for c in df.columns if c.startswith("VWC_") and "_raw_" in c]
            outliers = int(
                pd.concat(
                    [pd.to_numeric(df[c], errors="coerce") for c in vwc_cols],
                    axis=1,
                )
                .gt(150.0)
                .sum()
                .sum()
            )
            if outliers > 0:
                logger.warning(f"⚠️ {outliers} VWC>150% → NaN")
            for c in vwc_cols:
                series = pd.to_numeric(df[c], errors="coerce")
                df[c] = series.mask(series > 150.0)

            df = convert_soil_t_to_fahrenheit(df)
            df = add_swc_cylinder_volumes(df)
            df = df.set_index("timestamp").sort_index()
            aggregate_and_write(year, df)

        # weather data
        try:
            dfw = fetch_weather_data(year)
        except Exception as e:
            logger.error(f"❌ fetch_weather_data({year}) failed: {e}")
            continue

        required_cols = {"timestamp", "precip_in", "temp_air_degF"}
        missing = required_cols - set(dfw.columns)
        if missing:
            logger.error(
                f"❌ fetch_weather_data({year}) missing columns: {sorted(missing)}"
            )
            continue

        dfw_clean = clean_weather_frame(dfw).set_index("timestamp").sort_index()
        dfw_clean["precip_mm"] = dfw_clean["precip_in"].apply(
            UNIT_CONVERSIONS["us_to_metric"]["precip"]
        )
        dfw_clean["temp_air_degC"] = dfw_clean["temp_air_degF"].apply(
            UNIT_CONVERSIONS["us_to_metric"]["temp"]
        )

        weather_base = Path(PARQUET_DIR) / "summary" / "weather"
        for freq, code in GRANULARITIES:
            if code is None:
                continue
            out_dir = weather_base / freq
            out_dir.mkdir(parents=True, exist_ok=True)
            agg_map = {
                col: "sum" if col.startswith("precip") else "mean"
                for col in dfw_clean.columns
            }
            dfr = dfw_clean.resample(code).agg(agg_map).round(3).reset_index()
            fn = f"{year}_{freq}.parquet"
            dfr.to_parquet(out_dir / fn, index=False, compression="snappy")
            logger.info(f"✅ Weather {freq} for {year}")

    logger.info("🎉 ETL complete.")


if __name__ == "__main__":
    os.makedirs(PARQUET_DIR, exist_ok=True)
    generate_summaries(YEARS)