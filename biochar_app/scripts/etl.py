#!/usr/bin/env python3
"""
Full ETL including growing-season (gseason) summaries:
  - Read all .dat logger files per year (in data-raw/datfiles_{year})
  - Normalize logger timestamps as timezone-naive Mountain local wall time
  - Mask extreme placeholders → NaN (covers typical logger codes like -9999/6999/9999)
  - Convert VWC fractions → percent (×100) deterministically
  - Mask VWC > 150% → NaN
  - Compute SWC cylinder volumes & logger‐ratios
  - Compute ΔT (biochar − control) and ΔSWC volumes (biochar − control)
  - Resample to 15 min / hourly / daily / monthly; write Parquet + Parquet_ratios
  - Build DEFAULT gseason summaries from daily data (with cross-year support)
  - Fetch CoAgMet 5 min weather; clean precip increments; write resampled Parquet
  - Build bulk-download ZIPs for 15-min logger and weather data
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

import numpy as np
import pandas as pd
from pandas import Series

from biochar_app.scripts.get_weather_data import fetch_weather_data
from biochar_app.scripts.config import (
    DATA_RAW_DIR,
    PARQUET_DIR,
    YEARS,
    STRIPS,
    LOGGER_LOCATIONS,
    VALUE_COLS_2024_PLUS,
    GRANULARITIES,
    UNIT_CONVERSIONS,
    cylinder_volume_m3,
    DEFAULT_GSEASON_PERIODS,
    # CoAgMet URL bits for readme + reproducibility
    COLLECT_PERIOD,
    COAG_STATION,
    COAGMET_VARIABLE_MAP,
    DEFAULT_TIMEZONE,
    DEFAULT_UNITS,
)

from process_data import calculate_ratios

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Typical Campbell / logger placeholder codes are often in the +/- 9999 range.
# Using 999_999 was too high and allowed placeholders through, which then got scaled
# and counted as VWC>150%. This default catches -9999, 6999, 9999, etc.
DEFAULT_BAD_VALUE_THRESHOLD = 10_000.0

NAN = float("nan")
# ---------------------------------------------------------------------------
# Timestamp normalization
# ---------------------------------------------------------------------------

def ts_to_iso_date(ts: Optional[pd.Timestamp]) -> str:
    """
    Convert Timestamp-like -> 'YYYY-MM-DD' safely for type checkers.
    Returns '' if ts is None/NaT.
    """
    if ts is None or pd.isna(ts):
        return ""
    # At this point, it's a real Timestamp.
    return ts.strftime("%Y-%m-%d")

def normalize_logger_timestamp_series(ts: Series) -> Series:
    """
    Normalize logger timestamps.

    Project rule: logger timestamps are treated as *timezone-naive Mountain local wall time*.
    That means we DO NOT tz-localize them (avoids DST ambiguous-hour issues like 01:xx on fall-back).
    """
    s = ts.astype("string").str.strip()
    out = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    return out


def normalize_weather_timestamp_series(
    ts: pd.Series,
    tz: str = "America/Denver",
) -> pd.Series:
    """
    Normalize CoAgMet timestamps:
      - parse
      - localize naive to tz, shifting DST gaps forward
      - convert any tz-aware to tz
      - drop tz info (timezone-naive)
    """
    s = pd.to_datetime(ts, errors="coerce")
    tzinfo = s.dt.tz
    if tzinfo is None:
        s = s.dt.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward")
    else:
        s = s.dt.tz_convert(tz)
    return s.dt.tz_localize(None)


# ---------------------------------------------------------------------------
# Strip pairing assumptions for biochar vs. non-biochar comparisons
#   - First element = "treated" (biochar)
#   - Second element = "control" (no biochar)
# ---------------------------------------------------------------------------
STRIP_PAIRS = [
    ("S1", "S2"),
    ("S3", "S4"),
]

# ---------------------------------------------------------------------------
# Download directories for bulk 15-min ZIPs (loggers + weather)
#   These live alongside PARQUET_DIR in a sibling "downloads" folder.
# ---------------------------------------------------------------------------
DOWNLOADS_BASE_DIR = Path(PARQUET_DIR).parent / "downloads"
LOGGER_DOWNLOADS_DIR = DOWNLOADS_BASE_DIR / "loggers"
WEATHER_DOWNLOADS_DIR = DOWNLOADS_BASE_DIR / "weather"
LOGGER_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
WEATHER_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)


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

    df = pd.read_csv(
        datfile,
        header=None,
        skiprows=4,
        names=["timestamp", "RECORD"] + VALUE_COLS_2024_PLUS,
        na_values=["", "NA", "NAN"],
    ).drop(columns=["RECORD"], errors="ignore")

    if df.empty or "timestamp" not in df.columns:
        return None

    # Preserve raw timestamp text for logging
    raw_ts = df["timestamp"].copy()

    # Normalize logger timestamps as *naive wall time*
    norm_ts = normalize_logger_timestamp_series(raw_ts)

    df["timestamp"] = norm_ts

    bad_mask = df["timestamp"].isna()
    n_nat = int(bad_mask.sum())

    if n_nat:
        N = 10
        bad_idx = df.index[bad_mask][:N].tolist()

        examples: List[str] = []
        for i in bad_idx:
            raw_val: Any = raw_ts.iloc[i] if i < len(raw_ts) else None
            norm_val: Any = norm_ts.iloc[i] if i < len(norm_ts) else None
            examples.append(f"row={int(i)} raw={raw_val!r} normalized={norm_val!r}")

        logger.warning(
            f"⚠️ {n_nat} NaT timestamps in {name} ({datfile.name}). "
            f"Examples: " + "; ".join(examples)
        )
        df = df.loc[~bad_mask].copy()

    if df.empty:
        return None

    start = pd.Timestamp(year=year, month=1, day=1)
    end = pd.Timestamp(year=year + 1, month=1, day=1)
    df = df.loc[(df["timestamp"] >= start) & (df["timestamp"] < end)].copy()

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


def replace_bad_values(df: pd.DataFrame, threshold: float = DEFAULT_BAD_VALUE_THRESHOLD) -> pd.DataFrame:
    """
    Mask placeholder / sentinel values as NaN in numeric columns.

    Important: logger placeholder codes are commonly around +/-9999 (or similar).
    Using a huge threshold (e.g., 999_999) allows these through; after VWC scaling,
    they become enormous and get flagged as VWC>150%.
    """
    df = df.copy()
    for col in df.columns:
        if col == "timestamp":
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].mask(df[col].abs() >= threshold, NAN)

    logger.info(f"🧹 Replaced extreme placeholders with NaN (|x| ≥ {threshold:g})")
    return df


def scale_vwc_to_percent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert ALL raw VWC columns from fractions (0–1) to percent (0–100)
    by multiplying by 100. Use this exactly once in ETL.
    """
    df = df.copy()
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

    logger.info("💧 Added SWC cylinder volumes (L & gallons) per sensor")
    return df_copy


def add_temperature_differences(df: pd.DataFrame) -> pd.DataFrame:
    """Add ΔT columns (treated − control) in °F."""
    df_copy = df.copy()
    new_cols = 0

    for treated, control in STRIP_PAIRS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col_treated = f"T_{depth}_raw_{treated}_{loc}"
                col_control = f"T_{depth}_raw_{control}_{loc}"
                if col_treated not in df_copy.columns or col_control not in df_copy.columns:
                    continue

                diff_col = f"Tdiff_{depth}_{treated}_{control}_{loc}"
                df_copy[diff_col] = (
                    pd.to_numeric(df_copy[col_treated], errors="coerce")
                    - pd.to_numeric(df_copy[col_control], errors="coerce")
                )
                new_cols += 1

    logger.info(f"🌡 Added {new_cols} ΔT columns (biochar − control)" if new_cols else
                "🌡 No ΔT columns added (required T_*_raw_* columns missing)")
    return df_copy


def add_swc_differences(df: pd.DataFrame) -> pd.DataFrame:
    """Add ΔSWC volume columns (gallons & liters) (treated − control)."""
    df_copy = df.copy()
    new_cols = 0

    for treated, control in STRIP_PAIRS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col_treated_gal = f"SWC_vol_gal_{treated}_{loc}_{depth}"
                col_control_gal = f"SWC_vol_gal_{control}_{loc}_{depth}"
                col_treated_L = f"SWC_vol_L_{treated}_{loc}_{depth}"
                col_control_L = f"SWC_vol_L_{control}_{loc}_{depth}"

                if col_treated_gal in df_copy.columns and col_control_gal in df_copy.columns:
                    diff_col_gal = f"SWCdiff_gal_{treated}_{control}_{loc}_{depth}"
                    df_copy[diff_col_gal] = (
                        pd.to_numeric(df_copy[col_treated_gal], errors="coerce")
                        - pd.to_numeric(df_copy[col_control_gal], errors="coerce")
                    )
                    new_cols += 1

                if col_treated_L in df_copy.columns and col_control_L in df_copy.columns:
                    diff_col_L = f"SWCdiff_L_{treated}_{control}_{loc}_{depth}"
                    df_copy[diff_col_L] = (
                        pd.to_numeric(df_copy[col_treated_L], errors="coerce")
                        - pd.to_numeric(df_copy[col_control_L], errors="coerce")
                    )
                    new_cols += 1

    logger.info(f"💧 Added {new_cols} ΔSWC volume columns (biochar − control)" if new_cols else
                "💧 No ΔSWC columns added (required SWC_vol_* columns missing)")
    return df_copy


# ============================= Growing-season summary ============================= #

def write_gseason_summary(year: int, df_daily: pd.DataFrame) -> None:
    """
    Build a 3-row growing-season summary from DAILY logger data and write:
        PARQUET_DIR / "summary" / "gseason" / f"{year}_gseason.parquet"

    Supports BOTH config formats:

    A) dict-of-tuples:
        {"Q1_Winter": ("11-01","02-28"), ...}

    B) dict-of-dicts:
        {"Q1_Winter": {"start":"11-01","end":"02-28","label":"Winter"}, ...}
    """
    if "timestamp" not in df_daily.columns:
        logger.warning(f"⚠️ write_gseason_summary({year}) skipped: no 'timestamp' column")
        return

    df_daily = df_daily.copy()
    df_daily["timestamp"] = pd.to_datetime(df_daily["timestamp"], errors="coerce")
    df_daily = df_daily.dropna(subset=["timestamp"])
    if df_daily.empty:
        logger.warning(f"⚠️ write_gseason_summary({year}) skipped: empty daily frame")
        return

    value_cols = [c for c in df_daily.columns if c != "timestamp"]
    agg_map = {col: ("sum" if col.startswith("precip") else "mean") for col in value_cols}

    daily_dir = Path(PARQUET_DIR) / "summary" / "daily"
    prev_daily: Optional[pd.DataFrame] = None
    prev_loaded_for_year: Optional[int] = None

    def _unpack_period(period_code: str, meta):
        if isinstance(meta, (tuple, list)) and len(meta) == 2:
            start_mmdd, end_mmdd = meta[0], meta[1]
            label = period_code
            return label, start_mmdd, end_mmdd

        if isinstance(meta, dict):
            start_mmdd = meta.get("start")
            end_mmdd = meta.get("end")
            label = meta.get("label", period_code)
            if start_mmdd and end_mmdd:
                return label, start_mmdd, end_mmdd

        raise ValueError(
            f"DEFAULT_GSEASON_PERIODS[{period_code!r}] must be "
            f"('MM-DD','MM-DD') or {{'start':'MM-DD','end':'MM-DD','label':...}}; got {meta!r}"
        )

    rows = []

    for period_code, meta in DEFAULT_GSEASON_PERIODS.items():
        label, start_mmdd, end_mmdd = _unpack_period(period_code, meta)

        start_month = int(start_mmdd.split("-")[0])
        end_month = int(end_mmdd.split("-")[0])
        wraps_year = start_month > end_month

        start_year = year - 1 if wraps_year else year
        end_year = year

        start_ts = pd.Timestamp(f"{start_year}-{start_mmdd}")
        end_ts = pd.Timestamp(f"{end_year}-{end_mmdd}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        window_parts: List[pd.DataFrame] = []

        if wraps_year:
            prev_path = daily_dir / f"{start_year}_daily.parquet"
            if prev_path.exists():
                if prev_daily is None or prev_loaded_for_year != start_year:
                    prev_daily = pd.read_parquet(prev_path)
                    prev_daily["timestamp"] = pd.to_datetime(prev_daily["timestamp"], errors="coerce")
                    prev_daily = prev_daily.dropna(subset=["timestamp"])
                    prev_loaded_for_year = start_year

                mask_prev = (prev_daily["timestamp"] >= start_ts) & (prev_daily["timestamp"] <= end_ts)
                window_parts.append(prev_daily.loc[mask_prev])
            else:
                logger.warning(
                    f"⚠️ Missing prev-year daily parquet {prev_path.name} for {period_code} ({year}); "
                    f"using only current-year component."
                )

        mask_cur = (df_daily["timestamp"] >= start_ts) & (df_daily["timestamp"] <= end_ts)
        window_parts.append(df_daily.loc[mask_cur])

        window = pd.concat(window_parts, ignore_index=True) if window_parts else pd.DataFrame(columns=df_daily.columns)

        if window.empty:
            logger.warning(
                f"⚠️ No daily rows for gseason {period_code} in {year} "
                f"[{start_ts.date()} → {end_ts.date()}]; filling NaN."
            )
            stats = {col: np.nan for col in value_cols}
        else:
            stats_series = window[value_cols].agg(agg_map).round(3)
            stats = stats_series.to_dict()

        rows.append(
            {
                "period_code": period_code,
                "period_label": label,
                "period_start": ts_to_iso_date(start_ts),
                "period_end": ts_to_iso_date(end_ts),
                **stats,
            }
        )

    out_df = pd.DataFrame(rows)
    num_cols = out_df.select_dtypes(include=["float", "int"]).columns
    if len(num_cols) > 0:
        out_df[num_cols] = out_df[num_cols].round(3)

    out_dir = Path(PARQUET_DIR) / "summary" / "gseason"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{year}_gseason.parquet"
    out_df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"✅ Summary gseason (DEFAULT periods): {out_path.name}")


# ============================= Bulk-download helpers ============================= #

def write_logger_download_zip(year: int, df_15min: pd.DataFrame) -> None:
    """Build per-year ZIP for 15-min logger data (one CSV per logger) + README."""
    zip_path = LOGGER_DOWNLOADS_DIR / f"Biochar_Loggers_15min_{year}_USunits.zip"

    df = df_15min.copy()
    if "timestamp" not in df.columns:
        df = df.reset_index()
    if "timestamp" not in df.columns:
        raise ValueError("write_logger_download_zip: df_15min must have 'timestamp' as index or column")

    readme_lines: List[str] = [
        "Biochar Fruita CSU Experiment – Logger 15-minute Data",
        f"Year: {year}",
        "",
        "Files in this ZIP",
        "-----------------",
        "  One CSV per datalogger location (e.g., S1T, S1M, ..., S4B).",
        "  Each CSV contains a 15-minute time series for all depths (1, 2, 3).",
        "",
        "CSV files:",
    ]

    from io import StringIO
    from zipfile import ZipFile

    with ZipFile(zip_path, mode="w") as zf:
        for strip in STRIPS:
            for loc in LOGGER_LOCATIONS:
                tag = f"{strip}{loc}"
                suffix = f"_{strip}_{loc}"

                cols = [c for c in df.columns if c == "timestamp" or c.endswith(suffix)]
                if len(cols) <= 1:
                    continue

                sub = df[cols].copy()
                csv_name = f"{tag}_15min_{year}_USunits.csv"

                buf = StringIO()
                sub.to_csv(buf, index=False)
                zf.writestr(csv_name, buf.getvalue())

                readme_lines.append(f"  - {csv_name}: 15-min data for logger {tag}")

        readme_lines.extend(
            [
                "",
                "Column naming convention",
                "------------------------",
                "  timestamp                         : local time (America/Denver), 15-min step (timezone-naive)",
                "  VWC_<depth>_raw_<strip>_<loc>     : volumetric water content (%)",
                "  T_<depth>_raw_<strip>_<loc>       : soil temperature (°F)",
                "  EC_<depth>_raw_<strip>_<loc>      : electrical conductivity (dS/m)",
                "  SWC_vol_L_<strip>_<loc>_<depth>   : soil water content volume (liters)",
                "  SWC_vol_gal_<strip>_<loc>_<depth> : soil water content volume (gallons)",
                "",
                "Notes",
                "-----",
                "  - Placeholder/sentinel values (e.g., -9999/9999) have been converted to NaN.",
                "  - VWC values above 150% have been masked to NaN.",
                "  - Cross-strip comparison variables (ΔT, ΔSWC, ratio columns, etc.)",
                "    are not included in these per-logger CSVs.",
            ]
        )

        zf.writestr(f"README_Logger_15min_{year}.txt", "\n".join(readme_lines))

    logger.info(f"📦 Wrote logger download ZIP: {zip_path.name}")


def write_weather_download_zip(
    year: int,
    df_15min: pd.DataFrame,
    download_url: str = "",
    builder_url: str = "",
) -> None:
    """Build per-year ZIP for 15-min weather data + README."""
    zip_path = WEATHER_DOWNLOADS_DIR / f"Biochar_Weather_15min_{year}_USunits.zip"

    df = df_15min.copy()
    if "timestamp" not in df.columns:
        df = df.reset_index()
    if "timestamp" not in df.columns:
        raise ValueError("write_weather_download_zip: df_15min must have 'timestamp' as index or column")

    from io import StringIO
    from zipfile import ZipFile

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)

    readme_lines: List[str] = [
        f"Biochar Fruita CSU Experiment - 15-min Weather Data ({year})",
        "",
        "Source:",
        f"  - Direct CoAgMet-style download URL: {download_url or '[ADD_DOWNLOAD_URL_HERE]'}",
        f"  - CoAgMet builder page (construct custom URLs): {builder_url or 'https://coagmet.colostate.edu/data/url-builder'}",
        "",
        "Files in this ZIP",
        "-----------------",
        f"  - weather_15min_{year}_USunits.csv : 15-minute time series",
        "",
        "Notes:",
        "  - Timestamps are naive datetimes interpreted as America/Denver local time.",
        "  - Precipitation increments are clipped at 0; missing codes (-999) treated as NaN.",
    ]

    with ZipFile(zip_path, mode="w") as zf:
        zf.writestr(f"weather_15min_{year}_USunits.csv", csv_buf.getvalue())
        zf.writestr(f"README_Weather_15min_{year}.txt", "\n".join(readme_lines))

    logger.info(f"📦 Wrote weather download ZIP: {zip_path.name}")


# ============================= Aggregation (loggers) ============================= #

def aggregate_and_write(year: int, df: pd.DataFrame) -> None:
    """
    Given cleaned df (indexed on timestamp), write:
      - raw‐logger + raw‐logger_ratios
      - fixed‐frequency summaries + *_ratios
      - gseason summary built from daily data (using DEFAULT_GSEASON_PERIODS)
      - bulk-download ZIP for 15-min logger data
    """
    year_dir = Path(PARQUET_DIR) / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    raw_path = year_dir / f"{year}_raw_logger.parquet"
    ratio_path = year_dir / f"{year}_raw_logger_ratios.parquet"
    df.reset_index().to_parquet(raw_path, index=False, compression="snappy")
    calculate_ratios(df).reset_index().to_parquet(ratio_path, index=False, compression="snappy")
    logger.info(f"✅ Wrote raw & ratio: {raw_path.name}, {ratio_path.name}")

    sensor_prefixes = ("VWC_", "T_", "EC_", "SWC_", "Tdiff_", "SWCdiff_")
    sensor_cols = [c for c in df.columns if any(c.startswith(pref) for pref in sensor_prefixes)]
    summary_base = Path(PARQUET_DIR) / "summary"

    for freq, code in GRANULARITIES:
        if code is None:
            continue

        out_dir = summary_base / freq
        out_dir.mkdir(parents=True, exist_ok=True)

        agg_map = {col: "sum" if col.startswith("precip") else "mean" for col in df.columns}

        df_s = df.resample(code).agg(agg_map).round(3)
        df_s = df_s.dropna(subset=sensor_cols, how="all").reset_index()

        fn_raw = f"{year}_{freq}.parquet"
        df_s.to_parquet(out_dir / fn_raw, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq}: {fn_raw}")

        if freq == "daily":
            write_gseason_summary(year, df_s)

        if freq == "15min":
            write_logger_download_zip(year, df_s.set_index("timestamp"))

        df_s_ratio = calculate_ratios(df_s.set_index("timestamp"))
        fn_ratio = f"{year}_{freq}_ratios.parquet"
        df_s_ratio.reset_index().to_parquet(out_dir / fn_ratio, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq} ratios: {fn_ratio}")


# ============================= Weather (CoAgMet) ============================= #

def clean_weather_frame(dfw: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare CoAgMet 5 min weather for resampling:
      - normalize timestamps (DST-aware) → naive America/Denver
      - coerce precip_in, treat -999 as NA, fill NA with 0
      - clip negative increments to 0
      - warn on implausible spikes
    """
    df_copy = dfw.copy()
    df_copy["timestamp"] = normalize_weather_timestamp_series(df_copy["timestamp"])

    df_copy["precip_in"] = pd.to_numeric(df_copy["precip_in"], errors="coerce")
    df_copy.loc[df_copy["precip_in"] == -999, "precip_in"] = np.nan
    df_copy["precip_in"] = df_copy["precip_in"].fillna(0.0).clip(lower=0.0)

    spike = df_copy["precip_in"].max()
    if pd.notna(spike) and spike > 1.5:
        logger.warning(f"⚠️ CoAgMet 5 min precip spike detected: {spike:.2f} in")

    # Drop rows where timestamp went NaT due to ambiguous/unparseable times
    bad_ts = df_copy["timestamp"].isna().sum()
    if bad_ts:
        logger.warning(f"⚠️ Dropping {int(bad_ts)} weather rows with invalid/ambiguous timestamps")
        df_copy = df_copy.dropna(subset=["timestamp"])

    return df_copy


# ============================= Orchestration ============================= #

def generate_summaries(years: List[int]) -> None:
    """
    Run the full ETL for each year in `years`:
      - logger data → merge, clean, aggregate (+ DEFAULT gseason)
      - weather data → fetch, clean, aggregate (+ 15-min download ZIP)
    """
    for year in years:
        logger.info(f"🌱 Starting ETL for {year}")

        df = merge_all_loggers(year)
        if df is None or df.empty:
            logger.error(f"❌ No logger .dat data for {year}, skipping logger summaries.")
        else:
            # timestamps are already datetime from read_logger_data/merge_all_loggers
            df = df.dropna(subset=["timestamp"])

            df = replace_bad_values(df, threshold=DEFAULT_BAD_VALUE_THRESHOLD)
            df = scale_vwc_to_percent(df)

            # mask VWC > 150%
            vwc_cols = [c for c in df.columns if c.startswith("VWC_") and "_raw_" in c]
            if vwc_cols:
                vwc_block = pd.concat([pd.to_numeric(df[c], errors="coerce") for c in vwc_cols], axis=1)
                outliers = int(vwc_block.gt(150.0).sum().sum())
                if outliers > 0:
                    logger.warning(f"⚠️ {outliers} VWC>150% → NaN")
                for c in vwc_cols:
                    s = pd.to_numeric(df[c], errors="coerce")
                    df[c] = s.mask(s > 150.0)

            df = convert_soil_t_to_fahrenheit(df)
            df = add_swc_cylinder_volumes(df)
            df = add_temperature_differences(df)
            df = add_swc_differences(df)

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
            logger.error(f"❌ fetch_weather_data({year}) missing columns: {sorted(missing)}")
            continue

        dfw_clean = clean_weather_frame(dfw).set_index("timestamp").sort_index()

        dfw_clean["precip_mm"] = dfw_clean["precip_in"].apply(UNIT_CONVERSIONS["us_to_metric"]["precip"])
        dfw_clean["temp_air_degC"] = dfw_clean["temp_air_degF"].apply(UNIT_CONVERSIONS["us_to_metric"]["temp"])

        weather_base = Path(PARQUET_DIR) / "summary" / "weather"
        dfw_15min_for_zip: Optional[pd.DataFrame] = None

        for freq, code in GRANULARITIES:
            if code is None:
                continue
            out_dir = weather_base / freq
            out_dir.mkdir(parents=True, exist_ok=True)

            agg_map = {col: "sum" if col.startswith("precip") else "mean" for col in dfw_clean.columns}
            dfr = dfw_clean.resample(code).agg(agg_map).round(3).reset_index()

            fn = f"{year}_{freq}.parquet"
            dfr.to_parquet(out_dir / fn, index=False, compression="snappy")
            logger.info(f"✅ Weather {freq} for {year}")

            if freq == "15min":
                dfw_15min_for_zip = dfr

        if dfw_15min_for_zip is not None:
            start_ts = pd.Timestamp(f"{year}-01-01 00:00", tz=DEFAULT_TIMEZONE)
            year_end = pd.Timestamp(f"{year}-12-31 23:59", tz=DEFAULT_TIMEZONE)
            now_ts = pd.Timestamp.now(tz=DEFAULT_TIMEZONE).floor("min")
            end_ts = min(year_end, now_ts)

            start_iso = start_ts.strftime("%Y-%m-%dT%H:%M")
            end_iso = end_ts.strftime("%Y-%m-%dT%H:%M")

            fields_param = ",".join(COAGMET_VARIABLE_MAP.keys())
            coag_download_url = (
                f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
                f"?header=yes"
                f"&fields={fields_param}"
                f"&from={start_iso}&to={end_iso}"
                f"&tz=co&units={DEFAULT_UNITS}&dateFmt=iso"
            )
            builder_url = "https://coagmet.colostate.edu/data/url-builder"

            write_weather_download_zip(
                year,
                dfw_15min_for_zip,
                download_url=coag_download_url,
                builder_url=builder_url,
            )

    logger.info("🎉 ETL complete.")


def resolve_target_year(cli_year: Optional[int] = None) -> int:
    """
    Pick which year to run.
    Priority:
      1) explicit CLI --year
      2) max(YEARS) from config (most recent configured year)
      3) current calendar year as fallback
    """
    if cli_year is not None:
        return int(cli_year)
    try:
        return int(max(YEARS))
    except Exception:
        return int(datetime.now().year)


if __name__ == "__main__":
    os.makedirs(PARQUET_DIR, exist_ok=True)
    generate_summaries(YEARS)