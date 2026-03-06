#!/usr/bin/env python3
"""
etl.py

Full ETL including growing-season (gseason) summaries:
  - Read all .dat logger files per year (in data-raw/datfiles_{year})
  - Normalize logger timestamps as timezone-naive Mountain local wall time
  - Apply per-logger clock corrections (MST/MDT jumps, resets) using LOGGER_CLOCK_CORRECTIONS
  - Backfill late-2023 BattV_Min data that lives in datfiles_2024/*_Table1.dat
      * For year=2023, we also read datfiles_2024/{tag}_Table1.dat and keep rows < 2024-01-01
      * If you created *_late2023_withBattV.dat files inside datfiles_2023, we also read those
  - Mask extreme placeholders → NaN
  - Convert VWC fractions → percent (×100) deterministically
  - Convert soil temperature (°C → °F) deterministically
  - Apply site-specific value bounds (centralized in thresholds.py) + produce a report
  - Compute SWC cylinder volumes & logger‐ratios
  - Compute ΔT (biochar − control) and ΔSWC volumes (biochar − control)
  - Resample to 15 min / hourly / daily / monthly; write Parquet + Parquet_ratios
  - Build DEFAULT gseason summaries from daily data (with cross-year support)
  - Fetch CoAgMet 5 min weather; clean precip increments; write resampled Parquet
  - Build bulk-download ZIPs for 15-min logger and weather data
"""

from __future__ import annotations

import os
import csv
import math
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, cast, Iterable
from datetime import datetime

import pandas as pd
from pandas import Series

from biochar_app.scripts.get_weather_data import fetch_weather_data
from biochar_app.scripts.config import (
    DATA_RAW_DIR,
    PARQUET_DIR,
    LOGGER_DOWNLOADS_DIR,
    WEATHER_DOWNLOADS_DIR,
    YEARS,
    STRIPS,
    LOGGER_LOCATIONS,
    GRANULARITIES,
    UNIT_CONVERSIONS,
    cylinder_volume_m3,
    DEFAULT_GSEASON_PERIODS,
    COLLECT_PERIOD,
    COAG_STATION,
    COAGMET_VARIABLE_MAP,
    DEFAULT_TIMEZONE,
    DEFAULT_UNITS,
    DEFAULT_BAD_VALUE_THRESHOLD,
)

from biochar_app.scripts.type_utils import NAN, df_agg

# Centralized bounds + reporting
from biochar_app.config.thresholds import apply_value_bounds as enforce_value_bounds

from biochar_app.scripts.process_data import calculate_ratios

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

LOGGER_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
WEATHER_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logger clock corrections
# ---------------------------------------------------------------------------
# Semantics:
#   For a given logger tag, each tuple is (start_timestamp, add_minutes).
#   For rows with timestamp >= start_timestamp, we add add_minutes to the raw timestamp.
#
# This is intended to “stitch” together known clock mode changes (MST/MDT) and clock resets
# so that the final timeline is consistent for analysis.
#
# NOTE: This mapping was derived from your clock_state_timeline CSVs and *compressed*
# to only the points where the correction changes.
LOGGER_CLOCK_CORRECTIONS: dict[str, list[tuple[str, int]]] = {
    "S1B": [("2024-02-23 15:30:00", 60)],
    "S1M": [("2024-02-23 15:15:00", 60)],
    "S1T": [("2024-02-23 10:45:00", 60)],
    "S2B": [("2024-02-23 15:45:00", 60)],
    "S2M": [("2026-02-23 08:45:00", 60)],
    "S2T": [("2024-04-02 16:00:00", -60)],
    "S3B": [("2023-04-28 10:45:00", -60), ("2024-03-28 17:15:00", -120), ("2026-02-23 08:45:00", -60)],
    "S3M": [("2023-09-04 10:30:00", -60), ("2024-07-07 06:30:00", -120), ("2025-01-16 23:45:00", -180)],
    "S3T": [("2024-02-23 11:30:00", 60)],
    "S4B": [("2023-09-04 10:30:00", -60), ("2023-09-20 18:30:00", -120), ("2026-02-23 09:00:00", -60)],
    "S4M": [("2024-02-23 14:30:00", 60)],
    "S4T": [("2024-02-23 11:45:00", 60)],
}


def apply_logger_clock_corrections(ts: pd.Series, logger_tag: str) -> pd.Series:
    """
    Apply piecewise clock corrections (add minutes) to a timestamp series.
    """
    pts = LOGGER_CLOCK_CORRECTIONS.get(logger_tag)
    if not pts:
        return ts

    out = pd.to_datetime(ts, errors="coerce").astype("datetime64[ns]")
    for start_s, add_min in pts:
        start_ts = pd.Timestamp(start_s)
        mask = out >= start_ts
        if mask.any():
            out = out.where(~mask, out + pd.Timedelta(minutes=int(add_min)))
    return out


# ---------------------------------------------------------------------------
# Timestamp helpers (PyCharm-friendly)
# ---------------------------------------------------------------------------

def ts_to_iso_minute(ts_any: Any) -> str:
    """
    Format Timestamp-like -> 'YYYY-MM-DDTHH:MM' safely.
    Returns '' if None/NaT.
    """
    if ts_any is None or pd.isna(ts_any):
        return ""
    if not isinstance(ts_any, pd.Timestamp):
        ts_any = pd.to_datetime(ts_any, errors="coerce")
        if ts_any is pd.NaT or pd.isna(ts_any):
            return ""
        if not isinstance(ts_any, pd.Timestamp):
            ts_any = pd.Timestamp(ts_any)
    return ts_any.strftime("%Y-%m-%dT%H:%M")


def ts_to_iso_date(ts_any: Any) -> str:
    """
    Accepts Timestamp / NaT / None and returns YYYY-MM-DD or "".
    Avoids PyCharm warnings about NaTType.strftime().
    """
    if ts_any is None or pd.isna(ts_any):
        return ""
    if not isinstance(ts_any, pd.Timestamp):
        ts_any = pd.to_datetime(ts_any, errors="coerce")
        if ts_any is pd.NaT or pd.isna(ts_any):
            return ""
        if not isinstance(ts_any, pd.Timestamp):
            ts_any = pd.Timestamp(ts_any)
    return ts_any.strftime("%Y-%m-%d")


def make_timestamp_or_raise(value: str, *, context: str = "") -> pd.Timestamp:
    """
    Build a pd.Timestamp and guarantee it's not NaT.
    Raises ValueError if parsing fails.
    """
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError(f"Invalid timestamp {value!r}" + (f" ({context})" if context else ""))
    assert isinstance(ts, pd.Timestamp)
    return ts


def force_datetime64_ns(s: pd.Series) -> pd.Series:
    """
    Force a series to datetime64[ns], dropping NaT.
    Avoids pandas-stubs TimestampSeries vs Series[Timestamp] reassignment issues.
    """
    dt = pd.to_datetime(s, errors="coerce")
    dt_nonnull = dt.dropna()
    return cast(pd.Series, dt_nonnull.astype("datetime64[ns]"))


def normalize_logger_timestamp_series(ts: Series) -> Series:
    """
    Logger timestamps are treated as timezone-naive Mountain local wall time.
    DO NOT tz-localize them.
    """
    s = ts.astype("string").str.strip()
    return pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")


def normalize_weather_timestamp_series(ts: pd.Series, tz: str = "America/Denver") -> pd.Series:
    """
    Normalize CoAgMet timestamps:
      - parse
      - localize naive to tz, shifting DST gaps forward
      - convert any tz-aware to tz
      - drop tz info (timezone-naive)
    """
    s = pd.to_datetime(ts, errors="coerce")

    if s.dt.tz is None:
        s = s.dt.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward")
    else:
        s = s.dt.tz_convert(tz)

    return s.dt.tz_localize(None)


# ---------------------------------------------------------------------------
# Strip pairing assumptions (treated vs control)
# ---------------------------------------------------------------------------

STRIP_PAIRS = [
    ("S1", "S2"),
    ("S3", "S4"),
]


# ============================= Common helpers ============================= #

def convert_soil_t_to_fahrenheit(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()
    t_cols = [c for c in df_out.columns if c.startswith("T_") and "_raw_" in c]
    if not t_cols:
        return df_out

    to_f = UNIT_CONVERSIONS["metric_to_us"]["temp"]  # λ x: (x * 9/5) + 32
    for col_name in t_cols:
        df_out[col_name] = pd.to_numeric(df_out[col_name], errors="coerce").apply(to_f)

    logger.info(f"🌡 Converted {len(t_cols)} soil-temp columns from °C to °F")
    return df_out


def rename_logger_columns(df: pd.DataFrame, logger_name: str) -> pd.DataFrame:
    mapping: Dict[str, str] = {}
    prefix = logger_name[:2]
    loc = logger_name[2:]

    for col_name in df.columns:
        if col_name == "timestamp":
            continue

        if col_name == "BattV_Min":
            mapping[col_name] = f"BattV_Min_{prefix}_{loc}"
            continue

        if col_name.startswith(("VWC_", "T_", "EC_")):
            parts = col_name.split("_", maxsplit=2)
            if len(parts) == 3:
                var, depth, _agg = parts
                mapping[col_name] = f"{var}_{depth}_raw_{prefix}_{loc}"

    return df.rename(columns=mapping)


def _clean_col_name(s: object) -> str:
    return str(s).lstrip("\ufeff").strip().strip('"').strip("'").strip()


def _read_toa5_table1_dat(datfile: Path) -> pd.DataFrame:
    """
    TOA5 reader: column names are on the second row.
    """
    with datfile.open("r", newline="") as f:
        r = csv.reader(f)
        _meta = next(r, None)
        colnames = next(r, None)
        _units = next(r, None)
        _aggs = next(r, None)

    if not colnames:
        raise ValueError(f"{datfile.name}: missing TOA5 column-name row.")
    cols = [_clean_col_name(c) for c in colnames]
    if "TIMESTAMP" not in cols and "timestamp" not in cols:
        raise ValueError(f"{datfile.name}: TOA5 column-name row does not include TIMESTAMP.")

    return pd.read_csv(
        datfile,
        skiprows=4,
        header=None,
        names=cols,
        na_values=["", "NA", "NAN"],
        engine="python",
    )


def _candidate_logger_files(tag: str, year: int) -> list[Path]:
    """
    Resolve which .dat files should contribute to a (tag,year).

    Special case: year==2023
      - read the normal datfiles_2023/{tag}_Table1.dat (if present)
      - ALSO read datfiles_2024/{tag}_Table1.dat (if present) and keep rows < 2024-01-01
        (this is where BattV_Min first appears mid-Oct 2023 in your deployment)
      - ALSO read datfiles_2023/{tag}_Table1_late2023_withBattV.dat (if present)
        (if you wrote those extracted files into datfiles_2023)
    """
    files: list[Path] = []

    base = Path(DATA_RAW_DIR)

    p_main = base / f"datfiles_{year}" / f"{tag}_Table1.dat"
    if p_main.exists():
        files.append(p_main)

    if year == 2023:
        p_next = base / "datfiles_2024" / f"{tag}_Table1.dat"
        if p_next.exists():
            files.append(p_next)

        p_late = base / "datfiles_2023" / f"{tag}_Table1_late2023_withBattV.dat"
        if p_late.exists():
            files.append(p_late)

    return files


def read_logger_data(tag: str, year: int) -> Optional[pd.DataFrame]:
    files = _candidate_logger_files(tag, year)
    if not files:
        logger.warning(f"⚠️ Not found: datfiles_{year}/{tag}_Table1.dat (and no backfill sources)")
        return None

    frames: list[pd.DataFrame] = []
    raw_ts_examples: list[str] = []

    for datfile in files:
        try:
            df = _read_toa5_table1_dat(datfile)
        except Exception as e:
            logger.error(f"❌ Failed reading TOA5 file {datfile.name}: {e}")
            continue

        if "TIMESTAMP" in df.columns and "timestamp" not in df.columns:
            df = df.rename(columns={"TIMESTAMP": "timestamp"})
        df = df.drop(columns=["RECORD"], errors="ignore")

        if df.empty or "timestamp" not in df.columns:
            continue

        raw_ts = df["timestamp"].copy()
        df["timestamp"] = normalize_logger_timestamp_series(raw_ts)

        bad_mask = df["timestamp"].isna()
        n_nat = int(bad_mask.sum())
        if n_nat:
            bad_idx = df.index[bad_mask][:10].tolist()
            for i in bad_idx:
                raw_ts_examples.append(f"{datfile.name}: row={int(i)} raw={raw_ts.iloc[i]!r}")
            df = df.loc[~bad_mask].copy()

        if df.empty:
            continue

        # Apply clock corrections BEFORE year-window filtering
        df["timestamp"] = apply_logger_clock_corrections(df["timestamp"], tag)

        # Force dtype for PyCharm and safe comparisons
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").astype("datetime64[ns]")
        df = df.dropna(subset=["timestamp"])
        if df.empty:
            continue

        frames.append(df)

    if not frames:
        if raw_ts_examples:
            logger.warning(f"⚠️ NaT examples for {tag}: " + "; ".join(raw_ts_examples[:10]))
        return None

    df_all = pd.concat(frames, ignore_index=True)

    # Prefer “later” files in case of overlaps (e.g., the datfiles_2024 copy may have BattV_Min)
    df_all = df_all.sort_values("timestamp")
    df_all = df_all.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

    start_ts = pd.Timestamp(year=year, month=1, day=1)
    end_ts = pd.Timestamp(year=year + 1, month=1, day=1)

    ts_vals = df_all["timestamp"].to_numpy()
    mask_year = (ts_vals >= start_ts.to_datetime64()) & (ts_vals < end_ts.to_datetime64())
    df_all = df_all.loc[mask_year].copy()
    if df_all.empty:
        return None

    return rename_logger_columns(df_all, tag)


def merge_all_loggers(year: int) -> Optional[pd.DataFrame]:
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


def replace_bad_values(df_in: pd.DataFrame, threshold: float = DEFAULT_BAD_VALUE_THRESHOLD) -> pd.DataFrame:
    df_out = df_in.copy()
    for col_name in df_out.columns:
        if col_name == "timestamp":
            continue
        s = pd.to_numeric(df_out[col_name], errors="coerce")
        df_out[col_name] = s.mask(s.abs() >= threshold, NAN)
    logger.info(f"🧹 Replaced extreme placeholders with NaN (|x| ≥ {threshold:g})")
    return df_out


def scale_vwc_to_percent(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()
    vwc_cols = [c for c in df_out.columns if c.startswith("VWC_") and "_raw_" in c]
    if not vwc_cols:
        return df_out
    for col_name in vwc_cols:
        df_out[col_name] = pd.to_numeric(df_out[col_name], errors="coerce") * 100.0
    logger.info(f"📏 Scaled {len(vwc_cols)} VWC columns ×100 to percent")
    return df_out


def add_swc_cylinder_volumes(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()
    cyl_m3 = cylinder_volume_m3()
    cyl_l = cyl_m3 * 1000.0
    cyl_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](cyl_l)

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                vwc_col = f"VWC_{depth}_raw_{strip}_{loc}"
                if vwc_col not in df_out.columns:
                    continue
                frac = pd.to_numeric(df_out[vwc_col], errors="coerce") / 100.0
                df_out[f"SWC_vol_L_{strip}_{loc}_{depth}"] = frac * cyl_l
                df_out[f"SWC_vol_gal_{strip}_{loc}_{depth}"] = frac * cyl_gal

    logger.info("💧 Added SWC cylinder volumes (L & gallons) per sensor")
    return df_out


def add_temperature_differences(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()
    new_cols = 0

    for treated, control in STRIP_PAIRS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col_treated = f"T_{depth}_raw_{treated}_{loc}"
                col_control = f"T_{depth}_raw_{control}_{loc}"
                if col_treated not in df_out.columns or col_control not in df_out.columns:
                    continue
                diff_col = f"Tdiff_{depth}_{treated}_{control}_{loc}"
                df_out[diff_col] = (
                    pd.to_numeric(df_out[col_treated], errors="coerce")
                    - pd.to_numeric(df_out[col_control], errors="coerce")
                )
                new_cols += 1

    logger.info(
        f"🌡 Added {new_cols} ΔT columns (biochar − control)"
        if new_cols
        else "🌡 No ΔT columns added (required T_*_raw_* columns missing)"
    )
    return df_out


def add_swc_differences(df_in: pd.DataFrame) -> pd.DataFrame:
    df_out = df_in.copy()
    new_cols = 0

    for treated, control in STRIP_PAIRS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col_treated_gal = f"SWC_vol_gal_{treated}_{loc}_{depth}"
                col_control_gal = f"SWC_vol_gal_{control}_{loc}_{depth}"
                col_treated_L = f"SWC_vol_L_{treated}_{loc}_{depth}"
                col_control_L = f"SWC_vol_L_{control}_{loc}_{depth}"

                if col_treated_gal in df_out.columns and col_control_gal in df_out.columns:
                    diff_col_gal = f"SWCdiff_gal_{treated}_{control}_{loc}_{depth}"
                    df_out[diff_col_gal] = (
                        pd.to_numeric(df_out[col_treated_gal], errors="coerce")
                        - pd.to_numeric(df_out[col_control_gal], errors="coerce")
                    )
                    new_cols += 1

                if col_treated_L in df_out.columns and col_control_L in df_out.columns:
                    diff_col_L = f"SWCdiff_L_{treated}_{control}_{loc}_{depth}"
                    df_out[diff_col_L] = (
                        pd.to_numeric(df_out[col_treated_L], errors="coerce")
                        - pd.to_numeric(df_out[col_control_L], errors="coerce")
                    )
                    new_cols += 1

    logger.info(
        f"💧 Added {new_cols} ΔSWC volume columns (biochar − control)"
        if new_cols
        else "💧 No ΔSWC columns added (required SWC_vol_* columns missing)"
    )
    return df_out


# ============================= Growing-season summary ============================= #

def unpack_gseason_period(period_code: str, period_meta: Any) -> Tuple[str, str, str]:
    """
    Returns: (period_label, start_mmdd, end_mmdd)
    """
    if isinstance(period_meta, (tuple, list)) and len(period_meta) == 2:
        return period_code, str(period_meta[0]), str(period_meta[1])

    if isinstance(period_meta, dict):
        mmdd_start = period_meta.get("start")
        mmdd_end = period_meta.get("end")
        label = period_meta.get("label", period_code)
        if mmdd_start and mmdd_end:
            return str(label), str(mmdd_start), str(mmdd_end)

    raise ValueError(
        f"DEFAULT_GSEASON_PERIODS[{period_code!r}] must be "
        f"('MM-DD','MM-DD') or {{'start':'MM-DD','end':'MM-DD','label':...}}; got {period_meta!r}"
    )


def write_gseason_summary(year: int, df_daily: pd.DataFrame) -> None:
    if "timestamp" not in df_daily.columns:
        logger.warning(f"⚠️ write_gseason_summary({year}) skipped: no 'timestamp' column")
        return

    daily_df = df_daily.copy()
    daily_df["timestamp"] = pd.to_datetime(daily_df["timestamp"], errors="coerce")
    daily_df = daily_df.dropna(subset=["timestamp"])
    if daily_df.empty:
        logger.warning(f"⚠️ write_gseason_summary({year}) skipped: empty daily frame")
        return
    daily_df["timestamp"] = daily_df["timestamp"].astype("datetime64[ns]")

    value_cols: List[str] = [c for c in daily_df.columns if c != "timestamp"]
    agg_map: Dict[str, str] = {c: ("sum" if c.startswith("precip") else "mean") for c in value_cols}

    daily_dir = Path(PARQUET_DIR) / "summary" / "daily"
    prev_daily_df: Optional[pd.DataFrame] = None
    prev_loaded_year: Optional[int] = None

    rows: List[Dict[str, Any]] = []

    for period_code, meta in DEFAULT_GSEASON_PERIODS.items():
        period_label, mmdd_start, mmdd_end = unpack_gseason_period(period_code, meta)

        start_month = int(mmdd_start.split("-")[0])
        end_month = int(mmdd_end.split("-")[0])
        wraps_year = start_month > end_month

        period_start_year = year - 1 if wraps_year else year
        period_end_year = year

        start_ts = make_timestamp_or_raise(f"{period_start_year}-{mmdd_start}", context=f"{period_code} start")
        end_day = make_timestamp_or_raise(f"{period_end_year}-{mmdd_end}", context=f"{period_code} end")
        end_ts = end_day + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        window_parts: List[pd.DataFrame] = []

        if wraps_year:
            prev_path = daily_dir / f"{period_start_year}_daily.parquet"
            if prev_path.exists():
                if prev_daily_df is None or prev_loaded_year != period_start_year:
                    loaded_prev = pd.read_parquet(prev_path)

                    if "timestamp" not in loaded_prev.columns:
                        logger.warning(
                            f"⚠️ {prev_path.name} missing 'timestamp'; skipping prev-year part for {period_code}."
                        )
                        prev_daily_df = None
                    else:
                        loaded_prev = loaded_prev.copy()
                        loaded_prev["timestamp"] = pd.to_datetime(loaded_prev["timestamp"], errors="coerce")
                        loaded_prev = loaded_prev.dropna(subset=["timestamp"])
                        if not loaded_prev.empty:
                            loaded_prev["timestamp"] = loaded_prev["timestamp"].astype("datetime64[ns]")
                        prev_daily_df = loaded_prev
                        prev_loaded_year = period_start_year

                if prev_daily_df is not None and not prev_daily_df.empty:
                    ts_vals_prev = prev_daily_df["timestamp"].to_numpy()
                    mask_prev = (ts_vals_prev >= start_ts.to_datetime64()) & (ts_vals_prev <= end_ts.to_datetime64())
                    window_parts.append(prev_daily_df.loc[mask_prev])
            else:
                logger.warning(
                    f"⚠️ Missing prev-year daily parquet {prev_path.name} for {period_code} ({year}); "
                    f"using only current-year component."
                )

        ts_vals_cur = daily_df["timestamp"].to_numpy()
        mask_cur = (ts_vals_cur >= start_ts.to_datetime64()) & (ts_vals_cur <= end_ts.to_datetime64())
        window_parts.append(daily_df.loc[mask_cur])

        window = pd.concat(window_parts, ignore_index=True) if window_parts else pd.DataFrame(columns=daily_df.columns)

        if window.empty:
            logger.warning(
                f"⚠️ No daily rows for gseason {period_code} in {year} "
                f"[{start_ts.date()} → {end_ts.date()}]; filling NaN."
            )
            stats: Dict[str, Any] = {c: math.nan for c in value_cols}
        else:
            stats_series = window[value_cols].agg(agg_map).round(3)
            stats = stats_series.to_dict()

        rows.append(
            {
                "period_code": period_code,
                "period_label": period_label,
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
                "  - Clock corrections (MST/MDT jumps/resets) may have been applied to logger timestamps.",
                "  - Cross-strip comparison variables (ΔT, ΔSWC, ratio columns, etc.)",
                "    are not included in these per-logger CSVs.",
            ]
        )

        zf.writestr(f"README_Logger_15min_{year}.txt", "\n".join(readme_lines))

    logger.info(f"📦 Wrote logger download ZIP: {zip_path.name}")


def write_weather_download_zip(year: int, df_15min: pd.DataFrame, download_url: str = "", builder_url: str = "") -> None:
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
        df_s = df_agg(df.resample(code), agg_map).round(3)
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
    df_copy = dfw.copy()
    df_copy["timestamp"] = normalize_weather_timestamp_series(df_copy["timestamp"])

    df_copy["precip_in"] = pd.to_numeric(df_copy["precip_in"], errors="coerce")
    df_copy.loc[df_copy["precip_in"] == -999, "precip_in"] = math.nan
    df_copy["precip_in"] = df_copy["precip_in"].fillna(0.0).clip(lower=0.0)

    spike = df_copy["precip_in"].max()
    if pd.notna(spike) and spike > 1.5:
        logger.warning(f"⚠️ CoAgMet 5 min precip spike detected: {spike:.2f} in")

    bad_mask = df_copy["timestamp"].isna()
    bad_ts = int(bad_mask.sum())
    if bad_ts:
        # show a few examples *before* dropping
        ex = df_copy.loc[bad_mask].head(10).copy()
        cols_to_show = [c for c in ["timestamp", "precip_in", "temp_air_degF"] if c in ex.columns]
        logger.warning(
            "⚠️ Dropping %d weather rows with invalid/ambiguous timestamps. Examples:\n%s",
            bad_ts,
            ex[cols_to_show].to_string(index=False),
        )
        df_copy = df_copy.loc[~bad_mask].copy()

    return df_copy


# ============================= Orchestration ============================= #

def generate_summaries(years: List[int]) -> None:
    for year in years:
        logger.info(f"🌱 Starting ETL for {year}")

        df = merge_all_loggers(year)
        if df is None or df.empty:
            logger.error(f"❌ No logger .dat data for {year}, skipping logger summaries.")
        else:
            df = df.dropna(subset=["timestamp"]).copy()

            df = replace_bad_values(df, threshold=DEFAULT_BAD_VALUE_THRESHOLD)

            df = scale_vwc_to_percent(df)
            df = convert_soil_t_to_fahrenheit(df)

            df, bounds_reports = enforce_value_bounds(
                df,
                year=year,
                bad_value_threshold=None,
                collect_examples=5,
            )

            if bounds_reports:
                total_violations = sum(int(r.get("violations", 0) or 0) for r in bounds_reports)
                logger.warning(
                    f"⚠️ Bounds enforcement: {len(bounds_reports)} columns had violations "
                    f"({total_violations} total masked values). Showing up to 10 entries:"
                )

                def _fmt_ts(x: Any) -> str:
                    if x is None or pd.isna(x):
                        return ""
                    try:
                        return pd.to_datetime(x, errors="coerce").strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        return str(x)

                for r in bounds_reports[:10]:
                    logger.warning(
                        f"  - {r.get('rule')} col={r.get('column')} violations={r.get('violations')} "
                        f"min={r.get('min')} max={r.get('max')} label={r.get('label')}"
                    )

                    ex = r.get("examples") or []
                    if ex:
                        # Print a few concrete examples of what got masked
                        for e in ex:
                            logger.warning(
                                f"      example: ts={_fmt_ts(e.get('timestamp'))} value={e.get('value')}"
                            )

            df = add_swc_cylinder_volumes(df)
            df = add_temperature_differences(df)
            df = add_swc_differences(df)

            df = df.set_index("timestamp").sort_index()
            aggregate_and_write(year, df)

        # ---------------- Weather ----------------
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
            dfr = dfw_clean.resample(code).agg(cast(Any, agg_map)).round(3).reset_index()
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

            start_iso = ts_to_iso_minute(start_ts)
            end_iso = ts_to_iso_minute(end_ts)

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
    if cli_year is not None:
        return int(cli_year)
    try:
        return int(max(YEARS))
    except Exception:
        return int(datetime.now().year)


if __name__ == "__main__":
    os.makedirs(PARQUET_DIR, exist_ok=True)
    generate_summaries(YEARS)