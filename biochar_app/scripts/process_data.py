#!/usr/bin/env python3
"""
process_data.py

Logger + weather processing pipeline (legacy path used by the web app).

Type-checker / PyCharm notes:
- Use NAN = float("nan") instead of np.nan to avoid numpy stub warnings.
- Avoid pandas methods with keyword args that PyCharm stubs sometimes reject
  (e.g., sum(min_count=1) or mean(skipna=True) inside lambdas).
- Force DatetimeIndex before using df.index.month.
"""

from __future__ import annotations

import logging
import os
import zipfile
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from biochar_app.scripts.config import (
    UNIT_CONVERSIONS,
    cylinder_volume_m3,
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
    YEARS,
    DEPTHS,
    LOGGER_LOCATIONS,
    STRIPS,
    VALUE_COLS_STANDARD,
    VALUE_COLS_2024_PLUS,
    DEFAULT_GSEASON_PERIODS,
)
from biochar_app.scripts.get_weather_data import fetch_weather_data
from biochar_app.scripts.type_utils import df_agg, gb_agg, AggDict, AggSpec, NAN, POS_INF, NEG_INF


# ---------------------------------------------------------------------------
# Timestamp normalization
# ---------------------------------------------------------------------------

def normalize_timestamp_series(
    ts_series: pd.Series,
    *,
    make_naive: bool = True,
    timezone: str = "America/Denver",
) -> pd.Series:
    """
    Normalize a datetime-like Series into the given timezone, then (optionally)
    strip timezone info to return timezone-naive local wall time.

    Any DST ambiguity issues can yield NaT; callers should drop NaT rows as needed.
    """
    s = pd.to_datetime(ts_series, errors="coerce")

    tzinfo = s.dt.tz
    if tzinfo is None:
        s = s.dt.tz_localize(timezone, ambiguous="NaT", nonexistent="shift_forward")
    else:
        s = s.dt.tz_convert(timezone)

    if make_naive:
        s = s.dt.tz_localize(None)

    return s


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------
def safe_ratio(num: pd.Series, denom: pd.Series, eps: float = 1e-3) -> pd.Series:
    """
    Compute num / denom but avoid blow-ups when denom ≈ 0.

    Any |denom| < eps becomes NaN so ratio is NaN there too.
    Also removes ±inf values if they slip through.
    """
    num_f = pd.to_numeric(num, errors="coerce").astype(float)
    denom_f = pd.to_numeric(denom, errors="coerce").astype(float)

    denom_safe = denom_f.copy()
    small_mask = denom_safe.abs() < float(eps)
    denom_safe.loc[small_mask] = NAN

    ratio = num_f / denom_safe
    ratio = ratio.replace([POS_INF, NEG_INF], NAN)
    return ratio


def _agg_sum_min1(series: pd.Series) -> float:
    """
    Sum with the rule:
      - if all values are NA => return NAN (not 0)
      - else => numeric sum
    Implemented without sum(min_count=1) to keep PyCharm happy.
    """
    s = pd.to_numeric(series, errors="coerce")
    if bool(s.notna().any()):
        return float(s.sum())
    return NAN


def _agg_mean(series: pd.Series) -> float:
    """
    Mean with skipna behavior. Returns NAN if all values are NA.
    """
    s = pd.to_numeric(series, errors="coerce")
    if bool(s.notna().any()):
        return float(s.mean())
    return NAN


# ---------------------------------------------------------------------------
# Logger column renaming
# ---------------------------------------------------------------------------
def rename_logger_columns(df_in: pd.DataFrame, logger_name: str) -> pd.DataFrame:
    """
    Rename each column from the raw .dat to a standardized format:
      - BattV_Min → BattV_Min_{strip}_{loc}
      - VWC_1_in  → VWC_1_raw_{strip}_{loc}
      - etc.
    """
    df_out = df_in.copy()
    rename_dict: Dict[str, str] = {}

    strip = logger_name[:2]
    loc = logger_name[2:]

    for col in df_out.columns:
        if col == "timestamp":
            continue

        if col == "BattV_Min":
            rename_dict[col] = f"BattV_Min_{strip}_{loc}"
            continue

        if col in VALUE_COLS_STANDARD:
            parts = col.split("_")
            if len(parts) >= 2:
                base = parts[0]
                depth = parts[1]
                rename_dict[col] = f"{base}_{depth}_raw_{strip}_{loc}"

    return df_out.rename(columns=rename_dict)


# ---------------------------------------------------------------------------
# Logger file IO
# ---------------------------------------------------------------------------
def read_logger_data(name: str, year: int) -> Optional[pd.DataFrame]:
    """
    Read one strip+location .dat file, normalize its timestamps,
    drop any rows before Jan 1 of `year`, then rename columns.
    """
    filepath = os.path.join(str(DATA_RAW_DIR), f"datfiles_{year}", f"{name}_Table1.dat")

    try:
        df = pd.read_csv(
            filepath,
            skiprows=4,
            na_values=["", "NA", "NAN"],
            names=["timestamp", "RECORD"] + list(VALUE_COLS_2024_PLUS),
            parse_dates=["timestamp"],
        )
    except FileNotFoundError:
        logging.warning(f"⚠️ File not found: {filepath}")
        return None

    df = df.drop(columns=["RECORD"], errors="ignore")
    if df.empty or "timestamp" not in df.columns:
        return None

    df["timestamp"] = normalize_timestamp_series(df["timestamp"])
    nat_count = int(df["timestamp"].isna().sum())
    if nat_count:
        logging.warning(f"⚠️ {nat_count} NaT timestamps in {name} after normalization.")
        df = df.dropna(subset=["timestamp"])

    if df.empty:
        return None

    start = pd.Timestamp(year=year, month=1, day=1)
    df = df.loc[df["timestamp"] >= start].copy()
    if df.empty:
        return None

    return rename_logger_columns(df, name)


def merge_all_loggers(year: int) -> Optional[pd.DataFrame]:
    """
    Read each strip/logger .dat, drop duplicate timestamps, outer-join into one DataFrame.
    """
    frames: List[pd.DataFrame] = []

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            tag = f"{strip}{loc}"
            df = read_logger_data(tag, year)
            if df is None or df.empty:
                continue

            df_i = df.set_index("timestamp")
            df_i = df_i.loc[~df_i.index.duplicated(keep="first")]
            frames.append(df_i)

    if not frames:
        logging.warning(f"⚠️ No logger data found for year {year}")
        return None

    merged = pd.concat(frames, axis=1)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    return merged.reset_index()


# ---------------------------------------------------------------------------
# Cleaning / standardization
# ---------------------------------------------------------------------------
def replace_bad_values(df_in: pd.DataFrame, *, bad_threshold: float = 999999.0) -> pd.DataFrame:
    """
    Mask placeholder extreme values (|x| >= bad_threshold) as NaN in all non-timestamp columns.
    """
    df_out = df_in.copy()
    for col in df_out.columns:
        if col == "timestamp":
            continue
        s = pd.to_numeric(df_out[col], errors="coerce")
        df_out[col] = s.mask(s.abs() >= float(bad_threshold), NAN)

    logging.info(f"🧹 Replaced extreme placeholder values with NaN (|x| >= {bad_threshold:g})")
    return df_out


def scale_vwc_to_percent(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Convert VWC fraction (0–1) to percent (0–100): multiply VWC_*_raw_* by 100.
    """
    df_out = df_in.copy()
    vwc_cols = [c for c in df_out.columns if c.startswith("VWC_") and "_raw_" in c]
    if not vwc_cols:
        return df_out

    for col in vwc_cols:
        df_out[col] = pd.to_numeric(df_out[col], errors="coerce") * 100.0

    logging.info(f"📏 Scaled {len(vwc_cols)} VWC columns ×100 to percent")
    return df_out


def add_swc_cylinder_volumes(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Compute SWC cylinder volumes (L and gallons) from VWC percent.
    cylinder_volume_m3() returns m^3 → liters via *1000.
    """
    df_out = df_in.copy()

    cyl_m3 = float(cylinder_volume_m3())
    cyl_l = cyl_m3 * 1000.0
    cyl_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](cyl_l)

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in DEPTHS:
                vwc_col = f"VWC_{depth}_raw_{strip}_{loc}"
                if vwc_col not in df_out.columns:
                    continue

                vwc_pct = pd.to_numeric(df_out[vwc_col], errors="coerce")
                frac = vwc_pct / 100.0

                df_out[f"SWC_vol_L_{strip}_{loc}_{depth}"] = frac * cyl_l
                df_out[f"SWC_vol_gal_{strip}_{loc}_{depth}"] = frac * cyl_gal

    return df_out


# ---------------------------------------------------------------------------
# Ratios
# ---------------------------------------------------------------------------
def calculate_ratios(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    For VWC/T/EC compute (S1/S2) and (S3/S4) per depth and logger location.
    Also compute SWC ratios using SWC_vol_gal_* columns if present.
    """
    df_out = df_in.copy()
    pairings = [("S1", "S2"), ("S3", "S4")]

    for var in ["VWC", "T", "EC"]:
        for s1, s2 in pairings:
            for loc in LOGGER_LOCATIONS:
                for d in DEPTHS:
                    c1 = f"{var}_{d}_raw_{s1}_{loc}"
                    c2 = f"{var}_{d}_raw_{s2}_{loc}"
                    out_col = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"
                    if c1 in df_out.columns and c2 in df_out.columns:
                        df_out[out_col] = safe_ratio(df_out[c1], df_out[c2])
                    else:
                        df_out[out_col] = pd.NA

    # SWC ratios (gallons)
    for s1, s2 in pairings:
        for loc in LOGGER_LOCATIONS:
            for d in DEPTHS:
                c1 = f"SWC_vol_gal_{s1}_{loc}_{d}"
                c2 = f"SWC_vol_gal_{s2}_{loc}_{d}"
                out_col = f"SWC_vol_gal_{d}_ratio_{s1}_{s2}_{loc}"
                if c1 in df_out.columns and c2 in df_out.columns:
                    df_out[out_col] = safe_ratio(df_out[c1], df_out[c2])
                else:
                    df_out[out_col] = pd.NA

    return df_out


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def _build_agg_map(df_indexed: pd.DataFrame) -> Dict[str, Any]:
    """
    precip_* summed (min1) else mean.
    Uses named functions to avoid PyCharm lambda + kwargs warnings.
    """
    numeric_cols = df_indexed.select_dtypes("number").columns.tolist()
    precip_cols = [c for c in numeric_cols if c.startswith("precip_")]

    agg: Dict[str, Any] = {}
    for col in numeric_cols:
        agg[col] = _agg_sum_min1 if col in precip_cols else _agg_mean
    return agg


def aggregate(df_in: pd.DataFrame, year: int) -> Dict[str, pd.DataFrame]:
    """
    Resample into 15min/hourly/daily/monthly and compute a basic gseason table.
    """
    df = df_in.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    df = df.loc[df["timestamp"].dt.year == int(year)].copy()
    df = df.set_index("timestamp").sort_index()

    # Force a DatetimeIndex so .month is valid to PyCharm
    dt_index = pd.DatetimeIndex(df.index)

    agg_map = _build_agg_map(df)

    out: Dict[str, pd.DataFrame] = {
        "15min": df.reset_index(),
        "hourly": df_agg(df.resample("h"), agg_map).reset_index(),
        "daily": df_agg(df.resample("D"), agg_map).reset_index(),
        "monthly": df_agg(df.resample("ME"), agg_map).reset_index(),
    }

    # gseason (month windows)
    gseason_rows: List[Dict[str, Any]] = []
    for season_name, meta in DEFAULT_GSEASON_PERIODS.items():
        if not isinstance(meta, dict):
            continue
        start_mmdd = meta.get("start")
        end_mmdd = meta.get("end")
        if not start_mmdd or not end_mmdd:
            continue

        start_month = int(str(start_mmdd).split("-")[0])
        end_month = int(str(end_mmdd).split("-")[0])

        if start_month <= end_month:
            months = list(range(start_month, end_month + 1))
        else:
            months = list(range(start_month, 13)) + list(range(1, end_month + 1))

        # ✅ Fix PyCharm warning: DatetimeIndex has no ".index"
        df_season = df.loc[dt_index.month.isin(months)]
        if df_season.empty:
            continue

        season_stats = df_agg(df_season, agg_map)
        row: Dict[str, Any] = {"timestamp": season_name}
        for k, v in season_stats.to_dict().items():
            row[str(k)] = v
        gseason_rows.append(row)

    if gseason_rows:
        df_gseason = pd.DataFrame(gseason_rows)
        cols = ["timestamp"] + [c for c in df_gseason.columns if c != "timestamp"]
        out["gseason"] = df_gseason[cols]

    return out


# ---------------------------------------------------------------------------
# Output saving
# ---------------------------------------------------------------------------
def save_outputs(year: int, aggregated: Dict[str, pd.DataFrame]) -> None:
    """
    Save each granularity DataFrame to CSV→ZIP.
    Determine common end-date from the 15-min table if possible.
    """
    # ✅ Keep common_end_date unambiguously a 'date' for PyCharm
    common_end_date: date = pd.Timestamp(year=year, month=12, day=31).date()

    fifteen = aggregated.get("15min")
    if fifteen is not None and not fifteen.empty and "timestamp" in fifteen.columns:
        max_ts_any = pd.to_datetime(fifteen["timestamp"], errors="coerce").max()
        if isinstance(max_ts_any, pd.Timestamp) and not pd.isna(max_ts_any):
            common_end_date = max_ts_any.to_pydatetime().date()

    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)

    for gran, df_out in aggregated.items():
        if df_out is None or df_out.empty:
            continue

        # ✅ common_end_date is always 'date' here (no NaTType), so strftime is safe
        end_date_str = common_end_date.strftime("%Y-%m-%d")
        fname = f"dataloggerData_{year}-01-01_{end_date_str}_{gran}.csv"
        zipname = fname.replace(".csv", ".zip")

        csv_path = os.path.join(str(DATA_PROCESSED_DIR), fname)
        zip_path = os.path.join(str(DATA_PROCESSED_DIR), zipname)

        df_out.to_csv(csv_path, index=False, float_format="%.4f")

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname=fname)

        os.remove(csv_path)
        logging.info(f"✅ Saved: {zip_path}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def process_logger_and_climate_data(year: int) -> None:
    """
    Main driver: read loggers, merge with weather, compute SWC + ratios,
    aggregate and save outputs.
    """
    logging.info(f"🚀 Processing year: {year}")

    df_logger = merge_all_loggers(year)
    if df_logger is None or df_logger.empty:
        raise RuntimeError("❌ No logger data found")

    # normalize timestamps again defensively (in case upstream parsing changed)
    df_logger["timestamp"] = normalize_timestamp_series(df_logger["timestamp"])
    nat_count = int(df_logger["timestamp"].isna().sum())
    if nat_count:
        logging.warning(f"⚠️ Found {nat_count} NaT timestamps in logger data after normalization.")
        df_logger = df_logger.dropna(subset=["timestamp"])

    df_logger = replace_bad_values(df_logger, bad_threshold=999999.0)
    df_logger = scale_vwc_to_percent(df_logger)

    # end_timestamp for weather fetch (must not be NaT)
    end_ts_raw = df_logger["timestamp"].max()
    end_ts: Optional[pd.Timestamp] = None
    if isinstance(end_ts_raw, pd.Timestamp) and not pd.isna(end_ts_raw):
        end_ts = end_ts_raw

    logging.info("🔄 Calling fetch_weather_data(...) now…")
    if end_ts is not None:
        df_weather = fetch_weather_data(year, end_timestamp=end_ts)  # type: ignore[call-arg]
    else:
        df_weather = fetch_weather_data(year)

    if df_weather is None or df_weather.empty:
        logging.warning(f"⚠️ Weather data empty for {year}; continuing with logger-only data.")
        df_weather = pd.DataFrame({"timestamp": pd.Series(dtype="datetime64[ns]")})

    if "timestamp" in df_weather.columns:
        df_weather["timestamp"] = normalize_timestamp_series(df_weather["timestamp"])
        df_weather = df_weather.dropna(subset=["timestamp"])

    # derived metric columns (if present)
    if "precip_in" in df_weather.columns:
        df_weather["precip_mm"] = pd.to_numeric(df_weather["precip_in"], errors="coerce").apply(
            UNIT_CONVERSIONS["us_to_metric"]["precip"]
        )
    if "temp_air_degF" in df_weather.columns:
        df_weather["temp_air_degC"] = pd.to_numeric(df_weather["temp_air_degF"], errors="coerce").apply(
            UNIT_CONVERSIONS["us_to_metric"]["temp"]
        )

    # merge logger + weather
    if not df_weather.empty and "timestamp" in df_weather.columns:
        df_combined = pd.merge(df_logger, df_weather, on="timestamp", how="outer")
    else:
        df_combined = df_logger.copy()

    # compute SWC + ratios
    df_combined = add_swc_cylinder_volumes(df_combined)
    df_combined = calculate_ratios(df_combined)

    # mask VWC > 150% (since VWC is percent here)
    vwc_cols = [c for c in df_combined.columns if c.startswith("VWC_") and "_raw_" in c]
    if vwc_cols:
        vwc_block = df_combined[vwc_cols].astype("float64")
        outlier_mask = (vwc_block.to_numpy() > 150.0)
        outlier_count = int(outlier_mask.sum())
        if outlier_count:
            logging.warning(f"🧹 Found {outlier_count} VWC values > 150% — masking to NaN")
            df_combined[vwc_cols] = vwc_block.mask(vwc_block > 150.0, NAN)

    aggregated = aggregate(df_combined, year)
    save_outputs(year, aggregated)


if __name__ == "__main__":
    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    for y in YEARS:
        if int(y) >= 2024:
            try:
                process_logger_and_climate_data(int(y))
            except Exception as err:
                logging.error(f"❌ Error processing year {y}: {err}")
        else:
            logging.info(f"⚠️ Skipping {y}: growing season logic not supported for this year.")