import pandas as pd
import logging
import os
import zipfile
import numpy as np
import math
from biochar_app.scripts.config import (
    UNIT_CONVERSIONS,
    cylinder_volume_m3,
)

from typing import Any, Dict, List, Optional

from biochar_app.scripts.get_weather_data import fetch_weather_data
from biochar_app.scripts.config import (
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
    YEARS,
    DEPTHS,
    DEFAULT_UNITS,
    LOGGER_LOCATIONS,
    STRIPS,
    VALUE_COLS_STANDARD,
    VALUE_COLS_2024_PLUS,
    GSEASON_PERIODS
)


def rename_logger_columns(df, logger_name):
    """
    Rename each column from the raw .dat to a standardized format:
      - “BattV_Min” → BattV_Min_{strip}{loc}
      - e.g. “VWC_1_in” → VWC_1_raw_S1_T (etc.)
    """
    rename_dict = {}
    for col in df.columns:
        if col == "timestamp":
            continue
        elif col == "BattV_Min":
            rename_dict[col] = f"BattV_Min_{logger_name[:2]}_{logger_name[2:]}"
        elif col in VALUE_COLS_STANDARD:
            base, depth, _units = col.split("_")
            # e.g. “VWC_1_raw” → var=VWC, depth=1
            rename_dict[col] = f"{base}_{depth}_raw_{logger_name[:2]}_{logger_name[2:]}"
    df.rename(columns=rename_dict, inplace=True)
    return df


def read_logger_data(name, year):
    """
    Read one “strip+location” .dat file, normalize its timestamps,
    drop any rows before Jan 1 of `year`, then rename columns.
    """
    filepath = os.path.join(DATA_RAW_DIR, f"datfiles_{year}/{name}_Table1.dat")
    try:
        df = pd.read_csv(
            filepath,
            skiprows=4,
            na_values=["", "NA", "NAN"],
            names=["timestamp", "RECORD"] + VALUE_COLS_2024_PLUS,
            parse_dates=["timestamp"]
        ).drop(columns=["RECORD"])
    except FileNotFoundError:
        logging.warning(f"⚠️ File not found: {filepath}")
        return None

    # Normalize timezone, drop rows that become NaT
    df["timestamp"] = normalize_timestamp_series(df["timestamp"])
    num_nat = df["timestamp"].isna().sum()
    if num_nat > 0:
        logging.warning(
            f"⚠️ Found {num_nat} NaT timestamps in {name} after tz_localize — "
            "check DST gaps or data issues."
        )

    # Keep only rows from Jan 1 of `year` onward
    df = df[df["timestamp"] >= pd.Timestamp(f"{year}-01-01")]
    return rename_logger_columns(df, name)


def merge_all_loggers(year):
    """
    Read each strip/logger’s .dat, drop duplicate timestamps, and then
    outer‐join them all into one DataFrame. Return None if no data found.
    """
    frames = []
    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            df = read_logger_data(f"{strip}{loc}", year)
            if df is None:
                continue

            # 1) set index on timestamp
            df = df.set_index("timestamp")
            # 2) drop any duplicate timestamp rows (keep first)
            df = df[~df.index.duplicated(keep="first")]
            frames.append(df)

    if not frames:
        logging.warning(f"⚠️ No logger data found for year {year}")
        return None

    # Concatenate all frames side‐by‐side (outer join on timestamp)
    merged = pd.concat(frames, axis=1)
    # Drop any duplicated column names (in case two loggers had identical labels)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    # Reset index so that “timestamp” becomes a regular column again
    merged = merged.reset_index()
    return merged


def replace_bad_values(df):
    """
    Mask any placeholder extreme values (≥ 999999) as NaN in all numeric cols.
    """
    bad_threshold = 999999
    for col in df.select_dtypes(include=["float", "int"]):
        df[col] = df[col].mask(df[col].abs() >= bad_threshold, np.nan)
    logging.info("🧹 Replaced extreme placeholder values with NaN")
    return df


_liter_to_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"]


def add_swc_cylinder_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each strip+loc, compute the volume of water in the sensor cylinder
    at each depth, then add two new columns per sensor:
      - SWC_vol_L_{strip}_{loc}_{depth}
      - SWC_vol_gal_{strip}_{loc}_{depth}
    using:
       V_water = VWC_fraction * V_cylinder,
       V_cylinder = π * r^2 * L
    where r and L are from config (in cm).
    """
    df = df.copy()
    # compute cylinder volume in cm³
    cyl_vol_cm3 = cylinder_volume_m3()
    # convert to liters (1 L = 1000 cm³)
    cyl_vol_L = cyl_vol_cm3 / 1000.0
    # conversion to US gallons
    cyl_vol_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](cyl_vol_L)

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in DEPTHS:
                col = f"VWC_{depth}_raw_{strip}_{loc}"
                if col not in df.columns:
                    continue

                # fraction (e.g. 0.25 for 25%)
                frac = df[col].astype(float) / 100.0  # VWC was in percent
                # water volume in cylinder (L)
                vol_L = frac * cyl_vol_L
                # water volume in cylinder (gal)
                vol_gal = frac * cyl_vol_gal

                df[f"SWC_vol_L_{strip}_{loc}_{depth}"]   = vol_L
                df[f"SWC_vol_gal_{strip}_{loc}_{depth}"] = vol_gal

    return df

def calculate_ratios(df):
    """
    For each of VWC/T/EC/SWC at depths d, compute (strip1 / strip2) for (S1 vs. S2) and (S3 vs. S4).
    Output columns of form “{var}_{d}_ratio_{S1}_{S2}_{loc}”.
    """
    df = df.copy()
    for var in ["VWC", "T", "EC", "SWC"]:
        for s1, s2 in [("S1", "S2"), ("S3", "S4")]:
            for loc in LOGGER_LOCATIONS:
                for d in DEPTHS:
                    if var == "SWC" and d != "1":
                        # we only compute SWC ratio at “depth 1” since we stored SWC under depth 1
                        continue
                    c1 = f"{var}_{d}_raw_{s1}_{loc}"
                    c2 = f"{var}_{d}_raw_{s2}_{loc}"
                    out = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"
                    if c1 in df.columns and c2 in df.columns:
                        df[out] = df[c1] / df[c2]
                    else:
                        df[out] = pd.NA
    return df


def aggregate(df, year):
    """
    Given combined logger+weather DataFrame (with a datetime index),
    produce a dict of resampled DataFrames at 15min/1h/daily/monthly (any “precip_*” summed),
    plus a “gseason” table per GSEASON_PERIODS.
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df[df["timestamp"].dt.year == int(year)]
    df.set_index("timestamp", inplace=True)

    numeric = df.select_dtypes("number").columns.tolist()
    precip_cols = [c for c in numeric if c.startswith("precip_")]

    # build agg‐dict with skipna / min_count
    agg: dict[str, Any] = {}
    for col in numeric:
        if col in precip_cols:
            # sum precipitation, but only if at least one non‐NA,
            # otherwise produce NA to avoid 0‐sum warnings
            agg[col] = (lambda s: s.sum(skipna=True, min_count=1))
        else:
            # average everything else, skipping NA
            agg[col] = (lambda s: s.mean(skipna=True))

    out_dict: Dict[str, pd.DataFrame] = {
        "15min":  df.reset_index(),
        "hourly":  df.resample("h").agg(agg).reset_index(),
        "daily":  df.resample("D").agg(agg).reset_index(),
        "monthly":df.resample("ME").agg(agg).reset_index(),
    }

    # Now add “gseason”
    growing_season_results = []
    for season_name, months in GSEASON_PERIODS.items():
        start_m, _ = months["start"].split("-")
        end_m, _ = months["end"].split("-")
        sm, em = int(start_m), int(end_m)
        if sm <= em:
            month_numbers = list(range(sm, em + 1))
        else:
            # Wrap‐around (e.g. 11→02) means [11,12,1,2]
            month_numbers = list(range(sm, 13)) + list(range(1, em + 1))

        df_season = df[df.index.month.isin(month_numbers)]
        if not df_season.empty:
            season_means = df_season.agg(agg)
            season_means["timestamp"] = season_name
            growing_season_results.append(season_means)

    if growing_season_results:
        df_gseason = pd.DataFrame(growing_season_results)
        df_gseason.reset_index(drop=True, inplace=True)
        # Ensure 'timestamp' is the first column
        cols = ["timestamp"] + [c for c in df_gseason.columns if c != "timestamp"]
        df_gseason = df_gseason[cols]
        out_dict["gseason"] = df_gseason

    return out_dict


def save_outputs(year, aggregated):
    """
    For each granularity in aggregated (15min/hourly/daily/monthly/gseason),
    write a CSV → ZIP named “dataloggerData_{year}-01-01_{end_date}_{granularity}.zip”.
    We will use the 15-minute table’s last timestamp for every granularity.
    If the 15-minute table is missing or empty, we fall back to “{year}-12-31”.
    """
    # 1) Determine the “end_date” once by looking at the 15-minute DataFrame:
    common_end_date = pd.to_datetime(f"{year}-12-31").date()
    fifteen_min_df = aggregated.get("15min")
    if fifteen_min_df is not None and not fifteen_min_df.empty:
        max_ts_15min = pd.to_datetime(fifteen_min_df["timestamp"], errors="coerce").max()
        if not pd.isna(max_ts_15min):
            common_end_date = max_ts_15min.date()

    # 2) Loop through each granularity and use common_end_date for naming:
    for gran, df_out in aggregated.items():
        if df_out.empty:
            continue

        end_date_str = common_end_date.strftime("%Y-%m-%d")
        fname = f"dataloggerData_{year}-01-01_{end_date_str}_{gran}.csv"
        zipname_csv = fname.replace(".csv", ".zip")
        csv_path = os.path.join(DATA_PROCESSED_DIR, fname)
        zip_path = os.path.join(DATA_PROCESSED_DIR, zipname_csv)

        df_out.to_csv(csv_path, index=False, float_format="%.4f")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname=fname)
        os.remove(csv_path)

        logging.info(f"✅ Saved: {zip_path}")


def normalize_timestamp_series(ts_series, make_naive=True, timezone="America/Denver"):
    """
    Normalize a pandas datetime Series into the given timezone, then drop timezone
    so that downstream merging/resampling works in a naive‐datetime context.
    Any DST‐gaps become NaT.
    """
    if ts_series.dt.tz is None:
        ts_series = ts_series.dt.tz_localize(
            timezone, ambiguous="NaT", nonexistent="shift_forward"
        )
    else:
        ts_series = ts_series.dt.tz_convert(timezone)

    if make_naive:
        ts_series = ts_series.dt.tz_localize(None)

    return ts_series


def process_logger_and_climate_data(year):
    """
    Main driver: read all logger files, clean/merge with weather, add SWC and ratios,
    then aggregate into multiple granularities and save everything to CSV→ZIP.
    """
    logging.info(f"🚀 Processing year: {year}")

    # 1) Merge all strip+logger DataFrames (outer join on timestamp)
    df_logger = merge_all_loggers(year)
    if df_logger is None or df_logger.empty:
        raise RuntimeError("❌ No logger data found")

    # 2) Normalize logger timestamps (tz-aware → naive) and drop any NaT
    df_logger["timestamp"] = normalize_timestamp_series(df_logger["timestamp"])
    num_nat = df_logger["timestamp"].isna().sum()
    if num_nat > 0:
        logging.warning(
            f"⚠️ Found {num_nat} NaT timestamps in logger data after tz_localize — "
            "check DST gaps or data issues."
        )
    df_logger = df_logger.dropna(subset=["timestamp"])

    # 3) Replace placeholder extreme values
    df_logger = replace_bad_values(df_logger)

    # 4) Fetch weather data (CoAgMet) up to the last logger timestamp
    end_ts = df_logger["timestamp"].max()
    logging.info("🔄 Calling get_weather_data(…) now…")
    df_weather = fetch_weather_data(year, end_timestamp=end_ts)
    num_nat_weather = df_weather["timestamp"].isna().sum()
    if num_nat_weather > 0:
        logging.warning(
            f"⚠️ Found {num_nat_weather} NaT timestamps in weather data — "
            "check DST issues or malformed values."
        )

    # create both US and metric columns for each weather variable
    df_weather["precip_mm"] = df_weather["precip_in"].apply(UNIT_CONVERSIONS["us_to_metric"]["precip"])
    df_weather["temp_air_degC"] = df_weather["temp_air_degF"].apply(UNIT_CONVERSIONS["us_to_metric"]["temp"])

    #swc is calculated in this file
  #  df_weather["swc_in"] = df_weather["swc"].apply(UNIT_CONVERSIONS["us_to_metric"]["swc"])

    # 5) Merge logger+weather on “timestamp” (outer join)
    df_combined = pd.merge(df_logger, df_weather, on="timestamp", how="outer")

    # 6) Compute SWC and ratio columns
    df_combined = add_swc_cylinder_volumes(df_combined)
    df_combined = calculate_ratios(df_combined)

    # 7) Mask any VWC > 1.5 → NaN
    vwc_cols = [c for c in df_combined.columns if c.startswith("VWC_") and "_raw_" in c]
    num_outliers = (df_combined[vwc_cols] > 1.5).sum().sum()
    if num_outliers > 0:
        logging.warning(f"🧹 Found {num_outliers} VWC values > 1.5 — setting to NaN")
        df_combined[vwc_cols] = df_combined[vwc_cols].mask(df_combined[vwc_cols] > 1.5)

    # 8) Aggregate into multiple granularities (includes “gseason”)
    aggregated = aggregate(df_combined, year)

    # 9) Save each granularity’s CSV→ZIP
    save_outputs(year, aggregated)


if __name__ == "__main__":
    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    for y in YEARS:
        if y >= 2024:
            try:
                process_logger_and_climate_data(y)
            except Exception as err:
                logging.error(f"❌ Error processing year {y}: {err}")
        else:
            logging.info(f"⚠️ Skipping {y}: growing season logic not supported for this year.")