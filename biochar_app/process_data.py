import pandas as pd
import logging
import os
import zipfile
import numpy as np
from biochar_app.get_weather_data import get_weather_data
from biochar_app.config import (
    DATA_RAW_DIR, DATA_PROCESSED_DIR, YEARS, DEPTHS,
    SWC_DEPTH_WEIGHTS, LOGGER_LOCATIONS, STRIPS,
    VALUE_COLS_STANDARD, VALUE_COLS_2024_PLUS
)

def rename_logger_columns(df, logger_name):
    rename_dict = {}
    for col in df.columns:
        if col == "timestamp":
            continue
        elif col == "BattV_Min":
            rename_dict[col] = f"BattV_Min_{logger_name[:2]}_{logger_name[2:]}"
        elif col in VALUE_COLS_STANDARD:
            base, depth, _ = col.split("_")
            rename_dict[col] = f"{base}_{depth}_raw_{logger_name[:2]}_{logger_name[2:]}"
    df.rename(columns=rename_dict, inplace=True)
    return df

def read_logger_data(name, year):
    filepath = os.path.join(DATA_RAW_DIR, f"datfiles_{year}/{name}_Table1.dat")
    try:
        df = pd.read_csv(
            filepath, skiprows=4, na_values=["", "NA", "NAN"],
            names=["timestamp", "RECORD"] + VALUE_COLS_2024_PLUS,
            parse_dates=["timestamp"]
        ).drop(columns=["RECORD"])
        df["timestamp"] = df["timestamp"].dt.tz_localize("America/Denver", ambiguous="NaT", nonexistent="NaT")
        df = df[df["timestamp"] >= pd.Timestamp(f"{year}-01-01", tz="America/Denver")]
        return rename_logger_columns(df, name)
    except FileNotFoundError:
        logging.warning(f"⚠️ File not found: {filepath}")
        return None

def merge_all_loggers(year):
    merged = None
    found_any = False
    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            df = read_logger_data(f"{strip}{loc}", year)
            if df is not None:
                found_any = True
                merged = df if merged is None else pd.merge(merged, df, on="timestamp", how="outer")
        if not found_any:
            logging.warning(f"⚠️ No logger data found for year {year}")
    return merged

def replace_bad_values(df):
    bad_threshold = 999999
    for col in df.select_dtypes(include=["float", "int"]):
        df[col] = df[col].mask(df[col].abs() >= bad_threshold, np.nan)
    logging.info("🧹 Replaced extreme placeholder values with NaN")
    return df

def add_swc(df):
    df = df.copy()
    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            cols = [f"VWC_{d}_raw_{strip}_{loc}" for d in DEPTHS]
            if all(c in df.columns for c in cols):
                df[f"SWC_1_raw_{strip}_{loc}"] = sum(df[c] * SWC_DEPTH_WEIGHTS[d] for c, d in zip(cols, DEPTHS))
    return df

def calculate_ratios(df):
    df = df.copy()
    for var in ["VWC", "T", "EC", "SWC"]:
        for s1, s2 in [("S1", "S2"), ("S3", "S4")]:
            for loc in LOGGER_LOCATIONS:
                for d in DEPTHS:
                    if var == "SWC" and d != "1":
                        continue
                    c1, c2 = f"{var}_{d}_raw_{s1}_{loc}", f"{var}_{d}_raw_{s2}_{loc}"
                    out = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"
                    df[out] = df[c1] / df[c2] if c1 in df.columns and c2 in df.columns else pd.NA
    return df


def aggregate(df, year):
    if "timestamp" not in df.columns:
        raise ValueError("❌ DataFrame does not contain 'timestamp' column.")

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df[df["timestamp"].dt.year == int(year)]
    df.set_index("timestamp", inplace=True)
    agg = {col: "mean" for col in df.select_dtypes("number").columns}
    if "precip_mm" in df.columns:
        agg["precip_mm"] = "sum"

    return {
        "15min": df.reset_index(),
        "1hour": df.resample("h").agg(agg).reset_index(),
        "daily": df.resample("D").agg(agg).reset_index(),
        "monthly": df.resample("ME").agg(agg).reset_index()
    }

def save_outputs(year, aggregated):
    for gran, df_out in aggregated.items():
        if df_out.empty:
            continue

        if "timestamp" in df_out.columns:
            end_timestamp = pd.to_datetime(df_out["timestamp"]).max()
            end_date = end_timestamp.date()
        else:
            end_date = f"{year}-10-31"

        fname = f"dataloggerData_{year}-01-01_{end_date}_{gran}.csv"
        zipname_csv = fname.replace(".csv", ".zip")
        csv_path = os.path.join(DATA_PROCESSED_DIR, fname)
        zip_path = os.path.join(DATA_PROCESSED_DIR, zipname_csv)

        df_out.to_csv(csv_path, index=False, float_format="%.4f")

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname=fname)

        os.remove(csv_path)
        logging.info(f"✅ Saved: {zip_path}")

def process_logger_and_climate_data(year):
    logging.info(f"🚀 Processing year: {year}")
    df_logger = merge_all_loggers(year)
    if df_logger is None or df_logger.empty:
        raise RuntimeError("❌ No logger data found")

    df_logger["timestamp"] = df_logger["timestamp"].dt.tz_localize(None)
    df_logger = replace_bad_values(df_logger)

    end_ts = df_logger["timestamp"].max()
    df_weather = get_weather_data(year, end_timestamp=end_ts)

    df_combined = pd.merge(df_logger, df_weather, on="timestamp", how="outer")
    df_combined = add_swc(df_combined)
    df_combined = calculate_ratios(df_combined)
    # 🧹 Clean implausible VWC outliers (>150%)
    vwc_cols = [col for col in df_combined.columns if col.startswith("VWC_") and "_raw_" in col]
    num_outliers = (df_combined[vwc_cols] > 1.5).sum().sum()

    if num_outliers > 0:
        logging.warning(f"🧹 Found {num_outliers} VWC values > 1.5 — setting to NaN")
        df_combined[vwc_cols] = df_combined[vwc_cols].mask(df_combined[vwc_cols] > 1.5)
    aggregated = aggregate(df_combined, year)
    save_outputs(year, aggregated)

if __name__ == "__main__":
    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    for y in YEARS:
        if y >= 2024:
            process_logger_and_climate_data(y)
        else:
            logging.info(f"⚠️ Skipping {y}: growing season logic not supported for this year.")
