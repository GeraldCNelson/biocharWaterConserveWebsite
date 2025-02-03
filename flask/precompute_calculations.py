import os
import pandas as pd
import zipfile
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
STRIPS = ["S1", "S2", "S3", "S4"]
LOCATIONS = ["T", "M", "B"]
DEPTHS = ["1", "2", "3"]
DEPTH_WEIGHTS = {"1": 0.75, "2": 0.5, "3": 0.75}

DATA_RAW_DIR = "flask/data-raw"
DATA_PROCESSED_DIR = "flask/data-processed"


def get_latest_dataset(year, granularity="1hour"):
    """Find the most recent dataset ZIP file for a given year and granularity."""
    files = list(Path(DATA_RAW_DIR).glob(f"dataloggerData_{year}-01-01_*_{granularity}.zip"))

    if not files:
        logging.info(f"Generating new 1hour dataset for {year}...")
        files = list(Path(DATA_RAW_DIR).glob(f"dataloggerData_{year}-01-01_*.zip"))  # Fallback if no granularity

    if not files:
        logging.error(f"No dataset found for year {year}.")
        return None

    # Sort by filename to get the latest (e.g., 2025-01-26 is later than 2025-01-25)
    latest_file = max(files, key=lambda f: f.stem.split("_")[2])
    logging.info(f"📌 Latest {granularity} dataset for {year}: {latest_file.name}")
    return latest_file


def parse_and_rename_columns(file_path, granularity="15min"):
    logging.info(f"Parsing and renaming columns for {file_path} with granularity '{granularity}'.")
    with zipfile.ZipFile(file_path, "r") as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            df = pd.read_csv(f, dtype=str, low_memory=False)  # ✅ Fix mixed types warning

    renamed_columns = {}
    for col in df.columns:
        if col.startswith(("VWC", "T", "EC")):
            for strip in STRIPS:
                for location in LOCATIONS:
                    if f"{strip}{location}" in col:
                        depth = col.split("_")[1]
                        renamed_columns[col] = f"{col.split('_')[0]}_{depth}_raw_{strip}_{location}_{granularity}"
                        break

    # Rename columns
    df.rename(columns=renamed_columns, inplace=True)
    return df


def add_swc_calculations(df):
    logging.info("Adding SWC calculations.")
    swc_columns = []

    for strip in STRIPS:
        for location in LOCATIONS:
            swc_col = f"SWC_raw_{strip}_{location}_15min"
            required_columns = [f"VWC_{depth}_raw_{strip}_{location}_15min" for depth in DEPTHS]

            # ✅ Convert relevant columns to numeric (handling errors gracefully)
            for col in required_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')  # ✅ Convert strings to float, set errors to NaN

            if all(col in df.columns for col in required_columns):
                df[swc_col] = sum(df[col] * DEPTH_WEIGHTS[depth] for col, depth in zip(required_columns, DEPTHS))
                swc_columns.append(swc_col)
            else:
                missing_cols = [col for col in required_columns if col not in df.columns]
                logging.warning(f"Missing columns for SWC calculation for {strip} {location}: {missing_cols}")

    logging.info(f"Generated SWC columns: {swc_columns}")
    return df

def calculate_15min_ratios(df):
    logging.info("Calculating 15-minute ratios.")
    ratio_columns = []
    new_columns = {}

    for var in ["VWC", "T", "EC", "SWC"]:
        for strip1, strip2 in [("S1", "S2"), ("S3", "S4")]:
            for location in LOCATIONS:
                if var == "SWC":
                    col1 = f"SWC_raw_{strip1}_{location}_15min"
                    col2 = f"SWC_raw_{strip2}_{location}_15min"
                    ratio_col = f"SWC_ratio_{strip1}_{strip2}_{location}_15min"
                else:
                    for depth in DEPTHS:
                        col1 = f"{var}_{depth}_raw_{strip1}_{location}_15min"
                        col2 = f"{var}_{depth}_raw_{strip2}_{location}_15min"
                        ratio_col = f"{var}_{depth}_ratio_{strip1}_{strip2}_{location}_15min"

                # ✅ Ensure columns exist and convert to numeric
                if col1 in df.columns and col2 in df.columns:
                    df[col1] = pd.to_numeric(df[col1], errors='coerce')  # Convert non-numeric to NaN
                    df[col2] = pd.to_numeric(df[col2], errors='coerce')

                    # Perform ratio calculation
                    new_columns[ratio_col] = df[col1] / df[col2]

                    # ✅ Replace infinite values with NaN manually
                    new_columns[ratio_col].replace([float("inf"), -float("inf")], float("nan"), inplace=True)

                    ratio_columns.append(ratio_col)

    df = pd.concat([df, pd.DataFrame(new_columns)], axis=1)
    return df


def process_dataset(file_path, output_dir):
    logging.info(f"Processing dataset: {file_path}")

    year = file_path.stem.split("_")[1].split("-")[0]  # Extract year
    start_date = file_path.stem.split("_")[1]
    end_date = file_path.stem.split("_")[2]

    df = parse_and_rename_columns(file_path, granularity="15min")

    df = add_swc_calculations(df)  # ✅ Perform calculations first
    df = calculate_15min_ratios(df)  # ✅ Before setting index

    # ✅ Ensure datetime column is in the correct format
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")  # Convert to proper datetime format
    df.set_index("datetime", inplace=True)  # ✅ Set datetime index for resampling

    # ✅ Save 15-minute data before resampling
    zip_path_15min = Path(output_dir) / f"dataloggerData_{start_date}_{end_date}_15min.zip"
    save_to_zip(df, zip_path_15min, f"{year}_15min_results.csv")

    # ✅ Drop non-numeric columns before resampling
    numeric_cols = df.select_dtypes(include=["number"]).columns  # Select only numeric columns
    df_numeric = df[numeric_cols]  # Keep only numeric columns

    # ✅ Aggregate to hourly
    df_hourly = df_numeric.resample("h").mean().reset_index()

    # ✅ Save hourly results
    zip_path_hourly = Path(output_dir) / f"dataloggerData_{start_date}_{end_date}_1hour.zip"
    save_to_zip(df_hourly, zip_path_hourly, f"{year}_1hour_results.csv")

    # ✅ Aggregate to daily
    df_daily = df_numeric.resample("D").mean().reset_index()

    # ✅ Save daily results
    zip_path_daily = Path(output_dir) / f"dataloggerData_{start_date}_{end_date}_daily.zip"
    save_to_zip(df_daily, zip_path_daily, f"{year}_daily_results.csv")

    logging.info(f"✅ Processed and saved datasets for {year}: {start_date} to {end_date}")


def save_to_zip(df, zip_path, csv_filename):
    temp_csv_path = f"{csv_filename}.temp.csv"
    df.to_csv(temp_csv_path, index=False)
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(temp_csv_path, arcname=csv_filename)
    os.remove(temp_csv_path)
    logging.info(f"✅ Saved DataFrame to ZIP: {zip_path}")


def process_all_datasets(input_dir=DATA_RAW_DIR, output_dir=DATA_PROCESSED_DIR):
    """Finds the latest dataset per year and processes it for both 1hour and 15min granularities."""

    # ✅ Extract just the years dynamically from available datasets
    existing_years = sorted(set(file.stem.split("_")[1][:4] for file in Path(input_dir).glob("dataloggerData_*.zip")))

    for year in existing_years:
        dataset = get_latest_dataset(year)  # ✅ Get latest dataset without specifying granularity
        if dataset:
            process_dataset(dataset, output_dir)  # ✅ Process only ONCE per year
        else:
            logging.warning(f"No dataset found for year {year}.")


if __name__ == "__main__":
    process_all_datasets()