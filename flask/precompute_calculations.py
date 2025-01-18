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


def parse_and_rename_columns(file_path, granularity="15min"):
    logging.info(f"Parsing and renaming columns for {file_path} with granularity '{granularity}'.")
    with zipfile.ZipFile(file_path, "r") as z:
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            df = pd.read_csv(f)

    renamed_columns = {}
    for col in df.columns:
        if col.startswith("VWC") or col.startswith("T") or col.startswith("EC"):
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
                    if col1 in df.columns and col2 in df.columns:
                        new_columns[ratio_col] = df[col1] / df[col2]
                        ratio_columns.append(ratio_col)
                else:
                    for depth in DEPTHS:
                        col1 = f"{var}_{depth}_raw_{strip1}_{location}_15min"
                        col2 = f"{var}_{depth}_raw_{strip2}_{location}_15min"
                        ratio_col = f"{var}_{depth}_ratio_{strip1}_{strip2}_{location}_15min"
                        if col1 in df.columns and col2 in df.columns:
                            new_columns[ratio_col] = df[col1] / df[col2]
                            ratio_columns.append(ratio_col)

    df = pd.concat([df, pd.DataFrame(new_columns)], axis=1)
    return df


def aggregate_to_daily(df):
    """
    Aggregate the data to daily granularity while handling datetime parsing and ensuring numeric columns are valid.
    """
    logging.info("Aggregating data to daily granularity.")

    # Clean and preprocess the datetime column
    df['datetime'] = df['datetime'].str.strip()
    df['datetime'] = df['datetime'].replace(r'\s+', ' ', regex=True)  # Replace multiple spaces with single space
    logging.debug(f"Full datetime column (after cleanup): {df['datetime'].tolist()}")

    # Parse datetime column using multiple formats
    df['parsed_datetime'] = pd.NaT
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%m/%d/%y %H:%M:%S", "%m/%d/%y %H:%M", "%m/%d/%y"]:
        mask = df['parsed_datetime'].isna()
        if mask.any():
            df.loc[mask, 'parsed_datetime'] = pd.to_datetime(
                df.loc[mask, 'datetime'], errors='coerce', format=fmt
            )
            logging.debug(f"Datetime parsing attempt with format {fmt}: {mask.sum()} rows remaining unparsed.")

    # Log unparsed datetime values
    unparsed = df[df['parsed_datetime'].isna()]
    if not unparsed.empty:
        logging.warning(f"Unparsed datetime values ({len(unparsed)} rows): {unparsed['datetime'].tolist()}")

    # Drop rows with invalid datetime
    before_drop_count = len(df)
    df = df.dropna(subset=["parsed_datetime"]).copy()
    after_drop_count = len(df)
    logging.info(f"Dropped rows with invalid datetime: {before_drop_count - after_drop_count}")

    # Ensure numeric columns are properly cast
    numeric_cols = df.select_dtypes(include=['number']).columns
    df.loc[:, numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    logging.debug(f"Numeric columns after coercion: {numeric_cols}")

    # Drop non-numeric columns except datetime
    df = df.select_dtypes(include=['number', 'datetime'])
    logging.debug(f"DataFrame info before aggregation:\n{df.info()}")
    logging.debug(f"First few rows before aggregation:\n{df.head()}")

    # Perform resampling to daily granularity
    if not df.empty:
        try:
            df.set_index("parsed_datetime", inplace=True)
            aggregated = df.resample("D").mean()
            aggregated.reset_index(inplace=True)
            aggregated.rename(columns={"parsed_datetime": "datetime"}, inplace=True)
            aggregated.columns = [col.replace("15min", "daily") if "15min" in col else col for col in
                                  aggregated.columns]

            # Log aggregation results
            logging.info(f"Aggregation complete. Rows after aggregation: {aggregated.shape[0]}")
            logging.info(f"Number of NaN values after aggregation: {aggregated.isna().sum().sum()}")
            return aggregated
        except Exception as e:
            logging.error(f"Error during aggregation: {e}")
            raise
    else:
        logging.warning("No valid rows left after datetime parsing. Returning empty DataFrame.")
        return pd.DataFrame()


def save_to_zip(df: pd.DataFrame, zip_path: Path, csv_filename: str):
    temp_csv_path = f"{csv_filename}.temp.csv"
    df.to_csv(temp_csv_path, index=False)
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(temp_csv_path, arcname=csv_filename)
    os.remove(temp_csv_path)
    logging.info(f"Saved DataFrame to ZIP: {zip_path}")


def process_dataset(file_path: Path, output_dir: Path):
    logging.info(f"Processing dataset: {file_path}")

    year = file_path.stem.split("_")[1].split("-")[0]  # Extract year

    df = parse_and_rename_columns(file_path, granularity="15min")
    df = add_swc_calculations(df)
    df = calculate_15min_ratios(df)

    # Save 15-minute data
    csv_filename_15min = f"{year}_15min_results.csv"
    zip_path_15min = output_dir / file_path.name.replace(".zip", "_15min.zip")
    save_to_zip(df, zip_path_15min, csv_filename_15min)

    # Aggregate to daily
    logging.info(f"Number of NaN values before aggregation: {df.isna().sum().sum()}")
    df_daily = aggregate_to_daily(df)
    logging.info(f"Number of NaN values after aggregation: {df_daily.isna().sum().sum()}")

    csv_filename_daily = f"{year}_daily_results.csv"
    zip_path_daily = output_dir / file_path.name.replace(".zip", "_daily.zip")
    save_to_zip(df_daily, zip_path_daily, csv_filename_daily)


def process_all_datasets(input_dir: str = "flask/data-raw", output_dir: str = "flask/data-processed"):
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    files = list(input_path.glob("*.zip"))
    if not files:
        logging.warning(f"No dataset files found in {input_dir}.")
        return

    logging.info(f"Found files for processing: {[file.name for file in files]}")

    for file in files:
        try:
            process_dataset(file, output_path)
            logging.info(f"Successfully processed dataset: {file.name}")
        except Exception as e:
            logging.error(f"Failed to process dataset {file.name}: {e}")


if __name__ == "__main__":
    process_all_datasets()