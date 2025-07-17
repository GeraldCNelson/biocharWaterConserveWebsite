import os
import pandas as pd
import requests
import zipfile
import logging
from collections import Counter # to look for duplicates
#import psutil
# import traceback  # Add this import at the top

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
COAG_STATION = "frt03"
COLLECT_PERIOD = "5min"
METRICS_COAGDATA = ["t", "rh", "dewpt", "vp", "solarRad", "precip", "windSpeed", "windDir", "st5cm", "st15cm"]
METRICS_LABELS = ["timestamp", "temp_air_degC", "rh", "dewpoint_degC", "vaporpressure_kpa", "solarrad_wm-2",
                  "precip_mm", "wind_m_s", "winddir_degN", "temp_soil_5cm_degC", "temp_soil_15cm_degC"]
UNITS = "m"
# ✅ Paths relative to script location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get script directory
DATA_RAW_DIR = os.path.join(BASE_DIR, "data-raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data-processed")
# Ensure necessary directories exist
os.makedirs(DATA_RAW_DIR, exist_ok=True)
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)

COAG_OUTPUT_FILE = os.path.join(DATA_RAW_DIR, "coagdata.csv")

DATALOGGER_NAMES = ["S1T", "S1M", "S1B", "S2T", "S2M", "S2B", "S3T", "S3B", "S3M", "S4T", "S4M", "S4B"]
VALUE_COLS_STANDARD = ["VWC_1_Avg", "EC_1_Avg", "T_1_Avg", "VWC_2_Avg", "EC_2_Avg", "T_2_Avg", "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"]

STRIPS = ["S1", "S2", "S3", "S4"]
LOCATIONS = ["T", "M", "B"]
DEPTHS = ["1", "2", "3"]
DEPTH_WEIGHTS = {"1": 0.75, "2": 0.5, "3": 0.75}
VARS = {"VWC", "T", "EC", "SWC"}
collection_year = '2023'

# Define quarters based on agricultural cycles
QUARTERS = {
    "Q1_Winter": [11, 12, 1, 2],  # Nov - Feb (Dormant)
    "Q2_Early_Growing": [3, 4, 5],  # March - May (Early Growth)
    "Q3_Peak_Harvest": [6, 7, 8, 9, 10],  # June - Oct (Peak Growth & Harvest)
}


def debug_variable_values(df, function_name):
    """Logs values of key columns at the specified timestamp for debugging."""
    target_timestamp = "2023-05-15 11:30:00"
    if "timestamp" in df.columns:
        subset_df = df[df["timestamp"] >= target_timestamp].head(10)

        for col in ["VWC_1_raw_S1_T", "VWC_1_raw_S2_T", "VWC_1_ratio_S1_S2_T"]:
            if col in subset_df.columns:
                logging.info(
                    f"🔍 [{function_name}] {col} values starting at {target_timestamp}: {subset_df[['timestamp', col]].to_string(index=False)}")
            else:
                logging.warning(f"⚠️ [{function_name}] {col} not found in DataFrame.")


def standardize_logger_column_names(df, logger_name=None):
    """Standardizes logger column names for consistency across datasets.

    Args:
        df (pd.DataFrame): The dataframe containing logger data.
        logger_name (str, optional): The logger name (e.g., 'S1T') for renaming columns. Defaults to None.

    Returns:
        pd.DataFrame: The dataframe with standardized column names.
    """
    renamed_cols = {}
    for col in df.columns:
        if col == "timestamp":
            continue  # ✅ Keep timestamp as is

        parts = col.split("_")  # Expected format: VWC_1_Avg OR VWC_1_Avg_S1T
        if logger_name:
            if len(parts) == 3:  # VWC_1_Avg → VWC_1_raw_S1_T
                variable, depth, _ = parts
                standardized_name = f"{variable}_{depth}_raw_{logger_name[:2]}_{logger_name[2:]}"
                renamed_cols[col] = standardized_name
            else:
                logging.warning(f"⚠️ Unexpected column format ({col}) in {logger_name}, keeping original.")
        else:
            if len(parts) == 4:  # VWC_1_Avg_S1T → VWC_1_raw_S1_T
                variable, depth, _, location = parts
                standardized_name = f"{variable}_{depth}_raw_{location[:2]}_{location[2:]}"
                renamed_cols[col] = standardized_name
            else:
                logging.warning(f"⚠️ Unexpected column format ({col}), keeping original.")

    df.rename(columns=renamed_cols, inplace=True)
    #logging.info("✅ Standardized column names in standardize_logger_column_names.")
    return df


def move_timestamp_to_front(df):
    print("🔍 Checking duplicate columns in move timestamp:", df.columns[df.columns.duplicated()])
    """Moves the 'timestamp' column to the front of the DataFrame."""
    if "timestamp" in df.columns:
        cols = ["timestamp"] + [col for col in df.columns if col != "timestamp"]
        df = df.loc[:, cols]  # Use loc to safely reorder columns without duplicating
    return df


def detect_timestamp_format(series):
    """
    Detects the timestamp format in a given pandas Series by checking against known formats.
    Returns the detected format or None if no match is found.
    """
    known_formats = [
        "%Y-%m-%d %H:%M:%S",  # Standard ISO format
        "%Y-%m-%d %H:%M",      # Without seconds
        "%m/%d/%y %H:%M",      # US-style short year
        "%m/%d/%Y %H:%M",      # US-style full year
        "%m-%d-%y %H:%M",      # Dashed short year
        "%m-%d-%Y %H:%M",      # Dashed full year
    ]

    for fmt in known_formats:
        try:
            parsed_series = pd.to_datetime(series, format=fmt, errors="coerce")
            if parsed_series.notna().sum() > 0:  # If at least some values are valid
                return fmt
        except ValueError:  # Catch only ValueError (incorrect format issues)
            continue
        except TypeError:  # Catch TypeError (non-string values, etc.)
            continue
    return None  # No matching format found


def standardize_timestamp_format(df, column="timestamp", source="Unknown File"):
    """Ensures timestamp column is in standard format and converts it to datetime."""
    if column not in df.columns:
        print(f"❌ ERROR: {column} column is missing in {source}.")
        return df

    detected_format = detect_timestamp_format(df[column])

    if detected_format:
        df[column] = pd.to_datetime(df[column], format=detected_format, errors="coerce")
    else:
        print(f"⚠️ No known timestamp format detected in {source}. Attempting generic parsing.")
        df[column] = pd.to_datetime(df[column], errors="coerce")

    # # Use ensure_timestamp_is_datetime to finalize conversion
    # df = ensure_timestamp_is_datetime(df, column, dataset_name="standardize_timestamp_format")

    return df


def read_logger_data_2023():
    """Reads multiple logger .dat files for 2023, renames columns, and returns a combined DataFrame."""
    logging.info("📥 Reading main 2023 logger data...")

    df_merged = None  # Start with None so we can merge iteratively

    for logger_name in DATALOGGER_NAMES:
        file_path = os.path.join(DATA_RAW_DIR, f"datfiles_2023/{logger_name}_Table1.dat")
        if not os.path.exists(file_path):
            logging.warning(f"⚠️ Missing logger file: {file_path}")
            continue

        try:
            df = pd.read_csv(
                file_path,
                skiprows=4,
                na_values=["", "NA", "NAN"],
                names=["datetime", "RECORD"] + VALUE_COLS_STANDARD,
                dtype={col: "float64" for col in ["RECORD"] + VALUE_COLS_STANDARD},  # Explicitly set numeric columns
            )
            df.drop(columns=["RECORD"], inplace=True, errors="ignore")  # Remove unnecessary columns
            df.rename(columns={"datetime": "timestamp"}, inplace=True)  # Rename datetime → timestamp
            # ✅ Convert timestamp to datetime format **immediately**
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

            df.drop_duplicates(subset=["timestamp"], keep="first", inplace=True)

            # ✅ Standardize timestamp format before merging
            df = standardize_timestamp_format(df, column="timestamp", source="Logger Data")

            # ✅ Standardize column names
            df = standardize_logger_column_names(df, logger_name)

            # ✅ Merge on timestamp to align all logger data correctly
            if df_merged is None:
                df_merged = df  # First dataset initializes the merge
            else:
                df_merged = pd.merge(df_merged, df, on="timestamp", how="outer")  # Outer join keeps all timestamps

        except Exception as e:
            logging.error(f"❌ Error reading {file_path}: {e}")

    # ✅ Debugging: Print sample values before returning
    # if df_merged is not None:
    #     debug_variable_values(df_merged, "read_logger_data_2023")

    return df_merged


def read_extracted_logger_data_2023():
    """Reads and renames extracted logger data for late 2023."""
    extracted_2023_path = os.path.join(DATA_RAW_DIR, "datfiles_2023", "dataloggerData_2023-extractFrom2024.csv")

    if not os.path.exists(extracted_2023_path):
        logging.warning(f"⚠️ Extracted logger data for 2023 not found: {extracted_2023_path}")
        return None

    logging.info(f"📥 Reading extracted logger data: {extracted_2023_path}")

    try:
        # ✅ Read first row to extract actual column names
        first_row = pd.read_csv(extracted_2023_path, skiprows=0, nrows=1, header=None)

        # ✅ Get datalogger names from first row, skipping "datetime"
        full_column_names = ["timestamp"] + first_row.iloc[0, 1:].tolist()

        # ✅ Check for empty or unexpected column names
        if "" in full_column_names or None in full_column_names:
            raise ValueError("⚠️ Found empty column names in extracted data!")

        # ✅ Count occurrences of each column name
        column_counts = Counter(full_column_names)
        duplicates = {col: count for col, count in column_counts.items() if count > 1}
        if duplicates:
            raise ValueError(f"🔴 Duplicate column names detected: {duplicates}")

        df = pd.read_csv(
            extracted_2023_path,
            skiprows=1,  # Skip the first row since we manually extracted column names
            na_values=["", "NA", "NAN"],
            names=full_column_names,
            dtype={col: "float64" for col in full_column_names if col not in ["timestamp"]}
        )
        # ✅ Convert timestamp to datetime format **immediately**
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # ✅ Standardize timestamp format before any processing
        df = standardize_timestamp_format(df, column="timestamp", source="Extracted Logger Data")

        # ✅ Identify timestamps that failed conversion
        num_failed = df["timestamp"].isna().sum()
        if num_failed > 0:
            logging.warning(f"⚠️ Warning: {num_failed} timestamps could not be parsed correctly.")

        df.drop(columns=["temp_air_mean", "precip_sum"], inplace=True, errors="ignore")  # ✅ Remove unnecessary columns
        df.drop_duplicates(subset=["timestamp"], keep="first", inplace=True)  # ✅ Remove duplicate timestamps

        # ✅ Standardize column names
        df = standardize_logger_column_names(df)

        logging.info(f"✅ Extracted logger data loaded successfully. Rows: {len(df)}")
        return df

    except Exception as e:
        logging.error(f"❌ Error reading extracted logger data: {e}")
        return None


def combine_datasets(datasets_dict, mode="rows"):
    """Combines multiple datasets either by adding rows or columns using a common index.

    Args:
        datasets_dict (dict): Dictionary of dataset names as keys and DataFrames as values.
        mode (str): "rows" (append as new rows) or "columns" (merge on timestamp).

    Returns:
        pd.DataFrame: The combined dataset.
    """
    logging.info(f"📌 Combining datasets using mode: {mode}...")

    # Extract dataset names and DataFrames
    dataset_names = list(datasets_dict.keys())
    #datasets = list(datasets_dict.values())

    logging.info(f"🔍 Datasets received: {dataset_names}")

    # Remove None datasets
    cleaned_datasets = {name: df for name, df in datasets_dict.items() if df is not None}

    if not cleaned_datasets:
        logging.error("❌ No valid datasets provided for combination!")
        return None

    # ✅ Ensure all datasets contain a "timestamp" column
    for name, df in cleaned_datasets.items():
        if "timestamp" not in df.columns:
            logging.error(f"❌ Dataset '{name}' is missing the required 'timestamp' column!")
            raise KeyError(f"Dataset '{name}' is missing 'timestamp' column!")

    if mode == "rows":
        # Stack datasets (ensuring all timestamps are retained)
        combined_df = pd.concat(cleaned_datasets.values(), axis=0, ignore_index=True).sort_values("timestamp")
    elif mode == "columns":
        # Use timestamp as index for alignment before concatenation
        for name, df in cleaned_datasets.items():
            # df = ensure_timestamp_is_datetime(df, dataset_name=name)
            df = df.set_index("timestamp")  # Set timestamp as index
            df = df.sort_index()  # Ensure chronological order

            cleaned_datasets[name] = df  # ✅ Update the dictionary with the modified DataFrame

        # **Fix for duplicate column names** 🛠
        all_columns = []
        for df in cleaned_datasets.values():
            all_columns.extend(df.columns)
        duplicate_columns = {col for col in all_columns if all_columns.count(col) > 1}

        for name, df in cleaned_datasets.items():
            df.rename(columns={col: f"{col}_{name}" for col in duplicate_columns}, inplace=True)

        # Concatenate along columns while preserving timestamp index
        combined_df = pd.concat(cleaned_datasets.values(), axis=1)

        # ✅ Fix column names after merging
        combined_df.rename(columns=lambda x: x.replace("_merged", "").replace("_ratio_data", ""), inplace=True)

        # Reset index after merging
        combined_df.reset_index(inplace=True, names=["timestamp"])
    else:
        logging.error("❌ Invalid mode. Use 'rows' or 'columns'.")
        raise ValueError("Invalid mode. Use 'rows' or 'columns'.")

    # ✅ Drop duplicate timestamps (only relevant when mode is "rows")
    if mode == "rows":
        combined_df.drop_duplicates(subset=["timestamp"], keep="first", inplace=True)

    logging.info("✅ Successfully combined datasets.")

    # ✅ Debugging: Print duplicate timestamps
    duplicate_timestamps = combined_df[combined_df.duplicated(subset=["timestamp"], keep=False)]
    if not duplicate_timestamps.empty:
        logging.warning(f"⚠️ Duplicates found after merging:")
        logging.warning(f"{duplicate_timestamps}")

    return combined_df


def add_swc_calculations(df):
    """Computes Soil Water Content (SWC) based on weighted depth calculations."""
    new_swc_columns = {}  # Store new columns before adding them to df

    for strip in STRIPS:
        for location in LOCATIONS:
            swc_col = f"SWC_1_raw_{strip}_{location}"  # Match naming convention
            required_columns = [f"VWC_{depth}_raw_{strip}_{location}" for depth in DEPTHS]

            # Check if all required columns exist before proceeding
            existing_cols = [col for col in required_columns if col in df.columns]

            if len(existing_cols) != len(DEPTHS):
                missing_cols = [col for col in required_columns if col not in df.columns]
                logging.warning(f"⚠️ Missing columns for SWC calculation for {strip} {location}: {missing_cols}")
                continue  # Skip this SWC calculation if any required column is missing

            # Convert relevant columns to numeric and handle non-numeric values safely
            for col in existing_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Calculate SWC based on weighted depths
            new_swc_columns[swc_col] = sum(df[col] * DEPTH_WEIGHTS[depth] for col, depth in zip(existing_cols, DEPTHS))

    # ✅ Create a new DataFrame with SWC columns and timestamp
    swc_df = pd.DataFrame(new_swc_columns, index=df.index)

    # ✅ Ensure 'timestamp' is included
    if "timestamp" in df.columns:
        swc_df["timestamp"] = df["timestamp"]
    else:
        logging.error("❌ 'timestamp' column is missing from SWC data!")

    #logging.info(f"✅ Generated SWC columns: {list(new_swc_columns.keys())}")
    swc_df = move_timestamp_to_front(swc_df)
    return swc_df  # ✅ Return only the SWC DataFrame


def calculate_15min_ratios(df):
    """Computes the ratio between Biochar-injected strips and non-Biochar strips, returning only new ratio columns."""
    logging.info("🔢 Starting 15-minute ratios calculation...")

    if "timestamp" not in df.columns:
        raise KeyError("❌ 'timestamp' column is missing in `calculate_15min_ratios` input!")

    ratio_df = df.copy()  # ✅ Keep all data initially

    # ✅ Keep only timestamp, all ratio calculations will be added to this DataFrame
    ratio_df = ratio_df[["timestamp"]]

    for var in VARS:
        for strip1, strip2 in [("S1", "S2"), ("S3", "S4")]:
            for location in LOCATIONS:
                for depth in DEPTHS:
                    if var == "SWC" and depth != "1":
                        continue  # ✅ SWC ratios only use depth 1

                    col1 = f"{var}_{depth}_raw_{strip1}_{location}"
                    col2 = f"{var}_{depth}_raw_{strip2}_{location}"
                    ratio_col = f"{var}_{depth}_ratio_{strip1}_{strip2}_{location}"

                    if col1 not in df.columns or col2 not in df.columns:
                        continue  # ✅ Skip if required columns are missing

                    valid_rows = ratio_df.index[df[[col1, col2]].notna().all(axis=1) & df[col2].astype(bool)]
                    ratio_df.loc[valid_rows, ratio_col] = df.loc[valid_rows, col1] / df.loc[valid_rows, col2]

    logging.info(f"✅ Added {len([col for col in ratio_df.columns if '_ratio_' in col])} ratio columns.")

    # ✅ Ensure timestamp is datetime
    # ratio_df = ensure_timestamp_is_datetime(ratio_df, dataset_name="calculate_15min_ratios")

    return ratio_df  # ✅ Return only timestamp + ratio columns

def get_weather_data_2023():
    """Fetches climate data from CSU API for 2023, processes it, and saves it as a CSV file."""
    raw_weather_file = os.path.join(DATA_RAW_DIR, "coagmet_2023_5min.csv")
    processed_weather_file = os.path.join(DATA_PROCESSED_DIR, "coagmet_2023_15min.csv")

    if os.path.exists(raw_weather_file) and os.path.getsize(raw_weather_file) > 0:
        logging.info(f"📄 Raw weather data already exists: {raw_weather_file}")
    else:
        logging.info(f"📡 Fetching CoAgMet weather data for 2023...")
        url = (
            f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
            f"?header=yes&fields={','.join(METRICS_COAGDATA)}"
            f"&from=2022-12-31T20:00&to=2023-12-31T23:59&tz=co&units={UNITS}&dateFmt=iso"
        )

        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"❌ Failed to fetch data from {url}, Status Code: {response.status_code}")

        with open(raw_weather_file, "wb") as f:
            f.write(response.content)
        logging.info(f"✅ Weather data saved: {raw_weather_file}")

    df_climate = pd.read_csv(
        raw_weather_file, skiprows=2, na_values="-999",
        names=["station"] + METRICS_LABELS
    )
    # ✅ Convert timestamp to datetime format **immediately**
    df_climate["timestamp"] = pd.to_datetime(df_climate["timestamp"], errors="coerce")

    # ✅ Ensure timestamp column exists before processing
    if "timestamp" not in df_climate.columns:
        raise ValueError("❌ 'timestamp' column missing in weather data!")

    # ✅ Standardize timestamp format
    df_climate = standardize_timestamp_format(df_climate, column="timestamp", source="Weather Data (CoAgMet)")

    # ✅ Filter for 2023 timestamps only
    # df_climate = ensure_timestamp_is_datetime(df_climate, column="timestamp", dataset_name="Weather Data (CoAgMet)")

    num_failed = df_climate["timestamp"].isna().sum()
    if num_failed > 0:
        logging.warning(f"⚠️ Warning: {num_failed} timestamps could not be parsed correctly in weather data.")
        print("🔍 Sample of failed timestamps:")
        print(df_climate.loc[df_climate["timestamp"].isna(), "timestamp"].head(10))

    df_climate = df_climate.dropna(subset=["timestamp"])  # Drop invalid timestamps

    df_climate = df_climate[(df_climate["timestamp"] >= "2023-01-01 00:00") &
                            (df_climate["timestamp"] < "2024-01-01 00:00")]

    df_climate.to_csv(raw_weather_file, index=False)

    # ✅ Ensure timestamp is set as index before resampling
    df_climate.set_index("timestamp", inplace=True)

    if not isinstance(df_climate.index, pd.DatetimeIndex):
        raise TypeError(f"❌ Expected DatetimeIndex, but got {type(df_climate.index)} instead!")

    # ✅ Resample to 15-minute intervals
    agg_funcs = {col: "mean" for col in df_climate.select_dtypes(include=["number"]).columns}
    if "precip_mm" in df_climate.columns:
        agg_funcs["precip_mm"] = "sum"

    df_climate = df_climate.resample("15min").agg(agg_funcs).ffill()

    # ✅ Reset index before saving
    #df_climate.reset_index(inplace=True)
    df_climate.reset_index(inplace=True, names=["timestamp"])

    df_climate.to_csv(processed_weather_file, index=False)
    # logging.info(f"✅ Processed weather data saved: {processed_weather_file}, Rows: {len(df_climate)}")
    # df_climate.reset_index(inplace=True)  # Ensure timestamp is a column before returning
    return df_climate


def aggregate_data(df):
    """Aggregates data at different time resolutions and saves ZIP files, including growing season aggregation."""
    logging.info("\n🔹 Aggregating data...")

    df_agg = df.copy()  # ✅ Work on a copy to avoid modifying the original DataFrame
    # df_agg = ensure_timestamp_is_datetime(df_agg, dataset_name="aggregate data")
    df_agg.set_index("timestamp", inplace=True)
    df_agg.sort_index()

    # ✅ Define aggregation functions
    numeric_cols = df_agg.select_dtypes(include=["number"]).columns
    agg_funcs = {col: "mean" for col in numeric_cols}
    if "precip_mm" in df_agg.columns:
        agg_funcs["precip_mm"] = "sum"

    # ✅ Standard Aggregations (15min, 1hour, daily)
    agg_data = {
        "15min": df_agg.copy().reset_index(drop=False),
        "1hour": df_agg.resample("h").agg(agg_funcs).reset_index(drop=False),
        "daily": df_agg.resample("D").agg(agg_funcs).reset_index(drop=False),
    }

    # ✅ Filter only data from the collection year
    df_monthly = df_agg[df_agg.index.year == int(collection_year)]

    # ✅ Aggregate daily first
    df_monthly = df_monthly.resample("D").agg(agg_funcs)

    # ✅ Compute monthly mean by dividing sums by the number of days in the month
    df_monthly = df_monthly.groupby(df_monthly.index.to_period("M")).mean(numeric_only=True)

    # ✅ Reset index to keep the month as a string label ("YYYY-MM")
    df_monthly.index = df_monthly.index.astype(str)

    # ✅ Store in aggregation dictionary
    agg_data["monthly"] = df_monthly.reset_index()

    # ✅ Growing Season Aggregation: Compute Mean Across Full Season
    growing_season_results = []
    for season_name, months in QUARTERS.items():
        season_mask = df_agg.index.month.isin(months)
        df_season = df_agg[season_mask]

        if not df_season.empty:  # ✅ Avoid adding empty rows
            season_means = df_season.agg(agg_funcs)
            season_means["timestamp"] = season_name  # ✅ Label instead of datetime
            growing_season_results.append(season_means)

    df_growingseason = pd.DataFrame(growing_season_results)

    if not df_growingseason.empty:
        df_growingseason = df_growingseason[["timestamp"] + [col for col in df_growingseason.columns if col != "timestamp"]]
        df_growingseason.rename(columns=lambda x: x.replace("raw", "gseason") if "raw" in x else x, inplace=True)
        df_growingseason.reset_index(drop=True, inplace=True)  # ✅ Drop unnecessary index column

        # ✅ Store in aggregation dictionary
        agg_data["growingseason"] = df_growingseason

    # ✅ **Hardcode End Date** to "2023-12-31" (since 2023 is fully complete)
    end_date = "2023-12-31"

    # ✅ Save each aggregation as a ZIP file
    for key, df_out in agg_data.items():
        zip_filename = f"dataloggerData_{collection_year}-01-01_{end_date}_{key}.zip"
        zip_path = os.path.join(DATA_PROCESSED_DIR, zip_filename)
        csv_filename = zip_filename.replace(".zip", ".csv")
        csv_path = os.path.join(DATA_PROCESSED_DIR, csv_filename)

        # ✅ Ensure timestamp is properly formatted before saving
        if "timestamp" in df_out.columns:
            df_out["timestamp"] = df_out["timestamp"].astype(str)

        # ✅ Reduce precision of float columns to 4 decimal places
        numeric_cols = df_out.select_dtypes(include=["number"]).columns
        df_out[numeric_cols] = df_out[numeric_cols].round(4)

        # ✅ Save CSV properly (without index column)
        df_out.to_csv(csv_path, index=False, encoding="utf-8")

        # ✅ Create a ZIP file for each aggregation
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, csv_filename)

        os.remove(csv_path)  # ✅ Remove CSV file after adding to ZIP
        logging.info(f"✅ Saved: {zip_path}")


def process_logger_and_climate_data_2023():
    """Processes logger and climate data for 2023."""
    logging.info("🚀 Processing logger and climate data for 2023...")

    # ✅ Step 1: Read main logger data
    main_data = read_logger_data_2023()
    if main_data is None:
        logging.error("❌ No main logger data found. Exiting process.")
        return

    # ✅ Step 2: Read extracted late-2023 logger data
    extracted_logger_data = read_extracted_logger_data_2023()

    # ✅ Step 3: Merge extracted logger data by adding rows
    logging.info("📌 Merging extracted logger data with main logger data...")
    combined_data = combine_datasets({
        "main_data": main_data,
        "extracted_logger_data": extracted_logger_data
    }, mode="rows")

    # ✅ Step 4: Perform SWC calculations and add as columns
    logging.info("🌱 Adding SWC calculations (Soil Water Content).")
    swc_data = add_swc_calculations(combined_data)

    # ✅ Ensure 'timestamp' exists in swc_data
    if "timestamp" not in swc_data.columns:
        raise ValueError("❌ 'timestamp' is missing from SWC data before merging!")

    # ✅ Merge SWC data as new columns
    combined_data = combine_datasets({
        "combined_data": combined_data,
        "swc_data": swc_data
    }, mode="columns")

    # ✅ Step 5: Compute 15-minute ratios and add as columns
    logging.info("🔢 Calculating 15-minute ratios...")
    ratio_data = calculate_15min_ratios(combined_data)

    # ✅ Merge ratio data
    combined_data = combine_datasets({
        "combined_data": combined_data,
        "ratio_data": ratio_data
    }, mode="columns")

    # ✅ Step 6: Read weather data and merge it as new columns
    weather_data = get_weather_data_2023()
    if weather_data is not None:
        logging.info("🌤️ Adding weather data as rightmost columns...")
        combined_data = combine_datasets({
            "combined_data": combined_data,
            "weather_data": weather_data
        }, mode="columns")

    # ✅ Step 7: Aggregate data
    aggregate_data(combined_data)

    logging.info("✅ Successfully processed and saved logger and climate data for 2023.")

if __name__ == "__main__":
    process_logger_and_climate_data_2023()
    # # ✅ Set the working directory explicitly to ensure relative paths work
    # script_dir = os.path.dirname(os.path.abspath(__file__))  # Get current script location
    # os.chdir(script_dir)  # Change to that directory
    # logging.info(f"📂 Set working directory to: {script_dir}")
    #process_logger_and_climate_data_2023("2023")