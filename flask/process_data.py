import os
import pandas as pd
import requests
import zipfile
import logging
#from collections import Counter # to look for duplicates
import pytz
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
VALUE_COLS_2024_PLUS = ["BattV_Min"] + VALUE_COLS_STANDARD  # 2024 and later include "BattV_Min"

STRIPS = ["S1", "S2", "S3", "S4"]
LOCATIONS = ["T", "M", "B"]
DEPTHS = ["1", "2", "3"]
DEPTH_WEIGHTS = {"1": 0.75, "2": 0.5, "3": 0.75}
VARS = {"VWC", "T", "EC", "SWC"}

# Define quarters based on agricultural cycles
QUARTERS = {
    "Q1_Winter": [11, 12, 1, 2],  # Nov - Feb (Dormant)
    "Q2_Early_Growing": [3, 4, 5],  # March - May (Early Growth)
    "Q3_Peak_Harvest": [6, 7, 8, 9, 10],  # June - Oct (Peak Growth & Harvest)
}

# ✅ Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# ----------------------------------------------------
# Function Index for `process_data.py`
# ----------------------------------------------------
# 1. generate_timestamp_sequence(year)  - Generates a 15-min timestamp sequence in local time. [UPDATED]
# 2. read_logger_data(logger_name, year)     - Reads and cleans logger data. [UPDATED]
# 3. get_weather_data(year)             - Fetches & processes weather data. [UPDATED]
# 4. merge_logger_data(year)            - Merges all logger datasets into a single DataFrame. [UPDATED]
# 5. merge_all_data(year)               - Merges logger & weather data for full dataset. [UPDATED]
# 6. aggregate_data(df, aggregate_year) - Aggregates data (hourly, daily, monthly, growing season). [UPDATED]
# 7. save_to_zip(df, year, last_valid_date) - Saves aggregated data into ZIP files. [UPDATED]
# 8. process_logger_and_climate_data(year) - Orchestrates full processing pipeline. [UPDATED]
# ----------------------------------------------------

# def remove_combined_data_suffix(df):
#     df.rename(columns=lambda x: x.replace("_combined_data", ""), inplace=True)
#     return df

def check_duplicate_columns(df, step_name):
    """Checks for duplicate column names and prints them."""
    duplicate_columns = df.columns[df.columns.duplicated()]
    if len(duplicate_columns) > 0:
        print(f"🚨 Duplicate columns detected, {step_name}: {duplicate_columns.tolist()}")
    else:
        print(f"✅ No duplicate columns, {step_name}.")

def ensure_timestamp_is_datetime(df, column="timestamp", dataset_name="Unknown Dataset"):
    """Ensures a specified column is in datetime format, logs if conversion is needed, and flags issues."""
    if column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            print(f"✅ Column '{column}' in '{dataset_name}' is already a datetime dtype.")
        else:
            print(f"❌ Column '{column}' in '{dataset_name}' is NOT a datetime dtype. Current dtype: {df[column].dtype}")
            df[column] = pd.to_datetime(df[column], errors="coerce")
            print(f"🔄 Converted '{column}' in '{dataset_name}' to datetime dtype.")
    else:
        print(f"🚨 ERROR: Column '{column}' NOT FOUND in '{dataset_name}'. Available columns: {df.columns}")
        raise KeyError(f"Column '{column}' is missing in dataset '{dataset_name}'!")
    return df

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

        if col == "BattV_Min" and logger_name:
            renamed_cols[col] = f"{col}_{logger_name}"  # ✅ Append logger name to BattV_Min
            continue

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

    # Use ensure_timestamp_is_datetime to finalize conversion
    df = ensure_timestamp_is_datetime(df, column, dataset_name="standardize_timestamp_format")

    return df

### 🔹 **1: Generate 15-Minute Timestamp Sequence**
def generate_timestamp_sequence(year):
    """Generate a 15-minute timestamp sequence ensuring correct start date after timezone conversion."""
    utc = pytz.utc
    denver_tz = pytz.timezone("America/Denver")

    # ✅ FIX: Start at UTC Midnight + Offset for Mountain Time
    start_date_mt = denver_tz.localize(pd.Timestamp(f"{year}-01-01 00:00"))
    end_date_mt = denver_tz.localize(pd.Timestamp(f"{year}-12-31 23:59"))

    # ✅ Convert to UTC (which accounts for MST/MDT)
    start_date_utc = start_date_mt.astimezone(utc)
    end_date_utc = end_date_mt.astimezone(utc)

    # ✅ Generate timestamps in UTC first
    timestamp_seq_utc = pd.date_range(start=start_date_utc, end=end_date_utc, freq="15min", tz=utc)

    # ✅ Convert back to Mountain Time
    timestamp_seq_local = timestamp_seq_utc.tz_convert(denver_tz)

    # ✅ Ensure we start **exactly on** January 1st in Mountain Time
    timestamp_seq_local = timestamp_seq_local[timestamp_seq_local >= start_date_mt]

    return pd.DataFrame({"timestamp": timestamp_seq_local})

### 🔹 **2: Read & Clean Logger Data**
def read_logger_data(year):
    """Reads a logger .dat file, standardizes column names, and renames columns to match expected format."""
    data_dir = f"{DATA_RAW_DIR}/datfiles_{year}/"
    df_merged = None  # Start with None so we can merge iteratively

    for logger_name in DATALOGGER_NAMES:
        file_path = os.path.join(data_dir, f"{logger_name}_Table1.dat")
        value_cols = VALUE_COLS_2024_PLUS

        if not os.path.exists(file_path):
            logging.warning(f"⚠️ Missing logger file: {file_path}")
            continue

        try:
            df = pd.read_csv(
                file_path,
                skiprows=4,
                na_values=["", "NA", "NAN"],
                names=["datetime", "RECORD"] + value_cols,
                dtype={col: "float64" for col in ["RECORD"] + value_cols},  # Explicitly set numeric columns
            )
            df.drop(columns=["RECORD"], inplace=True, errors="ignore")  # Remove unnecessary columns
            df.rename(columns={"datetime": "timestamp"}, inplace=True)  # Rename datetime → timestamp
            # ✅ Convert timestamp to datetime format **immediately**
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df.drop_duplicates(subset=["timestamp"], keep="first", inplace=True)
            df = df[df["timestamp"] >= pd.Timestamp(f"{year}-01-01 00:00")]
            #df = remove_combined_data_suffix(df)

            # ✅ Standardize timestamp format before merging
            df = standardize_timestamp_format(df, column="timestamp", source="Logger Data")

            # ✅ Standardize column names
            df = standardize_logger_column_names(df, logger_name)

            # ✅ Merge on timestamp to align all logger data correctly
            if df_merged is None:
                df_merged = df  # First dataset initializes the merge
            else:
                df_merged = pd.merge(df_merged, df, on="timestamp", how="outer")  # Outer join keeps all timestamps

        except FileNotFoundError:
            print(f"Warning: {file_path} not found.")
            return None

        except Exception as e:
            logging.error(f"❌ Error reading {file_path}: {e}")
            df.drop(columns=["RECORD"], inplace=True, errors="ignore")  # ✅ Remove unnecessary RECORD column
            df["timestamp"] = df["timestamp"].dt.tz_localize("America/Denver", ambiguous="NaT", nonexistent="NaT")
            print(f"📊 Logger {logger_name} {year}: Min Date: {df['timestamp'].min()}, Max Date: {df['timestamp'].max()}")

    return df_merged

def swc_calculations(df):
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
    swc_df.insert(0, "timestamp", pd.to_datetime(df["timestamp"], errors="coerce"))

    logging.info(f"✅ Generated SWC columns: {list(new_swc_columns.keys())}")
    #swc_df = move_timestamp_to_front(swc_df)
    # ✅ Filter only ratio columns + timestamp
    swc_columns = ["timestamp"] + [col for col in swc_df.columns if "SWC" in col]
    swc_df = swc_df[swc_columns]
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
    ratio_df = ensure_timestamp_is_datetime(ratio_df, dataset_name="calculate_15min_ratios")

    return ratio_df  # ✅ Return only timestamp + ratio columns


def get_weather_data(combined_data, year):
    """Fetches climate data from CSU API for the year, processes it, and saves it as a CSV file."""
    year = int(year)
    raw_weather_file = os.path.join(DATA_RAW_DIR, f"coagmet_{year}_5min.csv")
    processed_weather_file = os.path.join(DATA_PROCESSED_DIR, f"coagmet_{year}_15min.csv")

    if os.path.exists(raw_weather_file) and os.path.getsize(raw_weather_file) > 0:
        logging.info(f"📄 Raw weather data already exists: {raw_weather_file}")
    else:
        logging.info(f"📡 Fetching CoAgMet weather data for {year}...")
        # Ensure last_valid_timestamp is available from the datalogger data

        last_valid_timestamp = combined_data["timestamp"].max().strftime("%Y-%m-%dT%H:%M")

        url = (
            f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
            f"?header=yes&fields={','.join(METRICS_COAGDATA)}"
            f"&from={year - 1}-12-31T22:00&to={last_valid_timestamp}&tz=co&units={UNITS}&dateFmt=iso"
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

    # ✅ Filter for timestamps for the year only
    df_climate = ensure_timestamp_is_datetime(df_climate, column="timestamp", dataset_name="Weather Data (CoAgMet)")

    num_failed = df_climate["timestamp"].isna().sum()
    if num_failed > 0:
        logging.warning(f"⚠️ Warning: {num_failed} timestamps could not be parsed correctly in weather data.")
        print("🔍 Sample of failed timestamps:")
        print(df_climate.loc[df_climate["timestamp"].isna(), "timestamp"].head(10))

    df_climate = df_climate.dropna(subset=["timestamp"])  # Drop invalid timestamps

    df_climate = df_climate[
        (df_climate["timestamp"] >= pd.Timestamp(f"{year}-01-01 00:00")) &
        (df_climate["timestamp"] < pd.Timestamp(f"{year + 1}-01-01 00:00"))
        ]

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


def combine_datasets(datasets_dict, mode="rows"):
    """Combines multiple datasets while logging duplicate columns, ensuring merge on timestamp index."""

    # Remove None datasets
    cleaned_datasets = {name: df for name, df in datasets_dict.items() if df is not None}

    # 📍 Prevent referencing `combined_df` before assignment
    if not cleaned_datasets:
        logging.error("❌ No valid datasets provided for combination!")
        return None

    logging.info(f"📌 Combining datasets: {list(cleaned_datasets.keys())} using mode: {mode}...")

    # ✅ Check for duplicate columns in each dataset before merging
    for name, df in cleaned_datasets.items():
        duplicate_columns = df.columns[df.columns.duplicated()]
        print(f"🔍 Checking duplicate columns in dataset '{name}' BEFORE merging:", list(duplicate_columns))

        if "timestamp" not in df.columns:
            logging.error(f"❌ Dataset '{name}' is missing the required 'timestamp' column!")
            raise KeyError(f"Dataset '{name}' is missing 'timestamp' column!")

    # ✅ Convert 'timestamp' to index in each dataset to prevent duplicate columns
    for name, df in cleaned_datasets.items():
        df.set_index('timestamp', inplace=True)

    if mode == "columns":
        # ✅ Merge datasets along columns using index
        combined_df = pd.concat(cleaned_datasets.values(), axis=1)

        # ✅ Reset index back to a column
        combined_df.reset_index(inplace=True)

        # ✅ Check for duplicate columns after merging
        duplicate_columns_after_merge = combined_df.columns[combined_df.columns.duplicated()]
        if not duplicate_columns_after_merge.empty:
            print(f"🚨 Duplicate columns detected AFTER merging {name} in combine_datasets: {list(duplicate_columns_after_merge)}")

    elif mode == "rows":
        # ✅ Concatenate along rows and remove duplicate timestamps
        combined_df = pd.concat(cleaned_datasets.values(), axis=0, ignore_index=False).sort_index()
        combined_df.reset_index(inplace=True)  # Ensure timestamp is a column again

    else:
        logging.error("❌ Invalid mode. Use 'rows' or 'columns'.")
        raise ValueError("Invalid mode. Use 'rows' or 'columns'.")

    return combined_df


### 🔹 **5: Aggregate Data (Hourly, Daily, Monthly, Growing Season)**
def aggregate_data(df, collection_year):
    """Aggregates data at different time resolutions and saves ZIP files, including growing season aggregation."""
    logging.info("\n🔹 Aggregating data...")

    df_agg = df.copy()  # ✅ Work on a copy to avoid modifying the original DataFrame
    df_agg = ensure_timestamp_is_datetime(df_agg, dataset_name="aggregate data")
    df_agg.set_index("timestamp", inplace=True)
    df_agg.sort_index()

    # ✅ Define aggregation functions
    numeric_cols = df_agg.select_dtypes(include=["number"]).columns
    agg_funcs = {col: "mean" for col in numeric_cols}
    if "precip_mm" in df_agg.columns:
        agg_funcs["precip_mm"] = "sum"  # ✅ Precipitation should be summed, not averaged

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

    # ✅ Determine the last valid timestamp with complete data
    last_valid_row = df_agg.dropna(how="any").index.max()
    end_date = last_valid_row.strftime("%Y-%m-%d")

    # ✅ Save each aggregation as a ZIP file
    for key, df_out in agg_data.items():
        if key == "growingseason":
            zip_filename = f"dataloggerData_growingseason_{collection_year}.zip"
        else:
            zip_filename = f"dataloggerData_{collection_year}-01-01_{end_date}_{key}.zip"
        zip_path = os.path.join(DATA_PROCESSED_DIR, zip_filename)
        csv_filename = zip_filename.replace(".zip", ".csv")
        csv_path = os.path.join(DATA_PROCESSED_DIR, csv_filename)

        # ✅ Ensure timestamp is properly formatted before saving
        if "timestamp" in df_out.columns:
            df_out["timestamp"] = df_out["timestamp"].astype(str)  # ✅ Ensure proper string format

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

def process_logger_and_climate_data(year):
    """Processes logger and climate data for {year}."""
    logging.info(f"🚀 Processing logger and climate data for {year}...")

    # ✅ Step 1: Read main logger data (columns added, timestamps merged)
    combined_data = read_logger_data(year)
    if any("ratio" in col for col in combined_data.columns):
        print(f"✅ Data set combined_data contains ratio columns after read_logger_data.")

    #print("First 5 column names in combined_data, logger data added:", list(combined_data.columns)[:5])
    if combined_data is None:
        logging.error("❌ No main logger data found. Exiting process.")
        return
    check_duplicate_columns(combined_data, "from logger data creating initial combined_data")

    # ✅ Step 2: Perform SWC calculations and add as columns
    logging.info("🌱 Adding SWC calculations (Soil Water Content).")
    swc_data = swc_calculations(combined_data)  # Returns a DataFrame with new SWC columns

    # ✅ Check for column overlaps before merging SWC data
    overlapping_cols = set(combined_data.columns) & set(swc_data.columns)
    if overlapping_cols:
        print(f"⚠️ Overlapping columns BEFORE merging SWC data: {overlapping_cols}")
    # ✅ Merge SWC data as new columns
    check_duplicate_columns(combined_data, "before merging SWC data")
    combined_data = combine_datasets({
        "combined_data": combined_data,
        "swc_data": swc_data
    }, mode="columns")
    check_duplicate_columns(combined_data, "after merging SWC data")
    # ✅ Check if the dataset contains any 'ratio' columns
    if any("ratio" in col for col in combined_data.columns):
        print(f"✅ Data set combined_data contains ratio columns after SWC merger.")

    #logging.info(f"Columns after merging swc data: {combined_data.columns.to_list()}")

    # ✅ Step 3: Compute 15-minute ratios and add as columns
    #logging.info("🔢 Calculating 15-minute ratios...")
    #logging.info(f"Columns available before ratio calculations: {combined_data.columns.to_list()}")
    check_duplicate_columns(combined_data, "combined_data as input into ratio_data calculation")
    ratio_data = calculate_15min_ratios(combined_data)  # Returns a DataFrame with new ratio columns

    # ✅ Check for column overlaps before merging ratio data
    print("🔍 Sorted columns in combined_data using set:", sorted(set(combined_data.columns)))
    overlapping_cols = set(combined_data.columns) & set(ratio_data.columns)
    if overlapping_cols:
        print(f"⚠️ Overlapping columns BEFORE merging ratio data: {overlapping_cols}")

        # ✅ Print out column names in combined_data before checking overlap
        print("🔍 Columns in combined_data before checking overlap:", combined_data.columns.tolist())

        # ✅ Print out column names in ratio_data for further debugging
        print("🔍 Columns in ratio_data before merging:", ratio_data.columns.tolist())

        if overlapping_cols:
            print(f"⚠️ Overlapping columns BEFORE merging ratio data: {overlapping_cols}")
    # ✅ Merge ratio data as new columns
    check_duplicate_columns(combined_data, "before merging ratio data")
    combined_data = combine_datasets({
        "combined_data": combined_data,
        "ratio_data": ratio_data
    }, mode="columns")
    check_duplicate_columns(combined_data, "after merging ratio data")
    if any("ratio" in col for col in combined_data.columns):
        print(f"✅ Data set combined_data contains ratio columns after ratio data merger.")

    # ✅ Step 4: Read weather data and merge it as new columns
    weather_data = get_weather_data(combined_data, year)
    check_duplicate_columns(combined_data, "before merging weather data")

    # ✅ Check for column overlaps before merging weather data
    overlapping_cols = set(combined_data.columns) & set(weather_data.columns)
    if overlapping_cols:
        print(f"⚠️ Overlapping columns BEFORE merging weather data: {overlapping_cols}")

    logging.info("🌤️ Adding weather data as rightmost columns...")
    combined_data = combine_datasets({
        "combined_data": combined_data,
        "weather_data": weather_data
    }, mode="columns")
    check_duplicate_columns(combined_data, "after merging weather data")

    #print("First 5 column names in combined_data, weather added:", list(combined_data.columns)[:5])

    # ✅ Step 5: Aggregate data
    aggregate_data(combined_data, collection_year = data_year)

    logging.info(f"✅ Successfully processed and saved logger and climate data for {year}.")
# def process_logger_and_climate_data_old(year):
#     print(f"🚀 Processing data for {year}...")
#     final_df = merge_all_data(year)
#
#     # # ✅ Find the latest common timestamp across all `.dat` files
#     # last_timestamps = final_df.drop(columns=["timestamp"]).apply(lambda col: col.last_valid_index(), axis=0)
#     # last_valid_index = last_timestamps.min()  # ✅ Find the earliest last valid row to maintain consistency
#     #
#     # if last_valid_index is not None:
#     #     final_df = final_df.loc[:last_valid_index]  # ✅ Trim dataset to the last shared timestamp
#     #     last_valid_date = final_df["timestamp"].max().strftime("%Y-%m-%d")  # ✅ Format for filename
#     #     print(f"✅ Trimmed dataset to {final_df['timestamp'].max()} (last consistent data).")
#     # else:
#     #     last_valid_date = f"{year}-12-31"  # ✅ Default to year-end if no valid data found
#     # ✅ Now call `aggregate_data` with the correct `last_valid_date`
#     logging.info(f"\n🔹 Aggregating data for {year}...")
#     aggregate_data(final_df, year)

### **8: Run for 2024 and Future Years**
if __name__ == "__main__":
    # ✅ Set the working directory explicitly to ensure relative paths work
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get current script location
    os.chdir(script_dir)  # Change to that directory
    logging.info(f"📂 Set working directory to: {script_dir}")

    # ✅ Ensure processing runs
    for data_year in ["2024", "2025"]:
        process_logger_and_climate_data(data_year)
