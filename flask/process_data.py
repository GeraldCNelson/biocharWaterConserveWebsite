import pandas as pd
import requests
import os
import zipfile

# Define Constants
STATION = "frt03"
COLLECT_PERIOD = "5min"
UNITS = "m"
DATA_DIR = "data-raw/datfiles_2024/"
OUTPUT_FILE = "data-raw/coagdata.csv"

DATALOGGER_NAMES = ["S1T", "S1M", "S1B", "S2T", "S2M", "S2B", "S3T", "S3B", "S3M", "S4T", "S4M", "S4B"]
VALUE_COLS = ["BattV_Min", "VWC_1_Avg", "EC_1_Avg", "T_1_Avg", "VWC_2_Avg", "EC_2_Avg", "T_2_Avg", "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"]

# Climate Data Metrics
METRICS_COAGDATA = ["t", "rh", "dewpt", "vp", "solarRad", "precip", "windSpeed", "windDir", "st5cm", "st15cm"]
METRICS_LABELS = ["datetime", "temp_air_degC", "rh", "dewpoint_degC", "vaporpressure_kpa", "solarrad_wm-2",
                  "precip_mm", "wind_m_s", "winddir_degN", "temp_soil_5cm_degC", "temp_soil_15cm_degC"]

### 🔹 **STEP 1: Generate 15-Minute Datetime Sequence**
def generate_datetime_sequence(year):
    start_date = pd.Timestamp(f"{year}-01-01 00:00", tz="America/Denver")
    end_date = pd.Timestamp(f"{year}-12-31 23:59", tz="America/Denver")
    datetime_seq = pd.date_range(start=start_date, end=end_date, freq="15min")
    return pd.DataFrame({"datetime": datetime_seq})

### 🔹 **STEP 2: Read & Clean Logger Data**
def read_logger_data(dlname):
    """Reads a logger .dat file and cleans missing data."""
    file_path = os.path.join(DATA_DIR, f"{dlname}_Table1.dat")
    try:
        df = pd.read_csv(
            file_path, skiprows=4, na_values=["", "NA", "NAN"],
            names=["datetime", "RECORD"] + VALUE_COLS, parse_dates=["datetime"]
        )
        df.drop(columns=["RECORD"], inplace=True)  # Remove RECORD column
        df["datetime"] = df["datetime"].dt.tz_localize("America/Denver")

        # Remove extreme values (e.g., 9999999)
        for col in VALUE_COLS:
            df[col] = df[col].apply(lambda x: None if x > 999 else x)

        return df
    except FileNotFoundError:
        print(f"Warning: {file_path} not found.")
        return None

### 🔹 **STEP 3: Fetch Colorado Ag Climate Data**
def get_coag_data(start_date, end_date):
    """Fetches climate data from CSU API & aggregates to 15-min averages."""
    start_iso = start_date.strftime("%Y-%m-%dT%H:%M")
    end_iso = end_date.strftime("%Y-%m-%dT%H:%M")

    fields = ",".join(METRICS_COAGDATA)
    url = f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{STATION}.csv?header=yes&fields={fields}&from={start_iso}&to={end_iso}&tz=co&units={UNITS}"

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from {url}")

    with open(OUTPUT_FILE, "wb") as f:
        f.write(response.content)

    df = pd.read_csv(
        OUTPUT_FILE, skiprows=2, na_values="-999", names=["station"] + METRICS_LABELS,
        parse_dates=["datetime"], dtype={"station": str}
    )

    # Aggregate to 15-minute intervals
    df["datetime_15min"] = df["datetime"].dt.floor("15T")
    df_agg = df.groupby("datetime_15min").agg({
        "temp_air_degC": "mean",
        "rh": "mean",
        "dewpoint_degC": "mean",
        "vaporpressure_kpa": "mean",
        "solarrad_wm-2": "mean",
        "wind_m_s": "mean",
        "winddir_degN": "mean",
        "temp_soil_5cm_degC": "mean",
        "temp_soil_15cm_degC": "mean",
        "precip_mm": "sum"
    }).reset_index()

    df_agg.rename(columns={"datetime_15min": "datetime", "temp_air_degC": "temp_air_mean", "precip_mm": "precip_sum"}, inplace=True)

    return df_agg

### 🔹 **STEP 4: Merge Logger & Climate Data**
def merge_all_data(year):
    datetime_df = generate_datetime_sequence(year)
    holder = None

    for dlname in DATALOGGER_NAMES:
        logger_df = read_logger_data(dlname)

        if logger_df is not None:
            merged_df = datetime_df.merge(logger_df, on="datetime", how="left")

            if holder is None:
                holder = merged_df
            else:
                holder = holder.merge(merged_df, on="datetime", how="left")

    # Merge with Climate Data
    climate_data = get_coag_data(datetime_df["datetime"].min(), datetime_df["datetime"].max())
    holder = holder.merge(climate_data, on="datetime", how="left")

    # Ensure all missing values are None
    holder = holder.where(pd.notna(holder), None)

    return holder

### 🔹 **STEP 5: Save as CSV & Zip File**
def save_final_dataset(df, year):
    csv_filename = f"data-raw/dataloggerData_{year}-01-01_{year}-12-31.csv"
    zip_filename = f"dataloggerData_{year}-01-01_{year}-12-31.zip"

    df.to_csv(csv_filename, index=False, float_format="%.4f")  # Keep precision to 4 decimals

    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_filename, arcname=os.path.basename(csv_filename))

### **Run the Full Processing Pipeline**
def process_logger_and_climate_data(year):
    print(f"Processing data for {year}...")
    final_df = merge_all_data(year)
    save_final_dataset(final_df, year)
    print(f"Processing complete! File saved as {year}-01-01.zip.")

# **Run for 2024**
if __name__ == "__main__":
    process_logger_and_climate_data(2024)