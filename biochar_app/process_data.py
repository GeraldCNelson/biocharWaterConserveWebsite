import os
import pandas as pd
import zipfile
import logging
import pytz
import requests  # Required if fetching weather data from CSU API in future


# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_RAW_DIR = os.path.join(BASE_DIR, "data-raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data-processed")
os.makedirs(DATA_RAW_DIR, exist_ok=True)
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)

STRIPS = ["S1", "S2", "S3", "S4"]
LOCATIONS = ["T", "M", "B"]
DEPTHS = ["1", "2", "3"]
DEPTH_WEIGHTS = {"1": 0.75, "2": 0.5, "3": 0.75}
VARS = {"VWC", "T", "EC", "SWC"}

QUARTERS = {
    "Q1_Winter": [11, 12, 1, 2],
    "Q2_Early_Growing": [3, 4, 5],
    "Q3_Peak_Harvest": [6, 7, 8, 9, 10],
}

# Weather config
COAG_OUTPUT_FILE = "coagmet_{data_year}_15min.csv"
METRICS_LABELS = ["timestamp", "temp_air_degC", "rh", "dewpoint_degC", "vaporpressure_kpa",
                  "solarrad_wm-2", "precip_mm", "wind_m_s", "winddir_degN",
                  "temp_soil_5cm_degC", "temp_soil_15cm_degC"]

def get_weather_data(data_year):
    processed_weather_file = os.path.join(DATA_PROCESSED_DIR, COAG_OUTPUT_FILE.format(year=data_year))

    if not os.path.exists(processed_weather_file):
        raise FileNotFoundError(f"❌ Expected weather data: {processed_weather_file}")

    df_climate = pd.read_csv(processed_weather_file)
    df_climate["timestamp"] = pd.to_datetime(df_climate["timestamp"], errors="coerce")
    df_climate.dropna(subset=["timestamp"], inplace=True)
    return df_climate

def generate_timestamp_sequence(data_year):
    utc = pytz.utc
    denver_tz = pytz.timezone("America/Denver")
    start_date_mt = denver_tz.localize(pd.Timestamp(f"{data_year}-01-01 00:00"))
    end_date_mt = denver_tz.localize(pd.Timestamp(f"{data_year}-12-31 23:59"))
    start_date_utc = start_date_mt.astimezone(utc)
    end_date_utc = end_date_mt.astimezone(utc)
    timestamp_seq_utc = pd.date_range(start=start_date_utc, end=end_date_utc, freq="15min", tz=utc)
    timestamp_seq_local = timestamp_seq_utc.tz_convert(denver_tz)
    timestamp_seq_local = timestamp_seq_local[timestamp_seq_local >= start_date_mt]
    return pd.DataFrame({"timestamp": timestamp_seq_local})

def read_logger_data(data_year):
    data_dir = f"{DATA_RAW_DIR}/datfiles_{data_year}/"
    df_merged = None
    for logger_name in ["S1T", "S1M", "S1B", "S2T", "S2M", "S2B", "S3T", "S3B", "S3M", "S4T", "S4M", "S4B"]:
        file_path = os.path.join(data_dir, f"{logger_name}_Table1.dat")
        if not os.path.exists(file_path):
            logging.warning(f"⚠️ Missing logger file: {file_path}")
            continue
        try:
            df = pd.read_csv(file_path, skiprows=4, na_values=["", "NA", "NAN"],
                             names=["timestamp", "RECORD"] + [f"{var}_{depth}_raw_{logger_name}" for var in VARS for depth in DEPTHS],
                             dtype={"RECORD": "float64"})
            df.drop(columns=["RECORD"], inplace=True, errors="ignore")
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            df.drop_duplicates(subset=["timestamp"], keep="first", inplace=True)
            df = df[df["timestamp"] >= pd.Timestamp(f"{data_year}-01-01 00:00", tz="UTC")]
            if df_merged is None:
                df_merged = df
            else:
                df_merged = pd.merge(df_merged, df, on="timestamp", how="outer")
        except Exception as e:
            logging.error(f"❌ Error reading {file_path}: {e}", exc_info=True)
    return df_merged

def aggregate_data(df, df_weather, data_year):
    df = pd.merge(df, df_weather, on="timestamp", how="outer")
    df.set_index("timestamp", inplace=True)
    df.index = pd.to_datetime(df.index)  # ✅ Ensures .year/.month are available
    df.sort_index(inplace=True)
    df.sort_index()
    numeric_cols = df.select_dtypes(include=["number"]).columns
    agg_funcs = {col: "mean" for col in numeric_cols}
    if "precip_mm" in df.columns:
        agg_funcs["precip_mm"] = "sum"
    end_date = df.index.max().strftime("%Y-%m-%d")
    agg_data = {
        "15min": df.copy().reset_index(),
        "1hour": df.resample("h").agg(agg_funcs).reset_index(),
        "daily": df.resample("D").agg(agg_funcs).reset_index(),
    }
    df_monthly = df[df.index.year == int(data_year)].resample("ME").agg(agg_funcs).reset_index()
    df_monthly["timestamp"] = df_monthly["timestamp"].dt.strftime("%Y-%m")
    agg_data["monthly"] = df_monthly
    growing_season_results = []
    for season_name, months in QUARTERS.items():
        season_mask = df.index.month.isin(months)
        df_season = df[season_mask]
        if not df_season.empty:
            season_means = df_season.agg(agg_funcs)
            season_means["timestamp"] = season_name
            growing_season_results.append(season_means)
    if growing_season_results:
        df_growingseason = pd.DataFrame(growing_season_results)
        df_growingseason.reset_index(drop=True, inplace=True)
        agg_data["growingseason"] = df_growingseason
    for key, df_out in agg_data.items():
        zip_filename = f"dataloggerData_{data_year}-01-01_{end_date}_{key}.zip"
        zip_path = os.path.join(DATA_PROCESSED_DIR, zip_filename)
        csv_filename = zip_filename.replace(".zip", ".csv")
        csv_path = os.path.join(DATA_PROCESSED_DIR, csv_filename)
        df_out.to_csv(csv_path, index=False, encoding="utf-8")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, csv_filename)
        os.remove(csv_path)
        logging.info(f"✅ Saved: {zip_path}")

if __name__ == "__main__":
    for year_to_process in ["2025"]:
        logging.info(f"🚀 Processing data for {year_to_process}...")
        df_logger = read_logger_data(year_to_process)
        df_weather = get_weather_data(year_to_process)
        if df_logger is not None and not df_logger.empty:
            aggregate_data(df_logger, df_weather, year_to_process)
        else:
            logging.error(f"❌ No valid data for {year_to_process}. Skipping aggregation.")
