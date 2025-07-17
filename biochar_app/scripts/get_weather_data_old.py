import pandas as pd
import logging
import os

# Example paths (adjust these if needed)
DATA_RAW_DIR = "data-raw"
DATA_PROCESSED_DIR = "data-processed"

def get_weather_data(year, end_timestamp=None):
    raw_file = os.path.join(DATA_RAW_DIR, f"coagmet_{year}_5min.csv")
    processed_file = os.path.join(DATA_PROCESSED_DIR, f"coagmet_{year}_15min.csv")

    if not os.path.exists(processed_file):
        logging.info(f"🌧️ Downloading CoAgMet weather data for {year}...")
        df_raw = pd.read_csv(raw_file)

        # Example: assume raw data has 'timestamp' and weather variables
        df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'], errors='coerce')

        # Resample to 15-minute intervals, adjusting as needed
        df_raw.set_index('timestamp', inplace=True)
        df_resampled = df_raw.resample('15min').mean().reset_index()

        # Ensure timestamp is timezone-localized
        df_resampled['timestamp'] = df_resampled['timestamp'].dt.tz_localize('America/Denver', ambiguous='NaT', nonexistent='NaT')

        df_resampled.to_csv(processed_file, index=False, float_format='%.4f')
        logging.info(f"✅ Processed weather data saved: {processed_file} ({len(df_resampled)} rows)")
    else:
        logging.info(f"📦 Using cached processed weather data: {processed_file}")
        df_resampled = pd.read_csv(processed_file)
        df_resampled['timestamp'] = pd.to_datetime(df_resampled['timestamp'], errors='coerce')
        df_resampled['timestamp'] = df_resampled['timestamp'].dt.tz_localize('America/Denver', ambiguous='NaT', nonexistent='NaT')

    # Optional: trim data to match end_timestamp if provided
    if end_timestamp is not None:
        df_resampled = df_resampled[df_resampled['timestamp'] <= end_timestamp]

    return df_resampled
