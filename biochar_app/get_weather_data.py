import os
import pandas as pd
import logging
import requests

from biochar_app.config import (
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
    COAG_STATION,
    COLLECT_PERIOD,
    METRICS_COAGDATA,
    METRICS_LABELS,
    UNITS
)

def get_weather_data(year, end_timestamp=None):
    """
    Downloads and processes CoAgMet weather data for a given year.

    Parameters:
        year (int): Year for which to retrieve data.
        end_timestamp (datetime, optional): Used for calculating dynamic cutoff.

    Returns:
        pd.DataFrame: Cleaned and resampled 15-minute weather data.
    """
    raw_path = os.path.join(DATA_RAW_DIR, f"coagmet_{year}_5min.csv")
    processed_path = os.path.join(DATA_PROCESSED_DIR, f"coagmet_{year}_15min.csv")

    logging.info(f"🌦️ Downloading CoAgMet weather data for {year}...")

    start_date = f"{int(year) - 1}-12-31T20:00"
    end_date = f"{year}-12-31T23:59"

    # Ensure end_timestamp is a datetime object
    if isinstance(end_timestamp, str):
        end_timestamp = pd.to_datetime(end_timestamp)

    # Format CoAgMet query window: from Dec 31 of previous year 20:00 to latest logger timestamp
    url = (
        f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
        f"?header=yes&fields={','.join(METRICS_COAGDATA)}"
        f"&from={year - 1}-12-31T20:00&to={end_timestamp.strftime('%Y-%m-%dT%H:%M')}"
        f"&tz=co&units={UNITS}&dateFmt=iso"
    )

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to fetch data from {url}, Status Code: {response.status_code}")

    with open(raw_path, "wb") as f:
        f.write(response.content)
    logging.info(f"✅ Raw weather data saved to {raw_path}")

    # Read and clean
    df = pd.read_csv(
        raw_path,
        skiprows=2,
        na_values=["-999"],
        names=["timestamp"] + METRICS_LABELS
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df.dropna(subset=["timestamp"], inplace=True)

    # Filter by year only
    df = df[(df["timestamp"] >= f"{year}-01-01") & (df["timestamp"] < f"{int(year)+1}-01-01")]

    df.set_index("timestamp", inplace=True)

    # Resample to 15-minute averages (sum for precip)
    agg_funcs = {col: "mean" for col in df.select_dtypes(include="number").columns}
    if "precip_mm" in agg_funcs:
        agg_funcs["precip_mm"] = "sum"

    df_15min = df.resample("15min").agg(agg_funcs).ffill().reset_index()

    df_15min.to_csv(processed_path, index=False)
    logging.info(f"✅ Processed weather data saved: {processed_path} ({len(df_15min)} rows)")

    return df_15min
