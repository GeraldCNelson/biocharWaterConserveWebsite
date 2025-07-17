import os
import logging
logger = logging.getLogger(__name__)
import requests
from io import StringIO
import pandas as pd
from datetime import datetime

from biochar_app.scripts.config import (
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
    COLLECT_PERIOD,
    COAG_STATION,
    COAGMET_VARIABLE_MAP,
    DEFAULT_UNITS,
    US_UNITS,
    METRIC_UNITS,
DEFAULT_TIMEZONE,
)

# our base 5-min column names
BASE_COLUMNS = list(COAGMET_VARIABLE_MAP.values())


def get_weather_column_labels(units: str = DEFAULT_UNITS) -> dict[str, str]:
    """
    Build a fresh mapping from the raw 15-min column names (e.g. "soil_temp_5cm",
    "precip") to the final column names with proper unit suffixes (e.g. "_inches"
    or "_mm", and converting "5cm" → "2in" when needed).
    """
    unit_map = US_UNITS if units == "us" else METRIC_UNITS
    final_map: dict[str, str] = {}

    for raw_key, base_name in COAGMET_VARIABLE_MAP.items():
        if base_name not in BASE_COLUMNS:
            continue

        label = base_name
        # convert soil‐temp depths for US units
        if units == "us":
            if base_name == "soil_temp_5cm":
                label = "soil_temp_2in"
            elif base_name == "soil_temp_15cm":
                label = "soil_temp_6in"

        # append unit suffix if defined
        suffix = unit_map.get(base_name, "")
        if suffix:
            label = f"{label}_{suffix}"

        final_map[base_name] = label

    return final_map


def fetch_weather_data(year: int, end=None) -> pd.DataFrame:
    """
    Fetches CoAgMet weather data for a given year and returns a 15-minute
    DataFrame (precip summed, other vars averaged), with proper unit labels.
    """
    raw_path = os.path.join(DATA_RAW_DIR, f"coagmet_{year}_5min.csv")

    # ↓ this used to be logging.info – now DEBUG so only your caller logs at INFO
    logger.debug(f"🌦️ Downloading CoAgMet weather data for {year}…")

    # build ISO ‘from’/‘to’
    start_iso = f"{year-1}-12-31T20:00"
    if end is None:
        end_iso = f"{year}-12-31T23:59"
    elif isinstance(end, str):
        dt_end = pd.to_datetime(end)
        end_iso = dt_end.strftime("%Y-%m-%dT%H:%M")
    elif isinstance(end, (pd.Timestamp, datetime)):
        end_iso = pd.to_datetime(end).strftime("%Y-%m-%dT%H:%M")
    else:
        raise ValueError(f"Unsupported type for `end`: {type(end)}")

    url = (
        f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
        f"?header=yes&fields={','.join(COAGMET_VARIABLE_MAP.keys())}"
        f"&from={start_iso}&to={end_iso}"
        f"&tz=co&units={DEFAULT_UNITS}&dateFmt=iso"
    )
    logger.debug("CoAgMet URL: %s", url)

    # download
    resp = requests.get(url); resp.raise_for_status()
    with open(raw_path, "wb") as f:
        f.write(resp.content)

    # load and parse
    df_raw = pd.read_csv(
        StringIO(resp.text),
        skiprows=2,
        na_values=["-999"],
        names=["timestamp"] + list(COAGMET_VARIABLE_MAP.values()),
        parse_dates=["timestamp"],
    )

    # localize & resample
    df_raw["timestamp"] = (
        df_raw["timestamp"]
            .dt.tz_localize(DEFAULT_TIMEZONE, ambiguous="NaT", nonexistent="shift_forward")
    )
    df_raw.set_index("timestamp", inplace=True)

    agg_map = {c: ("sum" if "precip" in c else "mean") for c in df_raw.columns}
    df_15min = df_raw.resample("15min").agg(agg_map).reset_index()

    # rename and strip tz
    df_15min.rename(columns=get_weather_column_labels(DEFAULT_UNITS), inplace=True)
    df_15min["timestamp"] = df_15min["timestamp"].dt.tz_localize(None)

    return df_15min