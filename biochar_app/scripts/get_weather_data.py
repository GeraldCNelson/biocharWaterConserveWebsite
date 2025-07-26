import os
import logging
logger = logging.getLogger(__name__)
import requests
from io import StringIO
import pandas as pd
from datetime import datetime
from pathlib import Path

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
    PARQUET_DIR,
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


def fetch_weather_data(year: int) -> pd.DataFrame:
    """
    Incrementally fetch CoAgMet data **from the last cached weather timestamp**
    up to the **last timestamp in the raw-logger data**, appending into the cache.
    """
    cache_dir  = Path(PARQUET_DIR) / "weather"
    cache_file = cache_dir / f"{year}_weather.parquet"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # 1) Load existing cache (if any)
    if cache_file.exists():
        df_cache = pd.read_parquet(cache_file)
        df_cache["timestamp"] = pd.to_datetime(df_cache["timestamp"])
        last_cache_ts = df_cache["timestamp"].max()
    else:
        df_cache = pd.DataFrame(columns=["timestamp"] + list(get_weather_column_labels(DEFAULT_UNITS).values()))
        last_cache_ts = None

    # 2) Find the last logger timestamp
    logger_file = Path(PARQUET_DIR) / str(year) / f"{year}_raw_logger.parquet"
    if logger_file.exists():
        df_logger = pd.read_parquet(logger_file)
        if "timestamp" not in df_logger.columns:
            df_logger = df_logger.reset_index()
        df_logger["timestamp"] = pd.to_datetime(df_logger["timestamp"])
        last_logger_ts = df_logger["timestamp"].max()
    else:
        # no logger data yet → nothing to fetch
        return df_cache.sort_values("timestamp").reset_index(drop=True)

    # 3) Determine if there’s anything new to fetch
    if last_cache_ts is not None and last_cache_ts >= last_logger_ts:
        # cache already covers up through the latest logger
        return df_cache.sort_values("timestamp").reset_index(drop=True)

    # 4) Build our fetch window
    start_iso = (
        (last_cache_ts + pd.Timedelta(minutes=1))
        .strftime("%Y-%m-%dT%H:%M")
        if last_cache_ts
        else f"{year-1}-12-31T20:00"
    )
    end_iso = last_logger_ts.strftime("%Y-%m-%dT%H:%M")

    # 5) Download only that slice
    url = (
        f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
        f"?header=yes&fields={','.join(COAGMET_VARIABLE_MAP.keys())}"
        f"&from={start_iso}&to={end_iso}"
        f"&tz=co&units={DEFAULT_UNITS}&dateFmt=iso"
    )
    logger.debug("CoAgMet URL: %s", url)
    resp = requests.get(url)
    resp.raise_for_status()

    df_new = pd.read_csv(
        StringIO(resp.text),
        skiprows=2,
        na_values=["-999"],
        names=["timestamp"] + list(COAGMET_VARIABLE_MAP.values()),
        parse_dates=["timestamp"],
    )

    # 6) Localize, resample to 15-minute, rename, strip tz
    df_new["timestamp"] = (
        df_new["timestamp"]
           .dt.tz_localize(DEFAULT_TIMEZONE, ambiguous="NaT", nonexistent="shift_forward")
    )
    df_new = (
        df_new
        .set_index("timestamp")
        .resample("15min")
        .agg({c: ("sum" if "precip" in c else "mean") for c in df_new.columns})
        .reset_index()
    )
    df_new.rename(columns=get_weather_column_labels(DEFAULT_UNITS), inplace=True)
    df_new["timestamp"] = df_new["timestamp"].dt.tz_localize(None)

    # 7) Append, dedupe, sort & re-save cache
    df_combined = pd.concat([df_cache, df_new], ignore_index=True)
    df_combined = df_combined.drop_duplicates("timestamp").sort_values("timestamp")
    df_combined.to_parquet(cache_file, index=False)

    return df_combined