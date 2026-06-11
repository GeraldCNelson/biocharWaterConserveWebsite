#!/usr/bin/env python3
"""
get_weather_data.py

Fetch CoAgMet 5-minute weather data for a given year and return a
clean 15-minute-aggregated DataFrame for use by etl.py.

This version is deliberately **stateless** for ETL/backfill:
  - It ignores any existing cache and always fetches the entire year:
        [year-01-01, min(year-12-31, now)] in DEFAULT_TIMEZONE
  - It does NOT append incrementally; incremental logic can be added
    later in a separate updater script.

Pipeline:

  - Use the CoAgMet API to request data for COAG_STATION at COLLECT_PERIOD
    with fields given in COAGMET_VARIABLE_MAP.
  - CSV format (with header=yes) typically has:
      * Row 1: column names, e.g.
            Station,"Date and Time","Air Temp",RH,Dewpoint,"Vapor Pressure",
            "Solar Rad","Liquid Precip","Wind","Wind Dir",
            "5cm Soil Temp","15cm Soil Temp"
      * Row 2: units / metadata, e.g.
            id,"date time","deg F","%",...
      * Rows 3+: data rows.
  - We:
      * Let pandas use the first row as the header.
      * Skip the units row.
      * Immediately rename the timestamp column ("Date and Time" / "DateTime")
        to "timestamp".
      * Subset to "timestamp" + the human-readable names corresponding to
        COAGMET_VARIABLE_MAP keys.
      * Rename those to internal base names ("temp_air", "precip", etc.).
      * Apply unit-aware final labels (e.g. "temp_air_degF", "precip_in").
      * Normalize timestamps to naive DEFAULT_TIMEZONE.
      * Resample to 15-minute intervals:
            - precip columns summed
            - all other columns averaged
      * Return a DataFrame with:
            timestamp (naive DEFAULT_TIMEZONE),
            one column per weather variable (final unit-suffixed name).

For details on CoAgMet’s URL & CSV format, see:
    https://coagmet.colostate.edu/data/url-builder

Example "latest" URL from the builder:
    https://coagmet.colostate.edu/data/latest/frt03.csv?header=yes&dateFmt=iso&tz=utc
"""

import logging
from io import StringIO

import pandas as pd
import requests

from biochar_app.config.core import (
    COLLECT_PERIOD,
    COAG_STATION,
    COAGMET_VARIABLE_MAP,
    DEFAULT_TIMEZONE,
)

from biochar_app.config.units import (
    DEFAULT_UNITS,
)

from biochar_app.config.paths import (
    PARQUET_DIR,
)


logger = logging.getLogger(__name__)

# Internal base 5-min column names (before unit suffix)
BASE_COLUMNS = list(COAGMET_VARIABLE_MAP.values())

# Map CoAgMet "field codes" (used in &fields=) to the *header names* that appear in CSV.
# Derived from your logs' "Available columns":
# ['Station', 'Date and Time', 'Air Temp', 'RH', 'Dewpoint', 'Vapor Pressure',
#  'Solar Rad', 'Liquid Precip', 'Wind', 'Wind Dir', '5cm Soil Temp', '15cm Soil Temp']
RAW_HEADER_MAP = {
    "t": "Air Temp",
    "rh": "RH",
    "dewpt": "Dewpoint",
    "vp": "Vapor Pressure",
    "solarRad": "Solar Rad",
    # 5-minute increment precip field:
    "precip": "Liquid Precip",
    "windSpeed": "Wind",
    "windDir": "Wind Dir",
    "st5cm": "5cm Soil Temp",
    "st15cm": "15cm Soil Temp",
}


def get_weather_column_labels(units: str = DEFAULT_UNITS) -> dict[str, str]:
    """
    Build a mapping from base 15-min column names
    (e.g. "soil_temp_5cm", "precip") to final column names
    with proper unit suffixes (e.g. "_in" or "_mm"), and
    convert 5 cm / 15 cm soil depths to 2 in / 6 in when using US units.

    Returns:
        dict[base_name, final_name]
        e.g. "temp_air" -> "temp_air_degF", "precip" -> "precip_in"
    """
    from biochar_app.config.units import US_UNITS, METRIC_UNITS

    unit_map = US_UNITS if units == "us" else METRIC_UNITS

    from biochar_app.scripts.type_utils import gb_agg, NAN, POS_INF, NEG_INF
    final_map: dict[str, str] = {}

    for raw_key, base_name in COAGMET_VARIABLE_MAP.items():
        if base_name not in BASE_COLUMNS:
            continue

        label = base_name
        # convert soil-temp depths for US units
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
    Fetch a full year of CoAgMet weather data for `year` and return a
    15-minute-aggregated DataFrame.

    This function does NOT use or maintain any cache. It is designed
    specifically for the historical ETL script (etl.py), where it is
    acceptable to re-fetch the full year.

    Returns:
        DataFrame with columns:
            - "timestamp" (naive in DEFAULT_TIMEZONE)
            - weather variables with final unit-suffixed names
              (e.g. "temp_air_degF", "precip_in", "soil_temp_2in_degF", ...)
    """
    # If we don't have logger data for this year, just return empty;
    # etl.py will skip weather summaries silently.
    logger_file = PARQUET_DIR / str(year) / f"{year}_raw_logger.parquet"
    if not logger_file.exists():
        logger.warning(
            "No logger parquet found for %s; skipping weather fetch.", year
        )
        return pd.DataFrame(columns=["timestamp"])

    # Determine time window for the year in DEFAULT_TIMEZONE
    start_ts = pd.Timestamp(f"{year}-01-01 00:00", tz=DEFAULT_TIMEZONE)
    year_end = pd.Timestamp(f"{year}-12-31 23:59", tz=DEFAULT_TIMEZONE)
    now_ts = pd.Timestamp.now(tz=DEFAULT_TIMEZONE).floor("min")
    end_ts = min(year_end, now_ts)

    if start_ts >= end_ts:
        logger.warning("Start >= end for weather fetch in %s; returning empty.", year)
        return pd.DataFrame(columns=["timestamp"])

    start_iso = start_ts.strftime("%Y-%m-%dT%H:%M")
    end_iso = end_ts.strftime("%Y-%m-%dT%H:%M")

    # Build URL and fetch CSV
    #
    # URL builder reference:
    #   https://coagmet.colostate.edu/data/url-builder
    fields_param = ",".join(COAGMET_VARIABLE_MAP.keys())
    url = (
        f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
        f"?header=yes"
        f"&fields={fields_param}"
        f"&from={start_iso}&to={end_iso}"
        f"&tz=co&units={DEFAULT_UNITS}&dateFmt=iso"
    )

    logger.info("Fetching CoAgMet weather CSV from %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # Read CSV using first row as header, skip the units row,
    # and disable low_memory chunking to avoid mixed-type warnings.
    df_raw = pd.read_csv(
        StringIO(resp.text),
        header=0,           # first row is header
        skiprows=[1],       # second row is units; treat it as non-data
        na_values=["-999", "-999.0"],
        low_memory=False,
    )

    if df_raw.empty:
        logger.warning("CoAgMet returned empty CSV for %s.", year)
        return pd.DataFrame(columns=["timestamp"])

    # Normalize the timestamp column name right away
    if "Date and Time" in df_raw.columns:
        df_raw.rename(columns={"Date and Time": "timestamp"}, inplace=True)
    elif "DateTime" in df_raw.columns:
        df_raw.rename(columns={"DateTime": "timestamp"}, inplace=True)
    else:
        raise ValueError(
            f"Could not find a timestamp column in CoAgMet CSV. "
            f"Columns: {list(df_raw.columns)}"
        )

    # Build list of header names we want for weather fields
    header_names = []
    for key in COAGMET_VARIABLE_MAP.keys():
        if key not in RAW_HEADER_MAP:
            raise ValueError(f"Missing RAW_HEADER_MAP entry for CoAgMet field '{key}'")
        header_names.append(RAW_HEADER_MAP[key])

    wanted_cols = ["timestamp"] + header_names
    missing_cols = [c for c in wanted_cols if c not in df_raw.columns]
    if missing_cols:
        raise ValueError(
            f"CoAgMet CSV missing expected columns {missing_cols}. "
            f"Available columns: {list(df_raw.columns)}"
        )

    df_new = df_raw[wanted_cols].copy()

    # Rename:
    #   "timestamp" stays "timestamp"
    #   human-readable names -> internal base names (temp_air, precip, etc.)
    rename_map = {"timestamp": "timestamp"}
    for key, base_name in COAGMET_VARIABLE_MAP.items():
        raw_header = RAW_HEADER_MAP[key]  # e.g. "Air Temp"
        rename_map[raw_header] = base_name

    df_new.rename(columns=rename_map, inplace=True)

    # Parse timestamp with explicit ISO-like format
    # (CoAgMet dateFmt=iso yields strings like "2025-11-19T16:30")
    df_new["timestamp"] = pd.to_datetime(
        df_new["timestamp"],
        format="%Y-%m-%dT%H:%M",
        errors="coerce",
    )

    # Drop rows with bad timestamps
    df_new = df_new.dropna(subset=["timestamp"]).sort_values("timestamp")

    if df_new.empty:
        logger.warning("All timestamps invalid in CoAgMet CSV for %s.", year)
        return pd.DataFrame(columns=["timestamp"])

    # Localize to DEFAULT_TIMEZONE and drop tz info (naive)
    df_new["timestamp"] = (
        df_new["timestamp"]
        .dt.tz_localize(DEFAULT_TIMEZONE, ambiguous="NaT", nonexistent="shift_forward")
        .dt.tz_localize(None)
    )
    df_new = df_new.dropna(subset=["timestamp"]).sort_values("timestamp")

    # Coerce data columns to numeric
    for col in df_new.columns:
        if col == "timestamp":
            continue
        df_new[col] = pd.to_numeric(df_new[col], errors="coerce")

    # Rename base names to final unit-aware labels
    label_map = get_weather_column_labels(DEFAULT_UNITS)
    df_new.rename(columns=label_map, inplace=True)

    # Make sure precip is non-negative
    precip_col = (
        "precip_in" if "precip_in" in df_new.columns
        else "precip_mm" if "precip_mm" in df_new.columns
        else None
    )
    if precip_col:
        df_new[precip_col] = df_new[precip_col].fillna(0.0).clip(lower=0.0)

    # ----- 15-minute resample -----
    # Build agg map EXCLUDING 'timestamp', since it becomes the index.
    data_cols = [c for c in df_new.columns if c != "timestamp"]
    agg_map = {
        c: ("sum" if c.startswith("precip") else "mean")
        for c in data_cols
    }
    from biochar_app.scripts.type_utils import df_agg
    df_15 = (
        df_agg(
            df_new.set_index("timestamp").resample("15min"),
            agg_map,
        )
        .round(3)
        .reset_index()
    )

    return df_15