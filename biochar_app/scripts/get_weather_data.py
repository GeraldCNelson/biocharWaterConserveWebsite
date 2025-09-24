import logging
logger = logging.getLogger(__name__)
import requests
from io import StringIO
import pandas as pd
from pathlib import Path

from biochar_app.scripts.config import (
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
    up to **right now**, appending into the cache.

    Changes vs prior:
      - Treat -999 as NA; coerce all numeric; clip negative precip to 0 (5-min increments).
      - Never fetch earlier than Jan 1 <year>.
      - Normalize timestamps to naive America/Denver (to match logger convention).
      - Guard against empty pulls and missing columns.
    """
    cache_dir  = Path(PARQUET_DIR) / "weather"
    cache_file = cache_dir / f"{year}_weather.parquet"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # 1) Load existing cache (if any)
    if cache_file.exists():
        df_cache = pd.read_parquet(cache_file)
        df_cache["timestamp"] = pd.to_datetime(df_cache["timestamp"], errors="coerce")
        df_cache = df_cache.dropna(subset=["timestamp"]).sort_values("timestamp")
        last_cache_ts = df_cache["timestamp"].max() if not df_cache.empty else None
    else:
        df_cache = pd.DataFrame(
            columns=["timestamp"] + list(get_weather_column_labels(DEFAULT_UNITS).values())
        )
        last_cache_ts = None

    # 2) If we have no logger file yet, just return what we have
    logger_file = Path(PARQUET_DIR) / str(year) / f"{year}_raw_logger.parquet"
    if not logger_file.exists():
        return df_cache.reset_index(drop=True)

    # 3) Decide fetch window (never before Jan 1 of target year)
    start_fallback = pd.Timestamp(f"{year}-01-01 00:00", tz=DEFAULT_TIMEZONE)
    if last_cache_ts is not None:
        # cache is naive; interpret as Denver-local then bump by 1 minute
        start_ts = last_cache_ts.tz_localize(DEFAULT_TIMEZONE) + pd.Timedelta(minutes=1)
        start_ts = max(start_ts, start_fallback)
    else:
        start_ts = start_fallback

    now_ts = pd.Timestamp.now(tz=DEFAULT_TIMEZONE).floor("min")
    if start_ts >= now_ts:
        # Nothing new to fetch; return cache
        return df_cache.reset_index(drop=True)

    start_iso = start_ts.strftime("%Y-%m-%dT%H:%M")
    end_iso   = now_ts.strftime("%Y-%m-%dT%H:%M")

    # 4) Pull CSV
    url = (
        f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
        f"?header=yes&fields={','.join(COAGMET_VARIABLE_MAP.keys())}"
        f"&from={start_iso}&to={end_iso}"
        f"&tz=co&units={DEFAULT_UNITS}&dateFmt=iso"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # Use names so we fully control the header row parsing; treat -999 as NA
    df_new = pd.read_csv(
        StringIO(resp.text),
        skiprows=2,
        na_values=["-999", -999, "-999.0"],
        names=["timestamp"] + list(COAGMET_VARIABLE_MAP.values()),
        parse_dates=["timestamp"],
    )

    if df_new.empty:
        return df_cache.reset_index(drop=True)

    # 5) Normalize timestamps to naive America/Denver (match logger convention)
    #    Incoming timestamps are ISO local; localize then drop tz.
    df_new["timestamp"] = (
        df_new["timestamp"]
        .dt.tz_localize(DEFAULT_TIMEZONE, ambiguous="NaT", nonexistent="shift_forward")
        .dt.tz_localize(None)
    )
    df_new = df_new.dropna(subset=["timestamp"]).sort_values("timestamp")

    # 6) Coerce numerics; fix precip: increments, NA->0, negatives->0
    #    After this block, we can safely resample with sum/mean.
    for c in df_new.columns:
        if c == "timestamp":
            continue
        df_new[c] = pd.to_numeric(df_new[c], errors="coerce")

    # Rename to standardized snake_case labels FIRST so we know precip col name
    df_new.rename(columns=get_weather_column_labels(DEFAULT_UNITS), inplace=True)

    precip_col = "precip_in" if "precip_in" in df_new.columns else None
    if precip_col is None:
        # Allow projects that use metric labels directly
        precip_col = "precip_mm" if "precip_mm" in df_new.columns else None

    if precip_col is not None:
        df_new[precip_col] = df_new[precip_col].fillna(0.0).clip(lower=0.0)

    # 7) Resample to 15-min in local naive time
    df_new = (
        df_new
        .set_index("timestamp")
        .resample("15min")
        .agg({col: ("sum" if col.startswith("precip") else "mean") for col in df_new.columns})
        .round(3)
        .reset_index()
    )

    # 8) Append to cache, dedupe, sort, and write back
    df_combined = pd.concat([df_cache, df_new], ignore_index=True)
    df_combined = df_combined.drop_duplicates("timestamp").sort_values("timestamp")

    # Optional: enforce year boundary if you keep one cache per year
    start_year = pd.Timestamp(f"{year}-01-01 00:00")
    end_year   = pd.Timestamp(f"{year}-12-31 23:59:59.999")
    df_combined = df_combined[(df_combined["timestamp"] >= start_year) & (df_combined["timestamp"] <= end_year)]

    df_combined.to_parquet(cache_file, index=False)
    return df_combined.reset_index(drop=True)