# weather_runtime.py
from functools import lru_cache
import pandas as pd
from datetime import datetime
from typing import Iterable
from biochar_app.scripts.get_weather_data import fetch_weather_data
from biochar_app.scripts.config import UNIT_CONVERSIONS

@lru_cache(maxsize=8)
def load_weather_year(year: int) -> pd.DataFrame:
    """Load one year of weather, normalized, with both inch/mm + °F/°C."""
    dfw = fetch_weather_data(year)
    dfw["timestamp"] = pd.to_datetime(dfw["timestamp"], errors="coerce")
    dfw = dfw.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()

    # Ensure numeric
    for c in ("precip_in", "precip_mm", "temp_air_degF", "temp_air_degC"):
        if c in dfw.columns:
            dfw[c] = pd.to_numeric(dfw[c], errors="coerce")

    # Fill missing unit twins
    if "precip_in" in dfw and "precip_mm" not in dfw:
        dfw["precip_mm"] = dfw["precip_in"].apply(UNIT_CONVERSIONS["us_to_metric"]["precip"])
    if "precip_mm" in dfw and "precip_in" not in dfw:
        dfw["precip_in"] = dfw["precip_mm"].apply(UNIT_CONVERSIONS["metric_to_us"]["precip"])

    if "temp_air_degF" in dfw and "temp_air_degC" not in dfw:
        dfw["temp_air_degC"] = dfw["temp_air_degF"].apply(UNIT_CONVERSIONS["us_to_metric"]["temp"])
    if "temp_air_degC" in dfw and "temp_air_degF" not in dfw:
        dfw["temp_air_degF"] = dfw["temp_air_degC"].apply(UNIT_CONVERSIONS["metric_to_us"]["temp"])

    return dfw[["precip_in","precip_mm","temp_air_degF","temp_air_degC"]]

def load_weather_range(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    years: Iterable[int] = range(start.year, end.year + 1)
    frames = [load_weather_year(y) for y in years]
    dfw = pd.concat(frames).sort_index()
    return dfw.loc[start:end]