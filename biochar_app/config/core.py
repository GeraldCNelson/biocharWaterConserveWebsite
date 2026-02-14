"""
biochar_app.config.core

Core experiment configuration:
- strips, depths, years, granularities
- display name mappings
- growing season defaults
- plotting constants (colors, bar widths)
"""

from __future__ import annotations

import datetime
from enum import Enum
from math import pi
from typing import Dict, List
from dataclasses import dataclass

# ---------------------------------------------------------------------
# Experiment structure
# ---------------------------------------------------------------------

STRIPS = ["S1", "S2", "S3", "S4"]
VARIABLES = ["VWC", "EC", "T", "SWC"]
DEPTHS = ["1", "2", "3"]
YEARS = [2023, 2024, 2025, 2026]
LOGGER_LOCATIONS = ["T", "M", "B"]
DATALOGGER_NAMES = ["S1T", "S1M", "S1B", "S2T", "S2M", "S2B", "S3T", "S3B", "S3M", "S4T", "S4M", "S4B"]
VALUE_COLS_2024_PLUS = [
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
]

# Granularities:
# - first element: UI value
# - second element: pandas offset alias used in resampling (or None)
GRANULARITIES = [
    ("15min",   "15min"),
    ("hourly",  "h"),
    ("daily",   "d"),
    ("monthly", "ME"),
    ("gseason", None),
]

TRACE_CHOICES = ["depths", "locations"]
PLOT_BASED_ON_OPTIONS = [
    {"value": "depth",          "label": "Depth"},
    {"value": "loggerLocation", "label": "Logger Location"},
]
TRACE_OPTION_MAP: dict[str, str] = {
    "depth":          "depths",
    "loggerLocation": "locations",
}

# ---------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------

DEFAULT_YEAR = 2025
DEFAULT_START_DATE = datetime.date(DEFAULT_YEAR, 1, 1).isoformat()
DEFAULT_END_DATE = datetime.date(DEFAULT_YEAR, 12, 31).isoformat()

DEFAULT_VARIABLE = "VWC"
DEFAULT_DEPTH = "1"
DEFAULT_STRIP = "S1"
DEFAULT_LOGGER_LOCATION = "T"
DEFAULT_GRANULARITY = "daily"

# ---------------------------------------------------------------------
# Growing-season defaults
# ---------------------------------------------------------------------

DEFAULT_GSEASON_PERIODS = {
    "Q1_Winter": {
        "label": "Winter",
        "start": "11-01",
        "end":   "03-31",
    },
    "Q2_Early_Growing": {
        "label": "Growing Season",
        "start": "04-01",
        "end":   "10-31",
    },
}

# ---------------------------------------------------------------------
# Mappings (names)
# ---------------------------------------------------------------------

logger_location_mapping = {"T": "Top", "M": "Middle", "B": "Bottom"}

variable_name_mapping = {
    "VWC": "Vol. Water Content",
    "T":   "Soil Temperature",
    "EC":  "Electrical Conductivity",
    "SWC": "Soil Water Content",
}

strip_name_mapping = {"S1": "Strip 1", "S2": "Strip 2", "S3": "Strip 3", "S4": "Strip 4"}

granularity_name_mapping = {
    "gseason": "Growing Season",
    "monthly": "Monthly",
    "daily":   "Daily",
    "15min":   "15 Minute",
    "hourly":  "Hourly",
}

variable_name_abbrev = {
    "VWC": "VWC",
    "EC":  "EC",
    "SWC": "SWC",
    "T":   "Soil Temp.",
    "temp_air": "Air Temp.",
    "precip_mm": "Precip.",
    "irrigation": "Irrigation",
}

# Depth labels are unit-aware (actual conversion logic lives in units.py)
sensor_depth_mapping = {
    "1": {"us": "6 inches",  "metric": "15 cm"},
    "2": {"us": "12 inches", "metric": "30 cm"},
    "3": {"us": "18 inches", "metric": "45 cm"},
}

# ---------------------------------------------------------------------
# Weather data (CoAgMet)
# ---------------------------------------------------------------------

COAG_STATION = "frt03"
COLLECT_PERIOD = "5min"
coagnames_complete = ["t","rh","dewpt","vp","bp_avg","solarRad","rso","precip","wetb","dt","windSpeed","windDir","windStdDev","gustSpeed","gustTime",
                      "gustDir","st5cm","st15cm","windSpeed10m","windDir10m","windStdDev10m","gustSpeed10m","gustTime10m","gustDir10m"]
COAGMET_VARIABLE_MAP = {
    "t": "temp_air",
    "rh": "rh",
    "dewpt": "dewpoint",
    "vp": "vapor_pressure",
    "solarRad": "solar_rad",
    "precip": "precip",
    "windSpeed": "wind_speed",
    "windDir": "wind_dir",
    "st5cm": "soil_temp_5cm",
    "st15cm": "soil_temp_15cm",
}
# ---------------------------------------------------------------------
# Plot appearance
# ---------------------------------------------------------------------

IRR_COLOR = "rgba(160, 82, 45, 0.55)"  # semi-transparent sienna

ms_per_day = 24 * 3600 * 1000
bar_width_map = {
    "15min":  15 * 60 * 1000,
    "hourly": 3600 * 1000,
    "daily":  ms_per_day * 0.8,
    "monthly": 30 * ms_per_day * 0.8,
}

# Okabe–Ito palette (colorblind-safe)
OKABE_ITO = {
    "black": "#000000",
    "orange": "#E69F00",
    "sky_blue": "#56B4E9",
    "bluish_green": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddish_purple": "#CC79A7",
}

PLOT_COLORS = {
    # Raw data traces
    "strip_S1": OKABE_ITO["blue"],
    "strip_S2": OKABE_ITO["orange"],
    "strip_S3": OKABE_ITO["bluish_green"],
    "strip_S4": OKABE_ITO["vermillion"],

    # Ratios
    "ratio_S1_S2": OKABE_ITO["blue"],
    "ratio_S3_S4": OKABE_ITO["vermillion"],

    # Temperature deltas
    "delta_T_S1_S2": OKABE_ITO["blue"],
    "delta_T_S3_S4": OKABE_ITO["vermillion"],

    # Weather overlays
    "precip": OKABE_ITO["sky_blue"],
    "air_temp": OKABE_ITO["reddish_purple"],

    "irrigation": OKABE_ITO["black"],

    # Reference lines / annotations
    "zero_line": OKABE_ITO["black"],

    # Depth traces
    "depth_1": OKABE_ITO["blue"],
    "depth_2": OKABE_ITO["orange"],
    "depth_3": OKABE_ITO["bluish_green"],
}

# ---------------------------------------------------------------------
# SWC geometry (kept here because it’s conceptually “core experiment”)
# ---------------------------------------------------------------------

SWC_CYLINDER_LENGTH_CM = 10   # reliable probe length
SWC_CYLINDER_RADIUS_CM = 4    # midpoint of 3–5 cm

def cylinder_volume_m3(length_cm: float = SWC_CYLINDER_LENGTH_CM,
                       radius_cm: float = SWC_CYLINDER_RADIUS_CM) -> float:
    """Compute π·r²·h in m³."""
    h = length_cm / 100.0
    r = radius_cm / 100.0
    return pi * r * r * h
