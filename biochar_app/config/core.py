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
from math import pi

from zoneinfo import ZoneInfo
import os

# ---------------------------------------------------------------------
# Experiment structure
# ---------------------------------------------------------------------

STRIPS = ["S1", "S2", "S3", "S4"]
VARIABLES = ["VWC", "EC", "T", "SWC"]
YEARS = [2023, 2024, 2025, 2026]
LOGGER_LOCATIONS = ["T", "M", "B"]
DATALOGGER_NAMES = ["S1T", "S1M", "S1B", "S2T", "S2M", "S2B", "S3T", "S3B", "S3M", "S4T", "S4M", "S4B"]
VALUE_COLS_2024_PLUS = [
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
]

VALUE_COLS_STANDARD = [
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
]

# ---------------------------------------------------------------------------
# Sensor depth definitions
# ---------------------------------------------------------------------------

SENSOR_DEPTH_CODES = ["1", "2", "3"]
DEFAULT_SENSOR_DEPTH_CODE = "1"

# User-facing labels
SENSOR_DEPTH_LABELS = {
    "1": {"us": "6 in", "metric": "15 cm"},
    "2": {"us": "12 in", "metric": "30 cm"},
    "3": {"us": "18 in", "metric": "45 cm"},
}

# Numeric depth values for calculations
SENSOR_DEPTH_VALUES = {
    "1": {"us": 6.0, "metric": 15.0},
    "2": {"us": 12.0, "metric": 30.0},
    "3": {"us": 18.0, "metric": 45.0},
}

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
DEFAULT_STRIP = "S1"
DEFAULT_LOGGER_LOCATION = "T"
DEFAULT_GRANULARITY = "daily"

TAB_LINKS = {
    "Interactive Plots": "main-tab",
    "Summary Statistics": "summary-tab",
    "Custom Season": "gseason-tab",
    "Pasture Quality Metrics": "nir-tab",
    "Biomass (Field Samples)": "biomass-field-tab",
    "Biological Health": "soilbio-tab",
    "Soil Chemistry": "soilchem-tab",
}
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

TITLE_FONT_SIZE = 18

PLOT_COLORS = {
    # Raw data traces
    "strip_S1": OKABE_ITO["blue"],
    "strip_S2": OKABE_ITO["orange"],
    "strip_S3": OKABE_ITO["bluish_green"],
    "strip_S4": OKABE_ITO["vermillion"],

    # Ratios
    "ratio_S1_S2": OKABE_ITO["blue"],
    "ratio_S3_S4": OKABE_ITO["vermillion"],
    "zero_line": "rgba(140,140,140,0.5)",

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

LOGGER_TIMEZONE = "America/Denver"  # semantic only, do NOT apply tzinfo
DEFAULT_TIMEZONE = ZoneInfo(os.getenv("DEFAULT_TIMEZONE", "America/Denver"))

# ---------------------------------------------------------------------
# Field geometry (source of truth)
# ---------------------------------------------------------------------

ACRE_TO_FT2 = 43560

FIELD_GEOMETRY = {
    "S1": {"width_ft": 47.0, "length_ft": 370.0},
    "S2": {"width_ft": 47.0, "length_ft": 370.0},
    "S3": {"width_ft": 47.0, "length_ft": 370.0},
    "S4": {"width_ft": 47.0, "length_ft": 370.0},
}

# ---------------------------------------------------------------------
# Derived geometry (do NOT hardcode elsewhere)
# ---------------------------------------------------------------------

STRIP_AREA_FT2 = {
    k: v["width_ft"] * v["length_ft"]
    for k, v in FIELD_GEOMETRY.items()
}

STRIP_AREA_ACRES = {
    k: area_ft2 / ACRE_TO_FT2
    for k, area_ft2 in STRIP_AREA_FT2.items()
}

# Optional (very useful)
STRIP_GROUP_AREA_ACRES = {
    "S1_S2": STRIP_AREA_ACRES["S1"] + STRIP_AREA_ACRES["S2"],
    "S3_S4": STRIP_AREA_ACRES["S3"] + STRIP_AREA_ACRES["S4"],
}

# --- Unit conversions ---

ACRE_TO_FT2 = 43560.0          # square feet per acre
INCH_TO_FT = 1.0 / 12.0        # feet per inch
FT3_TO_GALLONS = 7.48052       # gallons per cubic foo

GALLONS_PER_ACRE_INCH = ACRE_TO_FT2 * INCH_TO_FT * FT3_TO_GALLONS