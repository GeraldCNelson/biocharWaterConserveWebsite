"""
================================================================================
config.py — Central Configuration, Paths, Mappings, and Unit Definitions
================================================================================

This module is the single source of truth for configuration values and
mappings used throughout the Biochar Water Conservation project.

It defines:
    • File-system paths and data locations
        – DATA_RAW_DIR, DATA_PROCESSED_DIR, PARQUET_DIR, markdown paths, etc.
    • Year and granularity settings
        – YEARS, DEFAULT_YEAR, GRANULARITIES, default date windows.
    • Experiment layout
        – STRIPS (S1–S4), LOGGER_LOCATIONS (T/M/B), sensor_depth_mapping.
    • Variable names and labels
        – variable_name_mapping (internal → human readable)
        – variable_name_abbrev (VWC, T, EC, SWC, etc.)
        – label_name_mapping (axis labels, with unit-aware variants).
    • Growing-season configuration
        – GSEASON_PERIODS with custom Q1/Q2/Q3 definitions (MM-DD ranges).
    • Plot appearance parameters
        – bar_width_map for precipitation bars.
        – IRR_COLOR for irrigation overlays.
    • Soil water content and depth weighting
        – SWC_DEPTH_WEIGHTS and related depth constants.

------------------------------------------------------------------------------
UNIT SYSTEM DESIGN
------------------------------------------------------------------------------
All **stored data** are in US customary units:

    • Soil & air temperature: °F
    • Precipitation: inches
    • Irrigation volume: gallons
    • SWC volumes: gallons (with derived liters where needed)

The front end may request either "us" or "metric" display modes. Conversions
for plotting and downloads are driven by UNIT_CONVERSIONS:

    UNIT_CONVERSIONS = {
        "us_to_metric": {
            "temp":      °F → °C,
            "precip":    inches → mm,
            "irrigation": gallons → liters,
            "swc":       inches → cm (for depth / SWC-specific needs),
        },
        "metric_to_us": {
            "temp":      °C → °F,
            "precip":    mm → inches,
            "irrigation": liters → gallons,
            "swc":       cm → inches,
        },
    }

Other modules (plot_helpers.py, plot_utils.py, etl.py, get_weather_data.py)
import these conversion lambdas and apply them consistently for:
    • Display-time numeric conversions (plots, tables, downloads).
    • ETL-time conversions when needed for SWC volumes or derived metrics.

------------------------------------------------------------------------------
MAPPINGS & LABELS
------------------------------------------------------------------------------
Key dictionaries exported here include:

    • sensor_depth_mapping
          – Maps internal depth codes ("1", "2", "3") to human labels
            in US + metric units (e.g., "6 inches" / "15 cm").

    • logger_location_mapping
          – "T", "M", "B" → "Top", "Middle", "Bottom".

    • variable_name_mapping
          – Internal variable keys ("VWC", "T", "EC", "SWC") to concise names.

    • label_name_mapping
          – Axis-label text (with units) for each variable and overlay type
            (e.g., precip, irrigation, temp_air) in both unit systems.

    • PRECIP_COLS
          – Maps unit_system → underlying column name used for precip.

------------------------------------------------------------------------------
MAINTENANCE NOTES
------------------------------------------------------------------------------
• Any new variable should be added to:
      – variable_name_mapping
      – variable_name_abbrev
      – label_name_mapping
      – Any relevant depth/strip mappings.

• When changing default years, paths, or growing-season definitions, this is
  the primary place to edit.

• Keep config.py free of heavy logic; it should expose constants and simple
  helpers only, to avoid import cycles and keep tests fast.
------------------------------------------------------------------------------
"""
import os
import datetime
from enum import Enum
from math import pi
from pathlib import Path
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from typing import List, Dict

STRIPS = ["S1", "S2", "S3", "S4"]
VARIABLES = ["VWC", "EC", "T", "SWC"]
DEPTHS = ["1", "2", "3"]
YEARS = [2023, 2024, 2025]
LOGGER_LOCATIONS = ["T", "M", "B"]
GRANULARITIES = [
    ("15min",   "15min"),   # 15-minute bins
    ("hourly",  "h"),       # hourly
    ("daily",   "d"),       # daily
    ("monthly", "ME"),       # month-end
    ("gseason", None),      # growing-season summary
]

IRR_COLOR = "rgba(160, 82, 45, 0.55)"  # semi-transparent sienna

ms_per_day = 24 * 3600 * 1000
bar_width_map = {
    "15min": 15 * 60 * 1000,
    "hourly": 3600 * 1000,
    "daily": ms_per_day * 0.8,       # 80% of a day
    "monthly": 30 * ms_per_day * 0.8 # ~80% of a 30-day month
}

# Core default values
DEFAULT_YEAR = 2025
DEFAULT_START_DATE = datetime.date(DEFAULT_YEAR, 1, 1).isoformat()
DEFAULT_END_DATE = datetime.date(DEFAULT_YEAR, 12, 31).isoformat()
TRACE_CHOICES = ["depths", "locations"]

DEFAULT_VARIABLE = "VWC"
DEFAULT_DEPTH = "1"
DEFAULT_STRIP = "S1"
DEFAULT_LOGGER_LOCATION = "T"
DEFAULT_GRANULARITY = "daily"
DATALOGGER_NAMES = ["S1T", "S1M", "S1B", "S2T", "S2M", "S2B", "S3T", "S3B", "S3M", "S4T", "S4M", "S4B"]

# CoAgMet API units format ('m' for metric, 'us' for US)
UNITS_CHOICES: tuple[str, ...] = ("us", "metric", "imperial")
DEFAULT_UNITS = "us"

# trying out enums
class UnitSystem(str, Enum):
    US     = "us"
    METRIC = "metric"


# Base paths
# jump up one level from scripts/ into biochar_app/
BASE_DIR = Path(__file__).resolve().parent.parent

# now these are Path objects
DATA_RAW_DIR       = BASE_DIR / "data-raw"
DATA_PROCESSED_DIR = BASE_DIR / "data-processed"
PARQUET_DIR     = BASE_DIR / "data-processed" / "parquet"
PARQUET_GSEASON_DIR = PARQUET_DIR / "gseason"

# Default settings for bringing in the data from the dataloggers
DEFAULT_TABLE = "Table1"
DEFAULT_HOURS = 1
DEFAULT_TIMEZONE = ZoneInfo(os.getenv("DEFAULT_TIMEZONE", "America/Denver"))  # IANA timezone for logger locations
DEFAULT_LAG_MINUTES = 30  # Delay before current time to ensure data availability

# PakBus settings all in one place
STATION_BY_ID: Dict[int, str] = {
    1: "CR800",
    2: "S1T",
    3: "S1M",
    4: "S1B",
    5: "S2T",
    6: "S2M",
    7: "S2B",
    8: "S3T",
    9: "S3M",
    10: "S3B",
    11: "S4T",
    12: "S4M",
    13: "S4B",
}

ID_BY_STATION: Dict[str, int] = {v: k for k, v in STATION_BY_ID.items()}

def _parse_ids(s: str) -> List[int]:
    """
    Supports '2-13' or '2,3,5-7'. Defaults to an empty list on bad input.
    """
    out: list[int] = []
    for part in s.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out

@dataclass(frozen=True)
class PakbusConfig:
    host: str
    port: int
    base_id: int
    logger_ids: List[int]

PAKBUS = PakbusConfig(
    host=os.getenv("PAKBUS_HOST", "2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd"),
    port=int(os.getenv("PAKBUS_PORT", 6785)),
    base_id=int(os.getenv("PAKBUS_BASE_ID", 4094)),
    logger_ids=list(range(2, 14)),
)


# Weather data constants
COAG_STATION = "frt03"
COLLECT_PERIOD = "5min"
coagnames_complete = ["t","rh","dewpt","vp","bp_avg","solarRad","rso","precip","wetb","dt","windSpeed","windDir","windStdDev","gustSpeed","gustTime",
                      "gustDir","st5cm","st15cm","windSpeed10m","windDir10m","windStdDev10m","gustSpeed10m","gustTime10m","gustDir10m"]
datalogger_variable_column_names = ["VWC_1_raw_S1_T", "EC_1_raw_S1_T", "T_1_raw_S1_T", "VWC_2_raw_S1_T", "EC_2_raw_S1_T",
                                    "T_2_raw_S1_T", "VWC_3_raw_S1_T", "EC_3_raw_S1_T", "T_3_raw_S1_T", "VWC_1_raw_S1_M",
                                    "EC_1_raw_S1_M", "T_1_raw_S1_M", "VWC_2_raw_S1_M", "EC_2_raw_S1_M", "T_2_raw_S1_M",
                                    "VWC_3_raw_S1_M", "EC_3_raw_S1_M", "T_3_raw_S1_M", "VWC_1_raw_S1_B", "EC_1_raw_S1_B",
                                    "T_1_raw_S1_B", "VWC_2_raw_S1_B", "EC_2_raw_S1_B", "T_2_raw_S1_B", "VWC_3_raw_S1_B",
                                    "EC_3_raw_S1_B", "T_3_raw_S1_B", "VWC_1_raw_S2_T", "EC_1_raw_S2_T", "T_1_raw_S2_T",
                                    "VWC_2_raw_S2_T", "EC_2_raw_S2_T", "T_2_raw_S2_T", "VWC_3_raw_S2_T", "EC_3_raw_S2_T",
                                    "T_3_raw_S2_T", "VWC_1_raw_S2_M", "EC_1_raw_S2_M", "T_1_raw_S2_M", "VWC_2_raw_S2_M",
                                    "EC_2_raw_S2_M", "T_2_raw_S2_M", "VWC_3_raw_S2_M", "EC_3_raw_S2_M", "T_3_raw_S2_M",
                                    "VWC_1_raw_S2_B", "EC_1_raw_S2_B", "T_1_raw_S2_B", "VWC_2_raw_S2_B", "EC_2_raw_S2_B",
                                    "T_2_raw_S2_B", "VWC_3_raw_S2_B", "EC_3_raw_S2_B", "T_3_raw_S2_B", "VWC_1_raw_S3_T",
                                    "EC_1_raw_S3_T", "T_1_raw_S3_T", "VWC_2_raw_S3_T", "EC_2_raw_S3_T", "T_2_raw_S3_T",
                                    "VWC_3_raw_S3_T", "EC_3_raw_S3_T", "T_3_raw_S3_T", "VWC_1_raw_S3_B", "EC_1_raw_S3_B",
                                    "T_1_raw_S3_B", "VWC_2_raw_S3_B", "EC_2_raw_S3_B", "T_2_raw_S3_B", "VWC_3_raw_S3_B",
                                    "EC_3_raw_S3_B", "T_3_raw_S3_B", "VWC_1_raw_S3_M", "EC_1_raw_S3_M", "T_1_raw_S3_M",
                                    "VWC_2_raw_S3_M", "EC_2_raw_S3_M", "T_2_raw_S3_M", "VWC_3_raw_S3_M", "EC_3_raw_S3_M",
                                    "T_3_raw_S3_M", "VWC_1_raw_S4_T", "EC_1_raw_S4_T", "T_1_raw_S4_T", "VWC_2_raw_S4_T",
                                    "EC_2_raw_S4_T", "T_2_raw_S4_T", "VWC_3_raw_S4_T", "EC_3_raw_S4_T", "T_3_raw_S4_T",
                                    "VWC_1_raw_S4_M", "EC_1_raw_S4_M", "T_1_raw_S4_M", "VWC_2_raw_S4_M", "EC_2_raw_S4_M",
                                    "T_2_raw_S4_M", "VWC_3_raw_S4_M", "EC_3_raw_S4_M", "T_3_raw_S4_M", "VWC_1_raw_S4_B",
                                    "EC_1_raw_S4_B", "T_1_raw_S4_B", "VWC_2_raw_S4_B", "EC_2_raw_S4_B", "T_2_raw_S4_B",
                                    "VWC_3_raw_S4_B", "EC_3_raw_S4_B", "T_3_raw_S4_B", "SWC_1_raw_S1_T", "SWC_1_raw_S1_M",
                                    "SWC_1_raw_S1_B", "SWC_1_raw_S2_T", "SWC_1_raw_S2_M", "SWC_1_raw_S2_B", "SWC_1_raw_S3_T",
                                    "SWC_1_raw_S3_M", "SWC_1_raw_S3_B", "SWC_1_raw_S4_T", "SWC_1_raw_S4_M", "SWC_1_raw_S4_B",
                                    "VWC_1_ratio_S1_S2_T", "VWC_2_ratio_S1_S2_T", "VWC_3_ratio_S1_S2_T", "VWC_1_ratio_S1_S2_M",
                                    "VWC_2_ratio_S1_S2_M", "VWC_3_ratio_S1_S2_M", "VWC_1_ratio_S1_S2_B", "VWC_2_ratio_S1_S2_B",
                                    "VWC_3_ratio_S1_S2_B", "VWC_1_ratio_S3_S4_T", "VWC_2_ratio_S3_S4_T", "VWC_3_ratio_S3_S4_T",
                                    "VWC_1_ratio_S3_S4_M", "VWC_2_ratio_S3_S4_M", "VWC_3_ratio_S3_S4_M", "VWC_1_ratio_S3_S4_B",
                                    "VWC_2_ratio_S3_S4_B", "VWC_3_ratio_S3_S4_B", "SWC_1_ratio_S1_S2_T", "SWC_1_ratio_S1_S2_M",
                                    "SWC_1_ratio_S1_S2_B", "SWC_1_ratio_S3_S4_T", "SWC_1_ratio_S3_S4_M", "SWC_1_ratio_S3_S4_B",
                                    "EC_1_ratio_S1_S2_T", "EC_2_ratio_S1_S2_T", "EC_3_ratio_S1_S2_T", "EC_1_ratio_S1_S2_M",
                                    "EC_2_ratio_S1_S2_M", "EC_3_ratio_S1_S2_M", "EC_1_ratio_S1_S2_B", "EC_2_ratio_S1_S2_B",
                                    "EC_3_ratio_S1_S2_B", "EC_1_ratio_S3_S4_T", "EC_2_ratio_S3_S4_T", "EC_3_ratio_S3_S4_T",
                                    "EC_1_ratio_S3_S4_M", "EC_2_ratio_S3_S4_M", "EC_3_ratio_S3_S4_M", "EC_1_ratio_S3_S4_B",
                                    "EC_2_ratio_S3_S4_B", "EC_3_ratio_S3_S4_B", "T_1_ratio_S1_S2_T", "T_2_ratio_S1_S2_T",
                                    "T_3_ratio_S1_S2_T", "T_1_ratio_S1_S2_M", "T_2_ratio_S1_S2_M", "T_3_ratio_S1_S2_M",
                                    "T_1_ratio_S1_S2_B", "T_2_ratio_S1_S2_B", "T_3_ratio_S1_S2_B", "T_1_ratio_S3_S4_T",
                                    "T_2_ratio_S3_S4_T", "T_3_ratio_S3_S4_T", "T_1_ratio_S3_S4_M", "T_2_ratio_S3_S4_M",
                                    "T_3_ratio_S3_S4_M", "T_1_ratio_S3_S4_B", "T_2_ratio_S3_S4_B", "T_3_ratio_S3_S4_B",
                                    "temp_air_degC", "rh", "dewpoint_degC", "vaporpressure_kpa", "solarrad_wm-2", "precip_mm",
                                    "wind_m_s", "winddir_degN", "temp_soil_5cm_degC", "temp_soil_15cm_degC", "temp_air_degF", "dewpoint_degF", "precip_in"]
# Units for metric and US
METRIC_UNITS = {
    "temp_air": "degC",
    "dewpoint": "degC",
    "vapor_pressure": "kPa",
    "solar_rad": "Wm2",
    "precip": "mm",
    "wind_speed": "m_s",
    "wind_dir": "degN",
    "soil_temp_5cm": "degC",
    "soil_temp_15cm": "degC",
    "rh": "%"  # Same in both
}

US_UNITS = {
    "temp_air": "degF",
    "dewpoint": "degF",
    "vapor_pressure": "psi",
    "solar_rad": "Wm2",
    "precip": "in",
    "wind_speed": "mph",
    "wind_dir": "degN",
    "soil_temp_5cm": "degF",
    "soil_temp_15cm": "degF",
    "rh": "%"  # Same in both
}

# === Unit Conversion Factors ===
UNIT_CONVERSIONS = {
    "us_to_metric": {
        "temp": lambda x: (x - 32) * 5/9,   # °F to °C
        "precip": lambda x: x * 25.4,        # inches to mm
        "irrigation": lambda x: x * 3.78541, # gallons to liters
        "swc": lambda x: x * 2.54             # inches to cm (only if needed for SWC depths, not VWC itself)
    },
    "metric_to_us": {
        "temp": lambda x: (x * 9/5) + 32,   # °C to °F
        "precip": lambda x: x / 25.4,        # millimeters to inches
        "irrigation": lambda x: x / 3.78541, # liters to gallons
        "swc": lambda x: x / 2.54             # cm to inches
    }
}

# geometry of the “sensing cylinder” (all in cm)
SWC_CYLINDER_LENGTH_CM = 10   # reliable probe length
SWC_CYLINDER_RADIUS_CM = 4    # midpoint of 3–5 cm

def cylinder_volume_m3(length_cm=SWC_CYLINDER_LENGTH_CM,
                       radius_cm=SWC_CYLINDER_RADIUS_CM) -> float:
    """Compute π·r²·h in m³."""
    h = length_cm / 100.0
    r = radius_cm / 100.0
    return pi * r * r * h

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
    # Easily extendable:
    #"gustSpeed": "wind_gust_speed",
    #"gustDir": "wind_gust_dir",
}

# Base variable names (unit-agnostic)
METRICS_BASE_NAMES = list(COAGMET_VARIABLE_MAP.values())

# Dynamic label builder
METRICS_LABELS_METRIC = [
    f"{base}_{METRIC_UNITS[base]}" for base in METRICS_BASE_NAMES
]

# Custom US label handling for soil temp renaming
METRICS_LABELS_US = [
    "soil_temp_2in_degF" if base == "soil_temp_5cm" else
    "soil_temp_6in_degF" if base == "soil_temp_15cm" else
    f"{base}_{US_UNITS[base]}"
    for base in METRICS_BASE_NAMES
]

sensor_depth_mapping = {
    "1": {
        "us": "6 inches",
        "metric": "15 cm"
    },
    "2": {
        "us": "12 inches",
        "metric": "30 cm"
    },
    "3": {
        "us": "18 inches",
        "metric": "45 cm"
    }
}

DEFAULT_GSEASON_PERIODS = {
    "Q1_Winter": {
        "label": "Winter",
        "start": "11-01",
        "end": "04-30"
    },
    "Q2_Early_Growing": {
        "label": "Early Growing",
        "start": "04-01",
        "end": "06-30"
    },
    "Q3_Peak_Harvest": {
        "label": "Peak Harvest",
        "start": "07-01",
        "end": "10-31"
    }
}

VALUE_COLS_STANDARD = [
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"
]

VALUE_COLS_2024_PLUS = ["BattV_Min"] + VALUE_COLS_STANDARD

logger_location_mapping = {
    "T": "Top",
    "M": "Middle",
    "B": "Bottom"
}

variable_name_mapping = {
    "VWC": "Vol. Water Content",
    "T": "Soil Temperature",
    "EC": "Electrical Conductivity",
    "SWC": "Soil Water Content"
}

strip_name_mapping = {
    "S1": "Strip 1",
    "S2": "Strip 2",
    "S3": "Strip 3",
    "S4": "Strip 4"
}

granularity_name_mapping = {
    "gseason": "Growing Season",
    "monthly": "Monthly",
    "daily": "Daily",
    "15min": "15 Minute",
    "hourly": "Hourly"
}

# mapping of “column-name suffix” → UNIT_CONVERSIONS key
UNIT_SUFFIX_MAP = {
    "_degF":  "temp",
    "_in":    "precip",
    "_gal":   "irrigation",
    "_swc_in": "swc",
}

# which column holds precipitation for each unit system
PRECIP_COLS: dict[str, str] = {
    "metric": "precip_mm",
    "us":     "precip_in",
}

label_name_mapping = {
    "VWC": {
        "metric": "Volumetric Water Content (%)",
        "us":     "Volumetric Water Content (%)"
    },
    "EC": {
        "metric": "Electrical Conductivity (dS/m)",
        "us":     "Electrical Conductivity (dS/m)"
    },
    "SWC": {
        "metric": "Soil Water Content (cm)",
        "us":     "Soil Water Content (inches)"
    },
    "T": {
        "metric": "Soil Temperature (°C)",
        "us":     "Soil Temperature (°F)"
    },
    "temp_air": {
        "metric": "Air Temp (°C)",
        "us":     "Air Temp (°F)"
    },
    "precip": {
        "metric": "Precipitation (mm)",
        "us":     "Precipitation (inches)"
    },
    "irrigation": {
        "metric": "Irrigation Volume (000 L)",
        "us":     "Irrigation Volume (000 gal)"
    }
}

def human_label(col_name: str, unit_system: str) -> str:
    """
    Given a column like 'precip_mm', 'precip_in', 'temp_air_degF', or
    'soil_temp_6in_degF', return the appropriate human label
    (e.g. 'Precipitation (inches)' or 'Air Temp (°F)').
    """
    # find the first mapping key that matches the start of col_name
    for key in label_name_mapping:
        if col_name.startswith(key):
            return label_name_mapping[key][unit_system]

    # fallback to the raw column name if nothing matches
    return col_name

variable_name_abbrev = {
    "VWC": "VWC",
    "EC": "EC",
    "SWC": "SWC",
    "T": "Soil Temp.",
    "temp_air": "Air Temp.",
    "precip_mm": "Precip.",
    "irrigation": "Irrigation"
}

def conversion_for_column(colname: str):
    for suffix, var in UNIT_SUFFIX_MAP.items():
        if colname.endswith(suffix):
            return UNIT_CONVERSIONS["us_to_metric"][var]
    return None

PLOT_BASED_ON_OPTIONS = [
    {"value": "depth",          "label": "Depth"},
    {"value": "loggerLocation", "label": "Logger Location"},
]

# Map the front-end “traceOption” values to plot_utils’ trace keys
TRACE_OPTION_MAP: dict[str, str] = {
    "depth":          "depths",
    "loggerLocation": "locations",
}

# ----------------------------------------
# Color palette (Okabe–Ito, colorblind-safe)
# ----------------------------------------

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

# ----------------------------------------
# Semantic color mapping for plots
# (USE THESE KEYS IN CODE)
# ----------------------------------------

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

    # Depth traces (deterministic)
    "depth_1": OKABE_ITO["blue"],         # 6 inches (Depth "1")
    "depth_2": OKABE_ITO["orange"],       # 12 inches (Depth "2")
    "depth_3": OKABE_ITO["bluish_green"], # 18 inches (Depth "3")
}