"""
biochar_app.config.units

Unit system configuration and conversion helpers.

Project convention:
- stored data are in US customary units
- frontend can request display in "us" or "metric"
"""

from __future__ import annotations

from enum import Enum
from biochar_app.config.core import (COAGMET_VARIABLE_MAP,)

# Units metadata for CoAgMet-derived variables (labels/suffixes)
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
    "rh": "%",
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
    "rh": "%",
}

DEFAULT_UNITS = "us"
UNITS_CHOICES: tuple[str, ...] = ("us", "metric", "imperial")

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

class UnitSystem(str, Enum):
    US     = "us"
    METRIC = "metric"

UNIT_CONVERSIONS = {
    "us_to_metric": {
        "temp": lambda x: (x - 32) * 5/9,   # °F to °C
        "precip": lambda x: x * 25.4,        # inches to mm
        "irrigation": lambda x: x * 3.78541, # gallons to liters
        "swc": lambda x: x * 2.54,           # inches to cm (only if needed)
    },
    "metric_to_us": {
        "temp": lambda x: (x * 9/5) + 32,    # °C to °F
        "precip": lambda x: x / 25.4,        # mm to inches
        "irrigation": lambda x: x / 3.78541, # liters to gallons
        "swc": lambda x: x / 2.54,           # cm to inches
    }
}

# mapping of “column-name suffix” → UNIT_CONVERSIONS key
UNIT_SUFFIX_MAP = {
    "_degF":   "temp",
    "_in":     "precip",
    "_gal":    "irrigation",
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
        "us":     "Volumetric Water Content (%)",
    },
    "EC": {
        "metric": "Electrical Conductivity (dS/m)",
        "us":     "Electrical Conductivity (dS/m)",
    },
    "SWC": {
        "metric": "Soil Water Content (cm)",
        "us":     "Soil Water Content (inches)",
    },
    "T": {
        "metric": "Soil Temperature (°C)",
        "us":     "Soil Temperature (°F)",
    },
    "temp_air": {
        "metric": "Air Temp (°C)",
        "us":     "Air Temp (°F)",
    },
    "precip": {
        "metric": "Precipitation (mm)",
        "us":     "Precipitation (inches)",
    },
    "irrigation": {
        "metric": "Irrigation Volume (000 L)",
        "us":     "Irrigation Volume (000 gal)",
    },
}

def human_label(col_name: str, unit_system: str) -> str:
    """
    Given a column like 'precip_mm', 'precip_in', 'temp_air_degF', or
    'soil_temp_6in_degF', return a human-friendly label.
    """
    for key in label_name_mapping:
        if col_name.startswith(key):
            return label_name_mapping[key][unit_system]
    return col_name

def conversion_for_column(colname: str):
    """
    Returns a conversion lambda (us_to_metric) based on known suffixes,
    otherwise None.
    """
    for suffix, var in UNIT_SUFFIX_MAP.items():
        if colname.endswith(suffix):
            return UNIT_CONVERSIONS["us_to_metric"][var]
    return None
