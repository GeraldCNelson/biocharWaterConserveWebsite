"""
biochar_app.config.units

Unit system configuration and conversion helpers.

Project convention:
- Stored data are in US customary units.
- Frontend can request display in "us" or "metric".
- We validate unit systems strictly (no silent fallbacks).
"""

from __future__ import annotations

from enum import Enum
from typing import Callable, Literal, Optional

from biochar_app.config.core import COAGMET_VARIABLE_MAP


# -----------------------------------------------------------------------------
# Unit system types + strict validation
# -----------------------------------------------------------------------------

UnitSystemKey = Literal["us", "metric"]

DEFAULT_UNITS: UnitSystemKey = "us"
UNITS_CHOICES: tuple[UnitSystemKey, ...] = ("us", "metric")


def validate_unit_system(value: str | None) -> UnitSystemKey:
    """
    Strict validation: unknown values raise ValueError.
    Use at boundaries (API request parsing, UI bootstrap).
    """
    if value is None:
        raise ValueError("unitSystem is missing")
    v = value.strip().lower()
    if v not in UNITS_CHOICES:
        raise ValueError(
            f"Invalid unitSystem={value!r}. Expected one of {UNITS_CHOICES}."
        )
    return v  # type: ignore[return-value]


class UnitSystem(str, Enum):
    """
    Optional enum for places that prefer enums over strings.
    Keep this in sync with UnitSystemKey/UNITS_CHOICES.
    """
    US = "us"
    METRIC = "metric"


# -----------------------------------------------------------------------------
# CoAgMet-derived units & label suffix helpers
# -----------------------------------------------------------------------------

# Units metadata for CoAgMet-derived variables (labels/suffixes)
METRIC_UNITS: dict[str, str] = {
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

US_UNITS: dict[str, str] = {
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

# Base variable names (unit-agnostic) derived from the variable map
METRICS_BASE_NAMES = list(COAGMET_VARIABLE_MAP.values())

# Dynamic label builder
METRICS_LABELS_METRIC = [f"{base}_{METRIC_UNITS[base]}" for base in METRICS_BASE_NAMES]

# Custom US label handling for soil temp renaming
METRICS_LABELS_US = [
    "soil_temp_2in_degF"
    if base == "soil_temp_5cm"
    else "soil_temp_6in_degF"
    if base == "soil_temp_15cm"
    else f"{base}_{US_UNITS[base]}"
    for base in METRICS_BASE_NAMES
]


# -----------------------------------------------------------------------------
# Numeric conversions (stored US -> display metric, and reverse)
# -----------------------------------------------------------------------------

UNIT_CONVERSIONS: dict[str, dict[str, Callable[[float], float]]] = {
    "us_to_metric": {
        "temp": lambda x: (x - 32) * 5 / 9,      # °F -> °C
        "precip": lambda x: x * 25.4,            # inches -> mm
        "irrigation": lambda x: x * 3.78541,     # gallons -> liters
        "swc": lambda x: x * 2.54,               # inches -> cm (if needed)
    },
    "metric_to_us": {
        "temp": lambda x: (x * 9 / 5) + 32,      # °C -> °F
        "precip": lambda x: x / 25.4,            # mm -> inches
        "irrigation": lambda x: x / 3.78541,     # liters -> gallons
        "swc": lambda x: x / 2.54,               # cm -> inches
    },
}

# Mapping of “column-name suffix” → UNIT_CONVERSIONS key
UNIT_SUFFIX_MAP: dict[str, str] = {
    "_degF": "temp",
    "_in": "precip",
    "_gal": "irrigation",
    "_swc_in": "swc",
}

# Which column holds precipitation for each unit system
PRECIP_COLS: dict[UnitSystemKey, str] = {
    "metric": "precip_mm",
    "us": "precip_in",
}


# -----------------------------------------------------------------------------
# Human-readable labels (strict lookup; no "fallback to us/metric")
# -----------------------------------------------------------------------------

label_name_mapping: dict[str, dict[UnitSystemKey, str]] = {
    "VWC": {
        "metric": "Volumetric Water Content (%)",
        "us": "Volumetric Water Content (%)",
    },
    "EC": {
        "metric": "Electrical Conductivity (dS/m)",
        "us": "Electrical Conductivity (dS/m)",
    },
    "SWC": {
        "metric": "Soil Water Content (cm)",
        "us": "Soil Water Content (inches)",
    },
    "T": {
        "metric": "Soil Temperature (°C)",
        "us": "Soil Temperature (°F)",
    },
    "temp_air": {
        "metric": "Air Temp (°C)",
        "us": "Air Temp (°F)",
    },
    "precip": {
        "metric": "Precipitation (mm)",
        "us": "Precipitation (inches)",
    },
    "irrigation": {
        "metric": "Irrigation Volume (000 L)",
        "us": "Irrigation Volume (000 gal)",
    },
}


def human_label(col_name: str, unit_system: UnitSystemKey) -> str:
    """
    Given a column like 'precip_mm', 'precip_in', 'temp_air_degF', or
    'soil_temp_6in_degF', return a human-friendly label.

    Strict: unit_system must be validated before calling.
    If no key matches, returns the raw col_name (safe fallback).
    """
    # Ensure caller passed a supported unit system
    unit_system = validate_unit_system(unit_system)

    for key in label_name_mapping:
        if col_name.startswith(key):
            return label_name_mapping[key][unit_system]

    return col_name


def conversion_for_column(colname: str) -> Optional[Callable[[float], float]]:
    """
    Returns a conversion lambda (us_to_metric) based on known suffixes,
    otherwise None.

    Note: This is for display-time conversion (stored US -> metric).
    """
    for suffix, var in UNIT_SUFFIX_MAP.items():
        if colname.endswith(suffix):
            return UNIT_CONVERSIONS["us_to_metric"][var]
    return None


__all__ = [
    # types / validation
    "UnitSystemKey",
    "UnitSystem",
    "DEFAULT_UNITS",
    "UNITS_CHOICES",
    "validate_unit_system",
    # CoAgMet labels/suffixes
    "METRIC_UNITS",
    "US_UNITS",
    "METRICS_BASE_NAMES",
    "METRICS_LABELS_METRIC",
    "METRICS_LABELS_US",
    # conversions
    "UNIT_CONVERSIONS",
    "UNIT_SUFFIX_MAP",
    "PRECIP_COLS",
    # labels
    "label_name_mapping",
    "human_label",
    "conversion_for_column",
]