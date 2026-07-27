"""
experiment_config.py — Field experiment metadata for the Biochar project.

This file should contain project-specific design facts that are reused in
plots, downloads, README files, labels, and documentation.
"""

from __future__ import annotations

from typing import TypedDict


class SensorDepthSpec(TypedDict):
    value: float
    unit: str

SensorDepthValues = dict[str, dict[str, SensorDepthSpec]]

SENSOR_DEPTH_VALUES: SensorDepthValues = {
    "1": {
        "us": {"value": 6.0, "unit": "in"},
        "metric": {"value": 15.0, "unit": "cm"},
    },
    "2": {
        "us": {"value": 12.0, "unit": "in"},
        "metric": {"value": 30.0, "unit": "cm"},
    },
    "3": {
        "us": {"value": 18.0, "unit": "in"},
        "metric": {"value": 45.0, "unit": "cm"},
    },
}
# ---------------------------------------------------------------------
# Experimental units
# ---------------------------------------------------------------------

STRIPS = ["S1", "S2", "S3", "S4"]

STRIP_NAME_MAPPING = {
    "S1": "Strip 1",
    "S2": "Strip 2",
    "S3": "Strip 3",
    "S4": "Strip 4",
}

STRIP_DESCRIPTIONS = {
    "S1": "Biochar, monthly irrigation",
    "S2": "Control, monthly irrigation",
    "S3": "Biochar, biweekly irrigation",
    "S4": "Control, biweekly irrigation",
}

TREATMENT_PAIRS = {
    "S1/S2": "Biochar/control comparison for monthly irrigation",
    "S3/S4": "Biochar/control comparison for biweekly irrigation",
}

# ---------------------------------------------------------------------
# Logger layout
# ---------------------------------------------------------------------
DATALOGGER_NAMES = [
    "S1T", "S1M", "S1B",
    "S2T", "S2M", "S2B",
    "S3T", "S3B", "S3M",
    "S4T", "S4M", "S4B",
]


VARIABLES = ["VWC", "EC", "T", "SWC"]

# ---------------------------------------------------------------------
# Sensor depths
# ---------------------------------------------------------------------

SENSOR_DEPTH_CODES: tuple[str, ...] = (
    tuple(SENSOR_DEPTH_VALUES.keys())
)

SENSOR_DEPTH_LABELS = {
    depth_code: {
        system: f"{spec['value']:g} {spec['unit']}"
        for system, spec in systems.items()
    }
    for depth_code, systems in SENSOR_DEPTH_VALUES.items()

}

SENSOR_DEPTH_INDEX_TO_INCHES: dict[str, int] = {
    depth_code: int(systems["us"]["value"])
    for depth_code, systems in SENSOR_DEPTH_VALUES.items()
}

LOGGER_LOCATIONS = ["T", "M", "B"]

LOGGER_LOCATION_MAPPING = {
    "T": "Top",
    "M": "Middle",
    "B": "Bottom",
}

LOGGER_GEOMETRY = {
    "T": {
        "distance_from_furrow_start_ft": 54,
    },
    "M": {
        "distance_from_furrow_start_ft": 169,
    },
    "B": {
        "distance_from_furrow_start_ft": 284,
    },
}