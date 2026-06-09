"""
experiment_config.py — Field experiment metadata for the Biochar project.

This file should contain project-specific design facts that are reused in
plots, downloads, README files, labels, and documentation.
"""

from __future__ import annotations

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
LOGGER_LOCATIONS = ["T", "M", "B"]

LOGGER_LOCATION_MAPPING = {
    "T": "Top",
    "M": "Middle",
    "B": "Bottom",
}

VARIABLES = ["VWC", "EC", "T", "SWC"]

# ---------------------------------------------------------------------
# Sensor depths
# ---------------------------------------------------------------------

SENSOR_DEPTH_CODES = ["1", "2", "3"]

SENSOR_DEPTH_LABELS = {
    "1": {"us": "6 in", "metric": "15 cm"},
    "2": {"us": "12 in", "metric": "30 cm"},
    "3": {"us": "18 in", "metric": "45 cm"},
}

SENSOR_DEPTH_VALUES = {
    "1": {"us": 6.0, "metric": 15.0},
    "2": {"us": 12.0, "metric": 30.0},
    "3": {"us": 18.0, "metric": 45.0},
}