from pathlib import Path

from biochar_app.config.paths import BASE_DIR, IRRIGATION_DIR

"""
Irrigation configuration.

Purpose
-------
Centralizes irrigation-analysis paths and tuning parameters so they are not
hard-coded in analysis scripts or plotting utilities.

Scope
-----
This file is for irrigation-specific analysis settings, including:
- processed irrigation output directories
- holding-capacity analysis directories
- diagnostic plot windows
- pre-start response thresholds
- peak / plateau search windows

Field-design metadata such as strip IDs, logger locations, and sensor-depth
definitions should remain in experiment_config.py or core.py.

Notes
-----
Changing values here can affect generated CSV diagnostics, holding-capacity
summaries, and event inspection plots. Re-run the irrigation analysis after
changing any timing or threshold parameter.
"""


# ============================================================
# Directories
# ============================================================
from biochar_app.config.core import YEARS

ANALYSIS_DIR = IRRIGATION_DIR / "analysis"
HOLDING_CAPACITY_DIR = ANALYSIS_DIR / "holding_capacity"
DIAGNOSTICS_DIR = ANALYSIS_DIR / "diagnostics"
FIGURES_DIR = ANALYSIS_DIR / "figures"

MULTIDEPTH_PLOT_DIR = FIGURES_DIR / "event_multidepth"
TIMESTAMP_DIAGNOSTICS_DIR = DIAGNOSTICS_DIR / "timestamp_diagnostics"


# ============================================================
# Event-plot windows
# ============================================================

EVENT_PLOT_HOURS_BEFORE = 12.0
EVENT_PLOT_HOURS_AFTER = 30.0


# ============================================================
# Irrigation-response analysis windows
# ============================================================

BASELINE_LOOKBACK_HOURS = 6.0
PEAK_SEARCH_HOURS_AFTER_START = 24.0
PLATEAU_SEARCH_HOURS = 24.0


# ============================================================
# Diagnostic thresholds
# ============================================================

PRE_START_LOOKBACK_HOURS = 6.0
ARRIVAL_RESPONSE_THRESHOLD_VWC = 0.25
ALTERNATE_ARRIVAL_RESPONSE_THRESHOLD_VWC = 0.5
MIN_PRECIP_IN = 0.01

# ============================================================
# Backup raw data
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_BACKUP_DIR = (
    PROJECT_ROOT.parent
    / "BiocharBackups"
    /    "biochar_data_raw"
)

RAW_DATA_BACKUP_STATE = RAW_DATA_BACKUP_DIR / ".backup_state.json"
DEFAULT_ETL_YEAR = max(YEARS) # uses the last year in the list of YEARS as the default for backing up
RAW_DATA_BACKUP_INTERVAL_DAYS = 30
