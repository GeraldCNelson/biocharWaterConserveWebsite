"""
Irrigation configuration.

Purpose
-------
Centralizes irrigation-analysis tuning parameters and irrigation-specific
derived paths so they are not hard-coded in analysis scripts or plotting
utilities.

Filesystem root paths should come from config.paths. This file may define
irrigation-specific subdirectories derived from those canonical paths.

Field-design metadata such as strip IDs, logger locations, and sensor-depth
definitions should remain in experiment_config.py or core.py.
"""

from pathlib import Path

from biochar_app.config.core import YEARS
from biochar_app.config.paths import (
    IRRIGATION_DIAGNOSTICS_DIR,
    IRRIGATION_FIGURES_DIR,
)

# ============================================================
# Directories
# ============================================================

MULTIDEPTH_PLOT_DIR = IRRIGATION_FIGURES_DIR / "event_multidepth"
TIMESTAMP_DIAGNOSTICS_DIR = IRRIGATION_DIAGNOSTICS_DIR / "timestamp_diagnostics"

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
    / "biochar_data_raw"
)

RAW_DATA_BACKUP_STATE = RAW_DATA_BACKUP_DIR / ".backup_state.json"
DEFAULT_ETL_YEAR = max(YEARS)
RAW_DATA_BACKUP_INTERVAL_DAYS = 30