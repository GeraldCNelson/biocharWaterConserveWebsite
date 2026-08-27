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

from dataclasses import dataclass
from pathlib import Path

from biochar_app.config.core import YEARS
from biochar_app.config.paths import (
    IRRIGATION_DIAGNOSTICS_DIR,
    IRRIGATION_FIGURES_DIR,
    IRRIGATION_QC_CSV,
    IRRIGATION_PRODUCTION_CSV,
)

# ============================================================
# Directories
# ============================================================

MULTIDEPTH_PLOT_DIR = IRRIGATION_FIGURES_DIR / "event_multidepth"
TIMESTAMP_DIAGNOSTICS_DIR = IRRIGATION_DIAGNOSTICS_DIR / "timestamp_diagnostics"

# ---------------------------------------------------------------------
# Irrigation analysis variants
# ---------------------------------------------------------------------
IRRIGATION_DATASET_VARIANT = "production" # alternative is "production" or qc_candidate

@dataclass(frozen=True)
class IrrigationAnalysisOptions:
    """
    Configuration for one reproducible irrigation-analysis variant.
    """

    input_csv: Path
    output_variant: str
    description: str
    report_label: str

IRRIGATION_ANALYSIS_OPTIONS: dict[str, IrrigationAnalysisOptions] = {
    "production": IrrigationAnalysisOptions(
        input_csv=IRRIGATION_PRODUCTION_CSV,
        output_variant="production",
        description=(
            "Official irrigation analysis using the live canonical "
            "irrigation_clean.csv dataset."
        ),
        report_label="Production",
    ),
    "qc_candidate": IrrigationAnalysisOptions(
        input_csv=IRRIGATION_QC_CSV,
        output_variant="qc_candidate",
        description=(
            "Camera-QC candidate analysis using proposed timestamp and "
            "shared-meter corrections."
        ),
        report_label="QC Candidate",
    ),
}
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

ARRIVAL_RESPONSE_THRESHOLD_VWC = 0.25
ALTERNATE_ARRIVAL_RESPONSE_THRESHOLD_VWC = 0.5
MIN_PRECIP_IN = 0.01

PRE_START_LOOKBACK_HOURS = 6.0
PRE_START_MIN_INCREASE_VWC = 0.5
PRE_START_MIN_PRECIP_IN = 0.01

# ============================================================
# Irrigation timestamp QA
# ============================================================
PHOTO_TIMESTAMP_REVIEW_THRESHOLD_MIN = 15.0

# Workbook boundary-flow readings are point observations from the meter needle.
# Compare them with the event-average meter flow calculated from delivered
# gallons and duration. Require both an absolute and a relative difference
# before flagging an event, so ordinary needle-reading variation is not
# overinterpreted.
FLOW_RATE_REVIEW_ABSOLUTE_GPM = 50.0
FLOW_RATE_REVIEW_RELATIVE_FRACTION = 0.25
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

def get_irrigation_analysis_options(
    variant: str | None = None,
) -> IrrigationAnalysisOptions:
    """
    Return validated options for the requested irrigation-analysis variant.

    When variant is omitted, use IRRIGATION_ANALYSIS_VARIANT.
    """

    selected_variant = (
        IRRIGATION_DATASET_VARIANT
        if variant is None
        else variant
    )

    try:
        return IRRIGATION_ANALYSIS_OPTIONS[selected_variant]
    except KeyError as exc:
        available = ", ".join(
            sorted(IRRIGATION_ANALYSIS_OPTIONS)
        )

        raise ValueError(
            f"Unknown irrigation analysis variant: "
            f"{selected_variant!r}. "
            f"Available variants: {available}"
        ) from exc
