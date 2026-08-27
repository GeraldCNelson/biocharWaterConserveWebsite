"""Metadata for source reports shown in Biochar Material Characterization."""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from biochar_app.config.paths import BIOCHAR_LAB_REPORTS_DIR


class BiocharLabReport(TypedDict):
    filename: str
    download_name: str


BIOCHAR_LAB_REPORTS: dict[str, BiocharLabReport] = {
    "control-laboratories-2022": {
        "filename": "Control Laboratories Analysis - VGrid.pdf",
        "download_name": "VGrid_Control_Laboratories_2022.pdf",
    },
    "wyoming-analytical-2020": {
        "filename": "Wyoming Laboratory Analaysis - VGrid.pdf",
        "download_name": "VGrid_Wyoming_Analytical_Laboratories_2020.pdf",
    },
}


def biochar_lab_report_path(report_key: str) -> Path:
    """Return the configured source PDF path for a public report key."""
    return BIOCHAR_LAB_REPORTS_DIR / BIOCHAR_LAB_REPORTS[report_key]["filename"]
