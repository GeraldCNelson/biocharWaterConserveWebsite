"""
biochar_app.config.table_specs

Central configuration for lab-based wide tables.
- Defines table sources (CSV files)
- Defines sets (groupings of variables/columns)
- Keeps routes + builders generic

A "wide table" here means:
- rows represent an entity (strip or location)
- columns represent sampling events (dates)
- values are lab metrics (or biomass measurements)

Some tables are already-wide in the CSV (e.g., field biomass: date columns are headers).
Others are "long-ish" and should be pivoted by (row_key, period_key) for each metric.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from biochar_app.config import paths


# -----------------------------------------------------------------------------
# Set spec (group label + link to variable definitions)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class TableSetSpec:
    key: str
    label: str
    # Name of a variable list defined in this module (or another config module)
    # Example: "NIR_VARIABLES_SET1"
    variables_key: Optional[str] = None
    note: str = ""


# -----------------------------------------------------------------------------
# Source specs: where the data comes from + how to interpret it
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class TableSourceSpec:
    key: str
    label: str
    csv_path: Path
    kind: str  # e.g. "lab_wide" (reserved for future extensions)

    # How to interpret the CSV:
    # - If already_wide=True: first column row_key, remaining columns are periods (dates)
    # - Else: CSV is "long-ish": has row_key + period_key + metric columns, and builder pivots
    row_key: str
    period_key: Optional[str] = None
    already_wide: bool = False

    # Optional display notes (tab subtitle)
    notes: str = ""


# -----------------------------------------------------------------------------
# Wide-table definition: connects a UI tab/table to one source + its sets
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class WideTableSpec:
    key: str
    label: str
    source_key: str
    sets: Sequence[TableSetSpec]


# -----------------------------------------------------------------------------
# TABLE SOURCES (authoritative cleaned masters)
# -----------------------------------------------------------------------------
TABLE_SOURCES: dict[str, TableSourceSpec] = {
    "nir_master": TableSourceSpec(
        key="nir_master",
        label="Pasture Quality (NIR) master",
        csv_path=paths.WARD_MASTER_NIR_CSV,
        kind="lab_wide",
        row_key="strip",
        period_key="nir_date",
        already_wide=False,
        notes="Rows: STRIP 1–4 plus ratios (S1/S2 and S3/S4). Columns: sampling events.",
    ),
    "soilbio_master": TableSourceSpec(
        key="soilbio_master",
        label="Soil Biology master",
        csv_path=paths.WARD_MASTER_SOILBIO_CSV,
        kind="lab_wide",
        row_key="strip",
        period_key="date_rec",
        already_wide=False,
        notes="Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means.",
    ),
    "soilchem_master": TableSourceSpec(
        key="soilchem_master",
        label="Soil Chemistry master",
        csv_path=paths.WARD_MASTER_SOILCHEM_CSV,
        kind="lab_wide",
        # Some soil chem masters use sample_id; yours also has strip. Prefer strip if present.
        row_key="strip",
        period_key="date_rec",
        already_wide=False,
        notes="Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means.",
    ),
    "biomass_field_master": TableSourceSpec(
        key="biomass_field_master",
        label="Biomass field samples master",
        csv_path=paths.BIOMASS_FIELD_CSV,
        kind="lab_wide",
        row_key="location",
        period_key=None,
        already_wide=True,
        notes="Rows: field locations (e.g., S1T/S1M/S1B). Columns: sampling dates.",
    ),
}


# -----------------------------------------------------------------------------
# SETS (VARIABLE GROUPINGS)
# -----------------------------------------------------------------------------
NIR_SETS: list[TableSetSpec] = [
    TableSetSpec("set1", "Set 1: Pasture Quality Metrics", variables_key="NIR_VARIABLES_SET1"),
    TableSetSpec("set2", "Set 2: Carbohydrates & Energy Partitioning", variables_key="NIR_VARIABLES_SET2"),
    TableSetSpec("set3", "Set 3: Minerals & Ash", variables_key="NIR_VARIABLES_SET3"),
    TableSetSpec("set4", "Set 4: Digestibility Metrics", variables_key="NIR_VARIABLES_SET4"),
]

SOILBIO_SETS: list[TableSetSpec] = [
    TableSetSpec("set1", "Set 1: Soil Biological Health", variables_key="SOILBIO_VARIABLES_SET1"),
]

SOILCHEM_SETS: list[TableSetSpec] = [
    TableSetSpec("set1", "Set 1: Soil Chemistry", variables_key="SOILCHEM_VARIABLES_SET1"),
]

BIOMASS_FIELD_SETS: list[TableSetSpec] = [
    # For already-wide tables, variables_key can be None (builder will treat it as one “Value” variable).
    TableSetSpec("set1", "Set 1: Biomass (Field Samples)", variables_key=None),
]


# -----------------------------------------------------------------------------
# WIDE TABLE SPECS: each tab/table points at one source + its sets
# -----------------------------------------------------------------------------
WIDE_TABLES: dict[str, WideTableSpec] = {
    "nir": WideTableSpec(
        key="nir",
        label="Pasture Quality Metrics",
        source_key="nir_master",
        sets=NIR_SETS,
    ),
    "soilbio": WideTableSpec(
        key="soilbio",
        label="Soil Biological Health",
        source_key="soilbio_master",
        sets=SOILBIO_SETS,
    ),
    "soilchem": WideTableSpec(
        key="soilchem",
        label="Soil Chemistry",
        source_key="soilchem_master",
        sets=SOILCHEM_SETS,
    ),
    "biomass_field": WideTableSpec(
        key="biomass_field",
        label="Biomass (Field Samples)",
        source_key="biomass_field_master",
        sets=BIOMASS_FIELD_SETS,
    ),
}


# -----------------------------------------------------------------------------
# VARIABLE SPECS (placeholders)
# -----------------------------------------------------------------------------
# These must be filled from your existing tables_nir.py / tables_soil*.py definitions.
# The builder expects each list element to look like:
#   {"key": "...", "label": "...", "candidates": ["colA", "colB", ...]}
#
# Keep these in config so adding 2026 (or new datasets) is a config edit, not code surgery.

NIR_VARIABLES_SET1: list[dict[str, Any]] = []
NIR_VARIABLES_SET2: list[dict[str, Any]] = []
NIR_VARIABLES_SET3: list[dict[str, Any]] = []
NIR_VARIABLES_SET4: list[dict[str, Any]] = []

SOILBIO_VARIABLES_SET1: list[dict[str, Any]] = []
SOILCHEM_VARIABLES_SET1: list[dict[str, Any]] = []