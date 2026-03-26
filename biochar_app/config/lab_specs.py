# biochar_app/config/lab_specs.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from biochar_app.config.paths import (
    WARD_MASTER_NIR_CSV,
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_SOILCHEM_CSV,
    BIOMASS_FIELD_CSV,
)

# -----------------------------
# Common specs
# -----------------------------

@dataclass(frozen=True)
class LabVarSpec:
    key: str
    label: str
    candidates: Sequence[str]
    note: str = ""
    reference_key: Optional[str] = None


@dataclass(frozen=True)
class LabDatasetSpec:
    """
    Describes how to interpret a lab dataset for wide-table display.
    """
    key: str
    label: str
    source_csv: Path

    # shape: "long" => rows in file represent observations, we pivot to wide
    #        "wide" => file is already wide, date columns already exist
    shape: str  # "long" | "wide"

    # For "long"
    strip_col: Optional[str] = None
    date_col: Optional[str] = None
    location_col: Optional[str] = None
    begin_depth_col: Optional[str] = None
    end_depth_col: Optional[str] = None

    # For "wide"
    row_id_col: Optional[str] = None

    # Optional: explicit event ordering (ISO dates). If None, derive from data.
    event_order_iso: Optional[Sequence[str]] = None


# -----------------------------
# Dataset specs (tab sources)
# -----------------------------

NIR_DATASET = LabDatasetSpec(
    key="nir",
    label="Pasture Quality Metrics",
    source_csv=WARD_MASTER_NIR_CSV,
    shape="long",
    strip_col="strip",
    date_col="nir_date",
)

SOILBIO_DATASET = LabDatasetSpec(
    key="soilbio",
    label="Soil Biological Health",
    source_csv=WARD_MASTER_SOILBIO_CSV,
    shape="long",
    strip_col="strip",
    date_col="date_rec",
    begin_depth_col="begin_depth_in",
    end_depth_col="end_depth_in",
)

SOILCHEM_DATASET = LabDatasetSpec(
    key="soilchem",
    label="Soil Chemistry",
    source_csv=WARD_MASTER_SOILCHEM_CSV,
    shape="long",
    strip_col="strip",
    date_col="date_rec",
    begin_depth_col="begin_depth_in",
    end_depth_col="end_depth_in",
)

BIOMASS_FIELD_DATASET = LabDatasetSpec(
    key="biomass_field",
    label="Biomass (Field Samples)",
    source_csv=BIOMASS_FIELD_CSV,
    shape="wide",
    row_id_col="location",
)

# -----------------------------
# Variable sets
# Start by wiring reference_key only for the first few variables.
# -----------------------------

NIR_SET1_VARS: Sequence[LabVarSpec] = (
    LabVarSpec(
        key="crude_protein_pct_db",
        label="Crude Protein (Dry Basis, %)",
        candidates=(
            "crude_protein_pct_db",
            "cp_pct_db",
            "Crude Protein Dry Basis",
            "Crude Protein Dry Basis (%)",
            "Crude Protein Dry Basis %",
            "Crude Protein (Dry Basis, %)",
        ),
        reference_key="crude_protein_pct_db",
    ),
    LabVarSpec(
        key="adf_pct_db",
        label="Acid Detergent Fiber (Dry Basis, %)",
        candidates=(
            "adf_pct_db",
            "Acid Detergent Fiber Dry Basis",
            "Acid Detergent Fiber Dry Basis (%)",
            "ADF Dry Basis",
            "ADF (Dry Basis, %)",
        ),
        reference_key=None,
    ),
    LabVarSpec(
        key="ndf_pct_db",
        label="Neutral Detergent Fiber (Dry Basis, %)",
        candidates=(
            "ndf_pct_db",
            "Neutral Detergent Fiber Dry Basis",
            "Neutral Detergent Fiber Dry Basis (%)",
            "NDF Dry Basis",
            "NDF (Dry Basis, %)",
        ),
        reference_key=None,
    ),
    LabVarSpec(
        key="tdn_pct_db",
        label="Total Digestible Nutrients (Dry Basis, %)",
        candidates=(
            "tdn_pct_db",
            "TDN Est. Dry Basis",
            "TDN Est. Dry Basis (%)",
            "Total Digestible Nutrients Dry Basis",
            "Total Digestible Nutrients (Dry Basis, %)",
        ),
        reference_key=None,
    ),
    LabVarSpec(
        key="rfv",
        label="Relative Feed Value (RFV, unitless index)",
        candidates=("rfv", "RFV", "Relative Feed Value"),
        reference_key="rfv",
    ),
)

SOILBIO_CORE_VARS: Sequence[LabVarSpec] = (
    LabVarSpec(
        key="soil_respiration",
        label="Soil Respiration",
        candidates=("soil_respiration", "co2_c_respiration", "respiration"),
        reference_key="soil_respiration",
    ),
    LabVarSpec(
        key="weoc",
        label="Water Extractable Organic Carbon (WEOC)",
        candidates=("weoc", "water_extractable_organic_carbon"),
        reference_key="weoc",
    ),
    LabVarSpec(
        key="weon",
        label="Water Extractable Organic Nitrogen (WEON)",
        candidates=("weon", "water_extractable_organic_nitrogen"),
        reference_key="weon",
    ),
    LabVarSpec(
        key="mac",
        label="Microbially Active Carbon (%MAC)",
        candidates=("mac", "percent_mac", "microbially_active_carbon"),
        reference_key="mac",
    ),
    LabVarSpec(
        key="soil_health_score",
        label="Soil Health Score",
        candidates=("soil_health_score", "sha_score", "soil_health_number"),
        reference_key="soil_health_score",
    ),
)

SOILCHEM_CORE_VARS: Sequence[LabVarSpec] = (
    LabVarSpec(
        key="phosphorus",
        label="Phosphorus (ppm)",
        candidates=("phosphorus", "p", "olsen_p", "bray_p1", "mehlich_p3"),
        reference_key="phosphorus",
    ),
    LabVarSpec(
        key="potassium",
        label="Potassium (ppm)",
        candidates=("potassium", "k"),
        reference_key="potassium",
    ),
    LabVarSpec(
        key="organic_matter",
        label="Organic Matter (%)",
        candidates=("organic_matter", "om", "organic_matter_pct"),
        reference_key="organic_matter",
    ),
    LabVarSpec(
        key="cec",
        label="CEC",
        candidates=("cec", "cation_exchange_capacity"),
        reference_key="cec",
    ),
    LabVarSpec(
        key="ph",
        label="pH",
        candidates=("ph", "soil_ph", "ph_1_1"),
        reference_key="ph",
    ),
)