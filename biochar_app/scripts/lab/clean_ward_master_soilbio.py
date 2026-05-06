#!/usr/bin/env python3
"""
clean_ward_master_soilbio.py

Clean the compiled Ward / Lobato PLFA soil biology file into a canonical
machine-readable CSV for the Biochar dashboard.

Code to run in a terminal
python -m biochar_app.scripts.clean_ward_master_soilbio

Conventions
-----------
* lab/master CSV strip values are canonicalized to:
    strip_1, strip_2, strip_3, strip_4
* display/UI should render those as:
    STRIP 1, STRIP 2, STRIP 3, STRIP 4

Key behaviors
-------------
* Reads the compiled PLFA CSV from Ward/Lobato
* Standardizes strip/date/depth via clean_ward_master_common.py
* Preserves values as provided by Ward except for standard normalization:
    - "Not Reported" -> 0
    - "<0.01" / "< 0.01" -> 0
* Applies stable soil-bio canonical names
* Writes:
    - cleaned master CSV
    - machine->human headers JSON

Additional behavior
-------------------
If a late raw Ward biological file exists that matches the currently known
supplemental file naming pattern, it is merged into the canonical cleaned CSV
after the compiled master is written. This lets the canonical file remain:

    ward_master_soilbio_clean.csv

while still including the latest late-2025 rows.
"""

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from biochar_app.config.paths import (
    SOIL_BIO_PROCESSED_DIR,
    SOIL_BIO_RAW_DIR,
    WARD_MASTER_SOILBIO_CSV,
)
from biochar_app.scripts.lab.clean_ward_master_common import (
    clean_compiled_workbook,
    standardize_ward_dataframe,
    validate_and_report,
    write_clean_outputs,
)
from biochar_app.scripts.tables.tables_soil_bio import _prepare_soilbio_csv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ---------------------------------------------------------------------
# Input / outputs
# ---------------------------------------------------------------------
IN_MASTER_CSV = SOIL_BIO_RAW_DIR / "Lobato PLFA Data - Compiled.csv"

# Canonical processed output path lives in config.paths
OUT_CLEAN_CSV = WARD_MASTER_SOILBIO_CSV

OUT_HEADERS_JSON = (
    SOIL_BIO_PROCESSED_DIR / "ward_master_soilbio_headers_machine_to_human.json"
)

# Known supplemental raw file to merge after cleaning, if present.
# This is the late-2025 PLFA file you added manually.
LATE_RAW_BIO_CSV = SOIL_BIO_RAW_DIR / "Biological_2025-11-03.csv"

FIXED_BEGIN_DEPTH_IN = 0
FIXED_END_DEPTH_IN = 8


# ---------------------------------------------------------------------
# Canonical soil-bio column mapping
# ---------------------------------------------------------------------
SOILBIO_RENAME_MAP: Dict[str, str] = {
    # dates
    "date_recd": "date_rec",

    # biomass / percentages
    "total_biomass": "total_biomass",
    "total_bacteria_biomass": "total_bacteria_biomass",
    "bacteria_pct": "bacteria_pct",

    # duplicate "gram" headers: first is Gram (+), second becomes _1 and is Gram (-)
    "gram_biomass": "gram_pos_biomass",
    "gram_pct": "gram_pos_pct",

    "actinomycetes_biomass": "actinomycetes_biomass",
    "actinomycetes_pct": "actinomycetes_pct",

    "gram_biomass_1": "gram_neg_biomass",
    "gram_pct_1": "gram_neg_pct",

    "rhizobia_biomass": "rhizobia_biomass",
    "rhizobia_pct": "rhizobia_pct",

    "total_fungi_biomass": "total_fungi_biomass",
    "total_fungi_pct": "total_fungi_pct",

    "arbuscular_mycorrhizal_biomass": "arbuscular_mycorrhizal_biomass",
    "arbuscular_mycorrhizal_pct": "arbuscular_mycorrhizal_pct",

    "saprophytic_pct": "saprophytic_pct",
    "saprophytes_biomass": "saprophytes_biomass",

    "protozoan_pct": "protozoan_pct",
    "protozoa_biomass": "protozoa_biomass",

    "undifferentiated_pct": "undifferentiated_pct",
    "undifferentiated_biomass": "undifferentiated_biomass",

    # ratios / lipid fractions
    "fungi_bacteria": "fungi_bacteria",
    "predator_prey": "predator_prey",
    "gram_gram": "gram_pos_gram_neg_ratio",

    "saturated": "saturated",
    "unsaturated": "unsaturated",
    "saturated_unsaturated_ratio": "saturated_unsaturated_ratio",

    "monounsaturated": "monounsaturated",
    "polyunsaturated": "polyunsaturated",
    "monounsaturated_polyunsaturated_ratio": "monounsaturated_polyunsaturated_ratio",

    "pre_16_1_w7c": "pre_16_1_w7c",
    "cyclo_17_0": "cyclo_17_0",
    "pre_16_1w7c_cy17_0": "pre_16_1w7c_cy17_0",

    "pre_18_1_w7c": "pre_18_1_w7c",
    "cyclo_19_0": "cyclo_19_0",
    "pre_18_1w7c_cy19_0": "pre_18_1w7c_cy19_0",

    "diversity_index": "diversity_index",
}

EXPECTED_SOILBIO_COLUMNS = [
    "strip",
    "date_rec",
    "begin_depth_in",
    "end_depth_in",
    "total_biomass",
    "total_bacteria_biomass",
    "bacteria_pct",
    "gram_pos_biomass",
    "gram_pos_pct",
    "actinomycetes_biomass",
    "actinomycetes_pct",
    "gram_neg_biomass",
    "gram_neg_pct",
    "rhizobia_biomass",
    "rhizobia_pct",
    "total_fungi_biomass",
    "total_fungi_pct",
    "arbuscular_mycorrhizal_biomass",
    "arbuscular_mycorrhizal_pct",
    "saprophytic_pct",
    "saprophytes_biomass",
    "protozoan_pct",
    "protozoa_biomass",
    "undifferentiated_pct",
    "undifferentiated_biomass",
    "fungi_bacteria",
    "predator_prey",
    "gram_pos_gram_neg_ratio",
    "saturated",
    "unsaturated",
    "saturated_unsaturated_ratio",
    "monounsaturated",
    "polyunsaturated",
    "monounsaturated_polyunsaturated_ratio",
    "pre_16_1_w7c",
    "cyclo_17_0",
    "pre_16_1w7c_cy17_0",
    "pre_18_1_w7c",
    "cyclo_19_0",
    "pre_18_1w7c_cy19_0",
    "diversity_index",
]


def _apply_rename_map(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    applicable = {src: dst for src, dst in rename_map.items() if src in out.columns}
    out = out.rename(columns=applicable)
    return out


def _merge_late_raw_if_present() -> None:
    """
    Merge the known late raw Ward soil-bio file into the canonical cleaned CSV,
    if that raw file exists.

    The merge is done in place so WARD_MASTER_SOILBIO_CSV remains the canonical
    dataset referenced across the project.
    """
    if not OUT_CLEAN_CSV.exists():
        raise FileNotFoundError(f"Canonical cleaned soil-bio CSV not found: {OUT_CLEAN_CSV}")

    if not LATE_RAW_BIO_CSV.exists():
        print(f"ℹ️ No supplemental late raw soil-bio file found at: {LATE_RAW_BIO_CSV}")
        print("ℹ️ Leaving canonical cleaned soil-bio CSV as compiled-master-only output.")
        return

    print(f"➕ Merging supplemental late raw soil-bio CSV: {LATE_RAW_BIO_CSV}")

    # Write back to the same canonical file.
    _prepare_soilbio_csv(
        clean_csv=OUT_CLEAN_CSV,
        output_csv=OUT_CLEAN_CSV,
        supplemental_raw_csv=LATE_RAW_BIO_CSV,
    )

    merged_df = pd.read_csv(OUT_CLEAN_CSV)

    if "date_rec" in merged_df.columns:
        date_counts = (
            merged_df["date_rec"]
            .fillna("")
            .astype(str)
            .value_counts(dropna=False)
            .sort_index()
        )
        print("\nCounts by 'date_rec' after supplemental merge:")
        for date_value, count in date_counts.items():
            print(f"  {date_value}: {count}")

    print(f"✅ Updated canonical soil bio clean CSV with supplemental rows: {OUT_CLEAN_CSV}")


def clean_ward_master_soilbio() -> None:
    if not IN_MASTER_CSV.exists():
        raise FileNotFoundError(f"Input not found: {IN_MASTER_CSV}")

    print(f"📥 Reading Soil Bio compiled CSV: {IN_MASTER_CSV}")
    df_raw = pd.read_csv(
        IN_MASTER_CSV,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )
    print(f"🧠 Loaded {df_raw.shape[0]} rows × {df_raw.shape[1]} columns")

    # Normalize csv headers to machine names first
    df_clean, header_map = clean_compiled_workbook(
        df_raw,
        admin_drop_cols=[],
        preserve_cols=("sample_id_1",),
    )

    # Apply shared standardization.
    # Strip comes from Sample ID 1 in this file.
    df_clean = standardize_ward_dataframe(
        df_clean,
        strip_source_candidates=("sample_id_1", "sample_id", "strip", "sample_id_2"),
        date_cols={
            "date_recd": "date_rec",
        },
        below_detection_to_zero=True,
        extra_drop_cols=(),
        fixed_depth=(FIXED_BEGIN_DEPTH_IN, FIXED_END_DEPTH_IN),
        numeric_exclude_cols=("strip", "date_rec", "sample_id_1", "sample_id", "sample_id_2"),
        add_compatibility_aliases=True,
    )

    # Dataset-specific canonical names
    df_clean = _apply_rename_map(df_clean, SOILBIO_RENAME_MAP)

    # Put key columns first
    key_cols = [c for c in ["strip", "date_rec", "begin_depth_in", "end_depth_in"] if c in df_clean.columns]
    expected_rest = [c for c in EXPECTED_SOILBIO_COLUMNS if c not in key_cols and c in df_clean.columns]
    other_cols = [c for c in df_clean.columns if c not in key_cols and c not in expected_rest]
    df_clean = df_clean[key_cols + expected_rest + other_cols]

    # Diagnostics on the compiled-master clean output
    validate_and_report(
        df_clean,
        strip_col="strip",
        date_col="date_rec",
        expected_columns=EXPECTED_SOILBIO_COLUMNS,
        matched_output_columns=df_clean.columns,
        ignore_unmatched_columns=("sample_id_1", "sample_id", "sample_id_2"),
    )

    # Write compiled-master clean output first
    write_clean_outputs(
        df_clean,
        header_map,
        out_csv=OUT_CLEAN_CSV,
        out_headers_json=OUT_HEADERS_JSON,
    )

    print(f"✅ Wrote soil bio clean CSV: {OUT_CLEAN_CSV}")
    print(f"✅ Wrote headers map JSON:   {OUT_HEADERS_JSON}")

    # Then merge in the known late raw file if present.
    _merge_late_raw_if_present()


if __name__ == "__main__":
    clean_ward_master_soilbio()