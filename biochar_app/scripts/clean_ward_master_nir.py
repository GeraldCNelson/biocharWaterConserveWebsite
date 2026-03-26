#!/usr/bin/env python3
"""
clean_ward_master_nir.py

Clean the compiled Ward NIR / hay master file into a canonical machine-readable
CSV for the Biochar dashboard.

Code to run in a terminal
python -m biochar_app.scripts.clean_ward_master_nir

Ward format
-----------
The compiled Ward NIR file has:
* row 0: human headers
* row 1: snake/machine headers
* row 2+: data

Conventions
-----------
* lab/master CSV strip values are canonicalized to:
    strip_1, strip_2, strip_3, strip_4
* NIR date column in cleaned output is:
    nir_date

Key behaviors
-------------
* Reads the two-header Ward compiled CSV
* Normalizes strip from values like:
    S1HAY, S2HAY, STRIP 1, etc.
* Normalizes date_rec -> nir_date
* Drops rows missing strip or nir_date
* Writes:
    - cleaned master CSV
    - machine->human headers JSON
"""

from __future__ import annotations

import logging

import pandas as pd

from biochar_app.config.paths import (
    LAB_TESTS_RAW_DIR,
    HAY_TESTS_PROCESSED_DIR,
)
from biochar_app.scripts.clean_ward_master_common import (
    read_ward_two_header_csv,
    standardize_ward_dataframe,
    validate_and_report,
    write_clean_outputs,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

IN_MASTER_CSV = LAB_TESTS_RAW_DIR / "hay-tests" / "csv-files" / "Master 1.15.2026 Revision.csv"

OUT_CLEAN_CSV = HAY_TESTS_PROCESSED_DIR / "ward_master_nir_clean.csv"
OUT_HEADERS_JSON = HAY_TESTS_PROCESSED_DIR / "ward_master_nir_headers_machine_to_human.json"

DROP_COLUMNS = {
    "customer",
    "first_name",
    "last_name",
    "company",
    "address_1",
    "address_2",
    "city",
    "state",
    "zip",
    "date_reported",
    "lab_no",
    "results_for",
    "description",
}

EXPECTED_NIR_COLUMNS = [
    "strip",
    "nir_date",
]


def _rename_nir_date(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "date_rec" in out.columns and "nir_date" not in out.columns:
        out = out.rename(columns={"date_rec": "nir_date"})
    return out


def clean_ward_master_nir() -> None:
    logger.info(f"📥 Reading Ward NIR master CSV: {IN_MASTER_CSV}")
    df, header_map = read_ward_two_header_csv(IN_MASTER_CSV)
    logger.info(f"🧠 Loaded {df.shape[0]} rows × {df.shape[1]} columns")

    if "date_rec" not in df.columns:
        raise ValueError("Expected machine column 'date_rec' not found in NIR master.")
    if "sample_id" not in df.columns:
        raise ValueError("Expected machine column 'sample_id' not found in NIR master.")

    # Shared standardization
    df = standardize_ward_dataframe(
        df,
        strip_source_candidates=("strip", "sample_id"),
        date_cols={"date_rec": "date_rec"},
        below_detection_to_zero=True,
        extra_drop_cols=DROP_COLUMNS,
        fixed_depth=None,
        numeric_exclude_cols=("strip", "date_rec", "nir_date"),
        add_compatibility_aliases=True,
    )

    df = _rename_nir_date(df)

    # Require usable rows
    if "strip" in df.columns:
        df = df[df["strip"].notna()]
    if "nir_date" in df.columns:
        df = df[df["nir_date"].notna()]

    # Put keys first
    key_cols = [c for c in ["strip", "nir_date"] if c in df.columns]
    other_cols = [c for c in df.columns if c not in key_cols]
    df = df[key_cols + other_cols]

    # Diagnostics
    validate_and_report(
        df,
        strip_col="strip",
        date_col="nir_date",
        expected_columns=EXPECTED_NIR_COLUMNS,
        matched_output_columns=df.columns,
        ignore_unmatched_columns=(),
    )

    # Write outputs
    write_clean_outputs(
        df,
        header_map,
        out_csv=OUT_CLEAN_CSV,
        out_headers_json=OUT_HEADERS_JSON,
    )

    logger.info(f"✅ Wrote cleaned NIR master: {OUT_CLEAN_CSV}")
    logger.info(f"✅ Wrote header map JSON:    {OUT_HEADERS_JSON}")
    logger.info("✅ Done.")


if __name__ == "__main__":
    clean_ward_master_nir()