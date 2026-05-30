#!/usr/bin/env python3
"""
update_ward_master_soilbio.py

Rebuild and update the canonical Ward PLFA soil biology master dataset.

Code to run in a terminal:

    python -m biochar_app.scripts.lab.update_ward_master_soilbio

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
After rebuilding the canonical cleaned CSV from the compiled master, this script
merges known supplemental raw Ward biological CSV files that are not yet part of
the compiled master.

The canonical output remains:

    ward_master_soilbio_clean.csv
"""

from __future__ import annotations

import logging
from typing import Dict, Sequence
from pathlib import Path
import pandas as pd

from biochar_app.config.paths import (
    SOIL_BIO_PROCESSED_DIR,
    SOIL_BIO_RAW_DIR,
    WARD_MASTER_SOILBIO_CSV,
)

from biochar_app.config.lab_source_mappings import (
    RAW_TO_CANONICAL_SOILBIO,
    DROP_COLUMNS_SOILBIO,
    EXPECTED_SOILBIO_COLUMNS,
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
# Prefer Ward's newer all-dates compiled workbook when present.
# It includes the 2025-11-03 event, so that event should not be merged again
# from its individual Biological_*.csv file.
IN_MASTER_FILE = SOIL_BIO_RAW_DIR / "M Lobato PLFA Compiled All Dates.xlsx"

# Fallback retained for older checkouts or if the all-dates workbook is absent.
FALLBACK_IN_MASTER_CSV = SOIL_BIO_RAW_DIR / "Lobato PLFA Data - Compiled.csv"

# Canonical processed output path lives in config.paths
OUT_CLEAN_CSV = WARD_MASTER_SOILBIO_CSV

OUT_HEADERS_JSON = (
    SOIL_BIO_PROCESSED_DIR / "ward_master_soilbio_headers_machine_to_human.json"
)

# Supplemental raw Ward biological CSVs not yet included in the compiled master.
# Keep this explicit to avoid accidentally re-merging older files already present
# in the compiled/all-dates master.
#
# Biological_2025-11-03.csv is intentionally omitted because it should now be
# included in "M Lobato PLFA Compiled All Dates.xlsx".
SUPPLEMENTAL_RAW_BIO_CSVS = [
    SOIL_BIO_RAW_DIR / "Biological_2026-04-28.csv",
]

FIXED_BEGIN_DEPTH_IN = 0
FIXED_END_DEPTH_IN = 8



def _apply_rename_map(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    applicable = {src: dst for src, dst in rename_map.items() if src in out.columns}
    out = out.rename(columns=applicable)
    return out


def _print_date_counts(csv_path, label: str) -> None:
    merged_df = pd.read_csv(csv_path)

    if "date_rec" not in merged_df.columns:
        return

    date_counts = (
        merged_df["date_rec"]
        .fillna("")
        .astype(str)
        .value_counts(dropna=False)
        .sort_index()
    )

    print(f"\nCounts by 'date_rec' {label}:")
    for date_value, count in date_counts.items():
        print(f"  {date_value}: {count}")


def _merge_supplemental_raw_files_if_present(
    supplemental_csvs: Sequence,
) -> None:
    """
    Merge known supplemental raw Ward soil-bio files into the canonical cleaned CSV.

    The merge is done in place so WARD_MASTER_SOILBIO_CSV remains the canonical
    dataset referenced across the project.
    """
    if not OUT_CLEAN_CSV.exists():
        raise FileNotFoundError(f"Canonical cleaned soil-bio CSV not found: {OUT_CLEAN_CSV}")

    found_any = False

    for supplemental_csv in supplemental_csvs:
        if not supplemental_csv.exists():
            print(f"ℹ️ Supplemental raw soil-bio file not found, skipping: {supplemental_csv}")
            continue

        found_any = True
        print(f"\n➕ Merging supplemental raw soil-bio CSV: {supplemental_csv}")

        # Write back to the same canonical file.
        _prepare_soilbio_csv(
            clean_csv=OUT_CLEAN_CSV,
            output_csv=OUT_CLEAN_CSV,
            supplemental_raw_csv=supplemental_csv,
        )

        _print_date_counts(
            OUT_CLEAN_CSV,
            label=f"after merging {supplemental_csv.name}",
        )

    if not found_any:
        print("ℹ️ No supplemental raw soil-bio files were found.")
        print("ℹ️ Leaving canonical cleaned soil-bio CSV as compiled-master-only output.")
        return

    print(f"\n✅ Updated canonical soil bio clean CSV with supplemental rows: {OUT_CLEAN_CSV}")


def _resolve_master_input() -> Path:
    """Return the preferred compiled master file path, with CSV fallback."""
    if IN_MASTER_FILE.exists():
        return IN_MASTER_FILE

    if FALLBACK_IN_MASTER_CSV.exists():
        print(
            "ℹ️ All-dates compiled workbook not found; "
            f"falling back to: {FALLBACK_IN_MASTER_CSV}"
        )
        return FALLBACK_IN_MASTER_CSV

    raise FileNotFoundError(
        "No compiled soil-bio master file found. Checked:\n"
        f"  {IN_MASTER_FILE}\n"
        f"  {FALLBACK_IN_MASTER_CSV}"
    )


def _read_compiled_master(path: Path) -> pd.DataFrame:
    """Read Ward's compiled PLFA file from CSV or Excel."""
    suffix = path.suffix.lower()

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(
            path,
            dtype=str,
            keep_default_na=False,
        )

    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )


def update_ward_master_soilbio() -> None:
    input_master = _resolve_master_input()

    print(f"📥 Reading Soil Bio compiled master: {input_master}")
    df_raw = _read_compiled_master(input_master)
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

    df_clean = df_clean[
        df_clean["strip"].fillna("").astype(str).str.strip().ne("")
        & df_clean["date_rec"].fillna("").astype(str).str.strip().ne("")
        ].copy()


    # Dataset-specific canonical names
    df_clean = _apply_rename_map(df_clean, RAW_TO_CANONICAL_SOILBIO)

    # Put key columns first
    key_cols = [
        c
        for c in ["strip", "date_rec", "begin_depth_in", "end_depth_in"]
        if c in df_clean.columns
    ]
    expected_rest = [
        c
        for c in EXPECTED_SOILBIO_COLUMNS
        if c not in key_cols and c in df_clean.columns
    ]
    other_cols = [
        c
        for c in df_clean.columns
        if c not in key_cols and c not in expected_rest
    ]
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

    # Then merge in known supplemental files if present.
    _merge_supplemental_raw_files_if_present(SUPPLEMENTAL_RAW_BIO_CSVS)


# Backward-compatible alias in case any old imports still use the prior function name.
clean_ward_master_soilbio = update_ward_master_soilbio


if __name__ == "__main__":
    update_ward_master_soilbio()