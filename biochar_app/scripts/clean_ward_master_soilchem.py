#!/usr/bin/env python3
"""
clean_ward_master_soilchem.py

Clean the compiled Soil Chemistry "master" workbook into a machine-readable CSV.

Decision: all samples are from a fixed depth interval 0–8 inches (no variability).
This script enforces that by adding:
  - begin_depth_in = 0
  - end_depth_in   = 8

Style: matches clean_ward_master_nir.py (no argparse; paths are defined here).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from clean_ward_master_common import (
    ADMIN_DROP_COLS,
    add_fixed_depth_columns,
    clean_compiled_workbook,
    drop_admin_columns,
    normalize_date_columns,
    normalize_strip_column,
    write_clean_outputs,
)

# ============================================================
# Paths (restored; same pattern as clean_ward_master_nir.py)
# ============================================================
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]

# ------------------------------------------------------------
# INPUT: raw Lobato soil chemistry workbook (single header row)
# ------------------------------------------------------------
MASTER_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-raw"
    / "lab-tests"
    / "soil-tests-chem"
    / "csv-files"
    / "Lobato - Soil chemistry results compiled.xlsx"
)

# ------------------------------------------------------------
# OUTPUTS
# ------------------------------------------------------------
OUT_CLEAN_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "soil-tests-chem"
    / "csv-files"
    / "ward_master_soilchem_clean.csv"
)

OUT_HEADERS_JSON = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "soil-tests-chem"
    / "csv-files"
    / "ward_master_soilChem_headers_machine_to_human.json"
)


def clean_ward_master_soilchem() -> None:
    if not MASTER_CSV.exists():
        raise FileNotFoundError(f"Input not found: {MASTER_CSV}")

    print(f"📥 Reading Soil Chem master: {MASTER_CSV}")

    # Lobato soil chem workbook appears to be a single header row.
    # If there are multiple sheets, we take the first by default.
    df = pd.read_excel(MASTER_CSV, sheet_name=0)

    print(f"🧠 Loaded {df.shape[0]} rows × {df.shape[1]} columns")

    # 1) Machine names + base cleaning (common logic)
    df_clean, header_map = clean_compiled_workbook(df, admin_drop_cols=ADMIN_DROP_COLS)

    # 2) Make/normalize strip
    # Soil chem uses "Sample ID" in the raw file; common cleaner should have
    # converted that to machine "sample_id". If not, we still defensively handle it.
    if "strip" not in df_clean.columns:
        if "sample_id" in df_clean.columns:
            df_clean["strip"] = df_clean["sample_id"]
        elif "sampleid" in df_clean.columns:
            df_clean["strip"] = df_clean["sampleid"]
    df_clean = normalize_strip_column(df_clean, strip_col="strip")

    # 3) Normalize dates (Date Recd / Date Rept -> date_rec / date_rept)
    # Your soil chem file has "Date Recd" and "Date Rept" (single header row).
    # The common cleaner may already normalize these; we still map defensively.
    date_map: dict[str, str] = {}
    # received
    if "date_recd" in df_clean.columns:
        date_map["date_recd"] = "date_rec"
    if "date_rec" in df_clean.columns:
        date_map["date_rec"] = "date_rec"
    if "date_received" in df_clean.columns:
        date_map["date_received"] = "date_rec"
    # reported
    if "date_rept" in df_clean.columns:
        date_map["date_rept"] = "date_rept"
    if "date_reported" in df_clean.columns:
        date_map["date_reported"] = "date_rept"

    if date_map:
        df_clean = normalize_date_columns(df_clean, date_cols=date_map)

    # 4) Fixed depth 0–8 in (no variability)
    df_clean = add_fixed_depth_columns(df_clean, begin_in=0, end_in=8)

    # 5) Re-apply admin drop list (safe) after we created 'strip'
    df_clean = drop_admin_columns(df_clean, extra_drop=list(ADMIN_DROP_COLS))

    # 6) Put keys first (consistent with NIR cleaner)
    key_cols = [c for c in ["strip", "date_rec", "date_rept", "begin_depth_in", "end_depth_in"] if c in df_clean.columns]
    other_cols = [c for c in df_clean.columns if c not in key_cols]
    df_clean = df_clean[key_cols + other_cols]

    # 7) Write outputs
    write_clean_outputs(df_clean, header_map, out_csv=OUT_CLEAN_CSV, out_headers_json=OUT_HEADERS_JSON)

    print(f"✅ Wrote soil chem clean CSV: {OUT_CLEAN_CSV}")
    print(f"✅ Wrote headers map JSON:   {OUT_HEADERS_JSON}")


if __name__ == "__main__":
    clean_ward_master_soilchem()