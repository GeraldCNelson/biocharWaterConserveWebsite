#!/usr/bin/env python3
"""
clean_ward_master_soilchem.py

Clean the compiled Soil Chemistry workbook into a canonical machine-readable CSV
for the Biochar dashboard.

Code to run in a terminal
python -m biochar_app.scripts.clean_ward_master_soilchem

Conventions
-----------
* lab/master CSV strip values are canonicalized to:
    strip_1, strip_2, strip_3, strip_4

Key behaviors
-------------
* Reads the compiled soil chemistry workbook
* Preserves Sample ID through the first pass
* Builds canonical strip from Sample ID
* Drops non-project rows whose Sample ID does not resolve to strip_1..strip_4
  (e.g. WEST FIELD, EAST FIELD, HAY FIELD)
* Normalizes date fields to YYYY-MM-DD
* Enforces fixed depth 0–8 inches
* Creates stable columns used by downstream table builders
* Writes:
    - cleaned master CSV
    - machine->human headers JSON
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from biochar_app.config.paths import LAB_TESTS_RAW_DIR, SOIL_CHEM_PROCESSED_DIR
from biochar_app.scripts.clean_ward_master_common import (
    clean_compiled_workbook,
    standardize_ward_dataframe,
    validate_and_report,
    write_clean_outputs,
    normalize_strip,
)

MASTER_XLSX = LAB_TESTS_RAW_DIR / "soil-tests-chem" / "csv-files" / "Lobato - Soil chemistry results compiled.xlsx"

OUT_CLEAN_CSV = SOIL_CHEM_PROCESSED_DIR / "ward_master_soilchem_clean.csv"
OUT_HEADERS_JSON = SOIL_CHEM_PROCESSED_DIR / "ward_master_soilchem_headers_machine_to_human.json"


def _find_machine_col_by_human_header(header_map: dict[str, str], human_name: str) -> Optional[str]:
    target = " ".join(str(human_name).strip().split()).lower()
    for machine, human in header_map.items():
        h = " ".join(str(human).strip().split()).lower()
        if h == target:
            return machine
    return None


def _ensure_sample_id_column(df_clean: pd.DataFrame, header_map: dict[str, str]) -> pd.DataFrame:
    out = df_clean.copy()

    if "sample_id" in out.columns:
        return out

    machine = _find_machine_col_by_human_header(header_map, "Sample ID")
    if machine is None:
        for m, h in header_map.items():
            if "sample id" in str(h).strip().lower():
                machine = m
                break

    if machine is None or machine not in out.columns:
        sampleish = [c for c in out.columns if "sample" in c.lower()]
        raise ValueError(
            "Could not locate Sample ID column in cleaned soil chem workbook. "
            f"Columns containing 'sample': {sampleish}"
        )

    out["sample_id"] = out[machine].astype(str)
    return out


def _filter_to_project_rows(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only rows whose sample_id resolves to one of the project strips.
    This removes rows like WEST FIELD / EAST FIELD / HAY FIELD.
    """
    out = df_clean.copy()

    if "sample_id" not in out.columns:
        raise ValueError("Expected 'sample_id' before filtering project rows.")

    norm = out["sample_id"].apply(normalize_strip)
    mask = norm.notna()

    kept = int(mask.sum())
    dropped = int((~mask).sum())
    print(f"🔎 Soil Chem project-row filter: kept={kept}, dropped={dropped}")

    out = out.loc[mask].copy()
    out["sample_id"] = out["sample_id"].astype(str)
    return out


def _ensure_expected_soilchem_columns(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Create stable downstream columns without recomputing values.
    """
    out = df_clean.copy()

    if "soil_ph_1_1" not in out.columns and "1_1_soil_ph" in out.columns:
        out["soil_ph_1_1"] = out["1_1_soil_ph"]

    if "ec_1_1" not in out.columns and "1_1_s_salts_mmho_cm" in out.columns:
        out["ec_1_1"] = out["1_1_s_salts_mmho_cm"]

    if "cec_sum_of_cations_me_100g" in out.columns:
        if "cec_meq_100g" not in out.columns:
            out["cec_meq_100g"] = out["cec_sum_of_cations_me_100g"]
        if "sum_of_cations_meq_100g" not in out.columns:
            out["sum_of_cations_meq_100g"] = out["cec_sum_of_cations_me_100g"]

    return out


EXPECTED_SOILCHEM_COLUMNS = [
    "strip",
    "date_rec",
    "date_rept",
    "begin_depth_in",
    "end_depth_in",
]


def clean_ward_master_soilchem(sheet: Optional[str] = None) -> None:
    if not MASTER_XLSX.exists():
        raise FileNotFoundError(f"Input not found: {MASTER_XLSX}")

    df_raw = pd.read_excel(MASTER_XLSX, sheet_name=(0 if sheet is None else sheet))
    print(f"📥 Reading Soil Chem master: {MASTER_XLSX}")
    print(f"🧠 Loaded {df_raw.shape[0]} rows × {df_raw.shape[1]} columns")

    # 1) Normalize headers without early admin-drop so Sample ID survives
    df_clean, header_map = clean_compiled_workbook(df_raw, admin_drop_cols=[])

    # 2) Ensure sample_id exists
    df_clean = _ensure_sample_id_column(df_clean, header_map)

    # 3) Filter to actual project rows before shared standardization
    df_clean = _filter_to_project_rows(df_clean)

    # 4) Apply common standardization
    df_clean = standardize_ward_dataframe(
        df_clean,
        strip_source_candidates=("strip", "sample_id", "sample_id_1"),
        date_cols={
            "date_recd": "date_rec",
            "date_received": "date_rec",
            "date_rec": "date_rec",
            "date_rept": "date_rept",
            "date_reported": "date_rept",
        },
        below_detection_to_zero=True,
        extra_drop_cols=(),
        fixed_depth=(0, 8),
        numeric_exclude_cols=("strip", "date_rec", "date_rept", "date_recd", "sample_id"),
        add_compatibility_aliases=True,
    )

    # 5) Dataset-specific stable columns
    df_clean = _ensure_expected_soilchem_columns(df_clean)

    # 6) Put key columns first
    key_cols = [c for c in ["strip", "date_rec", "date_rept", "begin_depth_in", "end_depth_in"] if c in df_clean.columns]
    other_cols = [c for c in df_clean.columns if c not in key_cols]
    df_clean = df_clean[key_cols + other_cols]

    # Diagnostics
    validate_and_report(
        df_clean,
        strip_col="strip",
        date_col="date_rept",
        expected_columns=EXPECTED_SOILCHEM_COLUMNS,
        matched_output_columns=df_clean.columns,
        ignore_unmatched_columns=(),
    )

    # 7) Write outputs
    write_clean_outputs(
        df_clean,
        header_map,
        out_csv=OUT_CLEAN_CSV,
        out_headers_json=OUT_HEADERS_JSON,
    )

    print(f"✅ Wrote soil chem clean CSV: {OUT_CLEAN_CSV}")
    print(f"✅ Wrote headers map JSON:   {OUT_HEADERS_JSON}")


if __name__ == "__main__":
    clean_ward_master_soilchem()