#!/usr/bin/env python3
"""
clean_ward_master_soilchem.py

Clean the compiled Soil Chemistry "master" workbook into a machine-readable CSV.

Key behaviors:
- Preserve "Sample ID" through the first cleaning pass (do NOT admin-drop early)
- Build strip from Sample ID (special case: "WEST FIELD" -> STRIP 4)
- Normalize dates
- Enforce fixed depth 0–8 inches
- Drop admin columns AFTER strip is created
- Create canonical “expected” columns used by table builders:
    - soil_ph_1_1 from 1_1_soil_ph (if needed)
    - ec_1_1 from 1_1_s_salts_mmho_cm (if needed)
    - cec_meq_100g and sum_of_cations_meq_100g from cec_sum_of_cations_me_100g (if needed)
- Write:
  - ward_master_soilchem_clean.csv
  - ward_master_soilChem_headers_machine_to_human.json
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from clean_ward_master_common import (
    ADMIN_DROP_COLS,
    clean_compiled_workbook,
    drop_admin_columns,
    normalize_strip_column,
    normalize_date_columns,
    add_fixed_depth_columns,
    write_clean_outputs,
)

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]

# ------------------------------------------------------------
# INPUT: raw Lobato soil chemistry workbook
# ------------------------------------------------------------
MASTER_XLSX = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-raw"
    / "lab-tests"
    / "soil-tests-chem"
    / "csv-files"
    / "Lobato - Soil chemistry results compiled_v2.xlsx"
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


def _find_machine_col_by_human_header(header_map: dict, human_name: str) -> Optional[str]:
    target = " ".join(str(human_name).strip().split()).lower()
    for machine, human in header_map.items():
        h = " ".join(str(human).strip().split()).lower()
        if h == target:
            return machine
    return None


def _coerce_sample_id_column(df_clean: pd.DataFrame, header_map: dict) -> pd.DataFrame:
    df = df_clean.copy()

    if "sample_id" in df.columns:
        return df

    machine = _find_machine_col_by_human_header(header_map, "Sample ID")

    if machine is None:
        for m, h in header_map.items():
            if "sample id" in str(h).strip().lower():
                machine = m
                break

    if machine is None:
        for c in df.columns:
            if str(c).strip().lower() in ("sample_id", "sample_id_1", "sampleid"):
                machine = c
                break

    if machine is None or machine not in df.columns:
        sampleish = [c for c in df.columns if "sample" in c.lower()]
        raise ValueError(
            "Could not locate a cleaned Sample ID column. "
            f"Columns containing 'sample': {sampleish}. "
            "This likely means the cleaner dropped it; ensure admin-drop is disabled on first pass."
        )

    df["sample_id"] = df[machine].astype(str)
    return df


def _normalize_strip_from_sample_id(df_clean: pd.DataFrame) -> pd.DataFrame:
    df = df_clean.copy()

    if "sample_id" not in df.columns:
        raise ValueError("Expected 'sample_id' to exist before building strip.")

    def _fix_sample_id(x: object) -> str:
        s = str(x).strip()
        if s == "":
            return s
        if s.strip().lower() == "west field":
            return "STRIP 4"
        return s

    df["sample_id"] = df["sample_id"].map(_fix_sample_id)

    df["strip"] = df["sample_id"]
    df = normalize_strip_column(df, strip_col="strip")
    return df


def _ensure_expected_soilchem_columns(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Create the canonical columns your table builder expects, based on what Ward actually provides.
    We do NOT recompute values—just copy into stable names.

    - soil_ph_1_1 <- 1_1_soil_ph
    - ec_1_1      <- 1_1_s_salts_mmho_cm
    - cec_meq_100g, sum_of_cations_meq_100g <- cec_sum_of_cations_me_100g
    """
    df = df_clean.copy()

    # pH (1:1)
    if "soil_ph_1_1" not in df.columns and "1_1_soil_ph" in df.columns:
        df["soil_ph_1_1"] = df["1_1_soil_ph"]

    # EC / salts (1:1)
    if "ec_1_1" not in df.columns and "1_1_s_salts_mmho_cm" in df.columns:
        df["ec_1_1"] = df["1_1_s_salts_mmho_cm"]

    # Ward combined CEC/Sum-of-cations column
    if "cec_sum_of_cations_me_100g" in df.columns:
        if "cec_meq_100g" not in df.columns:
            df["cec_meq_100g"] = df["cec_sum_of_cations_me_100g"]
        if "sum_of_cations_meq_100g" not in df.columns:
            df["sum_of_cations_meq_100g"] = df["cec_sum_of_cations_me_100g"]

    return df


def clean_ward_master_soilchem(sheet: Optional[str] = None) -> None:
    if not MASTER_XLSX.exists():
        raise FileNotFoundError(f"Input not found: {MASTER_XLSX}")

    df_raw = pd.read_excel(MASTER_XLSX, sheet_name=(0 if sheet is None else sheet))
    print(f"📥 Reading Soil Chem master: {MASTER_XLSX}")
    print(f"🧠 Loaded {df_raw.shape[0]} rows × {df_raw.shape[1]} columns")

    # 1) Clean headers / normalize names (do NOT admin-drop yet)
    df_clean, header_map = clean_compiled_workbook(df_raw, admin_drop_cols=[])

    # Ensure sample_id survives
    df_clean = _coerce_sample_id_column(df_clean, header_map)

    # 2) Build strip from sample_id
    df_clean = _normalize_strip_from_sample_id(df_clean)

    # 3) Normalize dates
    date_map = {}
    if "date_recd" in df_clean.columns:
        date_map["date_recd"] = "date_rec"
    if "date_recd_" in df_clean.columns:
        date_map["date_recd_"] = "date_rec"
    if "date_rec" in df_clean.columns:
        date_map["date_rec"] = "date_rec"
    if "date_received" in df_clean.columns:
        date_map["date_received"] = "date_rec"

    if "date_rept" in df_clean.columns:
        date_map["date_rept"] = "date_rept"
    if "date_reported" in df_clean.columns:
        date_map["date_reported"] = "date_rept"

    if date_map:
        df_clean = normalize_date_columns(df_clean, date_cols=date_map)

    # 4) Fixed depth 0–8 in
    df_clean = add_fixed_depth_columns(df_clean, begin_in=0, end_in=8)

    # 5) Create canonical columns expected by table builder
    df_clean = _ensure_expected_soilchem_columns(df_clean)

    # 6) Drop admin columns AFTER strip is created
    df_clean = drop_admin_columns(df_clean, extra_drop=list(ADMIN_DROP_COLS))

    # 7) Write outputs
    OUT_CLEAN_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUT_HEADERS_JSON.parent.mkdir(parents=True, exist_ok=True)

    write_clean_outputs(df_clean, header_map, out_csv=OUT_CLEAN_CSV, out_headers_json=OUT_HEADERS_JSON)

    print(f"✅ Wrote soil chem clean CSV: {OUT_CLEAN_CSV}")
    print(f"✅ Wrote headers map JSON:   {OUT_HEADERS_JSON}")


if __name__ == "__main__":
    clean_ward_master_soilchem()