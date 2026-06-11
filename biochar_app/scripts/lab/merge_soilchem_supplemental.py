#!/usr/bin/env python3
"""
Append supplemental raw Ward soil chemistry CSV rows into the authoritative
ward_master_soilchem_clean.csv.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from biochar_app.config.paths import  LAB_TESTS_RAW_DIR, WARD_MASTER_SOILCHEM_CSV
from biochar_app.scripts.lab.clean_ward_master_common import normalize_strip

RAW_SUPPLEMENTAL_CSV = (
    LAB_TESTS_RAW_DIR
    / "soil-tests-chem"
    / "csv-files"
    / "Soil_2025-11-03.csv"
)


RAW_TO_CLEAN = {
    "Sample ID 2": "sample_id",
    # "Date Received": "date_rec",
    # "Date Reported": "date_rept",
    "Past Crop": "past_crop",
    "Begin Depth": "begin_depth_in",
    "End Depth": "end_depth_in",
    "Soil pH 1:1 ": "1_1_soil_ph",
    "BpH Modified WDRF": "wdrf_buffer_ph",
    "Soluble Salts 1:1 mmho/cm": "1_1_s_salts_mmho_cm",
    "Excess Lime Rating": "excess_lime",
    "Organic Matter LOI %": "organic_matter_loi_pct",
    "Phosphorus Olsen P ppm P": "olsen_p_ppm_p",
    "Potassium NH4OAc ppm K": "potassium_ppm_k",
    "Calcium NH4OAc ppm Ca": "calcium_ppm_ca",
    "Magnesium NH4OAc ppm Mg": "magnesium_ppm_mg",
    "Sodium NH4OAc ppm Na": "sodium_ppm_na",
    "Sum of Cations me/100g": "cec_sum_of_cations_me_100g",
    "H Saturation %": "pcth_sat",
    "K Saturation %": "pctk_sat",
    "Ca Saturation %": "pctca_sat",
    "Mg Saturation %": "pctmg_sat",
    "Na Saturation %": "pctna_sat",
    "Sulfate M-3 ppm S": "sulfate_s_ppm_s",
    "Zinc DTPA/Sorb. ppm Zn": "zinc_ppm_zn",
    "Iron DTPA/Sorb. ppm Fe": "iron_ppm_fe",
    "Manganese DTPA/Sorb. ppm Mn": "manganese_ppm_mn",
    "Copper DTPA/Sorb. ppm Cu": "copper_ppm_cu",
    "Boron B": "boron_ppm_b",
    "Nitrate ppm NO3-N": "nitrate_n_ppm",
    "Ammonium ppm NH4-N": "h2o_nh4_n",
    "Organic Carbon ppm C": "organic_c_h2o_ppm",
    "Organic Nitrogen ppm N": "organic_n_h2o_ppm",
    "Organic C:N ": "organic_c_n_h2o",
    "Soil Health Calculation": "soil_health_score",
    "Organic Nitrogen Release ppm N": "organic_nitrogen_release_ppm_n",
    "Organic Nitrogen Reserve ppm N": "organic_nitrogen_reserve_ppm_n",
    "Microbially Active Carbon % MAC": "microbially_active_carbon_pctma",
    "Total Nitrogen ppm N": "total_n_h2o_ppm_n",
    "Soil Respiration ppm CO2C": "co2_soil_respiration",
    "Water Stable Aggregates (Mod) %": "water_stable_aggregates_mod",
    "Nitrogen N": "nitrogen_rec",
    "Phosphorus P2O5": "p2o5_rec",
    "Potassium K2O": "k2o_rec",
    "Sulfur S": "sulfur_rec",
    "Zinc Zn": "zinc_rec",
    "Magnesium Mg": "magnesium_rec",
    "Iron Fe": "iron_rec",
    "Manganese Mn": "manganese_rec",
    "Copper Cu": "copper_rec",
}


TEXT_COLUMNS = {
    "strip",
    "sample_id",
    "date_rec",
    "date_rept",
    "past_crop",
    "excess_lime",
}


def parse_date(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


def coerce_value(col: str, value: Any) -> Any:
    if col in TEXT_COLUMNS:
        return "" if pd.isna(value) else str(value).strip()

    if pd.isna(value):
        return None

    return pd.to_numeric(value, errors="coerce")


def build_supplemental_clean(raw_csv: Path, clean_columns: list[str]) -> pd.DataFrame:
    raw = pd.read_csv(raw_csv)

    records = []

    for _, row in raw.iterrows():
        sample_id = row.get("Sample ID 2")
        strip = normalize_strip(sample_id)

        if pd.isna(strip) or not str(strip).strip():
            continue

        record = {col: None for col in clean_columns}
        record["strip"] = strip
        record["date_rec"] = parse_date(row.get("Date Received"))
        record["date_rept"] = parse_date(row.get("Date Reported"))
        record["begin_depth_in"] = row.get("Begin Depth")
        record["end_depth_in"] = row.get("End Depth")

        for raw_col, clean_col in RAW_TO_CLEAN.items():
            if clean_col in record and raw_col in raw.columns:
                record[clean_col] = coerce_value(clean_col, row.get(raw_col))

        # Stable compatibility aliases.
        if "soil_ph_1_1" in record:
            record["soil_ph_1_1"] = record.get("1_1_soil_ph")
        if "ec_1_1" in record:
            record["ec_1_1"] = record.get("1_1_s_salts_mmho_cm")
        if "cec_meq_100g" in record:
            record["cec_meq_100g"] = record.get("cec_sum_of_cations_me_100g")
        if "sum_of_cations_meq_100g" in record:
            record["sum_of_cations_meq_100g"] = record.get("cec_sum_of_cations_me_100g")

        records.append(record)

    return pd.DataFrame(records, columns=clean_columns)


def main() -> None:
    if not WARD_MASTER_SOILCHEM_CSV.exists():
        raise FileNotFoundError(WARD_MASTER_SOILCHEM_CSV)

    if not RAW_SUPPLEMENTAL_CSV.exists():
        raise FileNotFoundError(RAW_SUPPLEMENTAL_CSV)

    base = pd.read_csv(WARD_MASTER_SOILCHEM_CSV)
    base["date_rec"] = base["date_rec"].map(parse_date)
    base["date_rept"] = base["date_rept"].map(parse_date)
    clean_columns = list(base.columns)

    supplemental = build_supplemental_clean(
        raw_csv=RAW_SUPPLEMENTAL_CSV,
        clean_columns=clean_columns,
    )

    combined = pd.concat([base, supplemental], ignore_index=True, sort=False)
    combined = combined.drop_duplicates(subset=["strip", "date_rec"], keep="last")

    combined["_sort_date"] = pd.to_datetime(combined["date_rec"], errors="coerce")
    combined = combined.sort_values(["_sort_date", "strip"], kind="stable")
    combined = combined.drop(columns=["_sort_date"])

    combined.to_csv(WARD_MASTER_SOILCHEM_CSV, index=False)

    print(f"Base rows: {len(base)}")
    print(f"Supplemental rows: {len(supplemental)}")
    print(f"Final rows: {len(combined)}")
    print()
    print(combined[["strip", "date_rec", "date_rept", "excess_lime"]].to_string(index=False))


if __name__ == "__main__":
    main()