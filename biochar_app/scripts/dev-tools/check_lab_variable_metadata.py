#!/usr/bin/env python3
"""
Validate lab variable metadata coverage and basic structure.

Run:
python biochar_app/scripts/dev-tools/check_lab_variable_metadata.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from biochar_app.config.lab_variable_metadata import LAB_VARIABLE_METADATA
from biochar_app.config.paths import (
    WARD_MASTER_NIR_CSV,
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_SOILCHEM_CSV,
)

SOIL_BIO_CSV = WARD_MASTER_SOILBIO_CSV
NIR_CSV = WARD_MASTER_NIR_CSV
SOIL_CHEM_CSV = WARD_MASTER_SOILCHEM_CSV

IGNORE_SOIL_BIO_COLUMNS = {
    "strip",
    "date_rec",
    "date_rept",
    "begin_depth_in",
    "end_depth_in",
}

IGNORE_SOIL_CHEM_COLUMNS = {
    "strip",
    "date_rec",
    "date_rept",
    "begin_depth_in",
    "end_depth_in",
    "sample_id",
    "past_crop",
    "crop_1",
    "crop_2",
    "yg_1",
    "yg_2",
}

IGNORE_SOIL_CHEM_RECOMMENDATION_COLUMNS = {
    "nitrogen_rec",
    "p2o5_rec",
    "k2o_rec",
    "sulfur_rec",
    "zinc_rec",
    "magnesium_rec",
    "iron_rec",
    "manganese_rec",
    "copper_rec",
    "boron_rec",
    "nitrogen_rec_1",
    "p2o5_rec_1",
    "k2o_rec_1",
    "sulfur_rec_1",
    "zinc_rec_1",
    "magnesium_rec_1",
    "iron_rec_1",
    "manganese_rec_1",
    "copper_rec_1",
}

IGNORE_COMPATIBILITY_COLUMNS = {
    "1_1_soil_ph",
    "soil_ph",
    "1_1_s_salts_mmho_cm",
    "cec_sum_of_cations_me_100g",
    "sum_of_cations_meq_100g",
}

REQUIRED_FIELDS = {
    "display_label",
    "dataset_family",
    "group",
    "units",
    "value_type",
    "definition",
    "interpretation_note",
    "source_reference_group",
    "aliases",
    "related_terms",
}

def check_csv_coverage(
    csv_path: Path,
    dataset_family: str,
    label: str,
    ignore_columns: set[str],
    ignore_compatibility_columns: set[str] | None = None,
) -> list[str]:
    if not csv_path.exists():
        return [f"Missing CSV: {csv_path}"]

    ignore_compatibility_columns = ignore_compatibility_columns or set()
    df = pd.read_csv(csv_path, nrows=1)
    csv_cols = (
        set(df.columns)
        - ignore_columns
        - ignore_compatibility_columns
    )
    meta_cols = {
        key
        for key, meta in LAB_VARIABLE_METADATA.items()
        if meta.get("dataset_family") == dataset_family
    }

    missing = sorted(csv_cols - meta_cols)
    extra = sorted(meta_cols - csv_cols)
    problems: list[str] = []
    if missing:
        problems.append(
            f"Missing metadata entries for {label}:\n  " + "\n  ".join(missing)
        )
    if extra:
        problems.append(
            f"Metadata entries not in {label} CSV:\n  " + "\n  ".join(extra)
        )

    return problems


def check_required_fields() -> list[str]:
    problems: list[str] = []

    for key, meta in LAB_VARIABLE_METADATA.items():
        missing = sorted(REQUIRED_FIELDS - set(meta.keys()))
        if missing:
            problems.append(f"{key}: missing fields: {missing}")

        if not isinstance(meta.get("aliases"), list):
            problems.append(f"{key}: aliases must be a list")

        if not isinstance(meta.get("related_terms"), list):
            problems.append(f"{key}: related_terms must be a list")

    return problems


def check_duplicate_aliases() -> list[str]:
    problems: list[str] = []
    seen: dict[tuple[str, str], str] = {}

    for key, meta in LAB_VARIABLE_METADATA.items():
        for alias in meta.get("aliases", []):
            alias_norm = str(alias).strip().lower()
            if not alias_norm:
                continue

            dataset_family = str(meta.get("dataset_family", ""))
            seen_key = (dataset_family, alias_norm)

            if seen_key in seen:
                problems.append(
                    f"alias '{alias}' appears in both {seen[seen_key]} and {key} "
                    f"within dataset_family={dataset_family}"
                )
            else:
                seen[seen_key] = key

    return problems


def check_soil_bio_coverage() -> list[str]:
    return check_csv_coverage(
        csv_path=SOIL_BIO_CSV,
        dataset_family="soil_biology_plfa",
        label="soil bio",
        ignore_columns=IGNORE_SOIL_BIO_COLUMNS,
    )

def check_nir_coverage() -> list[str]:
    return check_csv_coverage(
        csv_path=NIR_CSV,
        dataset_family="hay_nir",
        label="NIR",
        ignore_columns={"strip", "nir_date", "sample_id"},
    )

def check_soil_chem_coverage() -> list[str]:
    if not SOIL_CHEM_CSV.exists():
        return [f"Missing soil chem CSV: {SOIL_CHEM_CSV}"]

    df = pd.read_csv(SOIL_CHEM_CSV, nrows=1)
    csv_cols = (set(df.columns)
                - IGNORE_SOIL_CHEM_COLUMNS
                - IGNORE_COMPATIBILITY_COLUMNS
                - IGNORE_SOIL_CHEM_RECOMMENDATION_COLUMNS)
    meta_cols = set(LAB_VARIABLE_METADATA)
    missing = sorted(csv_cols - meta_cols)
    problems: list[str] = []
    if missing:
        problems.append(
            "Missing metadata entries:\n  " + "\n  ".join(missing)
        )
    return problems


def main() -> None:
    checks: dict[str, list[str]] = {
        "Required fields": check_required_fields(),
        "Duplicate aliases": check_duplicate_aliases(),
        "Soil bio CSV coverage": check_soil_bio_coverage(),
        "NIR CSV coverage": check_nir_coverage(),
        "Soil chem CSV coverage": check_soil_chem_coverage(),
    }

    any_problems = False

    for label, problems in checks.items():
        print(f"\n--- {label} ---")
        if problems:
            any_problems = True
            for problem in problems:
                print(f"❌ {problem}")
        else:
            print("✅ OK")

    if any_problems:
        raise SystemExit(1)

    print("\n✅ Lab variable metadata validation passed.")


if __name__ == "__main__":
    main()