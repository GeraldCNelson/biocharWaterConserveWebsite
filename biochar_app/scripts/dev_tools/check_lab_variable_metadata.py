#!/usr/bin/env python3
"""
Validate lab variable metadata coverage and basic structure.

Run:
python biochar_app/scripts/dev_tools/check_lab_variable_metadata.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from biochar_app.config.lab_variable_metadata import LAB_VARIABLE_METADATA


SOIL_BIO_CSV = Path(
    "biochar_app/data-processed/lab-tests/soil-tests-bio/csv-files/"
    "ward_master_soilbio_clean.csv"
)

IGNORE_COLUMNS = {
    "strip",
    "date_rec",
    "date_rept",
    "begin_depth_in",
    "end_depth_in",
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
    seen: dict[str, str] = {}

    for key, meta in LAB_VARIABLE_METADATA.items():
        for alias in meta.get("aliases", []):
            alias_norm = str(alias).strip().lower()
            if not alias_norm:
                continue

            if alias_norm in seen:
                problems.append(
                    f"alias '{alias}' appears in both {seen[alias_norm]} and {key}"
                )
            else:
                seen[alias_norm] = key

    return problems


def check_soil_bio_coverage() -> list[str]:
    if not SOIL_BIO_CSV.exists():
        return [f"Missing soil bio CSV: {SOIL_BIO_CSV}"]

    df = pd.read_csv(SOIL_BIO_CSV, nrows=1)

    csv_cols = set(df.columns) - IGNORE_COLUMNS
    meta_cols = set(LAB_VARIABLE_METADATA)

    missing = sorted(csv_cols - meta_cols)
    extra = sorted(meta_cols - csv_cols)

    problems: list[str] = []

    if missing:
        problems.append("Missing metadata entries:\n  " + "\n  ".join(missing))

    if extra:
        problems.append(
            "Metadata entries not in soil bio CSV:\n  " + "\n  ".join(extra)
        )

    return problems


def main() -> None:
    checks: dict[str, list[str]] = {
        "Required fields": check_required_fields(),
        "Duplicate aliases": check_duplicate_aliases(),
        "Soil bio CSV coverage": check_soil_bio_coverage(),
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