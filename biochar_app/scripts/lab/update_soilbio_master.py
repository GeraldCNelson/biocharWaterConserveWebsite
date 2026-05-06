#!/usr/bin/env python3
"""
update_soilbio_master.py

Inspect and append a Ward biological sampling CSV into the
canonical soil biology master dataset.

This script:

1. Loads the canonical master soil bio CSV
2. Loads a raw Ward biological CSV event file
3. Converts its column names into the canonical schema
4. Reports unmatched or missing columns
5. Optionally appends the converted rows to the master

The canonical schema is defined by the existing master CSV.

CLI Examples
------------

Dry-run inspection (recommended first):

    python -m biochar_app.scripts.update_soilbio_master \
        --input Biological_2024-03-20.csv

or explicitly with path:

    python -m biochar_app.scripts.update_soilbio_master \
        --input data-raw/lab-tests/soil-tests-bio/csv-files/Biological_2024-03-20.csv


Write an inspection report JSON:

    python -m biochar_app.scripts.update_soilbio_master \
        --input Biological_2024-03-20.csv \
        --report


Append the sampling event to the master soil bio dataset:

    python -m biochar_app.scripts.update_soilbio_master \
        --input Biological_2024-03-20.csv \
        --append \
        --report


Typical workflow
----------------

1) Inspect the new sampling CSV

2) Confirm the column mapping is correct

3) Append the rows to the master dataset

4) Commit the updated master CSV
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from biochar_app.config.paths import (
    SOIL_BIO_RAW_DIR,
    WARD_MASTER_SOILBIO_CSV,
)


# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

def _norm_text(x: Any) -> str:
    return str(x or "").strip()


def _snake(s: str) -> str:
    """Convert header label → snake_case."""
    s = _norm_text(s)

    s = s.replace("%", " pct ")
    s = s.replace("(%)", " pct ")
    s = s.replace("(ng/g)", " ng_per_g ")
    s = s.replace(":", " ")
    s = s.replace("/", " ")
    s = s.replace("-", " ")
    s = s.replace("(", " ")
    s = s.replace(")", " ")

    s = re.sub(r"[^A-Za-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_").lower()

    return s


def _normalize_strip(x: Any) -> Optional[str]:

    s = _norm_text(x).upper()

    if not s:
        return None

    compact = re.sub(r"[\s_\-]", "", s)

    mapping = {
        "S1": "STRIP 1",
        "S2": "STRIP 2",
        "S3": "STRIP 3",
        "S4": "STRIP 4",
        "STRIP1": "STRIP 1",
        "STRIP2": "STRIP 2",
        "STRIP3": "STRIP 3",
        "STRIP4": "STRIP 4",
    }

    return mapping.get(compact, s)


def _parse_date(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def _to_numeric_if_possible(series: pd.Series) -> pd.Series:
    out = pd.to_numeric(series, errors="coerce")

    if out.notna().sum() == 0:
        return series

    return out


# ---------------------------------------------------------------------
# Column mapping
# ---------------------------------------------------------------------

RAW_TO_CANONICAL = {
    "sample_id": "strip",
    "date_received": "date_rec",
    "date_reported": "date_rept",

    "total_living_microbial_biomass_ng_per_g": "total_biomass",
    "functional_group_diversity_index": "diversity_index",

    "total_bacteria_ng_per_g": "total_bacteria_biomass",
    "total_bacteria_pct": "bacteria_pct",

    "gram_plus_ng_per_g": "gram_pos_biomass",
    "gram_plus_pct": "gram_pos_pct",

    "actinomycetes_ng_per_g": "actinomycetes_biomass",
    "actinomycetes_pct": "actinomycetes_pct",

    "gram_ng_per_g": "gram_biomass",
    "gram_pct": "gram_pct",

    "rhizobia_ng_per_g": "rhizobia_biomass",
    "rhizobia_pct": "rhizobia_pct",

    "total_fungi_ng_per_g": "total_fungi_biomass",
    "total_fungi_pct": "total_fungi_pct",

    "arbuscular_mycorrhizal_ng_per_g": "arbuscular_mycorrhizal_biomass",
    "arbuscular_mycorrhizal_pct": "arbuscular_mycorrhizal_pct",

    "saprophytes_ng_per_g": "saprophytes_biomass",
    "saprophytes_pct": "saprophytic_pct",

    "protozoa_ng_per_g": "protozoa_biomass",
    "protozoa_pct": "protozoan_pct",

    "undifferentiated_ng_per_g": "undifferentiated_biomass",
    "undifferentiated_pct": "undifferentiated_pct",

    "fungi_bacteria": "fungi_bacteria",
    "predator_prey": "predator_prey",
    "gram_gram": "gram_pos_gram_neg_ratio",

    # These next ones depend on how your cleaner wants to store them.
    # Since the master has both components and ratios, map only to the ratio columns
    # that already exist in the master.
    "saturated_unsaturated": "saturated_unsaturated_ratio",
    "monounsaturated_polyunsaturated": "monounsaturated_polyunsaturated_ratio",
    "pre16_1w7c_17_0cyclo": "pre_16_1w7c_cy17_0",
    "pre18_1w7c_19_0cyclo": "pre_18_1w7c_cy19_0",
}


DROP_COLUMNS = {

    "account_id",
    "address",
    "city",
    "st",
    "zip",
    "lab_id",
    "report_type",
}


# ---------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------

def convert_raw_to_master_schema(raw_df: pd.DataFrame, master_df: pd.DataFrame):

    raw_df = raw_df.copy()

    original_cols = list(raw_df.columns)

    rename_norm = {c: _snake(c) for c in raw_df.columns}

    raw_df = raw_df.rename(columns=rename_norm)

    raw_df = raw_df.drop(
        columns=[c for c in raw_df.columns if c in DROP_COLUMNS],
        errors="ignore"
    )

    rename_map = {}
    unmatched = []

    for col in raw_df.columns:

        if col in RAW_TO_CANONICAL:
            rename_map[col] = RAW_TO_CANONICAL[col]

        elif col in ("sample_id", "date_rec", "date_rept"):
            rename_map[col] = col

        else:
            unmatched.append(col)

    raw_df = raw_df.rename(columns=rename_map)

    if "sample_id" in raw_df.columns:

        raw_df["sample_id"] = raw_df["sample_id"].apply(_normalize_strip)
        raw_df["strip"] = raw_df["sample_id"]

    if "date_rec" in raw_df.columns:
        raw_df["date_rec"] = _parse_date(raw_df["date_rec"])

    if "date_rept" in raw_df.columns:
        raw_df["date_rept"] = _parse_date(raw_df["date_rept"])

    for c in raw_df.columns:
        raw_df[c] = _to_numeric_if_possible(raw_df[c])

    master_cols = list(master_df.columns)

    for c in master_cols:
        if c not in raw_df.columns:
            raw_df[c] = pd.NA

    converted = raw_df[master_cols]

    report = {
        "original_columns": original_cols,
        "normalized_columns": list(rename_norm.values()),
        "rename_map": rename_map,
        "unmatched_columns": unmatched,
        "rows": len(converted),
    }

    return converted, report


# ---------------------------------------------------------------------
# Append
# ---------------------------------------------------------------------

def append_to_master(master_df: pd.DataFrame, new_rows: pd.DataFrame):

    combined = pd.concat([master_df, new_rows], ignore_index=True)

    dedupe_cols = [c for c in ("sample_id", "date_rec") if c in combined.columns]

    if dedupe_cols:
        combined = combined.drop_duplicates(subset=dedupe_cols)

    return combined


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Raw biological CSV filename or path"
    )

    parser.add_argument(
        "--append",
        action="store_true",
        help="Append converted rows to the master file"
    )

    parser.add_argument(
        "--report",
        action="store_true",
        help="Write JSON inspection report"
    )

    args = parser.parse_args()

    input_path = Path(args.input).expanduser()

    candidate_paths = []

    # user-provided path
    candidate_paths.append(input_path)

    # raw soil bio directory
    candidate_paths.append(SOIL_BIO_RAW_DIR / args.input)

    # resolve the first one that exists
    resolved_path = None
    for p in candidate_paths:
        if p.exists():
            resolved_path = p.resolve()
            break

    if resolved_path is None:

        print("\nERROR: Raw soil bio CSV could not be located.\n")

        print("Paths checked:")
        for p in candidate_paths:
            print(f"   {p.resolve()}")

        print("\nExpected raw soil bio directory:")
        print(f"   {SOIL_BIO_RAW_DIR.resolve()}")

        raise FileNotFoundError(
            f"\nInput CSV not found: {args.input}"
        )

    input_path = resolved_path
    print(f"\nUsing input CSV: {input_path}")

    master_df = pd.read_csv(WARD_MASTER_SOILBIO_CSV, dtype=object)
    raw_df = pd.read_csv(input_path, dtype=object)

    converted, report = convert_raw_to_master_schema(raw_df, master_df)

    print("\nInspection Report")
    print("------------------")
    print("Rows in raw file:", len(raw_df))
    print("Rows after conversion:", len(converted))
    print("Unmatched columns:", report["unmatched_columns"])

    if args.report:
        report_path = input_path.with_suffix(".inspection.json")
        report_path.write_text(json.dumps(report, indent=2))
        print("Report written to", report_path)

    if args.append:

        new_master = append_to_master(master_df, converted)

        out = WARD_MASTER_SOILBIO_CSV.parent / (
            WARD_MASTER_SOILBIO_CSV.stem + "_updated.csv"
        )

        new_master.to_csv(out, index=False)

        print("Updated master written to:", out)


if __name__ == "__main__":
    main()