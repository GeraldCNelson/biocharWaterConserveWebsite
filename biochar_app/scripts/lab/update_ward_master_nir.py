#!/usr/bin/env python3
"""
update_ward_master_nir.py

Clean the compiled Ward NIR / hay master file into a canonical machine-readable
CSV for the Biochar dashboard, and patch missing 2024 mineral values using
supplemental Excel workbooks provided by Ward Labs.

Code to run in a terminal
python -m biochar_app.scripts.update_ward_master_nir

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

Supplement behavior
-------------------
The 2024 mineral updates are read from:
    .../hay-tests/csv-files/NIR_mineral_updates_2024/*.xlsx

For each workbook:
* the "Calculated Minerals" sheet provides Ca/P/K/Mg values
* the "NIR_..." sheet provides Lab No, Sample ID, and Date Recd
* we join Calculated Minerals[Sample name] to NIR sheet[Lab No]
* strip is derived from Sample ID (e.g. S1HAY, S1 HAY)
* nir_date is derived from Date Recd
* only missing mineral values in the cleaned master are filled

Writes
------
* cleaned master CSV
* machine->human headers JSON
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from biochar_app.config.paths import (
    LAB_TESTS_RAW_DIR,
    HAY_TESTS_RAW_DIR,
    HAY_TESTS_PROCESSED_DIR,
)
from biochar_app.config.lab_source_mappings import (
    RAW_TO_CANONICAL_NIR,
    DROP_COLUMNS_NIR,
    EXPECTED_NIR_COLUMNS,
)

from biochar_app.scripts.lab.clean_ward_master_common import (
    make_machine_name,
    read_ward_two_header_csv,
    standardize_ward_dataframe,
    validate_and_report,
    write_clean_outputs,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

IN_MASTER_CSV = LAB_TESTS_RAW_DIR / "hay-tests" / "csv-files" / "Master 1.15.2026 Revision.csv"

SUPPLEMENT_DIR = (
    LAB_TESTS_RAW_DIR
    / "hay-tests"
    / "csv-files"
    / "NIR_mineral_updates_2024"
)

SUPPLEMENT_FILES = [
    SUPPLEMENT_DIR / "Lablynx data 20240606.xlsx",
    SUPPLEMENT_DIR / "Second set of data 2024 Aug and Sept.xlsx",
]

OUT_CLEAN_CSV = HAY_TESTS_PROCESSED_DIR / "ward_master_nir_clean.csv"
OUT_HEADERS_JSON = HAY_TESTS_PROCESSED_DIR / "ward_master_nir_headers_machine_to_human.json"
SUPPLEMENTAL_NIR_FILES: list[Path] = [
    HAY_TESTS_RAW_DIR / "NIR_2026-07-28.csv",
]

MINERAL_COLUMNS = [
    "ca_pct",
    "ca_pct_db",
    "p_pct",
    "p_pct_db",
    "k_pct",
    "k_pct_db",
    "mg_pct",
    "mg_pct_db",
]

HEADER_MAP_ADDITIONS = {
    "ca_pct": "Calcium (As Received, %)",
    "ca_pct_db": "Calcium (Dry Basis, %)",
    "p_pct": "Phosphorus (As Received, %)",
    "p_pct_db": "Phosphorus (Dry Basis, %)",
    "k_pct": "Potassium (As Received, %)",
    "k_pct_db": "Potassium (Dry Basis, %)",
    "mg_pct": "Magnesium (As Received, %)",
    "mg_pct_db": "Magnesium (Dry Basis, %)",
}

def _apply_raw_to_canonical_map(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    applicable = {
        src: dst
        for src, dst in RAW_TO_CANONICAL_NIR.items()
        if src in out.columns
    }

    out = out.rename(columns=applicable)

    return out


def _canonicalize_header_map(header_map: dict[str, str]) -> dict[str, str]:
    """Keep the label map synchronized with source-to-canonical renames."""
    canonical: dict[str, str] = {}
    for source, human_label in header_map.items():
        canonical.setdefault(RAW_TO_CANONICAL_NIR.get(source, source), human_label)
    return canonical


def _read_supplemental_nir_csv(
    path: Path,
    header_map: dict[str, str],
) -> pd.DataFrame:
    """Read one-header Ward NIR data and return canonical cleaned rows."""
    logger.info(f"➕ Reading supplemental Ward NIR CSV: {path}")
    raw = pd.read_csv(path)
    raw.columns = [make_machine_name(column) for column in raw.columns]

    # Reuse the compiled master's human-to-machine vocabulary wherever the
    # Ward export labels match, then cover the two required identity fields.
    reverse_header_map = {
        make_machine_name(human): machine
        for machine, human in header_map.items()
    }
    raw = raw.rename(
        columns={
            column: reverse_header_map[column]
            for column in raw.columns
            if column in reverse_header_map
        }
    )
    raw = raw.rename(
        columns={
            "sample_id_1": "sample_id",
            "date_received": "date_rec",
        }
    )
    raw = _apply_raw_to_canonical_map(raw)

    if "sample_id" not in raw.columns:
        raise ValueError(f"Expected Sample ID 1 column in supplemental NIR file: {path}")

    match = re.fullmatch(r"NIR_(\d{4}-\d{2}-\d{2})\.csv", path.name, flags=re.IGNORECASE)
    if match is None:
        raise ValueError(f"Supplemental NIR filename must contain the sampling date: {path.name}")
    sampling_date = match.group(1)

    cleaned = standardize_ward_dataframe(
        raw,
        strip_source_candidates=("strip", "sample_id"),
        date_cols={"date_rec": "date_rec"},
        below_detection_to_zero=True,
        extra_drop_cols=DROP_COLUMNS_NIR,
        fixed_depth=None,
        numeric_exclude_cols=("strip", "date_rec", "nir_date"),
        add_compatibility_aliases=True,
    )
    cleaned["nir_date"] = sampling_date
    cleaned = cleaned[cleaned["strip"].notna()].copy()
    if len(cleaned) != 4 or cleaned["strip"].nunique() != 4:
        raise ValueError(f"Expected exactly one supplemental NIR row for each strip in {path.name}")
    return cleaned


def _merge_supplemental_nir_rows(
    dataframe: pd.DataFrame,
    header_map: dict[str, str],
) -> pd.DataFrame:
    out = dataframe.copy()
    for path in SUPPLEMENTAL_NIR_FILES:
        if not path.exists():
            logger.warning(f"⚠️ Supplemental NIR file not found, skipping: {path}")
            continue
        supplement = _read_supplemental_nir_csv(path, header_map)
        supplement = _drop_as_received_pct_when_dry_basis_exists(supplement)
        supplement = supplement.reindex(columns=out.columns, fill_value=pd.NA)
        out = pd.concat([out, supplement], ignore_index=True)
        out = out.drop_duplicates(subset=["strip", "nir_date"], keep="last")
    return out.sort_values(["nir_date", "strip"], kind="stable").reset_index(drop=True)

def _rename_nir_date(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "date_rec" in out.columns and "nir_date" not in out.columns:
        out = out.rename(columns={"date_rec": "nir_date"})
    return out

def _canonicalize_strip(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    s = str(value).strip().upper()
    if not s:
        return None

    s = s.replace("_", "").replace("-", "").replace(" ", "")

    match = re.search(r"S([1-4])", s)
    if match:
        return f"strip_{match.group(1)}"

    match = re.search(r"STRIP([1-4])", s)
    if match:
        return f"strip_{match.group(1)}"

    return None

def _find_sheet_name(xls: pd.ExcelFile, patterns: tuple[str, ...]) -> str:
    lower_map = {str(name).lower(): str(name) for name in xls.sheet_names}

    for lower_name, orig_name in lower_map.items():
        if all(p.lower() in lower_name for p in patterns):
            return orig_name

    raise ValueError(
        f"Could not find sheet matching patterns {patterns} in {xls.sheet_names}"
    )

def _read_one_2024_mineral_workbook(path: Path) -> pd.DataFrame:
    logger.info(f"📘 Reading 2024 mineral supplement workbook: {path.name}")

    xls = pd.ExcelFile(path)

    # Examples:
    # - "Minerals Calculated" / "Calculated Minerals"
    # - "NIR_2024-06-03" / "NIR_2024-08-06"
    mineral_sheet = _find_sheet_name(xls, ("calculated", "mineral"))
    nir_sheet = _find_sheet_name(xls, ("nir_",))

    minerals = pd.read_excel(xls, sheet_name=mineral_sheet)
    nir = pd.read_excel(xls, sheet_name=nir_sheet)

    minerals = minerals.rename(
        columns={
            "Sample name": "lab_no",
            "Ca Dry Basis": "ca_pct_db",
            "P Dry Basis": "p_pct_db",
            "K Dry Basis": "k_pct_db",
            "Mg Dry Basis": "mg_pct_db",
            "Ca As received": "ca_pct",
            "P As received": "p_pct",
            "K As received": "k_pct",
            "Mg As received": "mg_pct",
        }
    )

    nir = nir.rename(
        columns={
            "Lab No": "lab_no",
            "Sample ID": "sample_id",
            "Date Recd": "date_rec",
        }
    )

    required_mineral_cols = ["lab_no"] + MINERAL_COLUMNS
    required_nir_cols = ["lab_no", "sample_id", "date_rec"]

    for col in required_mineral_cols:
        if col not in minerals.columns:
            raise ValueError(f"Expected column '{col}' not found in {path.name}:{mineral_sheet}")

    for col in required_nir_cols:
        if col not in nir.columns:
            raise ValueError(f"Expected column '{col}' not found in {path.name}:{nir_sheet}")

    merged = minerals[required_mineral_cols].merge(
        nir[required_nir_cols],
        on="lab_no",
        how="left",
        validate="one_to_one",
    )

    merged["strip"] = merged["sample_id"].map(_canonicalize_strip)
    merged["nir_date"] = pd.to_datetime(merged["date_rec"], errors="coerce").dt.strftime("%Y-%m-%d")

    keep_cols = ["strip", "nir_date"] + MINERAL_COLUMNS
    merged = merged[keep_cols].copy()

    merged = merged[merged["strip"].notna() & merged["nir_date"].notna()].copy()

    logger.info(
        f"   ↳ Parsed {merged.shape[0]} supplement rows from {path.name}"
    )

    return merged

def _drop_as_received_pct_when_dry_basis_exists(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop as-received/wet-basis percentage columns when a dry-basis equivalent exists.

    Example:
    - keep ndf_pct_db
    - drop ndf_pct

    Always keep moisture/dry-matter columns because they describe sample water content.
    """
    keep_always = {
        "moisture_pct",
        "dry_matter_pct",
        "moisture_pct_db",
        "dry_matter_pct_db",
    }

    drop_cols: list[str] = []

    for col in df.columns:
        if col in keep_always:
            continue

        # Handle both patterns:
        # - ndf_pct → ndf_pct_db
        # - ndfd48_pctndf → ndfd48_pctndf_db

        if not col.endswith("_db"):
            db_col = f"{col}_db"
            if db_col in df.columns:
                drop_cols.append(col)

    if drop_cols:
        logger.info("🧹 Dropping as-received columns with dry-basis equivalents:")
        for col in drop_cols:
            logger.info(f"   {col}")

    return df.drop(columns=drop_cols, errors="ignore")

def _read_2024_mineral_supplements() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for path in SUPPLEMENT_FILES:
        if not path.exists():
            logger.warning(f"⚠️ Supplement workbook not found, skipping: {path}")
            continue
        frames.append(_read_one_2024_mineral_workbook(path))

    if not frames:
        logger.warning("⚠️ No 2024 mineral supplement workbooks found.")
        return pd.DataFrame(columns=["strip", "nir_date"] + MINERAL_COLUMNS)

    supp = pd.concat(frames, ignore_index=True)

    # If duplicate strip/date rows ever appear, keep the last one read.
    supp = supp.drop_duplicates(subset=["strip", "nir_date"], keep="last").copy()

    logger.info(f"🧪 Combined supplement rows: {supp.shape[0]}")
    return supp

def _patch_missing_minerals(
    df: pd.DataFrame,
    supplement_df: pd.DataFrame,
) -> pd.DataFrame:
    if supplement_df.empty:
        logger.info("ℹ️ No supplement mineral rows available; nothing to patch.")
        return df

    out = df.copy()

    out = out.set_index(["strip", "nir_date"])
    supp = supplement_df.set_index(["strip", "nir_date"])

    patched_counts: dict[str, int] = {}

    for col in MINERAL_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

        before_missing = out[col].isna().sum()
        out[col] = out[col].combine_first(supp[col])
        after_missing = out[col].isna().sum()
        patched_counts[col] = int(before_missing - after_missing)

    out = out.reset_index()

    logger.info("🩹 Patched missing mineral values from 2024 supplements:")
    for col, n in patched_counts.items():
        logger.info(f"   {col}: filled {n}")

    return out

def update_ward_master_nir() -> None:
    logger.info(f"📥 Reading Ward NIR master CSV: {IN_MASTER_CSV}")
    df, header_map = read_ward_two_header_csv(IN_MASTER_CSV)
    header_map = _canonicalize_header_map(header_map)
    df = _apply_raw_to_canonical_map(df)

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
        extra_drop_cols=DROP_COLUMNS_NIR,
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

    # Patch missing 2024 mineral values
    supplement_df = _read_2024_mineral_supplements()
    df = _patch_missing_minerals(df, supplement_df)

    # Prefer dry-basis forage/NIR values when both as-received and dry-basis exist
    df = _drop_as_received_pct_when_dry_basis_exists(df)

    # Append one-header Ward exports that have not yet reached the compiled master.
    df = _merge_supplemental_nir_rows(df, header_map)

    # Put keys first
    key_cols = [c for c in ["strip", "nir_date"] if c in df.columns]
    other_cols = [c for c in df.columns if c not in key_cols]
    df = df[key_cols + other_cols]

    # Ensure header map contains the supplemented columns
    for k, v in HEADER_MAP_ADDITIONS.items():
        header_map.setdefault(k, v)

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
    # TODO(delete after ETL adoption): remove this standalone entry point once
    # operations have used biochar_app/scripts/etl.py for a full update cycle.
    update_ward_master_nir()
