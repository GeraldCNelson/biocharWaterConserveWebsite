# biochar_app/scripts/clean_ward_master_nir.py
from __future__ import annotations

from pathlib import Path
import json
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ============================================================
# Paths (restored)
# ============================================================
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]

# ------------------------------------------------------------
# INPUT: your revised master (2 header rows: human, machine)
# ------------------------------------------------------------
IN_MASTER_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-raw"
    / "lab-tests"
    / "hay-tests"
    / "csv-files"
    / "Master 1.15.2026 Revision.csv"
)

# ------------------------------------------------------------
# OUTPUTS
# ------------------------------------------------------------
OUT_CLEAN_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "hay-tests"
    / "csv-files"
    / "ward_master_nir_clean.csv"
)

OUT_HEADERS_JSON = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "hay-tests"
    / "csv-files"
    / "ward_master_nir_headers_machine_to_human.json"
)

# ============================================================
# Cleaning config
# ============================================================

# Columns to DROP (machine-readable names)
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
    # We replace sample_id with normalized strip, so drop it.
    "sample_id",
}

# ============================================================
# Helpers
# ============================================================

def parse_to_iso_date(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None

    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def normalize_strip(sample_id: str | None) -> str | None:
    """
    Normalize Ward sample IDs to:
      strip_1, strip_2, strip_3, strip_4

    Handles:
      S1HAY, S1, STRIP 1, Strip1, strip_4, etc.
    """
    if sample_id is None:
        return None

    s = str(sample_id).upper()
    s = re.sub(r"[\s_\-]", "", s)

    m = re.search(r"STRIP([1-4])", s)
    if m:
        return f"strip_{m.group(1)}"

    m = re.search(r"S([1-4])", s)
    if m:
        return f"strip_{m.group(1)}"

    return None


def read_ward_master_two_header_rows(path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Ward master format:
      row 0: human-readable headers
      row 1: machine-readable headers
      row 2+: data

    Returns:
      df: data with machine headers as columns
      header_map: machine -> human dict
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Master CSV not found: {path}")

    raw = pd.read_csv(
        path,
        header=None,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )

    if raw.shape[0] < 3:
        raise ValueError(f"Expected ≥3 rows (human header, machine header, data). Got {raw.shape[0]}.")

    human = [str(x).strip() for x in raw.iloc[0].tolist()]
    machine = [str(x).strip() for x in raw.iloc[1].tolist()]

    # Build machine->human map (skip blanks)
    header_map: dict[str, str] = {}
    for m, h in zip(machine, human):
        if m and m.strip():
            header_map[m] = h

    df = raw.iloc[2:].copy()
    df.columns = machine

    # Drop completely empty trailing columns (sometimes appear)
    df = df.dropna(axis=1, how="all")

    return df, header_map


# ============================================================
# Main cleaning routine
# ============================================================

def clean_ward_master_nir() -> None:
    logger.info(f"📥 Reading Ward master CSV: {IN_MASTER_CSV}")
    df, header_map = read_ward_master_two_header_rows(IN_MASTER_CSV)
    logger.info(f"🧠 Loaded {df.shape[0]} rows × {df.shape[1]} columns")

    # Write header map JSON
    OUT_HEADERS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_HEADERS_JSON.open("w", encoding="utf-8") as f:
        json.dump(header_map, f, indent=2, ensure_ascii=False)
    logger.info(f"💾 Wrote header map: {OUT_HEADERS_JSON}")

    # Require expected machine columns
    if "date_rec" not in df.columns:
        raise ValueError("Expected machine column 'date_rec' not found in master.")
    if "sample_id" not in df.columns:
        raise ValueError("Expected machine column 'sample_id' not found in master.")

    # Normalize date + strip
    df["nir_date"] = df["date_rec"].apply(parse_to_iso_date)
    df["strip"] = df["sample_id"].apply(normalize_strip)

    # Drop unwanted columns
    drop_existing = [c for c in DROP_COLUMNS if c in df.columns]
    df = df.drop(columns=drop_existing)

    # Drop rows without essentials
    df = df[df["strip"].notna()]
    df = df[df["nir_date"].notna()]

    # Put keys first
    key_cols = ["strip", "nir_date"]
    other_cols = [c for c in df.columns if c not in key_cols]
    df = df[key_cols + other_cols]

    # Write cleaned CSV
    OUT_CLEAN_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CLEAN_CSV, index=False)
    logger.info(f"💾 Wrote cleaned NIR master: {OUT_CLEAN_CSV}")
    logger.info("✅ Done.")


if __name__ == "__main__":
    clean_ward_master_nir()