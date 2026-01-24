#!/usr/bin/env python3
"""
clean_ward_master_soilbio.py

Cleans the compiled Ward / Lobato PLFA (Soil Biological Health) *CSV* into a
machine-readable CSV with:

- snake_case column names (machine names)
- a machine->human header map JSON
- strip normalized to strip_1..strip_4 (derived from Sample ID 1/2)
- date_rec / date_rept normalized to YYYY-MM-DD (date_rec required)
- fixed depth interval enforced: 0–8 inches (begin_depth_in/end_depth_in)
- admin columns dropped (cust/name/address/etc + sample_id_* columns after strip derived)

IMPORTANT:
- This script does NOT round or recompute any lab values.
- It does NOT convert lab values to numeric at all (preserves CSV text values as-is).

Outputs:
  ward_master_soilbio_clean.csv
  ward_master_soilbio_headers_machine_to_human.json
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ============================================================
# Paths
# ============================================================
HERE = Path(__file__).resolve().parent
# scripts/ -> biochar_app/ -> PROJECT_ROOT
PROJECT_ROOT = HERE.parents[1]

IN_MASTER_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-raw"
    / "lab-tests"
    / "soil-tests-bio"
    / "csv-files"
    / "Lobato PLFA Data - Compiled.csv"
)

OUT_CLEAN_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "soil-tests-bio"
    / "csv-files"
 #   / "ward_master_soilbio_clean.csv"
    / "ward_master_soilbio_clean_plus_Biological_2025-11-03_v2.csv"
)

OUT_HEADERS_JSON = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "soil-tests-bio"
    / "csv-files"
    / "ward_master_soilbio_headers_machine_to_human.json"
)

# ============================================================
# Config
# ============================================================
FIXED_BEGIN_DEPTH_IN = 0
FIXED_END_DEPTH_IN = 8

# After snake-casing, these are the admin columns we want to drop.
# (We always keep: strip, date_rec, date_rept, begin_depth_in, end_depth_in)
ADMIN_DROP_COLS = {
    "cust_id",
    "customer_no",
    "cust_no",
    "name",
    "company",
    "address_1",
    "address_2",
    "city",
    "st",
    "state",
    "zip",
    "lab_no",
    "results_for",
    # sample_id_* are used to derive strip; drop after strip is created
    "sample_id",
    "sample_id_1",
    "sample_id_2",
    # depth columns from the sheet (we enforce fixed depth)
    "beginning_depth",
    "ending_depth",
    "b_depth",
    "e_depth",
}

# ============================================================
# Helpers
# ============================================================

def to_snake(name: Any) -> str:
    """
    Convert human column names to snake_case in a predictable way.

    Examples:
      "Date Recd" -> "date_recd"
      "Bacteria %" -> "bacteria_pct"
      "Fungi:Bacteria" -> "fungi_bacteria"
      "Gram (+) Biomass" -> "gram_pos_biomass"
      "Pre 16:1 w7c" -> "pre_16_1_w7c"
    """
    s = "" if name is None else str(name).strip()

    # normalize a few common symbols/phrases first
    s = s.replace("%", " pct ")
    s = s.replace(":", " ")
    s = s.replace("/", " ")
    s = s.replace("-", " ")
    s = s.replace("–", " ")
    s = s.replace("—", " ")

    # plus/minus conventions (keep “pos/neg” stable)
    s = s.replace("(+)", " pos ")
    s = s.replace("(-)", " neg ")
    s = s.replace("+", " pos ")
    s = s.replace("−", " neg ")  # unicode minus

    # remove parentheses and stray punctuation
    s = re.sub(r"[()\[\]{},]", " ", s)
    s = re.sub(r"[^0-9A-Za-z\s_]", " ", s)

    # collapse whitespace and underscores
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = s.replace(" ", "_")
    s = re.sub(r"_+", "_", s).strip("_")

    return s


def parse_to_iso_date(x: Any) -> Optional[str]:
    """Parse a date-like value into YYYY-MM-DD, or None if it can't be parsed."""
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def normalize_strip(sample_id: Any) -> Optional[str]:
    """
    Normalize to: strip_1..strip_4

    Handles:
      - "STRIP 1", "Strip1", "strip_1"
      - "N1 STRIP 1", "N1 & N2  STRIP 1", etc
      - "S1", "S1BIO", etc
    """
    if sample_id is None:
        return None
    s = str(sample_id).strip().upper()
    if not s:
        return None

    s_compact = re.sub(r"[\s_\-]", "", s)

    m = re.search(r"STRIP([1-4])", s_compact)
    if m:
        return f"strip_{m.group(1)}"

    # allow "S1" or embedded "S1XXX"
    m = re.search(r"S([1-4])", s_compact)
    if m:
        return f"strip_{m.group(1)}"

    return None


def find_first_existing(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Find the first candidate name present in df.columns (case-insensitive)."""
    for c in candidates:
        if c in df.columns:
            return c
    low_map = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        k = str(c).strip().lower()
        if k in low_map:
            return low_map[k]
    return None


def read_csv_one_header_snake(path: Path) -> tuple[pd.DataFrame, Dict[str, str]]:
    """
    Read the Soil Bio CSV:
      - keep everything as strings (no numeric coercion)
      - snake_case the headers
      - return df with machine headers + machine->human map
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Master CSV not found: {path}")

    df_human = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    df_human = df_human.dropna(axis=1, how="all")

    human_cols = [str(c).strip() for c in df_human.columns]
    machine_cols = [to_snake(c) for c in human_cols]

    # ensure uniqueness
    seen: Dict[str, int] = {}
    unique_machine_cols: list[str] = []
    for mc in machine_cols:
        if mc not in seen:
            seen[mc] = 1
            unique_machine_cols.append(mc)
        else:
            seen[mc] += 1
            unique_machine_cols.append(f"{mc}_{seen[mc]}")

    header_map = {m: h for m, h in zip(unique_machine_cols, human_cols)}
    df_human.columns = unique_machine_cols
    return df_human, header_map


def derive_strip_from_possible_columns(df: pd.DataFrame) -> pd.Series:
    """
    Prefer Sample ID 2 if it looks like "STRIP X" and Sample ID 1 is something like "N1 & N2".
    Strategy:
      1) try sample_id_1
      2) if missing/None, try sample_id_2
      3) if still missing, try sample_id
    """
    sid1 = find_first_existing(df, ["sample_id_1"])
    sid2 = find_first_existing(df, ["sample_id_2"])
    sid0 = find_first_existing(df, ["sample_id"])

    out = pd.Series([None] * len(df), index=df.index, dtype=object)

    def apply_if_available(colname: Optional[str]) -> None:
        nonlocal out
        if not colname:
            return
        vals = df[colname].apply(normalize_strip)
        out = out.where(out.notna(), vals)

    apply_if_available(sid1)
    apply_if_available(sid2)
    apply_if_available(sid0)

    return out


# ============================================================
# Main
# ============================================================

def clean_ward_master_soilbio() -> None:
    logger.info(f"📥 Reading Soil Bio master CSV: {IN_MASTER_CSV}")
    df, header_map = read_csv_one_header_snake(IN_MASTER_CSV)
    logger.info(f"🧠 Loaded {df.shape[0]} rows × {df.shape[1]} columns")

    # Write machine->human header map
    OUT_HEADERS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_HEADERS_JSON.open("w", encoding="utf-8") as f:
        json.dump(header_map, f, indent=2, ensure_ascii=False)
    logger.info(f"💾 Wrote header map: {OUT_HEADERS_JSON}")

    # Identify key columns (after snake_case)
    date_rec_col = find_first_existing(df, ["date_rec", "date_recd", "date_received"])
    date_rept_col = find_first_existing(df, ["date_rept", "date_reported"])

    if not date_rec_col:
        raise ValueError("Could not find a date received column (expected date_recd / date_rec).")

    # Normalize dates (ISO strings)
    df["date_rec"] = df[date_rec_col].apply(parse_to_iso_date)
    if date_rept_col:
        df["date_rept"] = df[date_rept_col].apply(parse_to_iso_date)
    else:
        df["date_rept"] = ""

    # Derive strip
    df["strip"] = derive_strip_from_possible_columns(df)

    # Fixed depth interval
    df["begin_depth_in"] = str(FIXED_BEGIN_DEPTH_IN)
    df["end_depth_in"] = str(FIXED_END_DEPTH_IN)

    # Drop admin columns (snake-cased)
    keep_cols = {"strip", "date_rec", "date_rept", "begin_depth_in", "end_depth_in"}
    drop_existing = [c for c in ADMIN_DROP_COLS if c in df.columns and c not in keep_cols]
    df = df.drop(columns=drop_existing)

    # Drop rows missing essentials
    df["strip"] = df["strip"].fillna("")
    df["date_rec"] = df["date_rec"].fillna("")
    df = df[(df["strip"].astype(str).str.len() > 0) & (df["date_rec"].astype(str).str.len() > 0)].copy()

    # Reorder columns: keys first
    key_cols = ["strip", "date_rec", "date_rept", "begin_depth_in", "end_depth_in"]
    other_cols = [c for c in df.columns if c not in key_cols]
    df = df[key_cols + other_cols]

    OUT_CLEAN_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CLEAN_CSV, index=False)
    logger.info(f"💾 Wrote cleaned Soil Bio master: {OUT_CLEAN_CSV}")
    logger.info("✅ Done.")


if __name__ == "__main__":
    clean_ward_master_soilbio()