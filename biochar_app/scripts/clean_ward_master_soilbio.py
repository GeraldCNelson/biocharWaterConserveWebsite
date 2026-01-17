# biochar_app/scripts/clean_ward_master_soilbio.py
from __future__ import annotations

from pathlib import Path
import json
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ============================================================
# Paths
# ============================================================
HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]

IN_MASTER_XLSX = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-raw"
    / "lab-tests"
    / "soil-tests-bio"
    / "csv-files"
    / "Lobato PLFA Data - Compiled.xlsx"
)

OUT_CLEAN_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "soil-tests-bio"
    / "csv-files"
    / "ward_master_soilBio_clean.csv"
)

OUT_HEADERS_JSON = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "soil-tests-bio"
    / "csv-files"
    / "ward_master_soilBio_headers_machine_to_human.json"
)

# ============================================================
# Cleaning config (same admin drop list idea as NIR)
# ============================================================
DROP_COLUMNS = {
    "customer",
    "customer_no",
    "cust_id",
    "first_name",
    "last_name",
    "company",
    "name",
    "address_1",
    "address_2",
    "city",
    "state",
    "st",
    "zip",
    "lab_no",
    "date_reported",
    "date_rept",
    "results_for",
    "description",
    "kind_of_sample",
    "feed_description",
    "feeder",
    # We normalize sample_id -> strip
    "sample_id",
    "sample_id_1",
    "sample_id_2",
    # You told me these were admin-only in the earlier cleanup
    "cust_id",
}

FIXED_BEGIN_DEPTH_IN = 0
FIXED_END_DEPTH_IN = 8

# ============================================================
# Helpers
# ============================================================
def parse_to_iso_date(x) -> str | None:
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
    Normalize to: strip_1..strip_4
    Handles: S1, S1BIO, STRIP 1, Strip1, etc.
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

def build_machine_headers_from_row(df_raw: pd.DataFrame, header_row_idx: int = 1) -> tuple[list[str], dict[str, str]]:
    """
    If the workbook has 2 header rows like Ward exports:
      row 0 = human
      row 1 = machine
    return (machine_headers, machine->human map)

    If it only has one header row, we’ll treat that as machine and make identity map.
    """
    # If read_excel with header=None, the first rows are in the dataframe.
    if df_raw.shape[0] >= 2:
        human = [str(x).strip() for x in df_raw.iloc[0].tolist()]
        machine = [str(x).strip() for x in df_raw.iloc[1].tolist()]
        header_map = {m: h for m, h in zip(machine, human) if m}
        return machine, header_map

    # fallback
    cols = [str(c).strip() for c in df_raw.columns]
    return cols, {c: c for c in cols}

def read_excel_one_header(path: Path, sheet_name: str | int | None = 0) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    For Lobato PLFA / soil bio: single header row (human names only).
    Returns:
      df: DataFrame with those headers
      header_map: identity map {col: col}
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Master XLSX not found: {path}")

    df = pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    df = df.dropna(axis=1, how="all")  # drop totally empty columns
    df = df.fillna("")

    header_map = {str(c).strip(): str(c).strip() for c in df.columns}
    df.columns = [str(c).strip() for c in df.columns]

    return df, header_map

def find_first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    # case-insensitive exact
    low_map = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        k = str(c).strip().lower()
        if k in low_map:
            return low_map[k]
    return None

# ============================================================
# Main
# ============================================================
def clean_ward_master_soilbio() -> None:
    logger.info(f"📥 Reading Soil Bio master: {IN_MASTER_XLSX}")
    df, header_map = read_excel_one_header(IN_MASTER_XLSX)
    logger.info(f"🧠 Loaded {df.shape[0]} rows × {df.shape[1]} columns")

    # write headers json
    OUT_HEADERS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_HEADERS_JSON.open("w", encoding="utf-8") as f:
        json.dump(header_map, f, indent=2, ensure_ascii=False)
    logger.info(f"💾 Wrote header map: {OUT_HEADERS_JSON}")

    # Required columns (be flexible)
    date_col = find_first_existing(df, ["date_rec", "date_recd", "date_received", "Date Recd", "Date Received"])
    sid_col = find_first_existing(df, ["sample_id", "Sample ID 1", "Sample ID", "SampleID"])

    if not date_col:
        raise ValueError("Could not find a date received column (e.g., date_rec/date_recd/Date Recd).")
    if not sid_col:
        raise ValueError("Could not find a sample id column (e.g., sample_id/Sample ID).")

    df["soil_date"] = df[date_col].apply(parse_to_iso_date)
    df["strip"] = df[sid_col].apply(normalize_strip)

    # fixed depths
    df["begin_depth_in"] = FIXED_BEGIN_DEPTH_IN
    df["end_depth_in"] = FIXED_END_DEPTH_IN

    # drop admin columns
    drop_existing = [c for c in DROP_COLUMNS if c in df.columns]
    df = df.drop(columns=drop_existing)

    # drop rows missing essentials
    df = df[df["strip"].notna()]
    df = df[df["soil_date"].notna()]

    # keys first
    key_cols = ["strip", "soil_date", "begin_depth_in", "end_depth_in"]
    other_cols = [c for c in df.columns if c not in key_cols]
    df = df[key_cols + other_cols]

    OUT_CLEAN_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CLEAN_CSV, index=False)
    logger.info(f"💾 Wrote cleaned Soil Bio master: {OUT_CLEAN_CSV}")
    logger.info("✅ Done.")

if __name__ == "__main__":
    clean_ward_master_soilbio()