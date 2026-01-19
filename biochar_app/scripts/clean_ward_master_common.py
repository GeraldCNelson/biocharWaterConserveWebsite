#!/usr/bin/env python3
"""
clean_ward_master_common.py

Shared helpers for cleaning Ward/Lobato "master" lab files into a consistent,
machine-readable format for the Biochar dashboard.

Design goals
------------
* Support both:
  - Ward CSV exports that contain TWO header rows (human + machine)
  - Compiled Excel workbooks (single header row)
* Produce:
  - a cleaned CSV with stable machine column names
  - a machine->human header map JSON (for UI labels / debugging)
* Normalize:
  - strip/sample id variants -> "STRIP 1"..."STRIP 4"
  - date fields -> ISO "YYYY-MM-DD"
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Mapping

import json
import re

import pandas as pd


# ---------------------------------------------------------------------
# Admin columns to drop (case-insensitive) across ALL Ward/Lobato masters
# ---------------------------------------------------------------------
ADMIN_DROP_COLS = {
    # Common admin fields
    "customer_no", "cust_no", "cust_id", "customer_id",
    "first_name", "last_name", "name", "company",
    "address_1", "address_2", "city", "st", "state", "zip",
    "lab_no",
    "kind_of_sample", "feed_description", "feeder",
    "cust", "customer",

    # Soil-specific admin-ish fields
    "grower", "field_id",
    "results_for",

    # Depth columns we don’t want when enforcing fixed depth
    "b_depth", "e_depth", "beginning_depth", "ending_depth",
}


# ---------------------------------------------------------------------
# Machine name normalization (Excel headers -> snake_case)
# ---------------------------------------------------------------------
def make_machine_name(name: str) -> str:
    s = str(name).strip().lower()
    s = s.replace("%", "pct")
    # collapse punctuation to underscores
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# ---------------------------------------------------------------------
# Strip normalization
# ---------------------------------------------------------------------
def normalize_strip(value: object) -> Optional[str]:
    """
    Convert common variants to canonical "STRIP 1"..."STRIP 4".
    Handles examples like:
      - "STRIP 1", "Strip1", "strip_1"
      - "S1", "S1HAY", "N1 STRIP 1", etc.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip().upper()
    if not s:
        return None

    # remove separators
    compact = s.replace("-", "").replace("_", "").replace(" ", "")

    # explicit STRIP#
    if "STRIP" in compact:
        for d in ("1", "2", "3", "4"):
            if f"STRIP{d}" in compact:
                return f"STRIP {d}"

    # S#
    for d in ("1", "2", "3", "4"):
        if f"S{d}" in compact:
            return f"STRIP {d}"

    return None


def normalize_strip_column(df: pd.DataFrame, *, strip_col: str = "strip") -> pd.DataFrame:
    """Return a copy with df[strip_col] normalized to 'STRIP N' strings."""
    out = df.copy()
    if strip_col not in out.columns:
        return out
    out[strip_col] = out[strip_col].apply(normalize_strip)
    return out


# ---------------------------------------------------------------------
# Date normalization
# ---------------------------------------------------------------------
def parse_to_iso_date(value: object) -> Optional[str]:
    """Parse a value into an ISO date (YYYY-MM-DD) string, or None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date().isoformat()


def normalize_date_columns(
    df: pd.DataFrame,
    *,
    date_cols: Mapping[str, str],
) -> pd.DataFrame:
    """
    Rename + normalize date columns to ISO 'YYYY-MM-DD'.

    Parameters
    ----------
    date_cols:
        Mapping from *existing* column name -> *desired* column name.
        Columns missing in df are ignored.
    """
    out = df.copy()
    for src, dst in date_cols.items():
        if src not in out.columns:
            continue
        if src != dst and dst not in out.columns:
            out = out.rename(columns={src: dst})
        out[dst] = out[dst].apply(parse_to_iso_date)
    return out


# ---------------------------------------------------------------------
# Depth (fixed interval)
# ---------------------------------------------------------------------
def add_fixed_depth_columns(df: pd.DataFrame, *, begin_in: int, end_in: int) -> pd.DataFrame:
    out = df.copy()
    out["begin_depth_in"] = begin_in
    out["end_depth_in"] = end_in
    return out


# ---------------------------------------------------------------------
# Admin column dropping
# ---------------------------------------------------------------------
def drop_admin_columns(df: pd.DataFrame, *, extra_drop: Iterable[str] | None = None) -> pd.DataFrame:
    """Drop known admin columns (case-insensitive) if present."""
    drop_set = {c.lower() for c in ADMIN_DROP_COLS}
    if extra_drop:
        drop_set |= {c.lower() for c in extra_drop}

    to_drop = [c for c in df.columns if str(c).strip().lower() in drop_set]
    return df.drop(columns=to_drop, errors="ignore")


# ---------------------------------------------------------------------
# Ward CSV readers
# ---------------------------------------------------------------------
def read_ward_two_header_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Ward export format:
      row 0 = human headers
      row 1 = machine headers
      row 2+ = data

    Returns:
      df (machine columns), header_map (machine -> human)
    """
    raw = pd.read_csv(path, header=None, dtype=str, keep_default_na=False, na_filter=False, engine="python")
    if raw.shape[0] < 3:
        raise ValueError(f"Expected >=3 rows (human header, machine header, data). Got {raw.shape[0]}")

    human = [str(x).strip() for x in raw.iloc[0, :].tolist()]
    machine = [str(x).strip() for x in raw.iloc[1, :].tolist()]
    data = raw.iloc[2:, :].copy()

    if len(machine) != data.shape[1]:
        raise ValueError("Machine header width does not match data width.")

    data.columns = machine
    header_map = {m: h for m, h in zip(machine, human)}
    return data, header_map


def read_clean_one_header_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Clean export format:
      single header row (already machine-readable)

    Returns:
      df, header_map where map is identity (machine -> machine)
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False, engine="python")
    header_map = {c: c for c in df.columns}
    return df, header_map


def read_ward_master_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Auto-detect:
      - Try one-header clean CSV first (works if file is already cleaned)
      - If that doesn't look like a cleaned file, fall back to two-header reader
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # Heuristic: if it already has machine columns like 'strip' or 'date_rec',
    # treat it as a clean one-header CSV.
    try:
        df1, hm1 = read_clean_one_header_csv(path)
        cols = set(df1.columns)
        if {"strip", "date_rec"} & cols:
            return df1, hm1
        # if it has lots of obvious machine names, also accept
        if any(c.endswith("_pct_db") for c in cols):
            return df1, hm1
    except Exception:
        pass

    # otherwise read two-header Ward export
    return read_ward_two_header_csv(path)


# ---------------------------------------------------------------------
# Excel workbook helpers (compiled soil chem / soil bio)
# ---------------------------------------------------------------------
def clean_compiled_workbook(
    df: pd.DataFrame,
    *,
    admin_drop_cols: Iterable[str] = ADMIN_DROP_COLS,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Normalize a compiled one-header workbook.

    This is used for the Lobato-compiled soil chemistry/biology Excel files.

    - Drops purely empty columns
    - Converts headers to machine names (snake_case)
    - De-dupes collisions with suffixes (_1, _2, ...)
    - Returns (df_clean, header_map) where header_map maps machine->original
    """
    df = df.copy()
    df = df.dropna(axis=1, how="all")

    orig_cols = list(df.columns)
    new_cols: list[str] = []
    counts: dict[str, int] = {}

    for c in orig_cols:
        m = make_machine_name(c)
        if m in counts:
            counts[m] += 1
            m2 = f"{m}_{counts[m]}"
            new_cols.append(m2)
        else:
            counts[m] = 0
            new_cols.append(m)

    df.columns = new_cols

    header_map = {m: str(h) for m, h in zip(new_cols, orig_cols)}
    df = drop_admin_columns(df, extra_drop=list(admin_drop_cols))
    return df, header_map


# ---------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------
def write_clean_outputs(
    df: pd.DataFrame,
    header_map: dict[str, str],
    *,
    out_csv: Path,
    out_headers_json: Optional[Path] = None,
) -> None:
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_csv, index=False)

    if out_headers_json is not None:
        out_headers_json = Path(out_headers_json)
        out_headers_json.parent.mkdir(parents=True, exist_ok=True)
        with out_headers_json.open("w", encoding="utf-8") as f:
            json.dump(header_map, f, indent=2, ensure_ascii=False)