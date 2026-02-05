#!/usr/bin/env python3
"""
tables_soil_common.py

Shared helpers for rendering Soil Chemistry / Soil Biology cleaned master CSVs into
the "standard table payload" used by the dashboard tabs.

Key behaviors (based on your latest requirements):
- STRICT: cleaned masters must contain a 'date_rec' column. If not, raise and stop.
- Preserve text cell values (e.g., "ALL PREY") instead of coercing everything to numeric.
- Numeric aggregation:
    - if all non-empty values in a (strip, date_rec) group are identical -> keep that value
    - else if all values are numeric -> mean
    - else -> None
- Ratio rows (S1/S2, S3/S4): compute only when both sides are numeric and denom != 0.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from biochar_app.scripts.config import ( WARD_MASTER_NIR_CSV,
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_SOILCHEM_CSV,
    BIOMASS_FIELD_CSV,)


# -----------------------------------------------------------------------------
# Small utilities
# -----------------------------------------------------------------------------
def _norm(s: Any) -> str:
    return str(s or "").strip()


def _keyify(s: str) -> str:
    """Make a stable key from a label."""
    s = _norm(s).lower()
    out = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    return "".join(out).strip("_")


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    if isinstance(v, str) and _norm(v) == "":
        return True
    return False


def _to_float(v: Any) -> Optional[float]:
    """Convert to float if possible; otherwise None."""
    if _is_missing(v):
        return None
    if isinstance(v, (int, float)) and pd.notna(v):
        try:
            return float(v)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None


def _is_numeric_value(v: Any) -> bool:
    return _to_float(v) is not None


def _normalize_strip(x: Any) -> str:
    """
    Normalize strip identifiers to STRIP 1..4 when possible.
    Assumes cleaned master already uses "strip_1" etc sometimes.
    """
    s = _norm(x).lower().replace(" ", "").replace("-", "_")
    if s in {"strip1", "strip_1", "s1"}:
        return "STRIP 1"
    if s in {"strip2", "strip_2", "s2"}:
        return "STRIP 2"
    if s in {"strip3", "strip_3", "s3"}:
        return "STRIP 3"
    if s in {"strip4", "strip_4", "s4"}:
        return "STRIP 4"
    # If it's already "STRIP 1" etc:
    s2 = _norm(x).upper().strip()
    if s2.startswith("STRIP "):
        return s2
    return _norm(x)


# -----------------------------------------------------------------------------
# Column / date handling (STRICT date_rec)
# -----------------------------------------------------------------------------
def require_date_rec(df: pd.DataFrame, source_name: str) -> None:
    if "date_rec" not in df.columns:
        raise ValueError(
            f"{source_name}: required column 'date_rec' not found. "
            f"Columns present: {list(df.columns)}"
        )


def _parse_date_rec(df: pd.DataFrame) -> pd.DataFrame:
    # Keep original string for payload keys, but parse for sorting.
    out = df.copy()
    out["_date_rec_dt"] = pd.to_datetime(out["date_rec"], errors="coerce")
    return out


# -----------------------------------------------------------------------------
# Reading cleaned master CSV (single-header)
# -----------------------------------------------------------------------------
def read_clean_master_csv(csv_path: Path, source_name: str) -> pd.DataFrame:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"{source_name}: CSV not found: {csv_path}")

    df = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )

    # drop fully empty columns
    df = df.dropna(axis=1, how="all")

    # normalize strip col
    if "strip" in df.columns:
        df["strip"] = df["strip"].apply(_normalize_strip)

    require_date_rec(df, source_name=source_name)
    return df


# -----------------------------------------------------------------------------
# Variable spec
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class VariableSpec:
    key: str
    label: str
    candidates: Tuple[str, ...]
    note: str = ""


def _pick_first_existing_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def _aggregate_value(series: pd.Series) -> Any:
    """
    Preserve text if unique; average numeric if multiple numeric values; else None.

    Rules:
    - Drop missing/blank.
    - If after cleanup there is exactly 1 unique value (string-compare), return it as:
        - float if numeric-like
        - else original string
    - If >1 unique values:
        - if all numeric-like -> mean(float)
        - else -> None
    """
    vals = [v for v in series.tolist() if not _is_missing(v)]
    if not vals:
        return None

    # normalize strings for uniqueness checks, but keep originals
    normed = [(_norm(v), v) for v in vals]
    unique_norm = {}
    for k, orig in normed:
        if k not in unique_norm:
            unique_norm[k] = orig

    if len(unique_norm) == 1:
        only_orig = next(iter(unique_norm.values()))
        f = _to_float(only_orig)
        return f if f is not None else _norm(only_orig)

    # multiple values
    floats = [_to_float(v) for v in vals]
    if all(f is not None for f in floats):
        return float(sum(floats) / len(floats))

    return None


def _compute_ratio(numer: Any, denom: Any) -> Optional[float]:
    n = _to_float(numer)
    d = _to_float(denom)
    if n is None or d in (None, 0.0):
        return None
    return n / d


# -----------------------------------------------------------------------------
# Payload builder
# -----------------------------------------------------------------------------
def build_soil_table_payload(
    clean_csv: Path,
    variables: Sequence[VariableSpec],
    min_year: int = 2023,
    include_ratio_rows: bool = True,
) -> Dict[str, Any]:
    """
    Standard set payload:
      {
        "periods": [{"key","label"}, ...],
        "variables": [{"key","label","note"} ...],
        "rows": ["strip_1","strip_2","strip_3","strip_4","s1_s2","s3_s4"],
        "rowLabels": {...},
        "data": { var_key: { rowKey: { periodKey: value } } }
      }
    Values may be float OR string (e.g. "ALL PREY").
    """
    df = read_clean_master_csv(Path(clean_csv), source_name="soil_master")
    df = _parse_date_rec(df)

    # filter by min_year using parsed dt where possible
    if "_date_rec_dt" in df.columns:
        df = df[df["_date_rec_dt"].dt.year.fillna(0).astype(int) >= int(min_year)]

    # Build period list (sorted)
    periods_df = (
        df[["date_rec", "_date_rec_dt"]]
        .drop_duplicates()
        .sort_values("_date_rec_dt", kind="mergesort")
    )
    periods: List[Dict[str, str]] = []
    for _, r in periods_df.iterrows():
        key = _norm(r["date_rec"])
        periods.append({"key": key, "label": key})

    # Row keys and labels
    rows: List[str] = ["strip_1", "strip_2", "strip_3", "strip_4"]
    row_labels: Dict[str, str] = {
        "strip_1": "STRIP 1",
        "strip_2": "STRIP 2",
        "strip_3": "STRIP 3",
        "strip_4": "STRIP 4",
    }
    if include_ratio_rows:
        rows.extend(["s1_s2", "s3_s4"])
        row_labels["s1_s2"] = "S1/S2"
        row_labels["s3_s4"] = "S3/S4"

    # Map STRIP -> row key used in payload
    strip_to_rowkey = {
        "STRIP 1": "strip_1",
        "STRIP 2": "strip_2",
        "STRIP 3": "strip_3",
        "STRIP 4": "strip_4",
    }

    out: Dict[str, Any] = {
        "periods": periods,
        "variables": [{"key": v.key, "label": v.label, "note": v.note} for v in variables],
        "rows": rows,
        "rowLabels": row_labels,
        "data": {},
    }

    # For each variable, build per-row/per-period values
    for spec in variables:
        col = _pick_first_existing_column(df, spec.candidates)
        # table expects rows->periods mapping
        table_for_var: Dict[str, Dict[str, Any]] = {rk: {} for rk in rows}

        if col is None:
            # leave empty; frontend shows warning
            out["data"][spec.key] = table_for_var
            continue

        # group by strip + date_rec (in case duplicates exist)
        grouped = df.groupby(["strip", "date_rec"], dropna=False)[col]

        # fill base strip rows
        for (strip_val, date_key), series in grouped:
            strip_norm = _normalize_strip(strip_val)
            rk = strip_to_rowkey.get(strip_norm)
            if not rk:
                continue
            dk = _norm(date_key)
            table_for_var[rk][dk] = _aggregate_value(series)

        # compute ratios (only numeric)
        if include_ratio_rows:
            for p in periods:
                dk = p["key"]
                s1 = table_for_var["strip_1"].get(dk)
                s2 = table_for_var["strip_2"].get(dk)
                s3 = table_for_var["strip_3"].get(dk)
                s4 = table_for_var["strip_4"].get(dk)
                table_for_var["s1_s2"][dk] = _compute_ratio(s1, s2)
                table_for_var["s3_s4"][dk] = _compute_ratio(s3, s4)

        out["data"][spec.key] = table_for_var

    return out