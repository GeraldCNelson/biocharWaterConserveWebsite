#!/usr/bin/env python3
"""
tables_soil_common.py

Shared helpers for rendering Soil Chemistry / Soil Biology cleaned master CSVs
into the standard dashboard table payload used by the app.

Design intent
-------------
- STRICT: cleaned masters must contain a `date_rec` column.
- Preserve text cell values (for example, "ALL PREY") instead of coercing
  everything to numeric.
- Numeric aggregation:
    * if all non-empty values in a (strip, date_rec) group are identical,
      keep that single value
    * else if all values are numeric, return the mean
    * else return None
- Ratio rows (S1/S2, S3/S4) are computed only when both sides are numeric
  and the denominator is non-zero.

Payload shape
-------------
{
    "periods": [{"key": "...", "label": "..."}, ...],
    "variables": [{"key": "...", "label": "...", "note": "..."}, ...],
    "rows": ["strip_1", "strip_2", "strip_3", "strip_4", "s1_s2", "s3_s4"],
    "rowLabels": {...},
    "data": {
        "<var_key>": {
            "<row_key>": {
                "<period_key>": value
            }
        }
    }
}
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from biochar_app.scripts.tables.tables_common import build_variable_meta


# -----------------------------------------------------------------------------
# Small utilities
# -----------------------------------------------------------------------------
def _norm(s: Any) -> str:
    return str(s or "").strip()


def _keyify(s: str) -> str:
    """Make a stable key from a label."""
    s = _norm(s).lower()
    out: List[str] = []
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
    Assumes cleaned master may already use values like "strip_1", "S1", etc.
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
    """
    Keep original date_rec string for payload keys, but also create a parsed
    datetime column for sorting/filtering.
    """
    out = df.copy()

    # Normalize raw strings
    s = out["date_rec"].astype(str).str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NaT": pd.NA})

    # First pass: ISO format
    parsed = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")

    # Second pass: slash dates with 4-digit year
    mask = parsed.isna() & s.notna()
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(
            s.loc[mask],
            format="%m/%d/%Y",
            errors="coerce",
        )

    # Third pass: slash dates with 2-digit year
    mask = parsed.isna() & s.notna()
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(
            s.loc[mask],
            format="%m/%d/%y",
            errors="coerce",
        )

    # Debug any leftovers that still failed
    failed_mask = parsed.isna() & s.notna()
    if failed_mask.any():
        bad_values = s.loc[failed_mask].unique()

        print("\n⚠️ DATE PARSE DEBUG — Unparsed values:")
        for val in bad_values[:20]:
            print(f"  -> '{val}'")
        print(f"Total unparsed: {len(bad_values)}\n")

    out["_date_rec_dt"] = parsed
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

    # Drop fully empty columns.
    df = df.dropna(axis=1, how="all")

    # Normalize strip column if present.
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
    note: str | None = None
    reference_key: Optional[str] = None


def _pick_first_existing_column(
    df: pd.DataFrame,
    candidates: Sequence[str],
) -> Optional[str]:
    """
    Prefer exact candidate matches, but allow a case-insensitive fallback.
    """
    cols = list(df.columns)
    cols_set = set(cols)

    for c in candidates:
        if c in cols_set:
            return c

    lowered = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        hit = lowered.get(str(cand).strip().lower())
        if hit is not None:
            return hit

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

    normed = [(_norm(v), v) for v in vals]
    unique_norm: Dict[str, Any] = {}
    for k, orig in normed:
        if k not in unique_norm:
            unique_norm[k] = orig

    if len(unique_norm) == 1:
        only_orig = next(iter(unique_norm.values()))
        f = _to_float(only_orig)
        return f if f is not None else _norm(only_orig)

    floats = [_to_float(v) for v in vals]
    float_vals: List[float] = [f for f in floats if f is not None]
    if len(float_vals) == len(floats) and float_vals:
        return float(sum(float_vals) / len(float_vals))

    return None


def _compute_ratio(numer: Any, denom: Any) -> Optional[float]:
    n = _to_float(numer)
    d = _to_float(denom)
    if n is None or d is None or d == 0.0:
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
    Build the standard soil table payload.

    Parameters
    ----------
    clean_csv
        Path to the cleaned authoritative soil CSV.
    variables
        Variable specifications for one table group.
    min_year
        Filter out periods earlier than this year using parsed date_rec.
    include_ratio_rows
        If True, include S1/S2 and S3/S4 rows.
    """
    df = read_clean_master_csv(Path(clean_csv), source_name="soil_master")
    df = _parse_date_rec(df)

    if "strip" not in df.columns:
        raise ValueError(
            "soil_master: required column 'strip' not found. "
            f"Columns present: {list(df.columns)}"
        )

    # Filter by min_year using parsed dates where possible.
    if "_date_rec_dt" in df.columns:
        dt = df["_date_rec_dt"]
        df = df[dt.dt.year.fillna(0).astype(int) >= int(min_year)]

    # Build period list in chronological order.
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

    strip_to_rowkey = {
        "STRIP 1": "strip_1",
        "STRIP 2": "strip_2",
        "STRIP 3": "strip_3",
        "STRIP 4": "strip_4",
    }

    out: Dict[str, Any] = {
        "periods": periods,
        "variables": [build_variable_meta(v) for v in variables],
        "rows": rows,
        "rowLabels": row_labels,
        "data": {},
    }

    for spec in variables:
        col = _pick_first_existing_column(df, spec.candidates)
        table_for_var: Dict[str, Dict[str, Any]] = {rk: {} for rk in rows}

        if col is None:
            out["data"][spec.key] = table_for_var
            continue

        grouped = df.groupby(["strip", "date_rec"], dropna=False)[col]

        # Fill base strip rows.
        for (strip_val, date_key), series in grouped:
            strip_norm = _normalize_strip(strip_val)
            rk = strip_to_rowkey.get(strip_norm)
            if not rk:
                continue

            dk = _norm(date_key)
            table_for_var[rk][dk] = _aggregate_value(series)

        # Compute ratio rows only for numeric values.
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