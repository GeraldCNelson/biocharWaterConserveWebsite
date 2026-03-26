# biochar_app/scripts/tables_nir.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import logging
import numpy as np
import pandas as pd

from biochar_app.config.paths import WARD_MASTER_NIR_CSV

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# NIR period rules (derived from data; filtered by year window)
# ---------------------------------------------------------------------
NIR_MIN_YEAR = 2023
NIR_MAX_YEAR = 2025  # inclusive

# ---------------------------------------------------------------------
# Variable sets
# ---------------------------------------------------------------------
NIR_VARIABLES_SET1: List[Dict[str, Any]] = [
    {
        "key": "crude_protein_pct_db",
        "label": "Crude Protein (Dry Basis, %)",
        "candidates": [
            "crude_protein_pct_db",
            "cp_pct_db",
            "Crude Protein Dry Basis",
            "Crude Protein Dry Basis (%)",
            "Crude Protein Dry Basis %",
            "Crude Protein (Dry Basis, %)",
        ],
    },
    {
        "key": "adf_pct_db",
        "label": "Acid Detergent Fiber (Dry Basis, %)",
        "candidates": [
            "adf_pct_db",
            "Acid Detergent Fiber Dry Basis",
            "Acid Detergent Fiber Dry Basis (%)",
            "ADF Dry Basis",
            "ADF (Dry Basis, %)",
        ],
    },
    {
        "key": "ndf_pct_db",
        "label": "Neutral Detergent Fiber (Dry Basis, %)",
        "candidates": [
            "ndf_pct_db",
            "Neutral Detergent Fiber Dry Basis",
            "Neutral Detergent Fiber Dry Basis (%)",
            "NDF Dry Basis",
            "NDF (Dry Basis, %)",
        ],
    },
    {
        "key": "tdn_pct_db",
        "label": "Total Digestible Nutrients (Dry Basis, %)",
        "candidates": [
            "tdn_pct_db",
            "TDN Est. Dry Basis",
            "TDN Est. Dry Basis (%)",
            "Total Digestible Nutrients Dry Basis",
            "Total Digestible Nutrients (Dry Basis, %)",
        ],
    },
    {
        "key": "rfv",
        "label": "Relative Feed Value (RFV, unitless index)",
        "candidates": ["rfv", "RFV", "Relative Feed Value"],
    },
]

NIR_VARIABLES_SET2: List[Dict[str, Any]] = [
    {"key": "nfc_pct_db", "label": "Non-Fiber Carbohydrates (Dry Basis, %)", "candidates": ["nfc_pct_db"]},
    {"key": "starch_pct_db", "label": "Starch (Dry Basis, %)", "candidates": ["starch_pct_db"]},
    {"key": "wsc_pct_db", "label": "Water-Soluble Carbohydrates (Dry Basis, %)", "candidates": ["wsc_pct_db"]},
    {"key": "fructan_pct_db", "label": "Fructans (Dry Basis, %)", "candidates": ["fructan_pct_db"]},
    {"key": "nel_pct_db", "label": "Net Energy for Lactation (Dry Basis, %)", "candidates": ["nel_pct_db"]},
    {"key": "nem_pct_db", "label": "Net Energy for Maintenance (Dry Basis, %)", "candidates": ["nem_pct_db"]},
    {"key": "neg_pct_db", "label": "Net Energy for Gain (Dry Basis, %)", "candidates": ["neg_pct_db"]},
]

NIR_VARIABLES_SET3: List[Dict[str, Any]] = [
    {"key": "ash_pct_db", "label": "Ash (Dry Basis, %)", "candidates": ["ash_pct_db"]},
    {"key": "ca_pct_db", "label": "Calcium (Dry Basis, %)", "candidates": ["ca_pct_db", "Ca_pct_db"]},
    {"key": "p_pct_db", "label": "Phosphorus (Dry Basis, %)", "candidates": ["p_pct_db", "P_pct_db"]},
    {"key": "k_pct_db", "label": "Potassium (Dry Basis, %)", "candidates": ["k_pct_db", "K_pct_db"]},
    {"key": "mg_pct_db", "label": "Magnesium (Dry Basis, %)", "candidates": ["mg_pct_db", "Mg_pct_db"]},
]

NIR_VARIABLES_SET4: List[Dict[str, Any]] = [
    {"key": "ndfd48_pctndf_db", "label": "NDF Digestibility at 48h (% of NDF)", "candidates": ["ndfd48_pctndf_db"]},
    {"key": "ivtdmd48_pctndf_db", "label": "In Vitro True Digestibility (48h, % of NDF)", "candidates": ["ivtdmd48_pctndf_db"]},
    {"key": "fat_pct_db", "label": "Crude Fat (Dry Basis, %)", "candidates": ["fat_pct_db"]},
    {"key": "lignin_pct_db", "label": "Lignin (Dry Basis, %)", "candidates": ["lignin_pct_db"]},
    {"key": "RFQ", "label": "Relative Forage Quality (RFQ)", "candidates": ["RFQ", "rfq"]},
]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _normalize_colname(s: str) -> str:
    return " ".join(str(s).strip().split())


def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    norm_map = {_normalize_colname(c): c for c in df.columns}
    for cand in candidates:
        key = _normalize_colname(cand)
        if key in norm_map:
            return norm_map[key]

    df_cols = list(df.columns)
    for cand in candidates:
        low = str(cand).strip().lower()
        for c in df_cols:
            if low == str(c).strip().lower():
                return c
    return None


def _parse_date_any(x: Any) -> Optional[pd.Timestamp]:
    if x is None:
        return None
    if isinstance(x, float) and np.isnan(x):
        return None
    ts = pd.to_datetime(x, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def _clean_strip_id(sample_id: Any) -> Optional[str]:
    if sample_id is None:
        return None
    if isinstance(sample_id, float) and np.isnan(sample_id):
        return None

    s = str(sample_id).strip().upper()
    s = s.replace("-", "").replace("_", "").replace(" ", "")
    if not s:
        return None

    if "STRIP" in s:
        for d in ("1", "2", "3", "4"):
            if f"STRIP{d}" in s:
                return f"STRIP {d}"

    for d in ("1", "2", "3", "4"):
        if f"S{d}" in s:
            return f"STRIP {d}"

    return None


def _canonicalize_strip_value(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, float) and np.isnan(x):
        return None

    s = str(x).strip()
    if not s:
        return None

    up = s.strip().upper().replace("-", "").replace("_", "").replace(" ", "")
    if "STRIP" in up:
        for d in ("1", "2", "3", "4"):
            if f"STRIP{d}" in up:
                return f"STRIP {d}"

    for d in ("1", "2", "3", "4"):
        if f"S{d}" in up:
            return f"STRIP {d}"

    return None


def _ensure_strip_and_date_columns(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    df = df.copy()

    if "strip" in df.columns:
        df["strip"] = df["strip"].apply(_canonicalize_strip_value)
    elif "sample_id" in df.columns:
        df["strip"] = df["sample_id"].apply(_clean_strip_id)
    else:
        sid_col = None
        for c in ("Sample ID", "SampleID", "Sample Id"):
            if c in df.columns:
                sid_col = c
                break
        if sid_col is None:
            raise ValueError(f"[{source_name}] Could not find a strip/sample id column to derive 'strip'.")
        df["strip"] = df[sid_col].apply(_clean_strip_id)

    if "nir_date" in df.columns:
        df["nir_date"] = df["nir_date"].apply(_parse_date_any)
    else:
        date_col = None
        for c in ("date_rec", "date_received", "date_recd", "Date Recd", "Date Received", "Date Rec'd"):
            if c in df.columns:
                date_col = c
                break
        df["nir_date"] = df[date_col].apply(_parse_date_any) if date_col else None

    df["nir_date"] = pd.to_datetime(df["nir_date"], errors="coerce")
    return df


def safe_ratio(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
    if numer is None or denom is None or denom == 0:
        return None
    return numer / denom


def _filter_period_years(df: pd.DataFrame) -> pd.DataFrame:
    if "nir_date" not in df.columns:
        return df
    out = df.copy()
    out = out[out["nir_date"].notna()]
    if out.empty:
        return out
    years = out["nir_date"].dt.year
    return out[(years >= NIR_MIN_YEAR) & (years <= NIR_MAX_YEAR)]


# ---------------------------------------------------------------------
# Loading sources
# ---------------------------------------------------------------------
def load_ward_master_csv(ward_master_csv: Path) -> pd.DataFrame:
    ward_master_csv = Path(ward_master_csv)
    if not ward_master_csv.exists():
        raise FileNotFoundError(f"Ward master CSV not found: {ward_master_csv}")

    try:
        df0 = pd.read_csv(
            ward_master_csv,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            engine="python",
        )
        if "strip" in df0.columns or "date_rec" in df0.columns or "sample_id" in df0.columns:
            df0 = df0.dropna(axis=1, how="all")
            df0 = _ensure_strip_and_date_columns(df0, source_name="ward_master(clean_header)")
            return df0
    except Exception as e:
        logger.info("Clean-header read failed; trying two-header fallback. Reason: %s", e)

    raw = pd.read_csv(
        ward_master_csv,
        header=None,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )

    if raw.shape[0] < 3:
        raise ValueError(
            "Ward master CSV must have at least 3 rows (human header, machine header, data). "
            f"Got {raw.shape[0]} rows from {ward_master_csv}"
        )

    machine_headers = [str(x).strip() for x in raw.iloc[1, :].tolist()]
    data = raw.iloc[2:, :].copy()

    if len(machine_headers) != data.shape[1]:
        raise ValueError(
            f"Machine header width ({len(machine_headers)}) does not match data width ({data.shape[1]}). "
            f"File: {ward_master_csv}"
        )

    data.columns = machine_headers
    data = data.dropna(axis=1, how="all")
    data = _ensure_strip_and_date_columns(data, source_name="ward_master(two_header)")
    return data


def load_single_event_csv(event_path: Path) -> pd.DataFrame:
    event_path = Path(event_path)
    df = pd.read_csv(event_path, dtype=str, keep_default_na=False, na_filter=False, engine="python")

    if "Date Recd" in df.columns:
        df["nir_date"] = pd.to_datetime(df["Date Recd"], errors="coerce")
    elif "Date Received" in df.columns:
        df["nir_date"] = pd.to_datetime(df["Date Received"], errors="coerce")
    else:
        stem = event_path.stem
        dt = stem.split("_", 1)[1] if "_" in stem else None
        df["nir_date"] = pd.to_datetime(dt, errors="coerce") if dt else pd.NaT

    df = _ensure_strip_and_date_columns(df, source_name=f"extra_event:{event_path.name}")
    return df


# ---------------------------------------------------------------------
# Period builder (derived from data; no hardcoded list)
# ---------------------------------------------------------------------
def _build_period_list(df: pd.DataFrame) -> List[Dict[str, str]]:
    if "nir_date" not in df.columns:
        return []

    dff = _filter_period_years(df)
    if dff.empty:
        return []

    days = (
        dff["nir_date"]
        .dropna()
        .dt.date
        .astype(str)
        .drop_duplicates()
        .sort_values(kind="mergesort")
        .tolist()
    )
    return [{"key": d, "label": d} for d in days]


def _build_nir_table_payload(
    df: pd.DataFrame,
    variables: List[Dict[str, Any]],
    extra_event_csvs: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    # IMPORTANT: don't reuse a loop var name across different types (mypy hates that)
    for event_path in (extra_event_csvs or []):
        extra = load_single_event_csv(Path(event_path))
        df = pd.concat([df, extra], ignore_index=True)

    if "strip" not in df.columns:
        raise ValueError("Ward master NIR data must include a 'strip' column (or sample_id to derive it).")
    if "nir_date" not in df.columns:
        raise ValueError("Ward master NIR data must include a 'nir_date' column (or date_rec/date_received).")

    df = df[df["strip"].notna()].copy()
    df = df[df["nir_date"].notna()].copy()
    df = _filter_period_years(df)

    df["period_key"] = df["nir_date"].dt.date.astype(str)

    periods = _build_period_list(df)
    rows = ["STRIP 1", "STRIP 2", "STRIP 3", "STRIP 4", "S1/S2", "S3/S4"]

    out: Dict[str, Any] = {
        "periods": periods,
        "variables": [{"key": v["key"], "label": v["label"]} for v in variables],
        "rows": rows,
        "rowLabels": {r: r for r in rows},
        "data": {},
    }

    for v in variables:
        var_key = str(v.get("key", "")).strip()
        candidates = v.get("candidates") or [var_key]
        if not var_key:
            continue

        col = _pick_first_existing(df, list(candidates))
        table_for_var: Dict[str, Dict[str, Optional[float]]] = {
            r: {period["key"]: None for period in periods} for r in rows
        }

        if col is None:
            out["data"][var_key] = table_for_var
            continue

        tmp = df[["strip", "period_key", col]].copy()
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce")

        means = (
            tmp.groupby(["strip", "period_key"], dropna=False)[col]
            .mean()
            .reset_index()
            .rename(columns={col: "value"})
        )

        for _, rr in means.iterrows():
            strip = str(rr["strip"])
            period_key = str(rr["period_key"])
            val = rr["value"]
            if strip in table_for_var and period_key in table_for_var[strip]:
                table_for_var[strip][period_key] = None if pd.isna(val) else float(val)

        # ratio rows
        for period in periods:
            k = period["key"]
            s1 = table_for_var["STRIP 1"][k]
            s2 = table_for_var["STRIP 2"][k]
            s3 = table_for_var["STRIP 3"][k]
            s4 = table_for_var["STRIP 4"][k]
            table_for_var["S1/S2"][k] = safe_ratio(s1, s2)
            table_for_var["S3/S4"][k] = safe_ratio(s3, s4)

        out["data"][var_key] = table_for_var

    return out


# ---------------------------------------------------------------------
# Public builders
# ---------------------------------------------------------------------
def build_nir_set1_table(ward_master_csv: Path, extra_event_csvs: Optional[List[Path]] = None) -> Dict[str, Any]:
    df = load_ward_master_csv(ward_master_csv)
    return _build_nir_table_payload(df, NIR_VARIABLES_SET1, extra_event_csvs=extra_event_csvs)


def build_nir_set2_table(ward_master_csv: Path, extra_event_csvs: Optional[List[Path]] = None) -> Dict[str, Any]:
    df = load_ward_master_csv(ward_master_csv)
    return _build_nir_table_payload(df, NIR_VARIABLES_SET2, extra_event_csvs=extra_event_csvs)


def build_nir_set3_table(ward_master_csv: Path, extra_event_csvs: Optional[List[Path]] = None) -> Dict[str, Any]:
    df = load_ward_master_csv(ward_master_csv)
    return _build_nir_table_payload(df, NIR_VARIABLES_SET3, extra_event_csvs=extra_event_csvs)


def build_nir_set4_table(ward_master_csv: Path, extra_event_csvs: Optional[List[Path]] = None) -> Dict[str, Any]:
    df = load_ward_master_csv(ward_master_csv)
    return _build_nir_table_payload(df, NIR_VARIABLES_SET4, extra_event_csvs=extra_event_csvs)


def build_nir_tables(
    ward_master_csv: Path = WARD_MASTER_NIR_CSV,
    extra_event_csvs: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    """
    Build the full NIR payload (title + 4 sets) from the authoritative Ward master CSV.

    Periods are derived from nir_date values present in the data (filtered to 2023–2025).
    """
    return {
        "title": "Pasture Quality Metrics",
        "sets": [
            {
                "key": "nir_set1",
                "label": "Set 1: Pasture Quality Metrics",
                **build_nir_set1_table(ward_master_csv, extra_event_csvs=extra_event_csvs),
            },
            {
                "key": "nir_set2",
                "label": "Set 2: Fiber & Digestibility",
                **build_nir_set2_table(ward_master_csv, extra_event_csvs=extra_event_csvs),
            },
            {
                "key": "nir_set3",
                "label": "Set 3: Energy Metrics",
                **build_nir_set3_table(ward_master_csv, extra_event_csvs=extra_event_csvs),
            },
            {
                "key": "nir_set4",
                "label": "Set 4: Additional Indicators",
                **build_nir_set4_table(ward_master_csv, extra_event_csvs=extra_event_csvs),
            },
        ],
    }