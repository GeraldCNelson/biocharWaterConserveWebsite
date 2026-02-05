# biochar_app/scripts/tables_nir.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Any, Optional

from biochar_app.scripts.csv_validation import normalize_dates

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
from biochar_app.scripts.config import ( WARD_MASTER_NIR_CSV,
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_SOILCHEM_CSV,
    BIOMASS_FIELD_CSV,)

# ---------------------------------------------------------------------
# Periods (preferred order)
# We ONLY support 2023–2025 and DO NOT append extra dates outside this list.
# ---------------------------------------------------------------------
NIR_PERIOD_DATES: List[str] = [
    "2023-06-14",
    "2023-08-01",
    "2023-10-09",
    "2024-06-03",
    "2024-08-06",
    "2024-09-17",
    "2025-06-16",
    "2025-08-06",
    "2025-11-03",
]

# ---------------------------------------------------------------------
# Variable sets
# You can expand these over time.
# IMPORTANT: candidates should be machine names first, then safe fallbacks.
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
        # STRICT dry-basis only
        "key": "tdn_pct_db",
        "label": "Total Digestible Nutrients (Dry Basis, %)",
        "candidates": [
            "tdn_pct_db",
            # safe fallbacks ONLY if they explicitly say dry basis
            "TDN Est. Dry Basis",
            "TDN Est. Dry Basis (%)",
            "Total Digestible Nutrients Dry Basis",
            "Total Digestible Nutrients (Dry Basis, %)",
        ],
    },
    {
        "key": "rfv",
        "label": "Relative Feed Value (RFV, unitless index)",
        "candidates": [
            "rfv",
            "RFV",
            "Relative Feed Value",
        ],
    },
]

# NOTE: These are starter placeholders for Sets 2–4.
# Replace/extend once you finalize variable selections for each set.

NIR_VARIABLES_SET2 = [
    {
        "key": "nfc_pct_db",
        "label": "Non-Fiber Carbohydrates (Dry Basis, %)",
        "candidates": ["nfc_pct_db"],
    },
    {
        "key": "starch_pct_db",
        "label": "Starch (Dry Basis, %)",
        "candidates": ["starch_pct_db"],
    },
    {
        "key": "wsc_pct_db",
        "label": "Water-Soluble Carbohydrates (Dry Basis, %)",
        "candidates": ["wsc_pct_db"],
    },
    {
        "key": "fructan_pct_db",
        "label": "Fructans (Dry Basis, %)",
        "candidates": ["fructan_pct_db"],
    },
    {
        "key": "nel_pct_db",
        "label": "Net Energy for Lactation (Dry Basis, %)",
        "candidates": ["nel_pct_db"],
    },
    {
        "key": "nem_pct_db",
        "label": "Net Energy for Maintenance (Dry Basis, %)",
        "candidates": ["nem_pct_db"],
    },
    {
        "key": "neg_pct_db",
        "label": "Net Energy for Gain (Dry Basis, %)",
        "candidates": ["neg_pct_db"],
    },
]

NIR_VARIABLES_SET3 = [
    {
        "key": "ash_pct_db",
        "label": "Ash (Dry Basis, %)",
        "candidates": ["ash_pct_db"],
    },
    {
        "key": "ca_pct_db",
        "label": "Calcium (Dry Basis, %)",
        "candidates": ["Ca_pct_db"],
    },
    {
        "key": "p_pct_db",
        "label": "Phosphorus (Dry Basis, %)",
        "candidates": ["P_pct_db"],
    },
    {
        "key": "k_pct_db",
        "label": "Potassium (Dry Basis, %)",
        "candidates": ["K_pct_db"],
    },
    {
        "key": "mg_pct_db",
        "label": "Magnesium (Dry Basis, %)",
        "candidates": ["Mg_pct_db"],
    },
]

NIR_VARIABLES_SET4 = [
    {
        "key": "ndfd48_pctndf_db",
        "label": "NDF Digestibility at 48h (% of NDF)",
        "candidates": ["ndfd48_pctndf_db"],
    },
    {
        "key": "ivtdmd48_pctndf_db",
        "label": "In Vitro True Digestibility (48h, % of NDF)",
        "candidates": ["ivtdmd48_pctndf_db"],
    },
    {
        "key": "fat_pct_db",
        "label": "Crude Fat (Dry Basis, %)",
        "candidates": ["fat_pct_db"],
    },
    {
        "key": "lignin_pct_db",
        "label": "Lignin (Dry Basis, %)",
        "candidates": ["lignin_pct_db"],
    },
    {
        "key": "RFQ",
        "label": "Relative Forage Quality (RFQ)",
        "candidates": ["RFQ"],
    },
]


# ---------------------------------------------------------------------
# Column normalization helpers
# ---------------------------------------------------------------------
def _normalize_colname(s: str) -> str:
    return " ".join(str(s).strip().split())


def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Find the first candidate column that exists in df.
    Matches by:
      1) normalized exact match
      2) case-insensitive exact match
    """
    norm_map = {_normalize_colname(c): c for c in df.columns}
    for cand in candidates:
        key = _normalize_colname(cand)
        if key in norm_map:
            return norm_map[key]

    # case-insensitive exact match
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
    try:
        ts = pd.to_datetime(x, errors="coerce")
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def _clean_strip_id(sample_id: Any) -> Optional[str]:
    """
    Conservative conversion of Ward sample_id variants to "STRIP 1"..."STRIP 4".
    Handles:
      - "STRIP 1", "Strip1"
      - "S1", "S1HAY", "S1 HAY"
    """
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
    """
    Accepts:
      - "STRIP 1"
      - "strip_1" / "Strip_1" / "strip 1"
      - "S1" / "S1HAY" / etc.
    Returns "STRIP 1"..."STRIP 4" or None.
    """
    if x is None:
        return None
    if isinstance(x, float) and np.isnan(x):
        return None

    s = str(x).strip()
    if not s:
        return None

    up = s.strip().upper().replace("-", "").replace("_", "").replace(" ", "")
    if up.startswith("STRIP"):
        for d in ("1", "2", "3", "4"):
            if up == f"STRIP{d}" or f"STRIP{d}" in up:
                return f"STRIP {d}"

    if "STRIP" in up:
        for d in ("1", "2", "3", "4"):
            if f"STRIP{d}" in up:
                return f"STRIP {d}"

    for d in ("1", "2", "3", "4"):
        if f"S{d}" in up:
            return f"STRIP {d}"

    return None


def _ensure_strip_and_date_columns(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Ensure df has:
      - df["strip"] as "STRIP 1"... format
      - df["nir_date"] as Timestamp
    """
    df = df.copy()

    # --- strip ---
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
        if sid_col is not None:
            df["strip"] = df[sid_col].apply(_clean_strip_id)
        else:
            raise ValueError(f"[{source_name}] Could not find a strip/sample id column to derive 'strip'.")

    # --- nir_date ---
    if "nir_date" in df.columns:
        df["nir_date"] = df["nir_date"].apply(_parse_date_any)
    else:
        date_col = None
        for c in (
            "date_rec",
            "date_received",
            "date_recd",
            "Date Recd",
            "Date Received",
            "Date Rec'd",
        ):
            if c in df.columns:
                date_col = c
                break

        if date_col is not None:
            df["nir_date"] = df[date_col].apply(_parse_date_any)
        else:
            df["nir_date"] = None

    df["nir_date"] = pd.to_datetime(df["nir_date"], errors="coerce")

    return df


# ---------------------------------------------------------------------
# Loading sources
# ---------------------------------------------------------------------
def load_ward_master_csv(ward_master_csv: Path) -> pd.DataFrame:
    """
    Supports:
    A) CLEANED MASTER (single header row, machine-readable)
    B) ORIGINAL WARD MASTER (two header rows; machine headers in row 1)
    """
    ward_master_csv = Path(ward_master_csv)
    if not ward_master_csv.exists():
        raise FileNotFoundError(f"Ward master CSV not found: {ward_master_csv}")

    # Try cleaned master
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

    # Two-header fallback
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
            f"Ward master CSV must have at least 3 rows (human header, machine header, data). "
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


def load_single_event_csv(path: Path) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False, engine="python")

    if "Date Recd" in df.columns:
        df["nir_date"] = pd.to_datetime(df["Date Recd"], errors="coerce")
    elif "Date Received" in df.columns:
        df["nir_date"] = pd.to_datetime(df["Date Received"], errors="coerce")
    else:
        stem = path.stem
        dt = None
        if "_" in stem:
            dt = stem.split("_", 1)[1]
        df["nir_date"] = pd.to_datetime(dt, errors="coerce") if dt else pd.NaT

    df = _ensure_strip_and_date_columns(df, source_name=f"extra_event:{path.name}")
    return df


def _build_period_list(df: pd.DataFrame) -> List[Dict[str, str]]:
    """
    2023–2025 only:
    Return periods in NIR_PERIOD_DATES order that are present in the data.
    Do NOT append extra dates.
    """
    if "nir_date" not in df.columns:
        return [{"key": d, "label": d} for d in NIR_PERIOD_DATES]

    df_dates = df["nir_date"].dropna()
    if df_dates.empty:
        return [{"key": d, "label": d} for d in NIR_PERIOD_DATES]

    present = {d.date().isoformat() for d in df_dates}
    final = [d for d in NIR_PERIOD_DATES if d in present]
    return [{"key": d, "label": d} for d in final]


def _build_nir_table_payload(
    df: pd.DataFrame,
    variables: List[Dict[str, Any]],
    extra_event_csvs: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    """
    Common builder for all sets.
    """
    # attach extra events (optional)
    for p in (extra_event_csvs or []):
        try:
            extra = load_single_event_csv(Path(p))
            df = pd.concat([df, extra], ignore_index=True)
        except Exception as e:
            logger.warning("Failed to load extra NIR event %s: %s", p, e)

    if "strip" not in df.columns:
        raise ValueError("Ward master NIR data must include a 'strip' column (or sample_id to derive it).")
    if "nir_date" not in df.columns:
        raise ValueError("Ward master NIR data must include a 'nir_date' column (or date_rec/date_received).")

    df = df[df["strip"].notna()].copy()
    df = df[df["nir_date"].notna()].copy()

    df["period_key"] = df["nir_date"].dt.date.astype(str)

    periods = _build_period_list(df)

    rows = ["STRIP 1", "STRIP 2", "STRIP 3", "STRIP 4", "S1/S2", "S3/S4"]

    out: Dict[str, Any] = {
        "periods": periods,
        "variables": [{"key": v["key"], "label": v["label"]} for v in variables],
        "rows": rows,
        "data": {},
    }

    for v in variables:
        var_key = v["key"]
        col = _pick_first_existing(df, v["candidates"])

        table_for_var: Dict[str, Dict[str, Optional[float]]] = {
            r: {p["key"]: None for p in periods} for r in rows
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
            strip = rr["strip"]
            period = rr["period_key"]
            val = rr["value"]
            if strip in table_for_var and period in table_for_var[strip]:
                table_for_var[strip][period] = None if pd.isna(val) else float(val)

        # ratio rows
        for p in periods:
            k = p["key"]
            s1 = table_for_var["STRIP 1"][k]
            s2 = table_for_var["STRIP 2"][k]
            s3 = table_for_var["STRIP 3"][k]
            s4 = table_for_var["STRIP 4"][k]

            table_for_var["S1/S2"][k] = (s1 / s2) if (s1 is not None and s2 not in (None, 0)) else None
            table_for_var["S3/S4"][k] = (s3 / s4) if (s3 is not None and s4 not in (None, 0)) else None

        out["data"][var_key] = table_for_var

    return out


# ---------------------------------------------------------------------
# Public builders (Sets 1–4)
# ---------------------------------------------------------------------
def build_nir_set1_table(
    ward_master_csv: Path,
    extra_event_csvs: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    df = load_ward_master_csv(Path(ward_master_csv))
    return _build_nir_table_payload(df, NIR_VARIABLES_SET1, extra_event_csvs=extra_event_csvs)


def build_nir_set2_table(
    ward_master_csv: Path,
    extra_event_csvs: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    df = load_ward_master_csv(Path(ward_master_csv))
    return _build_nir_table_payload(df, NIR_VARIABLES_SET2, extra_event_csvs=extra_event_csvs)


def build_nir_set3_table(
    ward_master_csv: Path,
    extra_event_csvs: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    df = load_ward_master_csv(Path(ward_master_csv))
    return _build_nir_table_payload(df, NIR_VARIABLES_SET3, extra_event_csvs=extra_event_csvs)


def build_nir_set4_table(
    ward_master_csv: Path,
    extra_event_csvs: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    df = load_ward_master_csv(Path(ward_master_csv))
    return _build_nir_table_payload(df, NIR_VARIABLES_SET4, extra_event_csvs=extra_event_csvs)

def build_nir_tables() -> dict:
    return {
        "title": "Pasture Quality Metrics",
        "sets": [
            {
                "key": "nir_set1",
                "label": "Set 1: Pasture Quality Metrics",
                **build_nir_set1_table(),
            },
            {
                "key": "nir_set2",
                "label": "Set 2: Fiber & Digestibility",
                **build_nir_set2_table(),
            },
            {
                "key": "nir_set3",
                "label": "Set 3: Energy Metrics",
                **build_nir_set3_table(),
            },
            {
                "key": "nir_set4",
                "label": "Set 4: Additional Indicators",
                **build_nir_set4_table(),
            },
        ],
    }