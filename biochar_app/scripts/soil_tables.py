# biochar_app/scripts/soil_tables.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import re

import numpy as np
import pandas as pd


# ------------------------------------------------------------
# Small, curated “Set 1” selections (keeps UI usable)
# You can expand later or add Set2/Set3 exactly like NIR.
# ------------------------------------------------------------

SOILBIO_VARIABLES_SET1: List[Dict[str, Any]] = [
    {
        "key": "total_biomass",
        "label": "Total Biomass (PLFA, unit per lab output)",
        "candidates": ["total_biomass", "total_biomass_ng_per_g", "total_biomass_biomass"],
    },
    {
        "key": "total_bacteria_biomass",
        "label": "Total Bacteria Biomass",
        "candidates": ["total_bacteria_biomass", "total_bacteria_biomass_ng_per_g"],
    },
    {
        "key": "total_fungi_biomass",
        "label": "Total Fungi Biomass",
        "candidates": ["total_fungi_biomass", "total_fungi_biomass_ng_per_g"],
    },
    {
        "key": "fungi_bacteria",
        "label": "Fungi:Bacteria (ratio)",
        "candidates": ["fungi_bacteria", "fungi_bacteria_ratio", "fungi_bacteria_ng_per_g"],
    },
    {
        "key": "diversity_index",
        "label": "Diversity Index (unitless)",
        "candidates": ["diversity_index", "diversity"],
    },
]

SOILCHEM_VARIABLES_SET1: List[Dict[str, Any]] = [
    {
        "key": "soil_ph_1_1",
        "label": "Soil pH (1:1)",
        "candidates": ["soil_ph_1_1", "soil_ph", "ph_1_1", "pH_1_1", "1_1_soil_ph"],
    },
    {
        "key": "organic_matter_loi_pct",
        "label": "Organic Matter (LOI, %)",
        "candidates": ["organic_matter_loi_pct", "organic_matter_pct", "om_loi_pct", "organic_matter_loi"],
    },
    {
        "key": "nitrate_n_ppm",
        "label": "Nitrate-N (ppm N)",
        "candidates": ["nitrate_n_ppm_n", "nitrate_n_ppm", "no3_n_ppm", "nitrate_n"],
    },
    {
        "key": "bray_p1_ppm",
        "label": "Bray P-1 (ppm P)",
        "candidates": ["bray_p_1_ppm_p", "bray_p1_ppm_p", "bray_p1_ppm", "bray_p_1"],
    },
    {
        "key": "potassium_ppm",
        "label": "Potassium (ppm K)",
        "candidates": ["potassium_ppm_k", "potassium_ppm", "k_ppm", "potassium"],
    },
    {
        "key": "cec_meq_100g",
        "label": "CEC / Sum of Cations (meq/100g)",
        "candidates": ["cec_sum_of_cations_me_100g", "cec_me_100g", "cec_meq_100g", "cec"],
    },
    {
        "key": "calcium_carbonate_pct",
        "label": "Calcium Carbonate (%)",
        "candidates": ["calcium_carbonate_pct", "caco3_pct", "calcium_carbonate"],
    },
]


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def _keyify(s: Any) -> str:
    """
    Produce a canonical key for matching human headers vs machine candidates.
      "Total Biomass"  -> "total_biomass"
      "1:1 Soil pH"    -> "1_1_soil_ph"
      "Fungi:Bacteria" -> "fungi_bacteria"
    """
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Pick the first candidate column found in df using _keyify matching.
    This allows snake_case candidates (total_biomass) to match human headers
    (Total Biomass) in the soilbio CSV.
    """
    key_to_actual = {_keyify(c): c for c in df.columns}
    for cand in candidates:
        k = _keyify(cand)
        if k in key_to_actual:
            return key_to_actual[k]
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


def _build_periods(df: pd.DataFrame, min_year: int = 2023) -> List[Dict[str, str]]:
    """
    Period list from date_rec / date_recd / *_date / etc.
    We keep only >= min_year (you said no need before 2023).
    """
    if "date_rec" in df.columns:
        d = pd.to_datetime(df["date_rec"], errors="coerce")
    elif "date_recd" in df.columns:
        d = pd.to_datetime(df["date_recd"], errors="coerce")
    elif "date_received" in df.columns:
        d = pd.to_datetime(df["date_received"], errors="coerce")
    elif "nir_date" in df.columns:
        d = pd.to_datetime(df["nir_date"], errors="coerce")
    elif "soil_date" in df.columns:
        d = pd.to_datetime(df["soil_date"], errors="coerce")
    else:
        # try any plausible
        candidates = [c for c in df.columns if "date" in c.lower()]
        d = (
            pd.to_datetime(df[candidates[0]], errors="coerce")
            if candidates
            else pd.Series([], dtype="datetime64[ns]")
        )

    d = d.dropna()
    if d.empty:
        return []

    d = d[d.dt.year >= min_year]
    if d.empty:
        return []

    keys = sorted({x.date().isoformat() for x in d})
    return [{"key": k, "label": k} for k in keys]


def _ensure_strip(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expect cleaned CSV already has strip like strip_1..strip_4.
    If not, try common fallbacks, then normalize to strip_1..strip_4.

    Also supports your special case mapping:
      "WEST FIELD" -> strip_4
    """
    df = df.copy()

    # Prefer explicit strip, else try sample_id, else try human "Sample ID"
    if "strip" not in df.columns:
        if "sample_id" in df.columns:
            df["strip"] = df["sample_id"]
        else:
            # common human headers (in case a cleaner didn't rename)
            for alt in ("Sample ID", "Sample ID 1", "Sample ID 2"):
                if alt in df.columns:
                    df["strip"] = df[alt]
                    break

    if "strip" not in df.columns:
        raise ValueError("Expected a 'strip' column (or Sample ID) in cleaned soil table CSV.")

    def norm_strip(x: Any) -> Optional[str]:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None

        raw = str(x).strip()
        low = raw.lower().strip()

        # Special case you reported for soil chem compilation
        if low.replace(" ", "") in ("westfield", "west_field"):
            return "strip_4"

        s = low.replace(" ", "").replace("-", "").replace("_", "")
        # strip1 / strip_1 / STRIP 1
        if s.startswith("strip"):
            for d in ("1", "2", "3", "4"):
                if f"strip{d}" in s:
                    return f"strip_{d}"
        # s1, s1hay, etc.
        for d in ("1", "2", "3", "4"):
            if f"s{d}" in s:
                return f"strip_{d}"
        return None

    df["strip"] = df["strip"].apply(norm_strip)
    return df


def _get_date_key_col(df: pd.DataFrame) -> str:
    for c in ("date_rec", "date_recd", "date_received", "nir_date", "soil_date"):
        if c in df.columns:
            return c
    # last resort: any column containing 'date'
    for c in df.columns:
        if "date" in c.lower():
            return c
    raise ValueError(
        "Could not find a date column (expected date_rec/date_recd/date_received/nir_date/soil_date)."
    )


def build_soil_table_payload(
    clean_csv: Path,
    variables: List[Dict[str, Any]],
    min_year: int = 2023,
) -> Dict[str, Any]:
    """
    Payload matches your NIR payload shape so your front-end can reuse the same renderer:
      {
        "periods": [{"key":"YYYY-MM-DD","label":"YYYY-MM-DD"}, ...],
        "variables": [{"key":..., "label":...}, ...],
        "rows": ["strip_1","strip_2","strip_3","strip_4","s1_s2","s3_s4"],
        "data": { var_key: { row_label: { period_key: value_or_null } } }
      }
    """
    clean_csv = Path(clean_csv)
    if not clean_csv.exists():
        raise FileNotFoundError(f"Clean soil CSV not found: {clean_csv}")

    df = pd.read_csv(clean_csv, dtype=str, keep_default_na=False, na_filter=False)
    df = _ensure_strip(df)

    date_col = _get_date_key_col(df)
    df[date_col] = df[date_col].apply(_parse_date_any)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # filter >= min_year
    df = df[df["strip"].notna()].copy()
    df = df[df[date_col].notna()].copy()
    df = df[df[date_col].dt.year >= min_year].copy()

    periods = _build_periods(df, min_year=min_year)

    rows = ["strip_1", "strip_2", "strip_3", "strip_4", "s1_s2", "s3_s4"]

    out: Dict[str, Any] = {
        "periods": periods,
        "variables": [{"key": v["key"], "label": v["label"]} for v in variables],
        "rows": rows,
        "data": {},
    }

    if not periods:
        # still return fully-shaped empties
        for v in variables:
            out["data"][v["key"]] = {r: {} for r in rows}
        return out

    df["period_key"] = df[date_col].dt.date.astype(str)

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
            s1 = table_for_var["strip_1"][k]
            s2 = table_for_var["strip_2"][k]
            s3 = table_for_var["strip_3"][k]
            s4 = table_for_var["strip_4"][k]
            table_for_var["s1_s2"][k] = (s1 / s2) if (s1 is not None and s2 not in (None, 0)) else None
            table_for_var["s3_s4"][k] = (s3 / s4) if (s3 is not None and s4 not in (None, 0)) else None

        out["data"][var_key] = table_for_var

    return out


def build_soilbio_set1_table(clean_csv: Path) -> Dict[str, Any]:
    return build_soil_table_payload(clean_csv=clean_csv, variables=SOILBIO_VARIABLES_SET1, min_year=2023)


def build_soilchem_set1_table(clean_csv: Path) -> Dict[str, Any]:
    return build_soil_table_payload(clean_csv=clean_csv, variables=SOILCHEM_VARIABLES_SET1, min_year=2023)