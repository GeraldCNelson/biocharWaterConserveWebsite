# biochar_app/scripts/soil_tables.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import re

import numpy as np
import pandas as pd


# ------------------------------------------------------------
# Row ordering + human-readable row labels (match NIR style)
# ------------------------------------------------------------
ROW_ORDER: List[str] = ["strip_1", "strip_2", "strip_3", "strip_4", "s1_s2", "s3_s4"]

ROW_LABELS: Dict[str, str] = {
    "strip_1": "STRIP 1",
    "strip_2": "STRIP 2",
    "strip_3": "STRIP 3",
    "strip_4": "STRIP 4",
    "s1_s2": "S1/S2",
    "s3_s4": "S3/S4",
}

# ------------------------------------------------------------
# Soil Bio: 5 groups (Ward PLFA report structure)
# Use PLFA spelled out + units spelled out.
# ------------------------------------------------------------
SOILBIO_VARIABLE_GROUPS: List[Dict[str, Any]] = [
    {
        "group_key": "functional_groups_biomass",
        "group_label": "Functional Groups — Biomass (Phospholipid Fatty Acids; nanogram per gram soil)",
        "variables": [
            {"key": "total_biomass",
             "label": "Total Biomass (Phospholipid Fatty Acids; nanogram per gram soil)",
             "candidates": ["total_biomass"]},
            {"key": "total_bacteria_biomass",
             "label": "Total Bacteria — Biomass (nanogram per gram soil)",
             "candidates": ["total_bacteria_biomass"]},
            {"key": "gram_pos_biomass",
             "label": "Gram (+) — Biomass (nanogram per gram soil)",
             "candidates": ["gram_pos_biomass"]},
            {"key": "actinomycetes_biomass",
             "label": "Actinomycetes — Biomass (nanogram per gram soil)",
             "candidates": ["actinomycetes_biomass"]},
            {"key": "gram_biomass",
             "label": "Gram (−) — Biomass (nanogram per gram soil)",
             "candidates": ["gram_biomass"]},
            {"key": "rhizobia_biomass",
             "label": "Rhizobia — Biomass (nanogram per gram soil)",
             "candidates": ["rhizobia_biomass"]},
            {"key": "total_fungi_biomass",
             "label": "Total Fungi — Biomass (nanogram per gram soil)",
             "candidates": ["total_fungi_biomass"]},
            {"key": "arbuscular_mycorrhizal_biomass",
             "label": "Arbuscular Mycorrhizal — Biomass (nanogram per gram soil)",
             "candidates": ["arbuscular_mycorrhizal_biomass"]},
            {"key": "saprophytes_biomass",
             "label": "Saprophytes — Biomass (nanogram per gram soil)",
             "candidates": ["saprophytes_biomass"]},
            {"key": "protozoa_biomass",
             "label": "Protozoan — Biomass (nanogram per gram soil)",
             "candidates": ["protozoa_biomass"]},
            {"key": "undifferentiated_biomass",
             "label": "Undifferentiated — Biomass (nanogram per gram soil)",
             "candidates": ["undifferentiated_biomass"]},
        ],
    },
    {
        "group_key": "functional_groups_percent",
        "group_label": "Functional Groups — Percent (percent of total PLFA)",
        "variables": [
            {"key": "bacteria_pct", "label": "Total Bacteria — Percent (percent)",
             "candidates": ["bacteria_pct"]},
            {"key": "gram_pos_pct", "label": "Gram (+) — Percent (percent)",
             "candidates": ["gram_pos_pct"]},
            {"key": "actinomycetes_pct", "label": "Actinomycetes — Percent (percent)",
             "candidates": ["actinomycetes_pct"]},
            {"key": "gram_pct", "label": "Gram (−) — Percent (percent)",
             "candidates": ["gram_pct"]},
            {"key": "rhizobia_pct", "label": "Rhizobia — Percent (percent)",
             "candidates": ["rhizobia_pct"]},
            {"key": "total_fungi_pct", "label": "Total Fungi — Percent (percent)",
             "candidates": ["total_fungi_pct"]},
            {"key": "arbusular_mycorrhizal_pct",
             "label": "Arbuscular Mycorrhizal — Percent (percent)",
             "candidates": ["arbusular_mycorrhizal_pct", "arbuscular_mycorrhizal_pct"]},
            {"key": "saprophytic_pct", "label": "Saprophytes — Percent (percent)",
             "candidates": ["saprophytic_pct"]},
            {"key": "protozoan_pct", "label": "Protozoan — Percent (percent)",
             "candidates": ["protozoan_pct"]},
            {"key": "undifferentiated_pct", "label": "Undifferentiated — Percent (percent)",
             "candidates": ["undifferentiated_pct"]},
        ],
    },
    {
        "group_key": "community_composition",
        "group_label": "Community Composition",
        "variables": [
            {"key": "fungi_bacteria", "label": "Fungi:Bacteria (ratio; unitless)",
             "candidates": ["fungi_bacteria"]},
            {"key": "gram_pos_gram", "label": "Gram (+):Gram (−) (ratio; unitless)",
             "candidates": ["gram_pos_gram"]},
            {"key": "predator_prey", "label": "Predator:Prey (ratio; unitless)",
             "candidates": ["predator_prey"]},
        ],
    },
    {
        "group_key": "lipid_structure",
        "group_label": "Lipid Structure",
        "variables": [
            {"key": "saturated", "label": "Saturated (nanogram per gram soil)",
             "candidates": ["saturated"]},
            {"key": "unsaturated", "label": "Unsaturated (nanogram per gram soil)",
             "candidates": ["unsaturated"]},
            {"key": "sat_unsat", "label": "Saturated:Unsaturated (ratio; unitless)",
             "candidates": ["sat_unsat"]},
            {"key": "monounsaturated", "label": "Monounsaturated (nanogram per gram soil)",
             "candidates": ["monounsaturated"]},
            {"key": "polyunsaturated", "label": "Polyunsaturated (nanogram per gram soil)",
             "candidates": ["polyunsaturated"]},
            {"key": "mono_poly", "label": "Monounsaturated:Polyunsaturated (ratio; unitless)",
             "candidates": ["mono_poly"]},
        ],
    },
    {
        "group_key": "diversity_stress_activity",
        "group_label": "Stress & Community Activity Ratios",
        "variables": [
            {"key": "diversity_index", "label": "Diversity Index (unitless)",
             "candidates": ["diversity_index", "diversity"]},
        ],
    },
]


# ------------------------------------------------------------
# Soil Chem: keep your curated set (already has units in labels)
# ------------------------------------------------------------
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
# NIR (Pasture Quality Metrics): sets mirroring your older Set1–Set4 approach,
# but returning the SAME top-level shape as soilbio/soilchem:
#   {"title": "...", "sets": [ {key,label,periods,variables,rows,rowLabels,data}, ... ] }
#
# IMPORTANT:
# - candidate names are best-effort defaults; extend as your clean NIR master evolves.
# - labels include units where those are stable/known.
# ------------------------------------------------------------
NIR_VARIABLE_SETS: List[Dict[str, Any]] = [
    {
        "set_key": "nir_set1",
        "set_label": "Pasture Quality Metrics — Set 1",
        "include_ratio_rows": True,
        "variables": [
            {"key": "crude_protein_pct", "label": "Crude Protein (percent)",
             "candidates": ["crude_protein_pct", "crude_protein", "cp_pct", "cp"]},
            {"key": "tdn_pct", "label": "Total Digestible Nutrients (percent)",
             "candidates": ["tdn_pct", "tdn"]},
            {"key": "rfq", "label": "Relative Forage Quality (unitless)",
             "candidates": ["rfq", "relative_forage_quality"]},
            {"key": "rfv", "label": "Relative Feed Value (unitless)",
             "candidates": ["rfv", "relative_feed_value"]},
            {"key": "digestibility_pct", "label": "Digestibility (percent)",
             "candidates": ["digestibility_pct", "digestibility", "ivtd_pct", "ivtd"]},
        ],
    },
    {
        "set_key": "nir_set2",
        "set_label": "Fiber & Structural Carbohydrates — Set 2",
        "include_ratio_rows": True,
        "variables": [
            {"key": "adf_pct", "label": "ADF (percent)",
             "candidates": ["adf_pct", "adf"]},
            {"key": "ndf_pct", "label": "NDF (percent)",
             "candidates": ["ndf_pct", "ndf"]},
            {"key": "lignin_pct", "label": "Lignin (percent)",
             "candidates": ["lignin_pct", "lignin"]},
            {"key": "nfc_pct", "label": "Non-Fiber Carbohydrates (percent)",
             "candidates": ["nfc_pct", "nfc"]},
        ],
    },
    {
        "set_key": "nir_set3",
        "set_label": "Energy Metrics — Set 3",
        "include_ratio_rows": True,
        "variables": [
            {"key": "nel_mcal_lb", "label": "Net Energy for Lactation (Mcal/lb)",
             "candidates": ["nel_mcal_lb", "nel"]},
            {"key": "nem_mcal_lb", "label": "Net Energy for Maintenance (Mcal/lb)",
             "candidates": ["nem_mcal_lb", "nem"]},
            {"key": "neg_mcal_lb", "label": "Net Energy for Gain (Mcal/lb)",
             "candidates": ["neg_mcal_lb", "neg"]},
        ],
    },
    {
        "set_key": "nir_set4",
        "set_label": "Minerals & Miscellaneous — Set 4",
        "include_ratio_rows": True,
        "variables": [
            {"key": "ash_pct", "label": "Ash (percent)",
             "candidates": ["ash_pct", "ash"]},
            {"key": "fat_pct", "label": "Crude Fat (percent)",
             "candidates": ["fat_pct", "fat", "crude_fat_pct", "crude_fat"]},
            {"key": "moisture_pct", "label": "Moisture (percent)",
             "candidates": ["moisture_pct", "moisture"]},
        ],
    },
]


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _keyify(s: Any) -> str:
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
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


def _get_date_key_col(df: pd.DataFrame) -> str:
    for c in ("date_rec", "date_recd", "date_received", "nir_date", "soil_date"):
        if c in df.columns:
            return c
    for c in df.columns:
        if "date" in c.lower():
            return c
    raise ValueError("Could not find a date column (expected date_rec/date_recd/date_received/nir_date/soil_date).")


def _build_periods(df: pd.DataFrame, date_col: str, min_year: int = 2023) -> List[Dict[str, str]]:
    d = pd.to_datetime(df[date_col], errors="coerce").dropna()
    if d.empty:
        return []
    d = d[d.dt.year >= min_year]
    if d.empty:
        return []
    keys = sorted({x.date().isoformat() for x in d})
    return [{"key": k, "label": k} for k in keys]


def _ensure_strip(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "strip" not in df.columns:
        if "sample_id" in df.columns:
            df["strip"] = df["sample_id"]
        else:
            for alt in ("Sample ID", "Sample ID 1", "Sample ID 2"):
                if alt in df.columns:
                    df["strip"] = df[alt]
                    break

    if "strip" not in df.columns:
        raise ValueError("Expected a 'strip' column (or Sample ID) in cleaned table CSV.")

    def norm_strip(x: Any) -> Optional[str]:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        raw = str(x).strip()
        low = raw.lower().strip()

        if low.replace(" ", "") in ("westfield", "west_field"):
            return "strip_4"

        s = low.replace(" ", "").replace("-", "").replace("_", "")
        if s.startswith("strip"):
            for d in ("1", "2", "3", "4"):
                if f"strip{d}" in s:
                    return f"strip_{d}"
        for d in ("1", "2", "3", "4"):
            if f"s{d}" in s:
                return f"strip_{d}"
        return None

    df["strip"] = df["strip"].apply(norm_strip)
    return df


def _aggregate_preserve_raw_if_unique(values: pd.Series) -> float:
    """
    If there is exactly 1 numeric value for a strip/date, return it (no averaging).
    If multiple values exist, return the mean (defensive).
    """
    vv = values.dropna()
    if vv.empty:
        return np.nan
    if len(vv) == 1:
        return float(vv.iloc[0])
    return float(vv.mean())


def build_soil_table_payload(
    clean_csv: Path,
    variables: List[Dict[str, Any]],
    min_year: int = 2023,
    include_ratio_rows: bool = True,
) -> Dict[str, Any]:
    """
    Single-set payload matching the NIR-ish shape, plus rowLabels.
      {
        "periods": [...],
        "variables": [...],
        "rows": [...],
        "rowLabels": {...},
        "data": {...}
      }
    """
    clean_csv = Path(clean_csv)
    if not clean_csv.exists():
        raise FileNotFoundError(f"Clean table CSV not found: {clean_csv}")

    df = pd.read_csv(clean_csv, dtype=str, keep_default_na=False, na_filter=False)
    df = _ensure_strip(df)

    date_col = _get_date_key_col(df)
    df[date_col] = df[date_col].apply(_parse_date_any)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    df = df[df["strip"].notna()].copy()
    df = df[df[date_col].notna()].copy()
    df = df[df[date_col].dt.year >= min_year].copy()

    periods = _build_periods(df, date_col=date_col, min_year=min_year)

    rows = ROW_ORDER[:] if include_ratio_rows else ["strip_1", "strip_2", "strip_3", "strip_4"]

    out: Dict[str, Any] = {
        "periods": periods,
        "variables": [{"key": v["key"], "label": v.get("label", v["key"])} for v in variables],
        "rows": rows,
        "rowLabels": {k: ROW_LABELS.get(k, k) for k in rows},
        "data": {},
    }

    if not periods:
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

        agg = (
            tmp.groupby(["strip", "period_key"], dropna=False)[col]
            .apply(_aggregate_preserve_raw_if_unique)
            .reset_index()
            .rename(columns={col: "value"})
        )

        for _, rr in agg.iterrows():
            strip = rr["strip"]
            period = rr["period_key"]
            val = rr["value"]
            if strip in table_for_var and period in table_for_var[strip]:
                table_for_var[strip][period] = None if pd.isna(val) else float(val)

        if include_ratio_rows:
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


# ------------------------------------------------------------
# Public builders (all NIR-parallel top-level shape)
# ------------------------------------------------------------
def build_soilbio_table(clean_csv: Path, min_year: int = 2023) -> Dict[str, Any]:
    sets: List[Dict[str, Any]] = []
    for grp in SOILBIO_VARIABLE_GROUPS:
        payload = build_soil_table_payload(
            clean_csv=clean_csv,
            variables=grp["variables"],
            min_year=min_year,
            include_ratio_rows=True,
        )
        sets.append({"key": grp["group_key"], "label": grp["group_label"], **payload})

    return {"title": "Soil Biological Health", "sets": sets}


def build_soilchem_table(clean_csv: Path, min_year: int = 2023) -> Dict[str, Any]:
    payload = build_soil_table_payload(
        clean_csv=clean_csv,
        variables=SOILCHEM_VARIABLES_SET1,
        min_year=min_year,
        include_ratio_rows=True,
    )
    return {
        "title": "Soil Chemistry",
        "sets": [
            {"key": "soilchem_set1", "label": "Soil Chemistry (Ward) — Set 1", **payload}
        ],
    }


def build_nir_table(clean_csv: Path, min_year: int = 2023) -> Dict[str, Any]:
    """
    NIR-parallel builder using the same payload structure as soilchem/soilbio:
      {
        "title": "Pasture Quality Metrics",
        "sets": [
            { "key": "...", "label": "...", "periods":..., "variables":..., "rows":..., "rowLabels":..., "data":... },
            ...
        ]
      }
    """
    sets: List[Dict[str, Any]] = []
    for s in NIR_VARIABLE_SETS:
        payload = build_soil_table_payload(
            clean_csv=clean_csv,
            variables=s["variables"],
            min_year=min_year,
            include_ratio_rows=bool(s.get("include_ratio_rows", True)),
        )
        sets.append({"key": s["set_key"], "label": s["set_label"], **payload})

    return {"title": "Pasture Quality Metrics", "sets": sets}