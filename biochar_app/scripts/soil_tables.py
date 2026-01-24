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

# ---------------------------------------------------------------------
# Soil Bio sets (4-set structure aligned to Ward report)
# ---------------------------------------------------------------------

SOILBIO_VARIABLE_GROUPS: List[Dict[str, Any]] = [
    # ------------------------------------------------------------------
    # Set 1: Functional Group Biomass
    # ------------------------------------------------------------------
    {
        "group_key": "soilbio_set1_biomass",
        "group_label": "Set 1: Functional Group Biomass (ng/g soil)",
        "notes": (
            "Ward PLFA: Functional Group Biomass values represent summed biomarker signals "
            "for each microbial functional group and indicate absolute living microbial biomass."
        ),
        "variables": [
            {
                "key": "total_biomass",
                "label": "Total Living Microbial Biomass",
                "note": (
                    "Sum of all quantified PLFA biomarkers. Indicates overall living microbial biomass "
                    "and is often used as a primary indicator of soil biological activity."
                ),
                "candidates": ["total_biomass", "total_living_microbial_biomass", "total_micro_biomass"],
            },
            {
                "key": "total_bacteria_biomass",
                "label": "Total Bacterial Biomass",
                "note": (
                    "Total biomass attributed to bacterial PLFA markers. Often responds quickly "
                    "to fresh carbon inputs and short-term management changes."
                ),
                "candidates": ["total_bacteria_biomass", "total_bacterial_biomass", "bacteria_biomass"],
            },
            {
                "key": "total_fungi_biomass",
                "label": "Total Fungal Biomass",
                "note": (
                    "Total fungal PLFA biomass. Higher values are often associated with greater "
                    "carbon stabilization and soil structural development."
                ),
                "candidates": ["total_fungi_biomass", "total_fungal_biomass", "fungi_biomass"],
            },
            {
                "key": "actinomycetes_biomass",
                "label": "Actinomycetes Biomass",
                "note": (
                    "Biomass of actinomycetes—organisms commonly linked to decomposition of complex "
                    "organic matter and more stable soil systems."
                ),
                "candidates": ["actinomycetes_biomass", "actino_biomass"],
            },
            {
                "key": "gram_biomass",
                "label": "Gram Bacteria Biomass",
                "note": (
                    "Total biomass attributed to Gram-type bacterial biomarkers. Interpretation is most useful "
                    "alongside Gram+/Gram− ratios."
                ),
                "candidates": ["gram_biomass", "gram_total_biomass"],
            },
            {
                "key": "rhizobia_biomass",
                "label": "Rhizobia Biomass",
                "note": (
                    "Biomass attributed to rhizobia-associated biomarkers. Often relevant to plant–microbe "
                    "interactions and nutrient cycling."
                ),
                "candidates": ["rhizobia_biomass"],
            },
            {
                "key": "saprophytes_biomass",
                "label": "Saprophytes Biomass",
                "note": (
                    "Biomass of saprophytic organisms that decompose organic residues. Can rise with residue inputs "
                    "and favorable moisture/temperature conditions."
                ),
                "candidates": ["saprophytes_biomass", "saprophytic_biomass"],
            },
            {
                "key": "arbuscular_mycorrhizal_biomass",
                "label": "Arbuscular Mycorrhizal Biomass",
                "note": (
                    "Biomass of arbuscular mycorrhizal fungi (AMF). Often associated with plant nutrient uptake and "
                    "soil aggregation processes."
                ),
                "candidates": ["arbuscular_mycorrhizal_biomass", "amf_biomass"],
            },
            {
                "key": "undifferentiated_biomass",
                "label": "Undifferentiated Biomass",
                "note": (
                    "Portion of biomass not assigned to a specific functional group in the reporting scheme. "
                    "Useful mainly for completeness (closing the total)."
                ),
                "candidates": ["undifferentiated_biomass"],
            },
        ],
    },

    # ------------------------------------------------------------------
    # Set 2: Functional Group Diversity
    # ------------------------------------------------------------------
    {
        "group_key": "soilbio_set2_diversity",
        "group_label": "Set 2: Functional Group Diversity (index)",
        "notes": (
            "Ward PLFA: Diversity indices summarize distribution/evenness across functional groups. "
            "They are derived metrics (not raw biomarker peaks)."
        ),
        "variables": [
            {
                "key": "functional_group_diversity_index",
                "label": "Functional Group Diversity Index",
                "note": (
                    "Derived index summarizing diversity/evenness across microbial functional groups. "
                    "Higher values generally indicate a more evenly distributed community."
                ),
                "candidates": [
                    "functional_group_diversity_index",
                    "functional_group_diversity",
                    "functional_diversity_index",
                ],
            },
        ],
    },

    # ------------------------------------------------------------------
    # Set 3: Community Composition Ratios
    # ------------------------------------------------------------------
    {
        "group_key": "soilbio_set3_composition",
        "group_label": "Set 3: Community Composition Ratios",
        "notes": (
            "Ward PLFA: Community composition ratios are interpretive diagnostics describing dominance and "
            "trophic structure (e.g., Predator:Prey, Gram+/Gram−, Fungi:Bacteria)."
        ),
        "variables": [
            {
                "key": "predator_prey",
                "label": "Predator : Prey",
                "note": (
                    "Community structure ratio reflecting predator-associated biomarkers relative to prey. "
                    "Often interpreted as a proxy for food-web complexity."
                ),
                "candidates": ["predator_prey", "predator_to_prey", "predator_prey_ratio"],
            },
            {
                "key": "gram_positive_gram_negative",
                "label": "Gram+ : Gram−",
                "note": (
                    "Ratio comparing Gram-positive to Gram-negative bacterial biomarkers. Often used to infer "
                    "shifts in carbon substrate use and community structure."
                ),
                "candidates": [
                    "gram_positive_gram_negative",
                    "gram_pos_gram_neg",
                    "gram_positive_to_gram_negative",
                    "gram_pos_to_gram_neg",
                ],
            },
            {
                "key": "fungi_bacteria",
                "label": "Fungi : Bacteria",
                "note": (
                    "Ratio comparing fungal to bacterial biomass. Often interpreted as an indicator of "
                    "carbon stabilization potential and longer-term soil development."
                ),
                "candidates": ["fungi_bacteria", "fungi_to_bacteria", "fungi_bacteria_ratio"],
            },
        ],
    },

    # ------------------------------------------------------------------
    # Set 4: Stress & Community Activity Ratios
    # ------------------------------------------------------------------
    {
        "group_key": "soilbio_set4_stress_activity",
        "group_label": "Set 4: Stress & Community Activity Ratios",
        "notes": (
            "Ward PLFA: Stress & activity ratios are physiological indicators derived from lipid profiles "
            "(e.g., Saturated:Unsaturated, Monounsaturated:Polyunsaturated, Pre16/Pre18 cyclo ratios)."
        ),
        "variables": [
            {
                "key": "sat_unsat",
                "label": "Saturated : Unsaturated",
                "note": (
                    "Physiological stress indicator based on saturated vs. unsaturated lipid proportions. "
                    "Often increases under stress conditions."
                ),
                "candidates": ["sat_unsat", "saturated_unsaturated", "saturated_to_unsaturated"],
            },
            {
                "key": "mono_poly",
                "label": "Monounsaturated : Polyunsaturated",
                "note": (
                    "Community activity / stress-related ratio comparing mono- vs polyunsaturated lipids. "
                    "Interpretation depends on ecosystem context."
                ),
                "candidates": ["mono_poly", "monounsaturated_polyunsaturated", "mono_to_poly"],
            },
            {
                "key": "pre16_pre18",
                "label": "Pre16 : Pre18 (Cyclo Ratio)",
                "note": (
                    "Cyclopropyl precursor ratio (Pre16/Pre18) used as a stress/community shift indicator "
                    "in some PLFA interpretation frameworks."
                ),
                "candidates": ["pre16_pre18", "pre16_to_pre18", "cyclo_pre16_pre18"],
            },
        ],
    },
]

# ------------------------------------------------------------
# Soil Chem: keep your curated set (already has units in labels)
# ------------------------------------------------------------
# ------------------------------------------------------------
# Soil Chem: grouped to parallel Soil Bio structure
# ------------------------------------------------------------

SOILCHEM_VARIABLE_GROUPS = [
    {
        "group_key": "basic_soil_chemistry",
        "group_label": "Basic Soil Chemistry (Ward Laboratories)",
        "group_note": (
            "Rows are STRIP 1–4 (0–8 in composite). Columns are sampling events "
            "(Ward report date). Values shown are strip means. "
            "S1/S2 and S3/S4 rows are ratios of the strip means (unitless)."
        ),
        "variables": [
            {
                "key": "soil_ph_1_1",
                "label": "Soil pH (1:1)",
                "candidates": ["soil_ph_1_1", "soil_ph", "ph_1_1"],
                "note": (
                    "pH measured on a 1:1 soil:water mixture. Higher values indicate "
                    "more alkaline soil; pH influences nutrient availability."
                ),
            },
            {
                "key": "organic_matter_loi_pct",
                "label": "Organic Matter (LOI, %)",
                "candidates": ["organic_matter_loi_pct", "organic_matter_pct"],
                "note": (
                    "Organic matter by Loss-on-Ignition (LOI). Reported as percent by mass. "
                    "Values are typically low in arid-region soils and can change slowly over time."
                ),
            },
            {
                "key": "nitrate_n_ppm_n",
                "label": "Nitrate-N (ppm N)",
                "candidates": ["nitrate_n_ppm_n", "nitrate_n_ppm", "no3_n_ppm"],
                "note": (
                    "Plant-available nitrate nitrogen. Units are ppm as N (mg/kg). "
                    "Often highly variable with recent fertilization, irrigation, and plant uptake."
                ),
            },
            {
                "key": "olsen_p_ppm",
                "label": "Olsen P (ppm P)",
                "candidates": ["olsen_p_ppm", "olsen_p_ppm_p"],
                "note": (
                    "Plant-available phosphorus via the Olsen extraction method (commonly used for "
                    "neutral to alkaline soils). Units are ppm P (mg/kg)."
                ),
            },
            {
                "key": "potassium_ppm_k",
                "label": "Potassium (ppm K)",
                "candidates": ["potassium_ppm_k", "potassium_ppm", "k_ppm"],
                "note": (
                    "Exchangeable potassium (K). Units are ppm K (mg/kg). "
                    "K is important for water regulation and stress tolerance."
                ),
            },
            {
                "key": "calcium_ppm_ca",
                "label": "Calcium (ppm Ca)",
                "candidates": ["calcium_ppm_ca", "calcium_ppm", "ca_ppm"],
                "note": (
                    "Exchangeable calcium (Ca). Units are ppm Ca (mg/kg). "
                    "Often high in calcareous/arid soils; supports soil structure and cation balance."
                ),
            },
            {
                "key": "cec_sum_of_cations",
                "label": "CEC / Sum of Cations (meq/100g)",
                "candidates": ["cec_sum_of_cations", "cec_meq_100g", "cec"],
                "note": (
                    "Cation Exchange Capacity (CEC), reported as meq/100g. "
                    "Higher CEC generally indicates greater nutrient-holding capacity; "
                    "often related to clay and organic matter."
                ),
            },
        ],
    }
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
    variables: List[Dict[str, Any]] | List[str],
    min_year: int = 2023,
    include_ratio_rows: bool = True,
) -> Dict[str, Any]:
    """
    Single-set payload matching the NIR-ish shape, plus rowLabels.
      {
        "periods": [...],
        "variables": [...],   # list of {key,label, note?}
        "rows": [...],
        "rowLabels": {...},
        "data": {...}         # { varKey: { rowKey: { periodKey: value|null } } }
      }

    Notes
    -----
    `variables` may be provided as either:
      - list[str] of machine column names (e.g., ["total_biomass", "predator_prey"])
      - list[dict] items. Supported dict shape:
          {"key": "...", "label": "...", "note": "...", "candidates": [...]}  (all optional except key)

    If `candidates` is not provided, we fall back to [key] as candidates.
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

    # ------------------------------------------------------------------
    # Normalize variables so callers can pass either:
    #   - list[str] of machine column names
    #   - list[{"key":..., "label":..., "note":..., "candidates":[...]}] dicts
    # Preserve optional metadata (note, candidates) for frontend rendering.
    # ------------------------------------------------------------------
    variables_norm: List[Dict[str, Any]] = []
    for v in variables:
        if isinstance(v, str):
            key = v
            variables_norm.append(
                {
                    "key": key,
                    "label": key,
                    "candidates": [key],
                }
            )
        elif isinstance(v, dict):
            if "key" not in v:
                raise ValueError(f"Variable dict missing 'key': {v}")

            key = str(v["key"])
            label = str(v.get("label", key))

            vv: Dict[str, Any] = {"key": key, "label": label}

            # candidates: optional explicit list of df column fallbacks
            candidates = v.get("candidates")
            if isinstance(candidates, (list, tuple)) and candidates:
                vv["candidates"] = [str(x) for x in candidates if str(x).strip()]
            else:
                vv["candidates"] = [key]

            # note: optional short explanatory note displayed above the table
            note = v.get("note")
            if isinstance(note, str) and note.strip():
                vv["note"] = note.strip()

            variables_norm.append(vv)
        else:
            raise TypeError(f"Unsupported variable spec (expected str or dict): {type(v)} {v}")

    out: Dict[str, Any] = {
        "periods": periods,
        # ✅ IMPORTANT: keep variable objects intact (so note/candidates survive)
        "variables": variables_norm,
        "rows": rows,
        "rowLabels": {k: ROW_LABELS.get(k, k) for k in rows},
        "data": {},
    }

    if not periods:
        # No periods -> return empty data blocks with correct var keys + row keys
        for v in variables_norm:
            out["data"][v["key"]] = {r: {} for r in rows}
        return out

    df["period_key"] = df[date_col].dt.date.astype(str)

    for v in variables_norm:
        var_key = v["key"]
        candidates = v.get("candidates") or [var_key]
        col = _pick_first_existing(df, candidates)

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
# ---------------------------------------------------------------------
# Builder (standard payload: {title, sets:[...]} )
# ---------------------------------------------------------------------

def build_soilbio_table(clean_csv: Path, min_year: int = 2023) -> Dict[str, Any]:
    """
    Returns the standard payload used by NIR / SoilChem / SoilBio tabs:
      {
        "title": "Soil Biological Health",
        "sets": [
          {
            "key": "...",
            "label": "...",
            "notes": "...",              # Ward-attributed explanatory text
            "periods": [...],
            "variables": [...],
            "rows": [...],
            "rowLabels": {...},
            "data": {...}
          },
          ...
        ]
      }
    """
    sets: List[Dict[str, Any]] = []

    for grp in SOILBIO_VARIABLE_GROUPS:
        payload = build_soil_table_payload(
            clean_csv=clean_csv,
            variables=grp["variables"],
            min_year=min_year,
            include_ratio_rows=True,   # adds S1/S2 and S3/S4 (if your helper supports this)
        )

        # Defensive: if helper returns {"title": ...} inside payload, drop it
        if isinstance(payload, dict) and "title" in payload:
            payload = {k: v for k, v in payload.items() if k != "title"}

        set_obj: Dict[str, Any] = {
            "key": grp["group_key"],
            "label": grp["group_label"],
            "notes": grp.get("notes", ""),
            **payload,
        }
        sets.append(set_obj)

    return {"title": "Soil Biological Health", "sets": sets}

def build_soilchem_table(clean_csv: Path, min_year: int = 2023) -> Dict[str, Any]:
    """
    NIR / SoilBio–parallel:
    Returns a grouped Soil Chemistry payload with one or more named sets.
    """
    sets: List[Dict[str, Any]] = []

    for grp in SOILCHEM_VARIABLE_GROUPS:
        payload = build_soil_table_payload(
            clean_csv=clean_csv,
            variables=grp["variables"],
            min_year=min_year,
            include_ratio_rows=True,
        )
        sets.append(
            {
                "key": grp["group_key"],
                "label": grp["group_label"],
                **payload,
            }
        )

    return {
        "title": "Soil Chemistry",
        "sets": sets,
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