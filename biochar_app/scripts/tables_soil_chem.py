#!/usr/bin/env python3
"""
tables_soil_chem.py

Soil Chemistry table builders (FULL set).

Notes:
- This file defines variable groups only.
- Payload shape conventions (top-level note + set building) are standardized via tables_common.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from biochar_app.scripts.tables_common import build_grouped_tab_payload
from biochar_app.scripts.tables_soil_common import VariableSpec, build_soil_table_payload


# -----------------------------------------------------------------------------
# Shared top-level note (STANDARD)
# -----------------------------------------------------------------------------
SOIL_TABLE_TOP_NOTE = "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means."


# -----------------------------------------------------------------------------
# Soil Chem variable groups
# -----------------------------------------------------------------------------
SOILCHEM_VARIABLE_GROUPS: List[Dict[str, Any]] = [
    {
        "group_key": "soilchem_ph_salinity_lime",
        "group_label": "pH / Salinity / Lime",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="soil_ph_1_1",
                label="Soil pH (1:1)",
                candidates=("1_1_soil_ph", "soil_ph_1_1", "soil_ph_1:1", "ph_1_1", "ph_1:1"),
                note="Soil pH measured in a 1:1 soil:water slurry.",
            ),
            VariableSpec(
                key="buffer_ph",
                label="Buffer pH (WDRF)",
                candidates=("wdrf_buffer_ph", "buffer_ph"),
                note="Buffer pH (WDRF) as provided in the compiled dataset.",
            ),
            VariableSpec(
                key="ec_1_1",
                label="EC / Salts (mmho/cm, 1:1)",
                candidates=("1_1_s_salts_mmho_cm", "ec_1_1", "ec_1:1", "ec"),
                note="Electrical conductivity / soluble salts in a 1:1 soil:water slurry (mmho/cm).",
            ),
            VariableSpec(
                key="excess_lime",
                label="Excess Lime",
                candidates=("excess_lime",),
                note="Excess lime indicator as reported by the laboratory.",
            ),
        ],
    },
    {
        "group_key": "soilchem_organic_macros",
        "group_label": "Organic Matter & Macronutrients",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="organic_matter_loi_pct",
                label="Organic matter (LOI, percent)",
                candidates=("organic_matter_loi_pct",),
                note="Organic matter by loss-on-ignition (LOI).",
            ),
            VariableSpec(
                key="olsen_p_ppm_p",
                label="Olsen phosphorus (ppm as P)",
                candidates=("olsen_p_ppm_p", "olsen_p"),
                note="Olsen extractable phosphorus (ppm as P).",
            ),
            VariableSpec(
                key="potassium_ppm_k",
                label="Potassium (ppm as K)",
                candidates=("potassium_ppm_k", "k_ppm"),
                note="Exchangeable potassium concentration.",
            ),
            VariableSpec(
                key="sulfate_s_ppm_s",
                label="Sulfate sulfur (ppm as S)",
                candidates=("sulfate_s_ppm_s",),
                note="Sulfate sulfur concentration.",
            ),
            VariableSpec(
                key="nitrate_n_ppm",
                label="Nitrate-N (ppm)",
                candidates=("nitrate_n_ppm", "nitrate_n", "no3_n_ppm"),
                note="Nitrate nitrogen concentration.",
            ),
        ],
    },
    {
        "group_key": "soilchem_micros",
        "group_label": "Micronutrients (ppm)",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec("zinc_ppm_zn", "Zinc (ppm as Zn)", ("zinc_ppm_zn",)),
            VariableSpec("iron_ppm_fe", "Iron (ppm as Fe)", ("iron_ppm_fe",)),
            VariableSpec("manganese_ppm_mn", "Manganese (ppm as Mn)", ("manganese_ppm_mn",)),
            VariableSpec("copper_ppm_cu", "Copper (ppm as Cu)", ("copper_ppm_cu",)),
        ],
    },
    {
        "group_key": "soilchem_base_cations",
        "group_label": "Base Cations (ppm)",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec("calcium_ppm_ca", "Calcium (ppm as Ca)", ("calcium_ppm_ca",)),
            VariableSpec("magnesium_ppm_mg", "Magnesium (ppm as Mg)", ("magnesium_ppm_mg",)),
            VariableSpec("sodium_ppm_na", "Sodium (ppm as Na)", ("sodium_ppm_na",)),
        ],
    },
    {
        "group_key": "soilchem_cec_saturation",
        "group_label": "CEC & Base Saturation",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="cec_meq_100g",
                label="Cation exchange capacity (meq/100g)",
                candidates=("cec_meq_100g",),
                note="Cation exchange capacity.",
            ),
            VariableSpec(
                key="cec_sum_of_cations_me_100g",
                label="CEC / Sum of cations (me/100g)",
                candidates=("cec_sum_of_cations_me_100g",),
                note="Ward-reported combined CEC / Sum of cations.",
            ),
            VariableSpec("pcth_sat", "Hydrogen saturation (percent)", ("pcth_sat",)),
            VariableSpec("pctk_sat", "Potassium saturation (percent)", ("pctk_sat",)),
            VariableSpec("pctca_sat", "Calcium saturation (percent)", ("pctca_sat",)),
            VariableSpec("pctmg_sat", "Magnesium saturation (percent)", ("pctmg_sat",)),
            VariableSpec("pctna_sat", "Sodium saturation (percent)", ("pctna_sat",)),
        ],
    },
    {
        "group_key": "soilchem_crop_recs",
        "group_label": "Crop & Fertility Recommendations",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec("yg_1", "Yield goal", ("yg_1",)),
            VariableSpec("nitrogen_rec", "Nitrogen recommendation", ("nitrogen_rec",)),
            VariableSpec("p2o5_rec", "Phosphorus pentoxide recommendation", ("p2o5_rec",)),
            VariableSpec("k2o_rec", "Potassium oxide recommendation", ("k2o_rec",)),
            VariableSpec("sulfur_rec", "Sulfur recommendation", ("sulfur_rec",)),
            VariableSpec("zinc_rec", "Zinc recommendation", ("zinc_rec",)),
            VariableSpec("magnesium_rec", "Magnesium recommendation", ("magnesium_rec",)),
            VariableSpec("iron_rec", "Iron recommendation", ("iron_rec",)),
            VariableSpec("manganese_rec", "Manganese recommendation", ("manganese_rec",)),
            VariableSpec("copper_rec", "Copper recommendation", ("copper_rec",)),
        ],
    },
    {
        "group_key": "soilchem_soil_health",
        "group_label": "Soil Health & Water Extract",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec("h2o_no3_n", "H₂O nitrate-N", ("h2o_no3_n",)),
            VariableSpec("h2o_nh4_n", "H₂O ammonium-N", ("h2o_nh4_n",)),
            VariableSpec("total_n_h2o_ppm_n", "Total N (H₂O, ppm as N)", ("total_n_h2o_ppm_n",)),
            VariableSpec("organic_c_h2o_ppm", "Organic C (H₂O, ppm)", ("organic_c_h2o_ppm",)),
            VariableSpec("organic_n_h2o_ppm", "Organic N (H₂O, ppm)", ("organic_n_h2o_ppm",)),
            VariableSpec("organic_c_n_h2o", "Organic C:N (H₂O)", ("organic_c_n_h2o",)),
            VariableSpec("co2_soil_respiration", "CO₂ soil respiration", ("co2_soil_respiration",)),
            VariableSpec("water_stable_aggregates_mod", "Water-stable aggregates (modified)", ("water_stable_aggregates_mod",)),
            VariableSpec("soil_health_score", "Soil health score", ("soil_health_score",)),
            VariableSpec("microbially_active_carbon_pctma", "Microbially active carbon (percent MA)", ("microbially_active_carbon_pctma",)),
            VariableSpec("organic_nitrogen_release_ppm_n", "Organic nitrogen release (ppm as N)", ("organic_nitrogen_release_ppm_n",)),
            VariableSpec("organic_nitrogen_reserve_ppm_n", "Organic nitrogen reserve (ppm as N)", ("organic_nitrogen_reserve_ppm_n",)),
        ],
    },
]


# -----------------------------------------------------------------------------
# Public builder
# -----------------------------------------------------------------------------
def build_soilchem_table(clean_csv: Path, min_year: int = 2023) -> Dict[str, Any]:
    def _builder(grp: Dict[str, Any]) -> Dict[str, Any]:
        return build_soil_table_payload(
            clean_csv=clean_csv,
            variables=grp["variables"],
            min_year=min_year,
            include_ratio_rows=True,
        )

    return build_grouped_tab_payload(
        title="Soil Chemistry",
        top_note=SOIL_TABLE_TOP_NOTE,
        groups=SOILCHEM_VARIABLE_GROUPS,
        build_payload_for_group=_builder,
        include_display_labels=True,
    )