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

from biochar_app.scripts.tables.tables_common import build_grouped_tab_payload
from biochar_app.scripts.tables.tables_soil_common import VariableSpec, build_soil_table_payload


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
                reference_key="ph",
            ),
            VariableSpec(
                key="buffer_ph",
                label="Buffer pH (WDRF)",
                candidates=("wdrf_buffer_ph", "buffer_ph"),
                note="Buffer pH (WDRF) as provided in the compiled dataset.",
                reference_key="buffer_ph",
            ),
            VariableSpec(
                key="ec_1_1",
                label="EC / Salts (mmho/cm, 1:1)",
                candidates=("1_1_s_salts_mmho_cm", "ec_1_1", "ec_1:1", "ec"),
                note="Electrical conductivity / soluble salts in a 1:1 soil:water slurry (mmho/cm).",
                reference_key="salinity",
            ),
            VariableSpec(
                key="excess_lime",
                label="Excess Lime",
                candidates=("excess_lime",),
                note="Excess lime indicator as reported by the laboratory.",
                reference_key="excess_lime",
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
                reference_key="organic_matter",
            ),
            VariableSpec(
                key="olsen_p_ppm_p",
                label="Olsen phosphorus (ppm as P)",
                candidates=("olsen_p_ppm_p", "olsen_p"),
                note="Olsen extractable phosphorus (ppm as P).",
                reference_key="phosphorus",
            ),
            VariableSpec(
                key="potassium_ppm_k",
                label="Potassium (ppm as K)",
                candidates=("potassium_ppm_k", "k_ppm"),
                note="Exchangeable potassium concentration.",
                reference_key="potassium",
            ),
            VariableSpec(
                key="sulfate_s_ppm_s",
                label="Sulfate sulfur (ppm as S)",
                candidates=("sulfate_s_ppm_s",),
                note="Sulfate sulfur concentration.",
                reference_key="sulfur",
            ),
            VariableSpec(
                key="nitrate_n_ppm",
                label="Nitrate-N (ppm)",
                candidates=("nitrate_n_ppm", "nitrate_n", "no3_n_ppm"),
                note="Nitrate nitrogen concentration.",
                reference_key="nitrate",
            ),
        ],
    },
    {
        "group_key": "soilchem_micros",
        "group_label": "Micronutrients (ppm)",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="zinc_ppm_zn",
                label="Zinc (ppm as Zn)",
                candidates=("zinc_ppm_zn",),
                note="Zinc concentration.",
                reference_key="zinc",
            ),
            VariableSpec(
                key="iron_ppm_fe",
                label="Iron (ppm as Fe)",
                candidates=("iron_ppm_fe",),
                note="Iron concentration.",
                reference_key="iron",
            ),
            VariableSpec(
                key="manganese_ppm_mn",
                label="Manganese (ppm as Mn)",
                candidates=("manganese_ppm_mn",),
                note="Manganese concentration.",
                reference_key="manganese",
            ),
            VariableSpec(
                key="copper_ppm_cu",
                label="Copper (ppm as Cu)",
                candidates=("copper_ppm_cu",),
                note="Copper concentration.",
                reference_key="copper",
            ),
        ],
    },
    {
        "group_key": "soilchem_base_cations",
        "group_label": "Base Cations (ppm)",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="calcium_ppm_ca",
                label="Calcium (ppm as Ca)",
                candidates=("calcium_ppm_ca",),
                note="Calcium concentration.",
                reference_key="calcium",
            ),
            VariableSpec(
                key="magnesium_ppm_mg",
                label="Magnesium (ppm as Mg)",
                candidates=("magnesium_ppm_mg",),
                note="Magnesium concentration.",
                reference_key="magnesium",
            ),
            VariableSpec(
                key="sodium_ppm_na",
                label="Sodium (ppm as Na)",
                candidates=("sodium_ppm_na",),
                note="Sodium concentration.",
                reference_key="sodium",
            ),
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
                reference_key="cec",
            ),
            VariableSpec(
                key="cec_sum_of_cations_me_100g",
                label="CEC / Sum of cations (me/100g)",
                candidates=("cec_sum_of_cations_me_100g",),
                note="Ward-reported combined CEC / Sum of cations.",
                reference_key="cec",
            ),
            VariableSpec(
                key="pcth_sat",
                label="Hydrogen saturation (percent)",
                candidates=("pcth_sat",),
                note="Hydrogen saturation percentage.",
                reference_key="base_saturation",
            ),
            VariableSpec(
                key="pctk_sat",
                label="Potassium saturation (percent)",
                candidates=("pctk_sat",),
                note="Potassium saturation percentage.",
                reference_key="base_saturation",
            ),
            VariableSpec(
                key="pctca_sat",
                label="Calcium saturation (percent)",
                candidates=("pctca_sat",),
                note="Calcium saturation percentage.",
                reference_key="base_saturation",
            ),
            VariableSpec(
                key="pctmg_sat",
                label="Magnesium saturation (percent)",
                candidates=("pctmg_sat",),
                note="Magnesium saturation percentage.",
                reference_key="base_saturation",
            ),
            VariableSpec(
                key="pctna_sat",
                label="Sodium saturation (percent)",
                candidates=("pctna_sat",),
                note="Sodium saturation percentage.",
                reference_key="base_saturation",
            ),
        ],
    },
    {
        "group_key": "soilchem_crop_recs",
        "group_label": "Crop & Fertility Recommendations",
        "notes": "Units are actual pounds of plant nutrient per acre for all nutrients.",
        "variables": [
            VariableSpec(
                key="yg_1",
                label="Yield goal (short tons per acre)",
                candidates=("yg_1",),
                note="Yield goal in short tons per acre.",
                reference_key=None,
            ),
            VariableSpec(
                key="nitrogen_rec",
                label="Nitrogen recommendation",
                candidates=("nitrogen_rec",),
                note="Ward-reported nitrogen recommendation.",
                reference_key="nitrate",
            ),
            VariableSpec(
                key="p2o5_rec",
                label="Phosphorus pentoxide recommendation",
                candidates=("p2o5_rec",),
                note="Ward-reported phosphorus pentoxide recommendation.",
                reference_key="phosphorus",
            ),
            VariableSpec(
                key="k2o_rec",
                label="Potassium oxide recommendation",
                candidates=("k2o_rec",),
                note="Ward-reported potassium oxide recommendation.",
                reference_key="potassium",
            ),
            VariableSpec(
                key="sulfur_rec",
                label="Sulfur recommendation",
                candidates=("sulfur_rec",),
                note="Ward-reported sulfur recommendation.",
                reference_key="sulfur",
            ),
            VariableSpec(
                key="zinc_rec",
                label="Zinc recommendation",
                candidates=("zinc_rec",),
                note="Ward-reported zinc recommendation.",
                reference_key="zinc",
            ),
            VariableSpec(
                key="magnesium_rec",
                label="Magnesium recommendation",
                candidates=("magnesium_rec",),
                note="Ward-reported magnesium recommendation.",
                reference_key="magnesium",
            ),
            VariableSpec(
                key="iron_rec",
                label="Iron recommendation",
                candidates=("iron_rec",),
                note="Ward-reported iron recommendation.",
                reference_key="iron",
            ),
            VariableSpec(
                key="manganese_rec",
                label="Manganese recommendation",
                candidates=("manganese_rec",),
                note="Ward-reported manganese recommendation.",
                reference_key="manganese",
            ),
            VariableSpec(
                key="copper_rec",
                label="Copper recommendation",
                candidates=("copper_rec",),
                note="Ward-reported copper recommendation.",
                reference_key="copper",
            ),
        ],
    },
    {
        "group_key": "soilchem_soil_health",
        "group_label": "Soil Health & Water Extract",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="h2o_no3_n",
                label="H₂O nitrate-N",
                candidates=("h2o_no3_n",),
                note="Water-extractable nitrate-N.",
                reference_key="nitrate",
            ),
            VariableSpec(
                key="h2o_nh4_n",
                label="H₂O ammonium-N",
                candidates=("h2o_nh4_n",),
                note="Water-extractable ammonium-N.",
                reference_key=None,
            ),
            VariableSpec(
                key="total_n_h2o_ppm_n",
                label="Total N (H₂O, ppm as N)",
                candidates=("total_n_h2o_ppm_n",),
                note="Total water-extractable nitrogen.",
                reference_key=None,
            ),
            VariableSpec(
                key="organic_c_h2o_ppm",
                label="Organic C (H₂O, ppm)",
                candidates=("organic_c_h2o_ppm",),
                note="Water-extractable organic carbon.",
                reference_key="weoc",
            ),
            VariableSpec(
                key="organic_n_h2o_ppm",
                label="Organic N (H₂O, ppm)",
                candidates=("organic_n_h2o_ppm",),
                note="Water-extractable organic nitrogen.",
                reference_key="weon",
            ),
            VariableSpec(
                key="organic_c_n_h2o",
                label="Organic C:N (H₂O)",
                candidates=("organic_c_n_h2o",),
                note="Water-extractable organic carbon to organic nitrogen ratio.",
                reference_key="organic_cn",
            ),
            VariableSpec(
                key="co2_soil_respiration",
                label="CO₂ soil respiration",
                candidates=("co2_soil_respiration",),
                note="Soil respiration measured as CO₂ release.",
                reference_key="soil_respiration",
            ),
            VariableSpec(
                key="water_stable_aggregates_mod",
                label="Water-stable aggregates (modified)",
                candidates=("water_stable_aggregates_mod",),
                note="Modified water-stable aggregates measurement.",
                reference_key="water_stable_aggregates",
            ),
            VariableSpec(
                key="soil_health_score",
                label="Soil health score",
                candidates=("soil_health_score",),
                note="Composite soil health score.",
                reference_key="soil_health_score",
            ),
            VariableSpec(
                key="microbially_active_carbon_pctma",
                label="Microbially active carbon (percent MA)",
                candidates=("microbially_active_carbon_pctma",),
                note="Microbially active carbon as percent MA.",
                reference_key="mac",
            ),
            VariableSpec(
                key="organic_nitrogen_release_ppm_n",
                label="Organic nitrogen release (ppm as N)",
                candidates=("organic_nitrogen_release_ppm_n",),
                note="Organic nitrogen release potential.",
                reference_key=None,
            ),
            VariableSpec(
                key="organic_nitrogen_reserve_ppm_n",
                label="Organic nitrogen reserve (ppm as N)",
                candidates=("organic_nitrogen_reserve_ppm_n",),
                note="Organic nitrogen reserve.",
                reference_key=None,
            ),
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