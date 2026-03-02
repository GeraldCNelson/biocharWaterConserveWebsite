#!/usr/bin/env python3
"""
tables_soil_bio.py

Soil Biological Health table builders.

This file defines variable groups only.
Payload envelope conventions (top-level note + set building) are standardized via tables_common.py.
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
# Soil Bio variable groups
# (Keep your existing list here; I’m not changing the variable content itself.)
# -----------------------------------------------------------------------------
SOILBIO_VARIABLE_GROUPS: List[Dict[str, Any]] = [
    {
        "group_key": "soilbio_micro_biomass",
        "group_label": "Microbial Biomass & Community",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="total_biomass",
                label="Total Biomass (ng/g)",
                candidates=("total_biomass", "total_biomass_ng_per_g", "total_biomass_ng_g"),
                note=(
                    "Total microbial biomass from phospholipid fatty acids (PLFA) (ng/g). "
                    "PLFAs are found in cell membranes of living organisms; different groups "
                    "have characteristic PLFA fingerprints."
                ),
            ),
            VariableSpec(
                key="bacteria_biomass",
                label="Bacteria Biomass (ng/g)",
                candidates=(
                    "total_bacteria_biomass",
                    "bacteria_biomass",
                    "total_bacteria_ng_per_g",
                    "bacteria_biomass_ng_per_g",
                    "bacteria_ng_per_g",
                ),
                note="Bacterial biomass from PLFA (ng/g).",
            ),
            VariableSpec(
                key="fungi_biomass",
                label="Fungi Biomass (ng/g)",
                candidates=(
                    "total_fungi_biomass",
                    "fungi_biomass",
                    "total_fungi_ng_per_g",
                    "fungi_biomass_ng_per_g",
                    "fungi_ng_per_g",
                ),
                note="Fungal biomass from PLFA (ng/g).",
            ),
            VariableSpec(
                key="fungi_bacteria",
                label="Fungi : Bacteria",
                candidates=("fungi_bacteria", "fungi_bacteria_ratio"),
                note="Fungal-to-bacterial biomass ratio (unitless).",
            ),
            VariableSpec(
                key="actinobacteria_biomass",
                label="Actinobacteria Biomass (ng/g)",
                candidates=(
                    "actinomycetes_biomass",
                    "actinobacteria_biomass",
                    "actino_biomass",
                    "actinobacteria_ng_per_g",
                ),
                note="Actinobacteria (actinomycetes) biomass from PLFA (ng/g).",
            ),
            VariableSpec(
                key="rhizobia_biomass",
                label="Rhizobia Biomass (ng/g)",
                candidates=("rhizobia_biomass", "rhizobia_ng_per_g"),
                note="Rhizobia biomass from PLFA (ng/g).",
            ),
            VariableSpec(
                key="mycorrhizae_biomass",
                label="Mycorrhizae Biomass (ng/g)",
                candidates=(
                    "arbuscular_mycorrhizal_biomass",
                    "arbusular_mycorrhizal_biomass",
                    "mycorrhizae_biomass",
                    "mycorrhizae_ng_per_g",
                ),
                note="Arbuscular mycorrhizal fungi biomass from PLFA (ng/g).",
            ),
        ],
    },
    {
        "group_key": "soilbio_functional_groups",
        "group_label": "Functional Groups",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="gram_pos_biomass",
                label="Gram+ Biomass (ng/g)",
                candidates=("gram_pos_biomass", "gram_pos_ng_per_g", "gram_positive_ng_per_g"),
                note="Gram-positive bacterial biomass (ng/g).",
            ),
            VariableSpec(
                key="gram_neg_biomass",
                label="Gram− Biomass (ng/g)",
                candidates=("gram_biomass", "gram_neg_biomass", "gram_neg_ng_per_g", "gram_negative_ng_per_g"),
                note="Gram-negative bacterial biomass (ng/g).",
            ),
            VariableSpec(
                key="protozoan_biomass",
                label="Protozoan Biomass (ng/g)",
                candidates=("protozoa_biomass", "protozoan_biomass", "protozoan_ng_per_g"),
                note="Protozoan biomass (ng/g).",
            ),
            VariableSpec(
                key="saprophytes_biomass",
                label="Saprophytes Biomass (ng/g)",
                candidates=("saprophytes_biomass",),
                note="Saprophytic fungi biomass (ng/g).",
            ),
            VariableSpec(
                key="undifferentiated_biomass",
                label="Undifferentiated Biomass (ng/g)",
                candidates=("undifferentiated_biomass",),
                note="Undifferentiated biomass (ng/g).",
            ),
        ],
    },
    {
        "group_key": "soilbio_stress_diversity",
        "group_label": "Stress Indicators & Diversity",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="diversity_index",
                label="Diversity Index",
                candidates=("diversity_index",),
                note="PLFA diversity index (unitless).",
            ),
            VariableSpec(
                key="pre_16_1w7c_cy17_0",
                label="Pre 16:1w7c : cy17:0",
                candidates=("pre_16_1w7c_cy17_0",),
                note="Stress indicator ratio (interpret in context).",
            ),
            VariableSpec(
                key="pre_18_1w7c_cy19_0",
                label="Pre 18:1w7c : cy19:0",
                candidates=("pre_18_1w7c_cy19_0",),
                note="Stress indicator ratio (interpret in context).",
            ),
            VariableSpec(
                key="sat_unsat",
                label="Saturated : Unsaturated",
                candidates=("sat_unsat",),
                note="Ratio of saturated to unsaturated fatty acids (unitless).",
            ),
            VariableSpec(
                key="mono_poly",
                label="Monounsaturated : Polyunsaturated",
                candidates=("mono_poly",),
                note="Ratio of mono- to polyunsaturated fatty acids (unitless).",
            ),
        ],
    },
    {
        "group_key": "soilbio_community_ratios",
        "group_label": "Community Composition Ratios",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="predator_prey",
                label="Predator : Prey",
                candidates=("predator_prey", "predator_pre", "predator_prey_ratio"),
                note="Often expressed as protozoa:bacteria; interpret in context.",
            ),
            VariableSpec(
                key="gram_pos_gram",
                label="Gram+ : Gram−",
                candidates=("gram_pos_gram", "gram_pos_neg", "gram_pos_gram_neg", "gram_pos_to_neg"),
                note="Ratio of Gram+ to Gram− bacterial biomass (unitless).",
            ),
        ],
    },
]


# -----------------------------------------------------------------------------
# Public builder
# -----------------------------------------------------------------------------
def build_soilbio_table(clean_csv: Path, min_year: int = 2023) -> Dict[str, Any]:
    def _builder(grp: Dict[str, Any]) -> Dict[str, Any]:
        return build_soil_table_payload(
            clean_csv=clean_csv,
            variables=grp["variables"],
            min_year=min_year,
            include_ratio_rows=True,
        )

    return build_grouped_tab_payload(
        title="Soil Biological Health",
        top_note=SOIL_TABLE_TOP_NOTE,
        groups=SOILBIO_VARIABLE_GROUPS,
        build_payload_for_group=_builder,
        include_display_labels=False,
    )