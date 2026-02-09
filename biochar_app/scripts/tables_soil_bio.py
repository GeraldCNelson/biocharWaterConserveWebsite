#!/usr/bin/env python3
"""
tables_soil_bio.py

Soil Biological Health table builders.
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Set

from biochar_app.scripts.tables_soil_common import VariableSpec, build_soil_table_payload
from biochar_app.scripts.config import ( WARD_MASTER_NIR_CSV,
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_SOILCHEM_CSV,
    BIOMASS_FIELD_CSV,)

# -----------------------------------------------------------------------------
# Soil Bio variable groups (copied from your existing soil_tables.py)
# -----------------------------------------------------------------------------
SOILBIO_VARIABLE_GROUPS = [
    {
        "group_key": "soilbio_micro_biomass",
        "group_label": "Microbial Biomass & Community",
        "notes": "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means.",
        "variables": [
            VariableSpec(
                key="total_biomass",
                label="Total Biomass (ng/g)",
                candidates=("total_biomass", "total_biomass_ng_per_g", "total_biomass_ng_g"),
                note="Total microbial biomass from PLFA (ng/g).",
            ),
            VariableSpec(
                key="bacteria_biomass",
                label="Bacteria Biomass (ng/g)",
                # ✅ your CSV uses total_bacteria_biomass
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
                # ✅ your CSV uses total_fungi_biomass
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
                # ✅ your CSV uses actinomycetes_biomass
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
                # ✅ your CSV uses arbuscular_mycorrhizal_biomass
                # and also has a common misspelling arbusular_...
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

    # “Functional Groups” — updated to only variables that exist in your CSV
    {
        "group_key": "soilbio_functional_groups",
        "group_label": "Functional Groups",
        "notes": "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means.",
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
                # ✅ your CSV has gram_biomass (this is what your current data likely intends)
                candidates=("gram_biomass", "gram_neg_biomass", "gram_neg_ng_per_g", "gram_negative_ng_per_g"),
                note="Gram-negative bacterial biomass (ng/g).",
            ),
            VariableSpec(
                key="protozoan_biomass",
                label="Protozoan Biomass (ng/g)",
                # ✅ your CSV has protozoa_biomass
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
    "notes": "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means.",
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
            note="Stress indicator ratio (higher often interpreted as less stress / more active growth, depending on context).",
        ),
        VariableSpec(
            key="pre_18_1w7c_cy19_0",
            label="Pre 18:1w7c : cy19:0",
            candidates=("pre_18_1w7c_cy19_0",),
            note="Stress indicator ratio (see Ward interpretation).",
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
        "notes": "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means.",
        "variables": [
            VariableSpec(
                key="predator_prey",
                label="Predator : Prey",
                candidates=("predator_prey", "predator_pre", "predator_prey_ratio"),
                note="Predator-to-prey ratio. This ratio is also expressed as protozoa to bacteria. Protozoa feed on bacteria which helps release nutrients, especially nitrogen. A higher ratio indicates an active community where base level nutrients are sufficient to support higher trophic levels or predators. However, this ratio will always be a relatively low number because the prey will greatly outnumber the predators.",
            ),
            VariableSpec(
                key="gram_pos_gram",
                label="Gram+ : Gram−",
                # ✅ your CSV has gram_pos_gram
                candidates=("gram_pos_gram", "gram_pos_neg", "gram_pos_gram_neg", "gram_pos_to_neg"),
                note="Ratio of Gram+ to Gram− bacterial biomass (unitless). Gram (+) bacteria typically dominate early in the growing season and/or following a fallow period. They also survive better under certain environmental conditions or stressors such as drought or extreme temperatures due to their ability to form spores. Therefore, it is common to see higher values when the community is coming out of dormancy or is stressed. These values will typically begin to approach those of a more balanced bacterial community as the soil conditions become more favorable throughout the growing season. A gram (-) dominated soil may be due to anaerobic conditions or other stressors such as pesticide application or heavy metal contamination.",
            ),
        ],
    },
{
    "group_key": "soilbio_stress_diversity",
    "group_label": "Stress Indicators & Diversity",
    "notes": "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means.",
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
            note="Stress indicator ratio (higher often interpreted as less stress / more active growth, depending on context).",
        ),
        VariableSpec(
            key="pre_18_1w7c_cy19_0",
            label="Pre 18:1w7c : cy19:0",
            candidates=("pre_18_1w7c_cy19_0",),
            note="Stress indicator ratio (see Ward interpretation).",
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
]

# -----------------------------------------------------------------------------
# Public builder
# -----------------------------------------------------------------------------
from typing import Any, Dict, List
from pathlib import Path

# Assumes VariableSpec and build_soil_table_payload already exist in this module
# or are imported above.


def build_soilbio_table(clean_csv: Path, min_year: int = 2023) -> Dict[str, Any]:
    """
    Build Soil Biological Health payload.

    Fixes:
    - Shared explanatory note appears ONCE at the top-level (not duplicated per set).
    - Per-set notes are only included if they are truly different from the shared note.
    - Includes variables explicitly listed in SOILBIO_VARIABLE_GROUPS (e.g., diversity_index).
    """
    sets: List[Dict[str, Any]] = []

    # One shared note for the whole tab (rendered once by the frontend)
    top_note = "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means."

    for grp in SOILBIO_VARIABLE_GROUPS:
        payload = build_soil_table_payload(
            clean_csv=clean_csv,
            variables=grp["variables"],
            min_year=min_year,
            include_ratio_rows=True,
        )

        group_note = (grp.get("notes", "") or "").strip()

        # If the group note is the same as the shared note, omit it (prevents repetition)
        if group_note == top_note:
            group_note = ""

        set_obj: Dict[str, Any] = {
            "key": grp["group_key"],
            "label": grp["group_label"],
            # Only include per-set note fields if non-empty
            **({"note": group_note, "notes": group_note} if group_note else {}),
            **payload,
        }
        sets.append(set_obj)

    return {
        "title": "Soil Biological Health",
        "note": top_note,  # shared note rendered once at top
        "sets": sets,
    }


