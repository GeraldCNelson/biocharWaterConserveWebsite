#!/usr/bin/env python3
"""
tables_soil_bio.py

Soil Biological Health table builders.
"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List

from biochar_app.scripts.tables_soil_common import VariableSpec, build_soil_table_payload

# -----------------------------------------------------------------------------
# Soil Bio variable groups (copied from your existing soil_tables.py)
# -----------------------------------------------------------------------------
SOILBIO_VARIABLE_GROUPS = [
    {
        "group_key": "soilbio_micro_biomass",
        "group_label": "Microbial Biomass & Community",
        "notes": "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means. In most ecosystems, more life and diversity exists underground than above. The soil is home to a vast array of organisms, including bacteria, cyanobacteria, algae, protozoa, fungi, nematodes and mites, insects of all sizes, worms, small mammals and plant roots. oil biological processes are responsible for supplying approximately 75 percent of the plant-available nitrogen and 65 percent of the available phosphorus in the soil. Source: https://extension.umn.edu/soil-management-and-health/soil-biology#:~:text=In%20most%20ecosystems%2C%20more%20life,small%20mammals%20and%20plant%20roots.",
        "variables": [
            VariableSpec(
                key="total_biomass",
                label="Total Biomass (ng/g)",
                candidates=("total_biomass", "total_biomass_ng_per_g", "total_biomass_ng_g"),
                note=(
                    "Total microbial biomass from phospholipid fatty acids (PLFA) (ng/g). "
                    "These fatty acids are found in the cell membranes of living organisms. "
                    "Different groups of organisms have a unique composition of these PLFA fatty acids. "
                    "Measuring and quantifying PLFAs can provide a fingerprint of the soil food web. "
                    "For example, the cell membranes of fungi consist of different PLFAs than those associated with bacteria. "
                    "Source: https://www.eurofins-agro.com/en/plfa"
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
                note="Fungal-to-bacterial biomass ratio (unitless). Bacteria tend to dominate in systems with fewer organic inputs or residues possibly leading to a lower C:N ratio. In addition, bacteria can be more prominent in the early spring or late fall as soil temperatures are usually cooler and vegetation is less active or absent. Dry conditions, slightly alkaline to alkaline pH values, or increased land disturbance through prolonged and extensive tillage, grazing, or compaction may also favor bacteria. While bacteria are important and needed in the soil ecosystem, fungi are desired and more often considered indicators of good soil health. Increased use of cover crops and/or other organic inputs and less soil disturbance should help the soil support more fungi. Adjustments to pH may a",
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
                note="Actinobacteria (actinomycetes) biomass from PLFA (ng/g). Actinobacteria is one of the largest phylum under Bacteria domain and can be found in a wide range of terrestrial and aquatic ecosystems. Actinobacteria are Gram-positive bacteria with > 50% of guanine and cytosine (G + C) content in their DNA. Generally, actinobacteria are recognized as filamentous bacteria due to their ability to form substrate mycelium and aerial mycelium. The phylum Actinobacteria represents the most recognized group of microorganisms with the ability to produce bioactive compounds. Thus, actinobacteria have received great interest in various applications in pharmaceuticals, biotechnology, food industries, agriculture and in the enzyme industry. Source: https://www.sciencedirect.com/topics/biochemistry-genetics-and-molecular-biology/actinobacteria",
            ),
            VariableSpec(
                key="rhizobia_biomass",
                label="Rhizobia Biomass (ng/g)",
                candidates=("rhizobia_biomass", "rhizobia_ng_per_g"),
                note="Rhizobia biomass from PLFA (ng/g). Rhizobia are Gram-negative bacteria that fix nitrogen in soil and aid in the growth and development of plants. Rhizobia comes from two Greek words — 'rhiza' meaning 'root', and 'bios', meaning 'life' [1]. Rhizobia can only fix nitrogen when associated with a plant that provides it with carbohydrates and are only associated with legumes, but not all legumes associate with rhizobia. Source: https://soil.evs.buffalo.edu/index.php/Rhizobia#:~:text=Rhizobia%20are%20Gram%2Dnegative%20bacteria%20that%20fix%20nitrogen,but%20not%20all%20legumes%20associate%20with%20rhizobia.",
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
                note="Arbuscular mycorrhizal fungi biomass from PLFA (ng/g). Mycorrhiza refers to the symbiotic association between fungi and plant roots, which plays a critical role in nutrient availability, carbon supply, and community structure within various ecosystems. This interaction influences belowground carbon sequestration and is affected by environmental factors and biotic interactions. Source: https://www.sciencedirect.com/topics/agricultural-and-biological-sciences/mycorrhiza",
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
    key="pre_16_1w7c_cy17_0_pre_18_1w7c_cy19_0",
    label="Pre 16:1w7c : cy17:0 – Pre 18:1w7c : cy19:0",
    candidates=(
        # Preferred combined-name variants (if/when they exist)
        "pre_16_1w7c_cy17_0_pre_18_1w7c_cy19_0",
        "pre16_1w7c_cy17_0_pre18_1w7c_cy19_0",
        "pre_16_1w7c_cy17_0__pre_18_1w7c_cy19_0",
        "pre_16_1w7c_cy17_0_-_pre_18_1w7c_cy19_0",
        "pre_16_1w7c_cy17_0_pre18_1w7c_cy19_0",

        # Back-compat fallbacks if the CSV really only has one of the two
        "pre_16_1w7c_cy17_0",
        "pre_18_1w7c_cy19_0",
    ),
    note=(
        "Combined microbial stress indicator ratios (unitless): "
        "Pre 16:1w7c : cy17:0 and Pre 18:1w7c : cy19:0. "
        "Cyclo (cy) fatty acids are more prominent during stationary phases of growth "
        "or under higher stress conditions that influence membrane fluidity and growth rates "
        "(e.g., temperature, pH, moisture, nutrient availability). "
        "In general, higher precursor-to-cyclo ratios are often interpreted as an actively growing "
        "community experiencing fewer stressors. These ratios are typically higher early in the growing season "
        "and may decline toward harvest as growth slows and conditions change. Source: Ward Labs."
    ),
),
        VariableSpec(
            key="sat_unsat",
            label="Saturated : Unsaturated",
            candidates=("sat_unsat",),
            note="Ratio of saturated to unsaturated fatty acids (unitless). Bacteria alter their membranes under various environmental conditions in order to maintain optimal fluidity for nutrient and waste transport into and out of the cell. Saturated fatty acids may reflect a better adapted community to current environmental conditions. Communities under stressed conditions will increase their proportion of unsaturated fatty acids. This will likely occur most often as a result of low soil moisture or drastic changes in acids. This will likely occur most often as a result of low soil moisture or drastic changes in temperature. In general, a higher number indicates a healthier and more stable community. Source: Ward Labs.",
        ),
        VariableSpec(
            key="mono_poly",
            label="Monounsaturated : Polyunsaturated",
            candidates=("mono_poly",),
            note="Ratio of mono- to polyunsaturated fatty acids (unitless). The ratio of monounsaturated to polyunsaturated fatty acids is used along with the sat:unsat ratio to further indicate the degree of community stress. A higher ratio indicates less stress, while a lower ratio would depict higher levels of prolonged stress due to conditions such as temperature, moisture, pH, or nutrient availability (starvation). Source: Ward Labs.",
        ),
    ],
},
    {
        "group_key": "soilbio_community_ratios",
        "group_label": "Community Composition Ratios",
        "notes": "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means. All ratios should be looked at separately, but should also be taken into context and compared with one another to better understand the big picture. These are general guidelines and statements regarding soil microbial communities. In addition, the scales and ranges presented here are specific for the type of extraction and analytical methods used for PLFA analysis at Ward Laboratories, Inc. They will not necessarily reflect ranges derived from other methods of analysis or the literature. The scales can and should be adjusted slightly depending on the time of year and conditions at sampling along with the climate and soil type of specific regions where comparisons are being made. Conditions such as time of year, past and present crop, moisture, pH, and fertility regions where comparisons are being made. Conditions such as time of year, past and present crop, moisture, pH, and fertility should be noted or measured close to sampling for PLFA analysis for a more in depth interpretation of results. Source: Ward Labs.",
        "variables": [
            VariableSpec(
                key="predator_prey",
                label="Predator : Prey",
                candidates=("predator_prey", "predator_pre", "predator_prey_ratio"),
                note="Predator-to-prey ratio. This ratio is also expressed as protozoa to bacteria. Protozoa feed on bacteria which helps release nutrients, especially nitrogen. A higher ratio indicates an active community where base level nutrients are sufficient to support higher trophic levels or predators. However, this ratio will always be a relatively low number because the prey will greatly outnumber the predators. Source: Ward Labs",
            ),
            VariableSpec(
                key="gram_pos_gram",
                label="Gram+ : Gram−",
                # ✅ your CSV has gram_pos_gram
                candidates=("gram_pos_gram", "gram_pos_neg", "gram_pos_gram_neg", "gram_pos_to_neg"),
                note="Ratio of Gram+ to Gram− bacterial biomass (unitless). Gram (+) bacteria typically dominate early in the growing season and/or following a fallow period. They also survive better under certain environmental conditions or stressors such as drought or extreme temperatures due to their ability to form spores. Therefore, it is common to see higher values when the community is coming out of dormancy or is stressed. These values will typically begin to approach those of a more balanced bacterial community as the soil conditions become more favorable throughout the growing season. A gram (-) dominated soil may be due to anaerobic conditions or other stressors such as pesticide application or heavy metal contamination. Source: Ward Labs",
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


