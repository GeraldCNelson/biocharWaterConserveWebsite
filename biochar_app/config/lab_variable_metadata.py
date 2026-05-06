"""

Central metadata registry for lab variables used in the Biochar Fruita CSU dashboard.

This file should describe existing standardized column names.

Do not rename source columns here.

"""

from __future__ import annotations

LAB_VARIABLE_METADATA = {

    "total_biomass": {
        "display_label": "Total Living Microbial Biomass",
        "dataset_family": "soil_biology_plfa",
        "group": "Microbial Biomass",
        "units": "ng/g",
        "value_type": "biomass",
        "definition": "Total living microbial biomass estimated from PLFA biomarkers.",
        "interpretation_note": "Higher values generally indicate greater living microbial biomass.",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": ["Total Biomass", "Total Living Microbial Biomass"],
        "related_terms": ["plfa", "microbial_biomass"],
    },

    "total_bacteria_biomass": {
        "display_label": "Total Bacterial Biomass",
        "source_label": "Total Bacteria",
        "dataset_family": "soil_biology_plfa",
        "group": "Functional Group Biomass",
        "units": "ng/g",
        "value_type": "biomass",
        "definition": "Estimated living bacterial biomass based on PLFA biomarkers.",
        "interpretation_note": "Higher values generally indicate greater bacterial biomass.",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": ["total bacteria", "bacterial biomass"],
        "related_terms": ["bacteria", "plfa", "microbial_biomass"],
    },

    "bacteria_pct": {
        "display_label": "Bacteria (%)",
        "source_label": "Bacteria %",
        "dataset_family": "soil_biology_plfa",
        "group": "Relative Abundance",
        "units": "%",
        "value_type": "percent",
        "definition": "Bacterial PLFA abundance as a percentage of total microbial biomass.",
        "interpretation_note": "This is a relative abundance measure, not an absolute biomass value.",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": ["bacteria percent", "bacterial relative abundance"],
        "related_terms": ["bacteria", "relative_abundance", "plfa"],
    },
    "fungi_bacteria": {
        "display_label": "Fungi:Bacteria Ratio",
        "source_label": "Fungi:Bacteria",
        "dataset_family": "soil_biology_plfa",
        "group": "Ratios and Indices",
        "units": None,
        "value_type": "ratio",
        "definition": "Ratio of fungal biomass to bacterial biomass.",
        "interpretation_note": "Higher values indicate greater fungal dominance relative to bacteria.",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": ["fungi bacteria ratio", "fungal bacterial ratio"],
        "related_terms": ["fungi", "bacteria", "plfa_ratio"],
    },
    "predator_prey": {
        "display_label": "Predator:Prey Ratio",
        "source_label": "Predator:Prey",
        "dataset_family": "soil_biology_plfa",
        "group": "Ratios and Indices",
        "units": None,
        "value_type": "ratio",
        "definition": "Ratio of predator biomass to prey biomass.",
        "interpretation_note": "",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": [],
        "related_terms": [],
        },
    "predator_prey": {
        "display_label": "Predator:Prey Ratio",
        "source_label": "Predator:Prey",
        "dataset_family": "soil_biology_plfa",
        "group": "Ratios and Indices",
        "units": None,
        "value_type": "ratio",
        "definition": "Ratio of predator biomass to prey biomass.",
        "interpretation_note": "",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": [],
        "related_terms": [],
        },

    "gram_pos_biomass": {
        "display_label": "Gram Positive Bacterial Biomass",
        "source_label": "Gram +",
        "dataset_family": "soil_biology_plfa",
        "group": "Functional Group Biomass",
        "units": "ng/g",
        "value_type": "biomass",
        "definition": "Estimated biomass of gram-positive bacteria based on PLFA biomarkers.",
        "interpretation_note": "Higher values indicate greater abundance of gram-positive bacterial biomass.",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": ["gram positive biomass", "gram positive bacteria"],
        "related_terms": ["bacteria", "plfa", "microbial_biomass"],
    },
    "gram_pos_pct": {
        "display_label": "Gram Positive Bacteria (%)",
        "source_label": "Gram + %",
        "dataset_family": "soil_biology_plfa",
        "group": "Relative Abundance",
        "units": "%",
        "value_type": "percent",
        "definition": "Relative abundance of gram-positive bacteria as a percentage of total microbial biomass.",
        "interpretation_note": "This represents proportional abundance rather than absolute biomass.",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": ["gram positive percent"],
        "related_terms": ["bacteria", "relative_abundance", "plfa"],
    },
    "saturated": {
        "display_label": "Saturated Fatty Acid Biomass",
        "source_label": "Saturated",
        "dataset_family": "soil_biology_plfa",
        "group": "Fatty Acid Pools",
        "units": "ng/g",
        "value_type": "biomass",
        "definition": "Estimated biomass associated with saturated fatty acid PLFA biomarkers.",
        "interpretation_note": "Used in PLFA stress and community structure interpretation.",
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": ["saturated fatty acids"],
        "related_terms": ["plfa", "fatty_acids"],
    },
    "unsaturated": {
        "display_label": "Unsaturated Fatty Acid Biomass",
        "source_label": "Unsaturated",
        "dataset_family": "soil_biology_plfa",
        "group": "Fatty Acid Pools",
        "units": "ng/g",
        "value_type": "biomass",
        "definition": (
            "Estimated biomass associated with unsaturated fatty acid "
            "PLFA biomarkers."
        ),
        "interpretation_note": (
            "Unsaturated fatty acid pools are commonly used in microbial "
            "community structure and physiological stress interpretation."
        ),
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": [
            "unsaturated fatty acids",
            "unsaturated PLFA pool",
        ],
        "related_terms": [
            "saturated",
            "monounsaturated",
            "polyunsaturated",
            "saturated_unsaturated_ratio",
            "monounsaturated_polyunsaturated_ratio",
            "plfa",
        ],
    },
    "monounsaturated": {
        "display_label": "Monounsaturated Fatty Acid Biomass",
        "source_label": "MonoUnsaturated",
        "dataset_family": "soil_biology_plfa",
        "group": "Fatty Acid Pools",
        "units": "ng/g",
        "value_type": "biomass",
        "definition": (
            "Estimated biomass associated with monounsaturated fatty acid "
            "PLFA biomarkers."
        ),
        "interpretation_note": (
            "Monounsaturated fatty acid pools may reflect shifts in microbial "
            "community composition and physiological condition."
        ),
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": [
            "mono unsaturated",
            "monounsaturated fatty acids",
        ],
        "related_terms": [
            "unsaturated",
            "polyunsaturated",
            "monounsaturated_polyunsaturated_ratio",
            "plfa",
        ],
    },
    "polyunsaturated": {
        "display_label": "Polyunsaturated Fatty Acid Biomass",
        "source_label": "PolyUnsaturated",
        "dataset_family": "soil_biology_plfa",
        "group": "Fatty Acid Pools",
        "units": "ng/g",
        "value_type": "biomass",
        "definition": (
            "Estimated biomass associated with polyunsaturated fatty acid "
            "PLFA biomarkers."
        ),
        "interpretation_note": (
            "Polyunsaturated fatty acid pools are often associated with fungal "
            "and eukaryotic microbial components."
        ),
        "source_reference_group": "Ward PLFA Biological Report",
        "aliases": [
            "poly unsaturated",
            "polyunsaturated fatty acids",
        ],
        "related_terms": [
            "unsaturated",
            "monounsaturated",
            "monounsaturated_polyunsaturated_ratio",
            "plfa",
        ],
    },


}

def get_lab_variable_metadata(key: str) -> dict:
    return LAB_VARIABLE_METADATA.get(key, {})

def get_display_label(key: str) -> str:
    return LAB_VARIABLE_METADATA.get(key, {}).get("display_label", key)

def get_units(key: str) -> str | None:
    return LAB_VARIABLE_METADATA.get(key, {}).get("units")