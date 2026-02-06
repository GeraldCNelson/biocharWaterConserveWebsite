"""
biochar_app.config.table_specs

Central "hard-wired" set labels/keys for the wide-table tabs (NIR, Soil Bio, Soil Chem, Biomass).
The goal: Future Jerry can edit labels/ordering here, without hunting through routes.py + tables_*.py.

This file is intentionally lightweight: only constants / simple metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

@dataclass(frozen=True)
class TableSetSpec:
    key: str
    label: str
    # Optional: name of the builder function (string) to keep this module import-light.
    # Code can resolve this via getattr(tables_module, spec.builder_name).
    builder_name: Optional[str] = None

# -----------------------------
# NIR (Hay / Pasture Quality)
# -----------------------------

NIR_SETS: list[TableSetSpec] = [
    TableSetSpec("nir_set1", "Set 1: Pasture Quality Metrics", builder_name="build_nir_set1_table"),
    TableSetSpec("nir_set2", "Set 2: Carbohydrates & Energy Partitioning", builder_name="build_nir_set2_table"),
    TableSetSpec("nir_set3", "Set 3: Minerals & Ash", builder_name="build_nir_set3_table"),
    TableSetSpec("nir_set4", "Set 4: Digestibility Metrics", builder_name="build_nir_set4_table"),
]

# -----------------------------
# Soil Biology
# (If you later split into multiple sets, define them here.)
# -----------------------------
SOILBIO_SETS: list[TableSetSpec] = [
    TableSetSpec("soilbio_set1", "Set 1: Soil Biological Health", builder_name=None),
]

# -----------------------------
# Soil Chemistry
# -----------------------------
SOILCHEM_SETS: list[TableSetSpec] = [
    TableSetSpec("soilchem_set1", "Set 1: Soil Chemistry", builder_name=None),
]

# -----------------------------
# Biomass Field Samples
# -----------------------------
BIOMASS_FIELD_SETS: list[TableSetSpec] = [
    TableSetSpec("biomass_field_set1", "Set 1: Biomass (Field Samples)", builder_name=None),
]
