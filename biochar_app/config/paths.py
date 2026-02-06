"""
biochar_app.config.paths

Filesystem path configuration only (no heavy logic).
"""

from __future__ import annotations

from pathlib import Path

# jump up one level from config/ into biochar_app/
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_RAW_DIR       = BASE_DIR / "data-raw"
DATA_PROCESSED_DIR = BASE_DIR / "data-processed"

PARQUET_DIR        = DATA_PROCESSED_DIR / "parquet"
PARQUET_GSEASON_DIR = PARQUET_DIR / "gseason"

# Lab tests / ancillary datasets
LAB_TESTS_DIR = DATA_PROCESSED_DIR / "lab-tests"
HAY_TESTS_DIR = LAB_TESTS_DIR / "hay-tests" / "csv-files"
SOIL_BIO_DIR  = LAB_TESTS_DIR / "soil-tests-bio" / "csv-files"
SOIL_CHEM_DIR = LAB_TESTS_DIR / "soil-tests-chem" / "csv-files"
BIOMASS_DIR   = LAB_TESTS_DIR / "biomass-field" / "csv-files"

# Authoritative cleaned masters (routes should prefer these)
WARD_MASTER_NIR_CSV      = HAY_TESTS_DIR  / "ward_master_nir_clean.csv"
WARD_MASTER_SOILBIO_CSV  = SOIL_BIO_DIR   / "ward_master_soilbio_clean_plus_Biological_2025-11-03_v2.csv"
WARD_MASTER_SOILCHEM_CSV = SOIL_CHEM_DIR  / "ward_master_soilchem_clean_plus_Soil_2025-11-03_v1.csv"
BIOMASS_FIELD_CSV        = BIOMASS_DIR    / "field_biomass_dry_g_wide_clean.csv"
