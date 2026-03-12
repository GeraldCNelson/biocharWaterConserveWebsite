"""
biochar_app.config.paths

Filesystem path configuration only (no heavy logic).

Conventions:
- Use hyphenated directories: data-raw, data-processed
- Parquet outputs live under: data-processed/parquet/summary/<granularity>/
"""

from __future__ import annotations

from pathlib import Path

# jump up one level from config/ into biochar_app/
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_RAW_DIR = BASE_DIR / "data-raw"
DATA_PROCESSED_DIR = BASE_DIR / "data-processed"

# Parquet layout
PARQUET_DIR = DATA_PROCESSED_DIR / "parquet"
PARQUET_SUMMARY_DIR = PARQUET_DIR / "summary"

PARQUET_SUMMARY_15MIN_DIR = PARQUET_SUMMARY_DIR / "15min"
PARQUET_SUMMARY_HOURLY_DIR = PARQUET_SUMMARY_DIR / "hourly"
PARQUET_SUMMARY_DAILY_DIR = PARQUET_SUMMARY_DIR / "daily"
PARQUET_SUMMARY_MONTHLY_DIR = PARQUET_SUMMARY_DIR / "monthly"
PARQUET_SUMMARY_GSEASON_DIR = PARQUET_SUMMARY_DIR / "gseason"
PARQUET_SUMMARY_WEATHER_DIR = PARQUET_SUMMARY_DIR / "weather"

# Weather sub-layout (as in your screenshot: weather/15min, weather/daily, etc.)
PARQUET_SUMMARY_WEATHER_15MIN_DIR = PARQUET_SUMMARY_WEATHER_DIR / "15min"
PARQUET_SUMMARY_WEATHER_HOURLY_DIR = PARQUET_SUMMARY_WEATHER_DIR / "hourly"
PARQUET_SUMMARY_WEATHER_DAILY_DIR = PARQUET_SUMMARY_WEATHER_DIR / "daily"
PARQUET_SUMMARY_WEATHER_MONTHLY_DIR = PARQUET_SUMMARY_WEATHER_DIR / "monthly"

# Lab tests / ancillary datasets
LAB_TESTS_RAW_DIR = DATA_RAW_DIR / "lab-tests"
LAB_TESTS_PROCESSED_DIR = DATA_PROCESSED_DIR / "lab-tests"
HAY_TESTS_PROCESSED_DIR = LAB_TESTS_PROCESSED_DIR / "hay-tests" / "csv-files"
SOIL_BIO_RAW_DIR = LAB_TESTS_RAW_DIR / "soil-tests-bio" / "csv-files"
SOIL_BIO_PROCESSED_DIR = LAB_TESTS_PROCESSED_DIR / "soil-tests-bio" / "csv-files"
SOIL_CHEM_PROCESSED_DIR = LAB_TESTS_PROCESSED_DIR / "soil-tests-chem" / "csv-files"
BIOMASS_PROCESSED_DIR = LAB_TESTS_PROCESSED_DIR / "biomass-field" / "csv-files"

# Authoritative cleaned masters
WARD_MASTER_NIR_CSV = HAY_TESTS_PROCESSED_DIR / "ward_master_nir_clean.csv"
WARD_MASTER_SOILBIO_CSV = SOIL_BIO_PROCESSED_DIR / "ward_master_soilbio_clean.csv"
WARD_MASTER_SOILCHEM_CSV = SOIL_CHEM_PROCESSED_DIR / "ward_master_soilchem_clean.csv"
BIOMASS_FIELD_CSV = BIOMASS_PROCESSED_DIR / "field_biomass_dry_g_wide_clean.csv"

# Management workbook / cleaned management datasets
BIOCHAR_MASTER_WORKBOOK = DATA_RAW_DIR / "biochar-data-master.xlsx"

MANAGEMENT_PROCESSED_DIR = DATA_PROCESSED_DIR / "management"
IRRIGATION_CSV = MANAGEMENT_PROCESSED_DIR / "irrigation_clean.csv"
FERTILIZER_CSV = MANAGEMENT_PROCESSED_DIR / "fertilizer_clean.csv"

DOWNLOADS_DIR = DATA_PROCESSED_DIR / "downloads"
LOGGER_DOWNLOADS_DIR = DOWNLOADS_DIR / "loggers"
WEATHER_DOWNLOADS_DIR = DOWNLOADS_DIR / "weather"