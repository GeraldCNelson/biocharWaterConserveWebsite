# Documentation Catalog

This generated index lists Markdown documentation throughout `biochar_app`.
Paths are relative to the repository root. Regenerate it with:

```bash
python biochar_app/scripts/dev-tools/build_documentation_catalog.py
```

Documents indexed: **58**

Status meanings: **current** is maintained guidance; **reference** is useful
background; **generated** is produced by code; **historical** is archived context.

## Archive and historical notes

### Field Log for fetchtable1live Experiments

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/fetch_table1_live_field_log.md`
- **Status:** historical
- **Summary:** - Pre-hello injection from disk - We can load ASCII-hex raw.hex files, split BD-framed messages, and pick the 0xAF hello frame(s). - The tool sends that 0xAF pre-hello and consistently gets a reply (0xEF NAK) from the...

### PakBus Data Pipeline — Master Notes

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_master.md`
- **Status:** historical
- **Summary:** Status (2025-10-20): Live pull of Table 1 via multi-hop PakBus is working robustly using BD-framed “HELLO” + replay of captured TX frames, with float-decoding knobs tuned per leaf/slice. This doc collects TL;DR, recip...

### PakBus Data Pipeline — Master Notes

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_notes_master.md`
- **Status:** historical
- **Summary:** Status (2025-10-20): Live pull of Table 1 via multi-hop PakBus is working robustly using BD-framed “HELLO” + replay of captured TX frames, with float-decoding knobs tuned per leaf/slice. This doc collects TL;DR, recip...

### PakBus Error and Fix Log

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_notes_v9.md`
- **Status:** historical
- **Summary:** - Logging errors: “ValueError: incomplete format key” Switched the root logger to {}-style formatting (logging.basicConfig(..., style='{' )) to avoid stray % characters in packet-dump log messages breaking the formatter.

### PakBus Fetch Attempts – Corrected Notes (v4)

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_notes_v4.md`
- **Status:** historical
- **Summary:** - S1M’s PakBus address is 3, not 0x79 (121). - Earlier experiments using --leaf 121 were invalid and guaranteed to fail. - All retry plans must use the correct PakBus addresses (2–13) from PC400 or config.py.

### PakBus Hex Summary

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_hex_summary_v1.md`
- **Status:** historical
- **Summary:** This file collects all key hex values and inner payloads identified so far.

### PakBus Live Pull Notes (v5 – refreshed 2025-10-07T20:11:07.070651Z)

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_notes_v5.md`
- **Status:** historical
- **Summary:** These notes consolidate what’s working now for live “pull” of Table 1 values from Campbell Scientific nodes on the multi‑hop PakBus path. They reflect the latest scripts, signatures, and tuning we validated in Oct 202...

### Pakbus Master Notes

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_master_notes.md`
- **Status:** historical
- **Summary:** - 2025-09-30 – Initial merge of pakbushexsummaryv1.md and pakbusnotesv5.md

### PakBus Notes v6

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_notes_v6.md`
- **Status:** historical
- **Summary:** Issue: bus.getcollectdatacmd() returns a tuple (cmdbytes, txnid). Calling bus.write(cmd) directly caused:

### PakBus Notes v6

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_notes_v7.md`
- **Status:** historical
- **Summary:** Issue: bus.getcollectdatacmd() returns a tuple (cmdbytes, txnid). Calling bus.write(cmd) directly caused:

### PakBus Notes v8

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_notes_v8.md`
- **Status:** historical
- **Summary:** 1. installurloverride - Changed from installurloverride(router=router, src=srcaddr, leaf=destaddr) to a simple call installurloverride() since the override picks up configuration from environment.

### pakbusnotesv2

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/pakbus_notes_v2.md`
- **Status:** historical
- **Summary:** This is an updated version of the previous notes file. It incorporates the latest run results and clarifications.

### Summary of Biochar Data Download Project

- **Path:** `biochar_app/markdown/chat_summaries/archive_md/biochar_data_pipeline_summary.md`
- **Status:** historical
- **Summary:** The Biochar Water Conservation website is a data‑visualization platform for the Biochar Water Conservation study. It uses Python (FastAPI) to expose API endpoints and Jinja2 templates to display plots and dashboards o...

## Component guides and README files

### Bulk Download Notes

- **Path:** `biochar_app/markdown/readmes/bulk_download_notes.md`
- **Status:** current
- **Summary:** Bulk Download Notes

### Plot Download All Notes

- **Path:** `biochar_app/markdown/readmes/plot_download_all_notes.md`
- **Status:** current
- **Summary:** Plot Download Notes - All

### Plot Download Ratio Notes

- **Path:** `biochar_app/markdown/readmes/plot_download_ratio_notes.md`
- **Status:** current
- **Summary:** Plot Download Notes - Ratio

### Plot Download Raw Notes

- **Path:** `biochar_app/markdown/readmes/plot_download_raw_notes.md`
- **Status:** current
- **Summary:** Plot Download Notes - Raw

### Readme

- **Path:** `biochar_app/scripts/experimental/README.md`
- **Status:** current
- **Summary:** Purpose

### Summary Download Notes

- **Path:** `biochar_app/markdown/readmes/summary_download_notes.md`
- **Status:** current
- **Summary:** Summary Statistics Notes

## Generated analyses and reports

### Irrigation Meter Photos

- **Path:** `biochar_app/data-processed/management/irrigation/photos/README.md`
- **Status:** generated
- **Summary:** This directory contains the canonical photo archive and supporting files used to extract, review, and maintain irrigation meter readings.

### Post-Irrigation Retention Statistical Summary

- **Path:** `biochar_app/data-processed/management/irrigation/analysis/reports/post_irrigation_retention_statistical_summary.md`
- **Status:** generated
- **Summary:** All reported differences are calculated as biochar strip minus non-biochar control strip. A positive difference in soil-water depth means the biochar strip contained more water. It does not merely mean that the measur...

### Weekly Logger Health Report

- **Path:** `biochar_app/diagnostics/reports/weekly_health_2026-02-28.md`
- **Status:** generated
- **Summary:** - Report date: 2026-02-28 - Battery rule (BattVMin): flag outside [11.0, 13.0] V

### Weekly Logger Health Report

- **Path:** `biochar_app/diagnostics/reports/weekly_health_2026-06-10.md`
- **Status:** generated
- **Summary:** - Report date: 2026-06-10 - Battery rule (BattVMin): flag outside [11.0, 13.0] V

## Generated website content

### Acknowledgements

- **Path:** `biochar_app/markdown/outputs_md/acknowledgements.md`
- **Status:** generated
- **Summary:** <head<stylehtml { color: 1a1a1a; background-color: fdfdfd; } body { margin: 0 auto; max-width: 1500px; padding: 50px; hyphens: auto; overflow-wrap: break-word; text-rendering: optimizeLegibility;

### Documentation Catalog

- **Path:** `biochar_app/docs/documentation_catalog.md`
- **Status:** generated
- **Summary:** This generated index lists Markdown documentation throughout biocharapp. Paths are relative to the repository root. Regenerate it with:

### Experimentdesign

- **Path:** `biochar_app/markdown/outputs_md/experimentDesign.md`
- **Status:** generated
- **Summary:** <head<stylehtml { color: 1a1a1a; background-color: fdfdfd; } body { margin: 0 auto; max-width: 1500px; padding: 50px; hyphens: auto; overflow-wrap: break-word; text-rendering: optimizeLegibility;

### Help Main

- **Path:** `biochar_app/markdown/outputs_md/help_main.md`
- **Status:** generated
- **Summary:** <head<stylehtml { color: 1a1a1a; background-color: fdfdfd; } body { margin: 0 auto; max-width: 1500px; padding: 50px; hyphens: auto; overflow-wrap: break-word; text-rendering: optimizeLegibility;

### Help Summary

- **Path:** `biochar_app/markdown/outputs_md/help_summary.md`
- **Status:** generated
- **Summary:** <head<stylehtml { color: 1a1a1a; background-color: fdfdfd; } body { margin: 0 auto; max-width: 1500px; padding: 50px; hyphens: auto; overflow-wrap: break-word; text-rendering: optimizeLegibility;

### Intro

- **Path:** `biochar_app/markdown/outputs_md/intro.md`
- **Status:** generated
- **Summary:** <head<stylehtml { color: 1a1a1a; background-color: fdfdfd; } body { margin: 0 auto; max-width: 1500px; padding: 50px; hyphens: auto; overflow-wrap: break-word; text-rendering: optimizeLegibility;

### Techdetails

- **Path:** `biochar_app/markdown/outputs_md/techDetails.md`
- **Status:** generated
- **Summary:** <head<stylehtml { color: 1a1a1a; background-color: fdfdfd; } body { margin: 0 auto; max-width: 1500px; padding: 50px; hyphens: auto; overflow-wrap: break-word; text-rendering: optimizeLegibility;

## Geospatial workflows

### Fruita Biochar Field Layout

- **Path:** `biochar_app/geospatial/field_layout/README.md`
- **Status:** current
- **Summary:** This directory contains the permanent geospatial reference layers for the CSU Fruita Biochar Irrigation Experiment.

### Fruita Field Topography Analysis

- **Path:** `biochar_app/geospatial/lidar/analysis/fruita_2016_lidar_topography_report.md`
- **Status:** current
- **Summary:** Generated: 2026-07-04

### Fruita LiDAR Data

- **Path:** `biochar_app/geospatial/lidar/README.md`
- **Status:** current
- **Summary:** Mesa County, Colorado QL2 LiDAR

### Readme

- **Path:** `biochar_app/geospatial/lidar/pipelines/README.md`
- **Status:** current
- **Summary:** fruitagrounddem20162ft.json

## Operations and workflows

### Deploying Updates to the Biochar Website

- **Path:** `biochar_app/docs/operations/deploy_to_main.md`
- **Status:** current
- **Summary:** This document describes the standard workflow for moving approved code and data from development on a local computer into production.

### Irrigation Analysis Pipeline

- **Path:** `biochar_app/docs/operations/irrigation_analysis_pipeline.md`
- **Status:** current
- **Summary:** This is the canonical operations guide for rebuilding and interpreting the Biochar project's irrigation-response analysis. It documents which program owns each stage, how timestamps are interpreted, and which outputs...

### Laboratory data update pipeline

- **Path:** `biochar_app/docs/operations/lab_data_pipeline.md`
- **Status:** current
- **Summary:** This document describes how laboratory and field-biomass data move from new source files into the Lab-Based Results tab. The preferred operational entry point is biocharapp/scripts/etl.py.

### 🛠️ Irrigation Overlay Update Notes

- **Path:** `biochar_app/docs/operations/irrigation_overlay_update_notes.md`
- **Status:** current
- **Summary:** This document summarizes the integration of irrigation overlay functionality into the Plotly plot generation process.

## Other project documentation

### Biochar Impact: Microbial, Nutrient, and System Insights

- **Path:** `biochar_app/results/biochar_microbe_nutrient_summary.md`
- **Status:** reference
- **Summary:** Result: Biochar → higher microbial biomass

### Colleague Quick Start — PC400/PC100 via Lightsail + SSH (no IPv6 on Windows needed)

- **Path:** `biochar_app/data-raw/lab-tests/spreadsheet names and worksheet type/Colleague_Quick_Start_PC400_PC100_Lightsail_SSH.md`
- **Status:** reference
- **Summary:** Last updated: Jan 2026

### Soil Biology Phase 1 – Ingestion Notes (v2)

- **Path:** `biochar_app/data-raw/lab-tests/spreadsheet names and worksheet type/soil_bio_notes_phase1_v2.md`
- **Status:** reference
- **Summary:** - Biological2023-04-04.csv - Biological2023-10-11.csv - Biological2024-03-22.csv - Biological2024-11-05.csv - Biological2025-04-02.csv - Biological2025-11-03.csv

### Soil Biology Phase 1 – Master Build Notes (v3)

- **Path:** `biochar_app/data-raw/lab-tests/spreadsheet names and worksheet type/soil_bio_notes_phase1_v3.md`
- **Status:** reference
- **Summary:** - Biological2023-04-04.csv: row-based (4 rows) - Biological2023-10-11.csv: row-based (4 rows) - Biological2024-03-22.csv: row-based (3 rows) - Biological2024-11-05.csv: row-based (4 rows)

### Soil Biology Phase 1 – master rebuild vs v7

- **Path:** `biochar_app/data-raw/lab-tests/spreadsheet names and worksheet type/soil_bio_phase1_master_rebuild_notes.md`
- **Status:** reference
- **Summary:** - Included (wide Ward CSVs): Biological2023-03-31.csv, Biological2023-10-09.csv, Biological2024-11-05.csv (after dropping 995 empty rows), Biological2025-03-31.csv. - Included (long/transposed → converted to wide): Bi...

### Thresholds and Range Checks: ETL vs Diagnostics

- **Path:** `biochar_app/diagnostics/THRESHOLDS_NOTE.md`
- **Status:** reference
- **Summary:** This project uses two different kinds of “thresholding,” with different goals:

## Project development

### App Structure Overview

- **Path:** `biochar_app/docs/project/app_structure.md`
- **Status:** current
- **Summary:** This markdown document outlines the high-level structure of the Biochar Water Conservation web application, including the frontend and backend components.

### Function Catalog

- **Path:** `biochar_app/docs/project/function_catalog.md`
- **Status:** current
- **Summary:** This file is generated from Python AST metadata and JavaScript source patterns. Regenerate both the Markdown and JSON catalogs with:

### Smoke Tests

- **Path:** `biochar_app/docs/project/testing.md`
- **Status:** current
- **Summary:** Verify that major website functions still work after code changes.

### 🛠 Developer Notes

- **Path:** `biochar_app/docs/project/developer_notes.md`
- **Status:** current
- **Summary:** This file collects hard-won solutions and best practices for maintaining and extending the Biochar Water Conservation App frontend and backend.

## Research and working notes

### Biochar Website Deployment Checklist (Test → Production)

- **Path:** `biochar_app/markdown/chat_summaries/biochar_deployment_checklist.md`
- **Status:** reference
- **Summary:** Production = biochar-webserver Test = biochar-test-fetch Code = GitHub (main ← etl-refactor) Data = rsync (parquet + downloads)

### Soil Organic Carbon and Organic Matter Dynamics by Strip, 2023–2025

- **Path:** `biochar_app/markdown/chat_summaries/deep-research_carbon-report.md`
- **Status:** reference
- **Summary:** The attached dataset reports soil organic matter by loss-on-ignition (LOI) as a percent (organicmatterloipct) at six sampling events from 2023-03-31 (baseline) through 2025-11-03 for all four strips. Because the file’...

## Website and technical reference

### Accessing the IPv6 CR800 Network from Any Internet Connection

- **Path:** `biochar_app/markdown/Accessing_IPv6_only_CR800_from_IPv4_only_networks_v3.md`
- **Status:** reference
- **Summary:** Updated: August 7, 2026

### Carbohydrates & Energy Partitioning (NIR Clean Data — Set 2)

- **Path:** `biochar_app/markdown/nir_set2_carbohydrates_energy_partitioning.md`
- **Status:** reference
- **Summary:** Set 2 focuses on non-structural carbohydrates that influence: - rapid energy availability, - rumen fermentation dynamics, - and plant stress/seasonal physiology.

### Cr650 Sensor Summary

- **Path:** `biochar_app/markdown/CR650_sensor_summary.md`
- **Status:** reference
- **Summary:** <!-- Auto-generated from CS650sensorsummary.tex -- <!-- math via MathJax --

### Digestibility Metrics (NIR Clean Data — Set 4)

- **Path:** `biochar_app/markdown/nir_set4_digestibility_metrics.md`
- **Status:** reference
- **Summary:** Set 4 focuses on variables that more directly describe digestibility and fiber utilization, often important for: - predicting intake and animal performance, - understanding forage quality changes with maturity,

### GitHub Management for Precompute Script and Project

- **Path:** `biochar_app/markdown/githubManagement.md`
- **Status:** reference
- **Summary:** Create a .gitignore file in the root of your project with the following content:

### Minerals & Ash (NIR Clean Data — Set 3)

- **Path:** `biochar_app/markdown/nir_set3_minerals_ash.md`
- **Status:** reference
- **Summary:** Set 3 tracks mineral content and ash. This is useful for: - linking soil chemistry and amendments to plant mineral uptake, - diagnosing nutrient imbalances, - and understanding forage mineral supply (especially for gr...

### Pasture Quality Metrics (NIR Clean Data — Set 1)

- **Path:** `biochar_app/markdown/nir_set1_pasture_quality_metrics.md`
- **Status:** reference
- **Summary:** Set 1 is a compact, interpretable snapshot of forage quality. It emphasizes variables that are: - widely used in forage science, - comparable across dates and years, - and directly useful for comparing treatment effec...

### Waterbalance Code

- **Path:** `biochar_app/markdown/waterbalance_code.md`
- **Status:** reference
- **Summary:** <!-- Auto-generated from waterbalancecode.tex -- <!-- math via MathJax --
