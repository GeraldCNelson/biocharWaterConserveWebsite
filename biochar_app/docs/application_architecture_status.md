# Biochar Application Architecture & Development Status

**Version:** 1 — October 14, 2026  
**Project:** Biochar Fruita CSU Experiment  
**Github Repository:** [biocharWaterConserveWebsite](https://github.com/GeraldCNelson/biocharWaterConserveWebsite)  
**Central repository location:** `biochar_app/docs/application_architecture_status.md`  
** Project website:**
https://biocharresearch.org
**Status:** Initial draft for review

## 1. Purpose and scope

This document provides a concise architectural overview of the Biochar Water Conservation application, its research objectives, data-processing systems, scientific analyses, and development priorities.

It serves as a navigation guide to more detailed documentation maintained in Git. It does not replace implementation guides, scientific methodology documents, or operational procedures.

**The Git repository is authoritative for code and technical documentation.** This overview should be updated when significant architectural changes occur.

The [documentation catalog](documentation_catalog.md) provides a broader inventory of available documentation.

## 2. Research context

The application was developed for and supports the Biochar Water Conservation experiment at the Colorado State University research station near Fruita, Colorado. It was also built modularly with the hope that it will prove useful to other agricutural experiment developers.

The experiment investigates whether biochar can improve soil-water retention and agricultural water-use efficiency under arid western Colorado conditions. Data on soil chemistry and health were also collected.

Four experimental strips are monitored:

| Strip | Biochar treatment | Irrigation regime |
|---|---|---|
| S1 | Biochar | Monthly |
| S2 | Control | Monthly |
| S3 | Biochar | Approximately biweekly |
| S4 | Control | Approximately biweekly |

Twelve Campbell Scientific dataloggers monitor soil conditions at three positions within each strip: Top, Middle, and Bottom.

Each position has soil sensors at nominal depths of 6, 12, and 18 inches. Measurements include volumetric water content, temperature, and bulk electrical conductivity.

The application integrates these measurements with irrigation records, weather observations, laboratory analyses, field measurements, and geospatial information.

**Related documentation:** [Experiment design](../markdown/outputs_md/experimentDesign.md) (generated website content), [field layout](../geospatial/field_layout/README.md), and [sensor documentation](../markdown/CR650_sensor_summary.md).

## 3. Software architecture

The application uses a Python backend built around FastAPI, with Jinja2 templates, JavaScript, Plotly, and Bootstrap for the browser interface.

Its major responsibilities are divided among:

| Component | Responsibility |
|---|---|
| Website runtime | Serve pages, API endpoints, plots, tables, and downloads |
| Data acquisition | Collect logger observations and retrieve external data |
| ETL pipeline | Validate, transform, aggregate, and publish datasets |
| Scientific analysis | Calculate treatment comparisons, irrigation responses, and other research results |
| Configuration | Maintain paths, experiment metadata, variable definitions, and analysis parameters |
| Diagnostics | Evaluate data quality, logger health, timestamps, and analysis eligibility |
| Developer tools | Generate catalogs, inspect code, and support maintenance |

The application includes multiple scientific and operational workflows that should remain distinguishable from the public website runtime.

The longer-term architecture aims to preserve those responsibilities while reducing unnecessary dependencies between components.

**Related documentation:** [Application structure](project/app_structure.md), [refactoring inventory](project/refactor_inventory.md), [function catalog](project/function_catalog.md), and [developer notes](project/developer_notes.md).

**Documentation caution:** `app_structure.md` still describes an older Flask-based architecture and should be updated before being treated as authoritative.

## 4. Data acquisition and processing

### Dataloggers and weather

The field instrumentation produces Campbell Scientific data files that enter the ETL pipeline.

The pipeline performs logger-specific clock corrections, converts fixed-MST logger times to Colorado civil time, calculates derived variables, and produces processed datasets for the website and scientific analyses.

The timestamp policy is especially important because incorrect alignment between irrigation records and soil-water measurements can produce misleading arrival-time estimates.

A change to clock corrections requires regeneration of affected downstream outputs.

**Related documentation:** [Irrigation analysis pipeline](operations/irrigation_analysis_pipeline.md), [logger clock evidence](../data-raw/logger_clock_evidence/README.md), and [ETL diagnostics](../diagnostics/THRESHOLDS_NOTE.md).

### Laboratory and field data

Laboratory workflows process soil chemistry, soil biology, pasture quality, biomass, and related measurements.

The website presents selected results through its Lab-Based Results interface.

**Related documentation:** [Laboratory data pipeline](operations/lab_data_pipeline.md).

### Irrigation management data

Irrigation records are derived from the project master workbook and related validated sources.

Meter photographs provide independent evidence for reviewing irrigation timestamps and meter readings.

The photo-comparison workflow is a quality-control process; it does not independently modify logger timestamps.

**Related documentation:** [Irrigation analysis pipeline](operations/irrigation_analysis_pipeline.md) and [meter-photo archive](../data-processed/management/irrigation/photos/README.md).

## 5. Scientific analysis

### Irrigation-response analysis

**Status: Implemented, with continuing development.**

The irrigation-analysis workflow estimates water arrival times, evaluates sensor responses, calculates event-level quantities, and produces diagnostics and figures.

Its primary orchestration command is:

`biochar_app/scripts/management/estimate_irrigation_holding_capacity.py`

The existing modular structure includes:

| Module | Responsibility |
|---|---|
| `irrigation_analysis/diagnostics.py` | Arrival-order and related diagnostics |
| `irrigation_analysis/plotting.py` | Event-response visualizations |
| `irrigation_analysis/reporting.py` | Word reports generated from analysis outputs |

Additional modularization should be assessed against the current implementation rather than assumed to be incomplete.

The workflow supports matched comparisons between S1/S2 and S3/S4, with explicit quality-control eligibility.

**Related documentation:** [Canonical irrigation analysis pipeline](operations/irrigation_analysis_pipeline.md).

### Precipitation retention and empirical water capacity

**Status: Implemented analytical workflows; scientific evaluation continues.**

The application also evaluates soil-water retention following precipitation events independently of irrigation.

Precipitation and irrigation evidence contribute to estimates of empirical upper retained volumetric water content.

These estimates support analyses of available storage before irrigation and retrospective comparisons with applied irrigation volumes.

The current research workflow also produces candidate operational inspection alerts based on modeled storage and observed soil-water responses.

These alerts are **retrospective research outputs, not automated irrigation controls**.

The estimated water not retained in the monitored soil profile must not be interpreted as measured runoff. It can include drainage, lateral movement, and other processes.

Similarly, repeated irrigation events provide useful evidence about treatment differences but do not constitute independent treatment replication.

**Related documentation:** [Irrigation analysis pipeline](operations/irrigation_analysis_pipeline.md) and [irrigation water-balance research](research/irrigation_flow_water_balance/README.md).

### Geospatial analysis

**Status: Established research infrastructure, with opportunities for further integration.**

The project includes field-layout information, LiDAR-derived terrain analysis, and supporting geospatial workflows.

These resources provide spatial context for interpreting field conditions and soil-water responses.

**Related documentation:** [Field layout](../geospatial/field_layout/README.md), [LiDAR documentation](../geospatial/lidar/README.md), and [LiDAR processing pipelines](../geospatial/lidar/pipelines/README.md).

## 6. Development priorities

The following priorities are proposed for continued development. Their implementation status should be verified against the current Git branch before work begins.

| Priority | Objective |
|---|---|
| Documentation modernization | Update outdated architecture and developer guidance |
| Irrigation architecture | Review existing modules and identify remaining opportunities for separation |
| Scientific validation | Continue evaluating water-retention and irrigation-efficiency estimates |
| Data quality | Preserve traceable clock corrections, source provenance, and QC decisions |
| Testing | Strengthen regression coverage for scientific calculations and processing pipelines |
| Geospatial integration | Connect field geometry and terrain analysis to scientific interpretation |
| Reusable architecture | Gradually separate website, pipeline, research, and developer-tool responsibilities |

The refactoring inventory establishes an important constraint: restructuring should preserve existing functionality, data provenance, and reproducibility.

**Related documentation:** [Refactoring inventory](project/refactor_inventory.md), [testing guidance](project/testing.md), and [deployment procedures](operations/deploy_to_main.md).

## 7. Documentation and maintenance principles

The project distinguishes authoritative source documents from generated outputs.

Technical, scientific-method, and operational documentation should generally be maintained in Markdown.

Existing narrative website pages retain their Word-based authoring workflow, with generated Markdown used for publication.

Generated reports, datasets, figures, and catalogs should remain reproducible from documented inputs.

Internal operational documentation should remain separate from material intended for publication on the website.

The architecture overview is maintained manually. The broader documentation catalog and function catalog are generated from repository content.

**Primary navigation:** [Documentation catalog](documentation_catalog.md).

**Development references:** [Developer notes](project/developer_notes.md), [function catalog](project/function_catalog.md), [testing](project/testing.md).

**Operational references:** [Irrigation pipeline](operations/irrigation_analysis_pipeline.md), [laboratory pipeline](operations/lab_data_pipeline.md), [deployment](operations/deploy_to_main.md).

## 8. Review notes for Version 1

This draft is based on the documentation reviewed through October 14, 2026. It is not a complete audit of the current repository.

Before treating the document as fully authoritative:

1. Confirm the current application entry points and module organization.
2. Update `app_structure.md` to describe FastAPI rather than Flask.
3. Review `developer_notes.md` for superseded recommendations.
4. Confirm current irrigation module boundaries and remaining refactoring needs.
5. Verify all documentation links against the repository.

The architecture map should be reviewed after major structural changes, rather than updated for every individual commit.
