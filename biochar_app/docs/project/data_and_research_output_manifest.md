# Data and research-output manifest

This inventory distinguishes source evidence, application data, deployment
products, and research records so later cleanup decisions can be made safely.

**Initial inventory date:** 2026-10-17

**Last updated:** 2026-10-17

**Branch:** `etl-refactor`

**Status:** Active cleanup working document. Approved actions are recorded in
the cleanup history; unreviewed families remain inventory-only.

## Purpose

A **file family** is a group of related files that share a purpose, source, or
generating process and should therefore be reviewed together. Examples include
one research report with its supporting tables and figures, annual logger
files, or the clean laboratory datasets used by one website tab.

The manifest answers four questions for each file family:

1. What is the source or generating process?
2. What currently uses the files?
3. Must the files be in Git or on the deployed website?
4. Is a retention decision safe now, or does it require further review?

The inventory is organized by file family rather than listing every annual or
event-level output separately. Counts refer to files tracked by Git on the
inventory date. Files intentionally kept outside Git are included when that
handling is part of the design.

## Classification terms

| Term | Meaning |
|---|---|
| File family | Related files that share a purpose, source, generating command, or current use and should be reviewed as a group. |
| Source evidence | A workbook, instrument record, photograph, laboratory report, or other input whose original content cannot be recreated by project code alone. |
| Canonical | The designated current and approved version when several source, intermediate, historical, or candidate versions exist. “Canonical” does not necessarily mean original; it means the version current code and procedures are expected to use. |
| Canonical application dataset | A reviewed, normalized file designated as the version the website or analysis code should use. It may be reproducible, but its accepted review status makes the named file operationally important. |
| Generated deployment product | A Parquet, ZIP, Markdown, HTML, image, or cache product built from named sources and needed by the deployed website. |
| Research record | A report and its supporting tables or figures that document a scientific analysis, including uncertainty and interpretation. |
| Reproducible intermediate | A diagnostic or intermediate output that can be rebuilt and is not itself the reviewed result or a runtime requirement. |
| Cleanup candidate | Evidence supports removal from the active Git tree, but this inventory does not perform the removal. |
| Tracked by Git | Git records the file and its revisions, so it moves through branches, pulls, and pushes. |
| Ignored by Git | Git deliberately excludes the file. It must be regenerated, retained locally, or transferred separately—for example with `rsync`—when another computer or server needs it. |

“Source evidence” is assigned from the role of the file in a particular data
workflow, not merely from the name of its directory. For example, a reviewed
historical CSV can be source evidence for a canonical clean dataset, while a
CSV in an analysis directory can be a reproducible intermediate.

## Inventory summary

| Tracked family | Files | Approximate size | Inventory conclusion |
|---|---:|---:|---|
| Root `.env.txt` weather CSV | 0 current; 1 initially | 0 current; 1.90 MB initially | Removed from the active tree on 2026-10-17 |
| Tracked raw laboratory reports | 2 | 0.09 MB | Published source evidence; retain |
| Processed laboratory datasets | 16 | 0.08 MB | Canonical files plus versioned intermediates; separate before cleanup |
| Processed management datasets | 19 | 0.24 MB | Mixed canonical, historical, and diagnostic files; review by workflow |
| Irrigation-analysis outputs | 85 | 2.79 MB | Generated family; retain pending generator/current-use review |
| Generated Ward HTML | 5 | 0.70 MB | Website runtime requirement; retain |
| Other research documents and report bundles | 33 | 6.43 MB | Reviewed or interpretable research record; retain as coherent families |
| Historical soil-health research family | 15 | 0.03 MB | Preserved in a visible, tracked research directory with limitations documented |
| Website Word sources | 6 | 1.05 MB | Editable sources for existing narrative pages; retain |
| Generated website Markdown | 6 | 0.03 MB | Website runtime requirement; retain |
| Published static laboratory/generated media | 55 | 8.41 MB | Website runtime content; retain pending publishing redesign |
| Permanent geospatial data and figures | 4 | 0.42 MB | Field-layout source/product family; retain |
| NIR research notes | 4 | less than 0.01 MB | Research documentation; retain provisionally |

The former ignored `biochar_app/results/` directory contained one narrative
summary and 13 small CSV files concerning soil stocks, nutrient flows,
phosphorus turnover, and treatment effects for 2023–2025. On 2026-10-17 all 14
files were preserved under `biochar_app/docs/research/soil_health/`, together
with a dated inventory. The family is now visible and can be versioned in Git.

First-pass, rounded, rebuilt, and updated tables were retained because no
generation code or complete provenance record was found that would make a
supersession decision safe. The narrative now distinguishes descriptive
patterns and hypotheses from established causal conclusions. Scientific review
and a reproducible builder are still required before any part of the family is
published in the proposed Technical Analyses section.

Parquet summaries, bulk-download ZIPs, seasonal caches, and most raw
acquisition files are different: they can be large, are intentionally kept
outside Git, and are regenerated or transferred separately with `rsync`.

## Acquisition and source evidence

| Data family and present location | Source and update path | Current use or deployment role | Git status | Inventory decision |
|---|---|---|---|---|
| Project master workbook: `biochar_app/data-raw/biochar-data-master.xlsx` | The shared OneDrive workbook is downloaded and validated by `scripts/management/update_master_workbook_snapshot.py`; ETL can deliberately use an installed snapshot. | Irrigation, fertilizer, and field-biomass builders | Ignored | Preserve the validated local snapshot and provenance outside Git. Do not treat the workbook as the source of independent laboratory datasets. |
| Logger `.dat` files and accepted PakBus archives under `biochar_app/data-raw/` | CR-series dataloggers through historical TOA5 downloads or the current PakBus acquisition and acceptance workflow | ETL, diagnostics, and scientific analyses | Ignored | Preserve as source evidence with archive validation and checksums. Do not add routine raw archives to Git. |
| CoAgMET Fruita station (FRT3) observations | Retrieved from CoAgMET by `scripts/get_weather_data.py`; ETL produces aligned weather products | Website plots, summaries, downloads, and water analyses | Normal retrieval output is excluded from Git; the misplaced tracked `.env.txt` snapshot was removed on 2026-10-17 | The maintainer confirms historical CoAgMET downloads are reproducible. The deleted snapshot was not unique source evidence and remains recoverable through Git history. |
| Raw Ward and other laboratory exports under `biochar_app/data-raw/lab-tests/` | Laboratory CSV, DOCX, or PDF deliveries registered by the laboratory update scripts | Canonical laboratory builders and manual review | Excluded by the `biochar_app/data-raw/` Git-ignore rule and retained locally or in external source storage | Preserve the original deliveries outside Git or in an explicitly documented evidence store. Record sampling date and source before replacing a file. |
| Biochar material reports under `biochar_app/data-raw/labs_biochar/` | Control Laboratories and Wyoming Laboratory reports | Public Technical Details content through configured report routes | 2 PDFs tracked and required by the deployment manifest | Retain in Git unless publication is redesigned. These are source reports, not generated project output. |
| Irrigation meter photographs and review workbooks under `data-processed/management/irrigation/photos/` | Field photographs, metadata, manual counter review, and correction files | Canonical photo inventory, irrigation boundary review, and research audits | Originals and review workbooks ignored; selected correction/audit files tracked | Preserve reviewed source material outside Git. Keep only the small canonical correction and audit records needed to reproduce accepted decisions. |
| Drone and other raw geospatial observations | Field capture placed under `biochar_app/geospatial/drone/` and geospatial source areas | Future mapping and field-performance workflows | Raw imagery and flight data ignored | Preserve externally with flight/date/provenance metadata. No cleanup decision is appropriate until a drone workflow is defined. |

### Root `.env.txt` finding

The deleted `.env.txt` file was not environment configuration. It was a
1,896,610-byte CoAgMET CSV with 24,229 lines. Its first row contains weather
field names and its final observation is for station `frt03` at
`2025-03-25T22:55`. No operational code reads this path.

Because the current retriever requests a complete year from CoAgMET and the
maintainer confirmed that a repeated download is equivalent to the earlier
download, `.env.txt` did not need to be retained as a unique historical
source. It was removed from the active tree on 2026-10-17 and remains
recoverable through Git history.

## Canonical and generated application data

| Data family and present location | Generator or authority | Current use or deployment role | Git status | Inventory decision |
|---|---|---|---|---|
| Canonical irrigation data: `data-processed/management/irrigation/irrigation_clean.csv` | Dedicated irrigation builder using the validated workbook, reviewed history, photo-supported corrections, and stable event identifiers | ETL, website, irrigation analyses, and deployment | Tracked; also deployed | Retain. It is the named accepted dataset, not disposable cache output. |
| Fertilizer and biomass management data under `data-processed/management/` | Dedicated builders from the workbook and reviewed historical inputs | Website tables and future treatment analyses | Tracked | Retain canonical outputs; classify old extracts and debugging comparisons separately before deletion. |
| Canonical laboratory clean files under `data-processed/lab-tests/` | Laboratory-specific update scripts described in `docs/operations/lab_data_pipeline.md` | Lab-Based Results tab and research analyses | 16 files tracked | Retain the four named canonical datasets and header metadata. Review version-suffixed merge intermediates independently; do not delete them as one batch. |
| Irrigation analysis under `data-processed/management/irrigation/analysis/` | Irrigation response, timing, holding-capacity, and reporting modules | Scientific review and inputs to selected report bundles | 85 files tracked; additional reproducible outputs ignored | Mixed family. First map each subgroup to its generating command and downstream report, then keep reviewed summaries while considering untracking reproducible debugging and per-event intermediates. |
| Logger/weather Parquet summaries and bulk-download ZIPs under `data-processed/parquet/` and `data-processed/downloads/` | ETL and bulk-download builders | Website APIs, plots, tables, and public downloads | Ignored; transferred to production separately | Keep outside Git. Their explicit `external` delivery status in `config/deployment_manifest.py` is intentional. |
| Ward reference HTML under `data-processed/ward-html/` | `scripts/dev-tools/convert_ward_docx_to_html.py` from reviewed Ward DOCX files | Active website routes for laboratory guides and reports | 5 files tracked | Retain while the current routes depend on them. They are generated but operationally required. |
| Website narrative Markdown under `markdown/outputs_md/` | Word sources selected by `markdown/tools/markdown_config.py`, converted by the Word-to-Markdown publishing workflow | Main website pages and help modals | 6 generated Markdown files tracked | Retain with their six tracked Word sources. Word remains the editable source for these existing narrative pages. |
| Static laboratory and generated media | Ward conversion and Word publishing processes | Active website content | 55 files tracked | Retain while referenced by published content. Future publishing work should add automated orphan checks. |
| Permanent field-layout products under `geospatial/field_layout/` | `geospatial/build_fruita_field_layout.py` plus reviewed control points | Experiment Design page and spatial interpretation | 4 data/figure files tracked | Retain the GPKG/GeoJSON sources and current published layout. Ignore reproducible scratch GIS products. |

### Root `.env` finding

The tracked `.env` contains only `FLASK_APP`, `FLASK_ENV`, and
`PYTHONUNBUFFERED`. Application startup calls `load_dotenv()`, while production
PakBus credentials and operational settings are loaded from
`/etc/biochar/pipeline.env`. This file is not a data product, but it belongs in
the same source-control hygiene review as `.env.txt`.

A separate `.env.example` is not necessary while `.env` intentionally contains
only safe settings shared by every development checkout. Keeping and updating
the existing tracked `.env` is the simpler choice for the present workflow.
The file must remain limited to non-secret shared defaults; machine-specific
values or credentials would require an ignored local `.env` and a safe tracked
example instead.

## Research records

| Research family | Generator and inputs | Tracked products | Inventory decision |
|---|---|---|---|
| Persistent deep-over-shallow S3/S4 VWC audit | `scripts/research/analyze_s3_s4_multiyear_water.py`; canonical logger, irrigation, weather, and photo-QC inputs | 18 report, table, and figure files under `docs/research/s3_s4_multiyear_water_audit/` | Retain as one reproducible diagnostic record. It documents an unresolved measurement pattern rather than a causal conclusion. |
| Irrigation flow and water-balance analysis | `scripts/management/irrigation_analysis/analyze_flow_infiltration.py` and `plot_zone_capacity_field_layout.py` | 9 report, table, JSON, and figure files under `docs/research/irrigation_flow_water_balance/` | Retain as one report bundle. Its README names the regeneration commands and supporting outputs. |
| Historical soil-health and nutrient analysis | Original generator not identified; tables appear to combine laboratory, biomass, fertilizer, area, and bulk-density inputs | One qualified narrative, one dated inventory, and 13 supporting CSV tables under `docs/research/soil_health/` | Retain as a historical preliminary record and starting point for a reproducible current analysis. Do not publish its causal interpretations without scientific review. |
| EC temperature-sensitivity assessment | `scripts/research/analyze_ec25_temperature_sensitivity.py`; canonical Parquet and EC measurements | Markdown assessment tracked; machine-readable tables and figures ignored under `data-processed/research/ec25_temperature_sensitivity/` | Retain the interpretive report; keep reproducible machine outputs outside Git unless selected for publication. |
| EC irrigation-interval and moisture/temperature-adjusted analyses | `scripts/research/analyze_ec_irrigation_intervals.py` and `analyze_ec_adjustment.py` | Markdown reports plus `docs/research/ec_irrigation_interval_analysis.html` and `docs/research/ec_moisture_temperature_adjusted_analysis.html`; machine-readable outputs excluded from Git under `data-processed/research/` | Retain Markdown reports. The two unreferenced Pandoc HTML exports are medium-confidence cleanup candidates after confirming they are not the preferred review copies. |
| NIR topic notes under `biochar_app/markdown/nir_set*.md` | Historical explanatory research notes; current website NIR tables are built from canonical CSV by `scripts/tables/tables_nir.py` | 4 Markdown files | Retain provisionally as research documentation. Decide later whether to incorporate their useful interpretation into public data documentation. |
| Water-balance technical source | `docs/research/water_balance/waterbalance_code.tex` | 1 LaTeX source | Retain as a possible source document until superseded or incorporated into a maintained report. |

## Cleanup decisions produced by this pass

Completed actions and pending reviews are listed together so the sequence of
cleanup decisions remains visible.

| Priority | File or review | Evidence | Proposed next decision |
|---:|---|---|---|
| 1 | Root `.env.txt` | Misnamed reproducible CoAgMET download; no operational references; maintainer confirms repeat retrieval is equivalent | **Completed 2026-10-17:** deleted from the active tree; retained in Git history. |
| 2 | Keep the tracked root `.env` as shared development configuration | It contains only safe shared defaults, application startup reads it, and production uses a separate environment file | Update `.env` directly when shared defaults change. Never add credentials or machine-specific values. |
| 3 | Review and relocate the soil-health family formerly under `biochar_app/results/` | One narrative and 13 CSVs are small, topically related, and scientifically useful despite their age | **Completed 2026-10-17:** preserved the entire compact family under `docs/research/soil_health/`, added a dated inventory, and qualified the narrative. Reproducibility and scientific review remain future work. |
| 4 | Review version-suffixed Ward clean/merge CSVs | Canonical filenames exist, while several older versions have no current references; one standalone soil-biology table script still names versions 5 and 6 | Confirm the standalone script's status and preserve only required source evidence plus the canonical output. |
| 5 | Review `docs/research/ec_irrigation_interval_analysis.html` and `docs/research/ec_moisture_temperature_adjusted_analysis.html` | Markdown reports and generation scripts exist; no runtime reference was found | Preserve them until the Technical Analyses publishing format is agreed; then retain Markdown as the maintained source if the application can render it directly. |
| 6 | Split the 85 tracked irrigation-analysis files by generating command and current use | The directory mixes reviewed summaries with reproducible diagnostics and per-event products | Create a dated sub-manifest before untracking any files; preserve report-ready analyses for possible inclusion in Technical Analyses. |

## Cleanup history

| Date | File family | Action | Verification |
|---|---|---|---|
| 2026-10-17 | Misplaced CoAgMET snapshot `.env.txt` | Removed the reproducible 24,229-line download from the active Git tree | Confirmed no operational references; current CoAgMET retriever requests complete annual data; prior version remains in Git history |
| 2026-10-17 | Historical soil-health results | Moved one narrative and 13 CSVs from ignored `biochar_app/results/` to tracked `docs/research/soil_health/`; added a dated inventory and interpretation cautions | Confirmed all 14 substantive files were retained; documented that the generator and complete provenance are not yet known |

## How this manifest supports the cleanup process

This is the maintained current manifest, so its stable filename allows project
documents and scripts to link to one current location. Update `Last updated`
whenever a cleanup decision changes a classification or file family.

One-time supporting inventories and decision records should include an ISO date
in the filename, for example
`irrigation_analysis_output_inventory_2026-10-17.md`. This gives completed
point-in-time reviews an easy-to-find timestamp without creating several files
that all appear to be the current manifest.

After each approved cleanup batch:

1. Update the affected inventory row and cleanup decision.
2. Record whether files were deleted, moved, untracked, or retained.
3. Record the verification performed and the commit containing the decision.
4. Update the file counts when tracked membership changes materially.

## Guardrails for the next pass

- Do not delete raw logger, photo, laboratory, workbook, or geospatial evidence
  merely because it is ignored by Git.
- Do not remove a generated file required by `config/deployment_manifest.py` or
  an active route until that use has been changed and tested.
- Keep a research report together with the tables and figures needed to
  interpret it unless a documented regeneration-and-release policy replaces
  versioning those products.
- Treat current canonical clean CSVs as reviewed application datasets even when
  their builders are reproducible.
- Keep the Technical Analyses tab as the destination for reviewed research
  records, but finish classifying and relocating the soil-health, EC, and
  irrigation-analysis families before implementing the website feature.
