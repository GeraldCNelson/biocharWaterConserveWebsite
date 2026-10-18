# Website, pipeline, research, and development inventory

**Status:** Current-state classification, reviewed October 17, 2026.

No directory migration has begun. Reviewed obsolete files were retired in the
preceding cleanup commits, so this inventory now describes the smaller active
tree rather than the historical experiments preserved in Git history.

## Review decisions

### Public documentation boundary

The future **Using the Data and Code** website section will be a curated,
public guide. It may explain how to download and interpret data, reproduce
analyses and figures, understand methods, and reuse supported tools.

Server administration, credentials, deployment details, internal operational
procedures, experimental troubleshooting, and one-time repair scripts will
remain repository-only and will not be exposed through the public website.

### Questions and issue reports

The future public section must include a clearly visible invitation for readers
to ask questions, report problems, and suggest improvements. The primary link
should be the repository's GitHub Issues page:

`https://github.com/GeraldCNelson/biocharWaterConserveWebsite/issues`

The notice should briefly ask users to include the affected page or dataset,
the steps that produced the problem, and any relevant error message. A personal
email address should not be required or exposed for routine support.

### Authoring formats

Markdown will be the designated editable format for the public **Using the
Data and Code** section and for developer, research-method, and operational
documentation. Markdown is preferred for these materials because it supports
code examples, commands, links, version review, and outside contributions
directly in GitHub.

The existing narrative website pages will retain their current Word-authored
workflow. Their selected `.docx` files remain the sources of truth and the
Markdown files produced from them remain generated website content.

Documentation must have one declared editable source. The same document
should not be maintained independently in both Word and Markdown.

## Purpose

This inventory is the first step in separating the public website from the
operational data pipeline, reproducible research code, development tools, and
historical material. It records the role of the current directories, proposes
their eventual ownership, and identifies cases that require review before a
move.

The classification labels used below are:

- **Website runtime** — required to serve the public application.
- **Published content** — source or generated content deliberately displayed
  by the website.
- **Production pipeline** — supported acquisition, validation, transformation,
  and publication processes.
- **Operations** — deployment, monitoring, troubleshooting, and field/server
  procedures.
- **Data stewardship** — review, correction, provenance, and promotion of field
  or laboratory evidence into canonical datasets.
- **Research** — reproducible scientific and geospatial analysis.
- **Developer tooling** — audits, one-off maintenance, code inspection, and
  build utilities not needed by the running website.
- **Tests** — automated verification of website and pipeline behavior.
- **Generated output/data** — reproducible artifacts or project observations,
  not application source code.
- **Archive/reference** — retained historical or third-party material.

## Directory classification

| Current location | Current role | Proposed ownership | Notes |
|---|---|---|---|
| `biochar_app/templates/` | FastAPI/Jinja page templates | Website runtime | Keep with the web application. |
| `biochar_app/static/` | Browser CSS, JavaScript, images, and downloadable assets | Website runtime / published content | Separate hand-authored assets from generated images in a later step. |
| `biochar_app/scripts/` | Web modules, ETL, analysis, maintenance tools, and CLIs | Mixed; must be split | This is the largest source of ambiguity. See the detailed classification below. |
| `biochar_app/config/` | Runtime, pipeline, experiment, and presentation configuration | Shared website/pipeline configuration | Split only after import relationships and deployment requirements are documented. |
| `biochar_app/markdown/` | Website source documents, generated Markdown, old notes, tools, and reference material | Mixed; must be split | Website-facing documents are only a small subset of this directory. |
| `biochar_app/docs/` | Operations, project, research, and reference documentation | Documentation | The existing subdirectories are a good foundation for the refactor. |
| `biochar_app/pakbus/core/` | Supported logger communication and daily acquisition | Production pipeline | `client.py` and `daily_download.py` are operational production code. Historical experiments, examples, and empty utility scaffolding were retired after review. |
| `biochar_app/diagnostics/` | Logger health checks, historical investigations, reports, and generated plots | Mixed production diagnostics / developer tooling | `logger_health.py`, `timestamp_health.py`, and `weekly_health.py` may belong in the supported pipeline. |
| `biochar_app/geospatial/` | Field layout, LiDAR, drone, and QGIS analysis | Research | Some generated maps are published by copying them into `static/images/generated/`. |
| `biochar_app/tests/` | Automated tests and browser smoke checks | Tests | Tests should mirror the eventual package boundaries. |
| `deployment/` | systemd service and timer definitions | Operations | Required on the test acquisition server, not by the web process itself. |
| `tools/` | Supported command wrappers | Operations | `pakbus-download` and `sync-published-data` are the two retained user-facing wrappers. Historical protocol and data experiments were retired. |
| `references/` | Setup notes, plans, and source/reference documents | Archive/reference | Generated LaTeX build artifacts were removed; remaining documents require individual retention decisions. |
| `biochar_app/data-raw/` | Source observations and daily logger acquisition | Data | Data placement and server synchronization are separate from the code refactor. |
| `biochar_app/data-processed/` | Parquet, downloads, diagnostics, and analysis outputs | Generated output/data | Runtime reads some of these files; deployment requirements must remain explicit. |
| `biochar_app/results/` | One ignored narrative and 13 small CSVs about 2023–2025 soil stocks, nutrient flows, phosphorus turnover, and treatment effects | Soil-health research working output | Review sources and causal language, separate final records from first-pass/rounded/rebuilt intermediates, and move the accepted family to `docs/research/soil_health/`. |
| `biochar_app/archive/` | Superseded code and historical files | Archive/reference | Keep outside import and deployment paths. |

## Detailed classification of `biochar_app/scripts`

### Website runtime

These modules participate directly in serving pages, APIs, plots, tables, or
downloads:

- `app.py`, `wsgi.py`, `routes.py`, `routes_utils.py`
- `state.py`, `cache.py`, `errors.py`
- `aggregation.py`, `data_loading.py`, `date_ranges.py`
- `plot_builder.py`, `plot_components.py`
- `gseason.py`, `gseason_utils.py`, `glossary.py`
- `bulk_downloads.py`, `bulk_download_utils.py`, `readme_builders.py`
- `type_utils.py`, `utils/type_coercion.py`
- `tables/`
- `lab/biomass_field_tables.py`, `lab/reference_helpers.py`, and
  `lab/serializers.py`
- `management/management_routes.py` and `management/management_db.py`

### Production pipeline

These are supported transformations or acquisition processes whose outputs are
consumed by the website:

- `etl.py`
- `get_weather_data.py`, `weather_runtime.py`
- `csv_validation.py`
- `lab/build_field_biomass_from_master.py`
- `lab/clean_ward_master_common.py`
- `lab/merge_nir_into_master_v2.py`
- `lab/merge_soilchem_supplemental.py`
- `lab/update_ward_master_nir.py`
- `lab/update_ward_master_soilbio.py`
- `lab/update_ward_master_soilchem.py`
- `management/build_irrigation_from_master.py`
- `management/generate_fertilizer_clean.py`
- `management/update_master_workbook_snapshot.py`

`logger_toa5.py` is a tested consolidation candidate rather than the current
ETL owner. Its output matches the active ETL reader on the reviewed standard
files, but the ETL still contains the production implementation. The staged
plan is recorded in [`toa5_reader_review.md`](toa5_reader_review.md).

### Research analysis

- `management/analyze_precipitation_retention.py`
- `management/estimate_irrigation_holding_capacity.py`
- `management/irrigation_analysis/`

The `irrigation_analysis/diagnostics/` subdirectory is development support for
the research analysis rather than part of the published website.

### Operations and data stewardship

These modules support review and promotion of irrigation evidence. They are
not generic developer utilities merely because they are run from a command
line:

- `management/apply_duplicate_actions.py`
- `management/audit_irrigation_event_ids.py`
- `management/build_irrigation_qc_candidate.py`
- `management/build_meter_photo_inventory.py`
- `management/build_meter_review_workbook.py`
- `management/compare_meter_photos_to_irrigation.py`
- `management/consolidate_meter_photo_review.py`
- `management/extract_irrigation_photo_events.py`
- `management/finalize_meter_photo_inventory.py`

Their commands, inputs, review states, and promotion rules should remain
documented with the irrigation operations workflow.

### Developer tooling

- Everything under `scripts/dev-tools/`
- `lab/suggest_glossary_aliases.py`
- `lab/ward_find_image_context.py`
- `lab/ward_search_reference_terms.py`
- `routes_smoke_check.py`
- `archive_dataset.py`, pending a maintainer-use decision

These classifications describe intent, not code quality. Some developer tools
are important and should remain supported commands even though the website does
not import them.

## Detailed classification of documentation and Markdown

### Current published website sources

The website document mapping in `markdown/tools/markdown_config.py` identifies
these Word files as the designated editable sources for page and help content:

- `markdown/docx/intro.docx`
- `markdown/docx/experimentDesign.docx`
- `markdown/docx/techDetails_updated.docx`
- `markdown/docx/acknowledgements.docx`
- `markdown/docx/help_main.docx`
- `markdown/docx/help_summary.docx`

The corresponding files in `markdown/outputs_md/` are generated published
content. They are served through the existing `/markdown/{filename}` route.

The files in `markdown/readmes/` are generated-download README fragments and
therefore belong with the production download pipeline, even though their
format is Markdown.

### Operations documentation

The following already live in the appropriate documentation family:

- `docs/operations/deploy_to_main.md`
- `docs/operations/irrigation_analysis_pipeline.md`
- `docs/operations/irrigation_overlay_update_notes.md`
- `docs/operations/lab_data_pipeline.md`
- `docs/operations/lightsail_setup.txt`
- `docs/operations/logger_power_troubleshooting.md`

The following should eventually join operations documentation:

- `markdown/Accessing_IPv6_only_CR800_from_IPv4_only_networks_v3.md`
- `markdown/githubManagement.md`
- `markdown/chat_summaries/biochar_deployment_checklist.md`

### Research documentation

- `docs/research/water_balance/`
- `markdown/waterbalance_code.md`
- `markdown/nir_set1_pasture_quality_metrics.md`
- `markdown/nir_set2_carbohydrates_energy_partitioning.md`
- `markdown/nir_set3_minerals_ash.md`
- `markdown/nir_set4_digestibility_metrics.md`
- `markdown/chat_summaries/deep-research_carbon-report.md`
- `results/biochar_microbe_nutrient_summary.md`

An initial reference search found no runtime code loading these four Markdown
files. The website's NIR tables are built from CSV data by
`scripts/tables/tables_nir.py`, so the Markdown files are provisionally
classified as research documentation.

### Developer and project documentation

- `docs/project/app_structure.md`
- `docs/project/developer_notes.md`
- `docs/project/testing.md`
- `docs/project/function_catalog.md` and `.json` (generated catalogs)
- `docs/documentation_catalog.md` (generated catalog)
- `geospatial/field_layout/README.md`
- `geospatial/lidar/README.md`
- `geospatial/lidar/pipelines/README.md`

### Archive/reference material

- `markdown/chat_summaries/archive_md/`
- `markdown/converted_html/` (superseded conversion output unless proven used)
- `markdown/outputs_html/` and `markdown/pdfs/` (third-party/reference output)
- `docs/reference_manuals/`
- `docs/reference_notes/`
- `references/`

## Generated files and sources of truth

The refactor should explicitly preserve these source-to-output relationships:

| Source of truth | Generated artifact | Consumer |
|---|---|---|
| `markdown/docx/*.docx` selected in `markdown_config.py` | `markdown/outputs_md/*.md` | Website Markdown route and browser renderer |
| `geospatial/build_fruita_field_layout.py` plus field geometry inputs | Field-layout PNG/WebP files | Experiment-design website content |
| Raw logger/weather/management/lab inputs | `data-processed/parquet/` and download files | Website APIs, plots, tables, and bulk downloads |
| Source modules and docstrings | Project function/documentation catalogs | Developers |

A generated artifact should not silently become the editable source. Each build
process should state where its designated input lives and how to regenerate
the output.

## Items requiring review before movement

1. Trace imports of `biochar_app/config/` before splitting website and pipeline
   configuration.
2. Confirm which `biochar_app/diagnostics/` modules are invoked by routine ETL
   or scheduled health checks.
3. Confirm the provisional finding that the four NIR Markdown files are
   research documentation. No direct runtime references were found in the
   initial inventory.
4. Confirm that `markdown/converted_html/` and `markdown/outputs_html/` are
   obsolete or reference outputs. The initial inventory found no route that
   serves either directory.
5. Apply the agreed authoring boundary: Markdown is the designated editable
   format for public code-use and technical documentation; Word remains the
   designated editable format for the
   existing narrative website pages.
6. Apply the agreed public-content boundary: operational documents containing
   server details remain repository-only and are not exposed through the public
   documentation tab.
7. Apply the classifications recorded in
   [`data_and_research_output_manifest.md`](data_and_research_output_manifest.md):
   `.env.txt` was a reproducible CoAgMET download and was removed on
   2026-10-17;
   `.env` remains tracked as shared development configuration while it contains
   only safe, non-secret defaults. Credential values must not be tracked.
8. Decide whether maintainers still use `scripts/archive_dataset.py`; document
   its archives and restoration purpose if retained, or retire it if not.
9. Implement the tested TOA5 consolidation only after agreeing to the
   functional change described in `toa5_reader_review.md`.

## Proposed next step

Use the maintained documentation families already present under `docs/`:

```text
biochar_app/docs/
├── application_architecture_status.md
├── operations/
├── project/
├── research/
├── reference_manuals/
└── reference_notes/
```

Add a `website/` documentation family only when the public **Use the Data and
Code** content is ready to be authored. Do not move code merely to make it
resemble the documentation tree.

Before code movement, resolve the remaining content, environment-file, and
manual-utility decisions because they affect what is public, what is generated,
and what is safe to deploy. Then assign each code area an owner, interface,
deployment role, and regression check.
