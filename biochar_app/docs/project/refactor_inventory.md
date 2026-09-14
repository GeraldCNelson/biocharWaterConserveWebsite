# Website, pipeline, research, and development inventory

Status: classification only. No files have been moved or renamed.

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

Markdown will be the authoritative source format for the public **Using the
Data and Code** section and for developer, research-method, and operational
documentation. Markdown is preferred for these materials because it supports
code examples, commands, links, version review, and outside contributions
directly in GitHub.

The existing narrative website pages will retain their current Word-authored
workflow. Their selected `.docx` files remain the sources of truth and the
Markdown files produced from them remain generated website content.

Documentation must have one declared authoritative source. The same document
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
| `biochar_app/pakbus/core/` | Supported logger communication and daily acquisition | Production pipeline | `client.py` and `daily_download.py` are operational production code. |
| `biochar_app/pakbus/scripts/` | Protocol experiments, packet decoding, replay, and old download approaches | Developer tooling / archive | Review for still-useful diagnostic commands before archiving. |
| `biochar_app/pakbus/examples/` | Protocol examples | Developer tooling / reference | Not needed by the website runtime. |
| `biochar_app/pakbus/utils/` | PakBus frame, CRC, and hexadecimal helpers | Review needed | Determine whether supported core clients still import these modules. |
| `biochar_app/diagnostics/` | Logger health checks, historical investigations, reports, and generated plots | Mixed production diagnostics / developer tooling | `logger_health.py`, `timestamp_health.py`, and `weekly_health.py` may belong in the supported pipeline. |
| `biochar_app/geospatial/` | Field layout, LiDAR, drone, and QGIS analysis | Research | Some generated maps are published by copying them into `static/images/generated/`. |
| `biochar_app/tests/` | Automated tests and browser smoke checks | Tests | Tests should mirror the eventual package boundaries. |
| `deployment/` | systemd service and timer definitions | Operations | Required on the test acquisition server, not by the web process itself. |
| `tools/` | Older standalone data and PakBus tools | Developer tooling / archive | Check for duplication with `biochar_app/scripts/dev-tools/` and `biochar_app/pakbus/`. |
| `references/` | Setup notes, plans, and LaTeX build artifacts | Archive/reference | `.aux` and `.synctex.gz` are generated LaTeX artifacts and should not be source documents. |
| `biochar_app/data-raw/` | Source observations and daily logger acquisition | Data | Data placement and server synchronization are separate from the code refactor. |
| `biochar_app/data-processed/` | Parquet, downloads, diagnostics, and analysis outputs | Generated output/data | Runtime reads some of these files; deployment requirements must remain explicit. |
| `biochar_app/results/` | Derived research tables and summaries | Generated research output | Decide which outputs are reproducible and which should be regenerated. |
| `biochar_app/archive/` | Superseded code and historical files | Archive/reference | Keep outside import and deployment paths. |

## Detailed classification of `biochar_app/scripts`

### Website runtime

These modules participate directly in serving pages, APIs, plots, tables, or
downloads:

- `app.py`, `wsgi.py`, `routes.py`, `routes_utils.py`
- `custom_app.py`, `state.py`, `cache.py`, `errors.py`
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
- `csv_validation.py`, `archive_dataset.py`, `convert_json.py`
- `logger_toa5.py`, `pull_leaves.py`, `replay_collect.py`
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

### Research analysis

- `management/analyze_precipitation_retention.py`
- `management/estimate_irrigation_holding_capacity.py`
- `management/irrigation_analysis/`

The `irrigation_analysis/diagnostics/` subdirectory is development support for
the research analysis rather than part of the published website.

### Developer tooling

- Everything under `scripts/dev-tools/`
- `lab/suggest_glossary_aliases.py`
- `lab/ward_find_image_context.py`
- `lab/ward_search_reference_terms.py`
- `management/apply_duplicate_actions.py`
- `management/audit_irrigation_event_ids.py`
- `management/build_irrigation_qc_candidate.py`
- `management/build_meter_photo_inventory.py`
- `management/build_meter_review_workbook.py`
- `management/compare_meter_photos_to_irrigation.py`
- `management/extract_irrigation_photo_events.py`
- `management/finalize_meter_photo_inventory.py`
- `routes_smoke_check.py`
- `CLIscripts.txt`

These classifications describe intent, not code quality. Some developer tools
are important and should remain supported commands even though the website does
not import them.

## Detailed classification of documentation and Markdown

### Current published website sources

The website document mapping in `markdown/tools/markdown_config.py` identifies
these Word files as the authoritative sources for page and help content:

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
- `scripts/experimental/README.md`
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
process should state where its authoritative input lives and how to regenerate
the output.

## Items requiring review before movement

1. Determine whether `biochar_app/etf.py` is active, obsolete, or a misspelled
   historical entry point.
2. Trace imports of `biochar_app/config/` before splitting website and pipeline
   configuration.
3. Confirm which `biochar_app/diagnostics/` modules are invoked by routine ETL
   or scheduled health checks.
4. Confirm the provisional finding that the four NIR Markdown files are
   research documentation. No direct runtime references were found in the
   initial inventory.
5. Identify any still-used commands under `biochar_app/pakbus/scripts/`,
   `biochar_app/pakbus/scripts_zsh/`, and top-level `tools/`.
6. Confirm that `markdown/converted_html/` and `markdown/outputs_html/` are
   obsolete or reference outputs. The initial inventory found no route that
   serves either directory.
7. Apply the agreed authoring boundary: Markdown is authoritative for public
   code-use and technical documentation; Word remains authoritative for the
   existing narrative website pages.
8. Apply the agreed public-content boundary: operational documents containing
   server details remain repository-only and are not exposed through the public
   documentation tab.
9. Classify `.env` and `.env.txt` at the repository root. Credential values must
   not be tracked; example configuration should use an explicit `.example`
   filename with placeholders only.

## Proposed next step

Create the new documentation directory boundaries without moving content:

```text
biochar_app/docs/
├── website/
├── operations/
├── research/
├── developer/
└── reference/
```

Before step 2, resolve items 4, 6, and 9 because they affect what is public,
what is generated, and what is safe to deploy.
