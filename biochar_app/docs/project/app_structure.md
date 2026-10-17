# Current Application Structure

**Reviewed branch:** `etl-refactor`

**Review date:** October 17, 2026

**Scope:** Current-state map; not a proposed directory migration

## 1. Runtime overview

The Biochar Water Conservation website is a FastAPI application with Jinja2
templates and a JavaScript browser interface.

The runtime entry point is:

```text
biochar_app/scripts/app.py
```

It:

1. Loads environment settings.
2. Creates the FastAPI application.
3. Mounts `biochar_app/static/`.
4. Includes the main, API, bulk-download, and management routers.
5. Serves `biochar_app/templates/index.html` at `/`.
6. Ensures the processed-data directory exists.
7. Runs ETL when no Parquet files are available.
8. Builds available date ranges and preloads common logger datasets.

Step 7 is an existing recovery convenience and a coupling between website
startup and data preparation. A future deployment refactor should replace it
with an explicit, documented recovery operation before removing the behavior.

## 2. System map

```text
Raw and external sources
  ├── Campbell .dat files and accepted PakBus archive
  ├── CoAgMet weather
  ├── shared project master workbook
  ├── Ward and other laboratory files
  └── photographs and geospatial inputs
                    |
                    v
       acquisition, validation, and ETL
                    |
                    v
   canonical CSV, Parquet, metadata, and downloads
             /                        \
            v                          v
 scientific and geospatial analysis   FastAPI website
                                      ├── APIs
                                      ├── Jinja templates
                                      ├── JavaScript
                                      └── plots, tables, and downloads
```

## 3. Repository structure

```text
biochar_app/
├── config/                  Python configuration and metadata
├── data-raw/                Source observations and validated snapshots
├── data-processed/          Canonical and generated data products
├── diagnostics/             Operational checks and historical investigations
├── docs/
│   ├── operations/          Runbooks and update procedures
│   ├── project/             Architecture and developer documentation
│   ├── research/            Scientific methods and reports
│   ├── reference_manuals/   External manuals
│   └── reference_notes/     Technical reference notes
├── geospatial/              Field layout, LiDAR, drone, and QGIS work
├── markdown/                Website narrative sources, outputs, and tools
├── pakbus/                  Logger communication and acquisition
├── results/                 Derived research results
├── scripts/                 Runtime, ETL, analysis, and maintenance modules
├── static/                  Browser code, styles, images, and public assets
├── templates/               Jinja page templates
└── tests/                   Automated tests
```

The present structure is intentionally described rather than endorsed. In
particular, `scripts/`, `markdown/`, `diagnostics/`, and
`data-processed/` contain more than one responsibility.

## 4. Website backend

### Application and route registration

| Module | Current responsibility |
|---|---|
| `scripts/app.py` | FastAPI creation, router registration, SPA entry point, startup data checks, date-range creation, and cache warming |
| `scripts/routes.py` | Plot, summary, table, document, reference-report, download, custom-season, and management-page routes |
| `scripts/management/management_routes.py` | Irrigation field-entry API |
| `scripts/bulk_downloads.py` | Registry-driven bulk-download construction |
| `scripts/data_loading.py` | Processed logger and supporting-data loading |
| `scripts/aggregation.py` | Runtime data aggregation |
| `scripts/plot_builder.py` and `plot_components.py` | Plot construction |
| `scripts/tables/` | Laboratory and other table builders |
| `scripts/cache.py` and `state.py` | Runtime cache and shared application state |

`routes.py` currently combines several visitor capabilities. It is a
candidate for later router separation, but route behavior and calculations
should be protected by tests before it is divided.

### Browser and templates

| Location | Current responsibility |
|---|---|
| `templates/index.html` | Main navigation, application panes, controls, modals, and script loading |
| `templates/_custom_gseason.html` | Custom-season interface fragment |
| `templates/irrigation_entry.html` | Phone-oriented irrigation-entry prototype |
| `static/js/main.js` | Browser initialization and tab rendering |
| `static/js/api_requests.js` | API requests |
| `static/js/control_panel.js` and `ui_controls.js` | Interactive controls |
| `static/js/plots.js` and `plot_utils.js` | Plot display and browser-side helpers |
| `static/js/tab_summary.js` | Summary-statistics behavior |
| `static/js/tab_nir.js`, `tab_soil.js`, and `tab_biomass_field.js` | Laboratory result panes |
| `static/js/downloads.js` | Download actions |
| `static/js/markdown.js` | Generated narrative-content loading |
| `static/js/glossary.js` | Glossary loading, rendering, and tooltips |

`index.html` contains most public navigation and page-pane markup. Future
navigation changes should first establish content ownership, then extract
stable template components without changing visitor behavior.

## 5. ETL and data products

The principal pipeline entry point is:

```bash
python -m biochar_app.scripts.etl
```

The pipeline can:

- Refresh and validate the shared master-workbook snapshot.
- Rebuild irrigation, fertilizer, field-biomass, and Ward NIR products.
- Validate and normalize logger observations and timestamps.
- Calculate derived variables and treatment-control contrasts.
- Retrieve and process CoAgMet weather data.
- Aggregate supported time resolutions.
- Write Parquet, metadata, CSV, and download artifacts.

Detailed commands and exceptions belong in operations documentation rather than
this structure map.

### Principal data locations

| Location | Meaning |
|---|---|
| `data-raw/` | Source observations, laboratory inputs, and validated snapshots |
| `data-processed/parquet/` | Processed datasets consumed by the website and analyses |
| `data-processed/downloads/` | Prebuilt public download products |
| `data-processed/management/` | Canonical management datasets, review artifacts, and analysis outputs |
| `data-processed/lab-tests/` | Canonical cleaned laboratory and biomass products |
| `data-processed/research/` | Generated research products |

Directory placement alone does not establish control. Dataset-specific
builders and operations documentation define which input is the designated source,
which records are reviewed exceptions, and which outputs are generated.

## 6. Configuration responsibilities

Three similarly named locations have distinct roles:

| Location | Role |
|---|---|
| `biochar_app/config/` | Maintained Python configuration package for experiment facts, paths, data sources, units, thresholds, table definitions, and deployment metadata |
| `biochar_app/scripts/config.py` | Compatibility shim that re-exports the curated `biochar_app.config` API for legacy imports |
| `biochar_app/static/js/config.js` | Small browser module that fetches the server-generated narrative-document mapping and defines a local debug flag |

The Python and JavaScript files are not duplicate configuration stores and do
not need to be kept manually synchronized. New Python code should import the
specific module under `biochar_app.config` when practical. Legacy imports
through `scripts.config` can be migrated gradually and removed only after an
import audit.

Experiment facts currently live primarily in
`config/experiment_config.py`. Paths live in `config/paths.py`; external
sources in `config/data_sources.py`; analysis policies in
`config/irrigation_config.py`; and units in `config/units.py`.

## 7. Narrative website publication

Selected Word files under `markdown/docx/` are the designated editable sources for the existing
narrative pages and help text.

`markdown/tools/markdown_config.py` maps those documents to page containers.
`markdown/tools/convert_word_to_html.py` converts them into generated HTML
fragments stored with historical `.md` suffixes under
`markdown/outputs_md/`, and writes generated images under
`static/images/generated/`.

Technical, scientific-method, operations, and future public data-and-code
documentation use Markdown as their designated editable format.

## 8. Scientific and geospatial analysis

Research code currently spans:

- `scripts/management/irrigation_analysis/`
- `scripts/management/estimate_irrigation_holding_capacity.py`
- `scripts/research/`
- `geospatial/`

Generated research outputs occur in `data-processed/management/`,
`data-processed/research/`, `results/`, and `docs/research/`.

This distribution is a discoverability problem, not sufficient justification
for immediate file movement. The first improvements should be a supported-tool
catalog, explicit inputs and outputs, and a second-experiment portability test.

## 9. Tests

`biochar_app/tests/` covers portions of:

- Core defaults and experiment configuration.
- ETL timestamp handling and PakBus archive integration.
- Master-workbook validation and anonymous retrieval.
- Irrigation and fertilizer builders.
- Meter-photo review.
- Precipitation retention and irrigation analysis.
- Plot construction and custom-season behavior.
- Laboratory builders and configured reports.
- Geospatial field-layout calculations.

Future tests should mirror the intended package boundaries before modules are
moved.

## 10. Known structural issues

- Website startup and data preparation are coupled when Parquet is absent.
- `scripts/routes.py` and `templates/index.html` each contain multiple
  separable responsibilities.
- `scripts/` mixes runtime, pipeline, research, operations, and developer
  tools.
- Reusable calculations and Fruita-specific orchestration are not consistently
  separated.
- The repository does not define a supported installable package.
- Generated and reviewed artifacts require dataset-specific authority rules.
- The private irrigation-entry prototype is not yet an authenticated,
  offline-capable production field tool.

These issues should be addressed through small, tested changes after ownership
and interfaces are agreed. See the
[architecture overview](../application_architecture_status.md) and
[refactoring inventory](refactor_inventory.md).
