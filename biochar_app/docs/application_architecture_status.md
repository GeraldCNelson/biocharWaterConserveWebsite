# Biochar Application Architecture and Development Status

**Version:** 2 — October 17, 2026

**Project:** Biochar Fruita CSU Experiment

**GitHub repository:** [biocharWaterConserveWebsite](https://github.com/GeraldCNelson/biocharWaterConserveWebsite)

**Project website:** [biocharresearch.org](https://biocharresearch.org)

**Document path:** `biochar_app/docs/application_architecture_status.md`

**Status:** Current architecture overview and development guide

## 1. Purpose, readers, and scope

This document explains the Biochar Water Conservation application's research
purpose, data lifecycle, software responsibilities, scientific-analysis
workflows, publication system, and development priorities.

It is intended for:

- Researchers interpreting or comparing biochar results.
- Researchers considering the tools for another agricultural experiment.
- Field operators and data stewards collecting and reviewing observations.
- Maintainers responsible for data updates, testing, and deployment.
- Public users exploring or downloading project data.

The application was developed for the Fruita experiment. Some components are
modular, but the repository is not yet a supported general-purpose software
package. The distinction between project-specific configuration and reusable
logic is therefore both an architectural objective and a subject for testing.

This overview is a navigation guide. It does not replace the detailed
scientific, operational, or developer documents linked below. The Git
repository is the maintained source for code and technical documentation.

Links to repository files are relative links. They open the corresponding file
in a local checkout and resolve to the currently viewed branch or commit when
this document is read on GitHub.

**Primary references:** [documentation catalog](documentation_catalog.md),
[current application structure](project/app_structure.md),
[architecture harmonization assessment](project/architecture_harmonization_proposal.md),
and [refactoring inventory](project/refactor_inventory.md).

## 2. Research questions and experimental design

The application supports the Biochar Water Conservation experiment at the
Colorado State University research station near Fruita, Colorado.

The experiment investigates whether biochar can improve water availability and
agricultural performance under arid western Colorado conditions. It also
provides evidence relevant to soil health, nutrient needs, fertilizer effects,
microbial communities, pasture quality, and crop biomass.

Four experimental strips are monitored:

| Strip | Treatment | Irrigation regime |
|---|---|---|
| S1 | Biochar | Monthly |
| S2 | Control | Monthly |
| S3 | Biochar | Approximately biweekly |
| S4 | Control | Approximately biweekly |

See the [field-layout map](../geospatial/field_layout/fruita_field_layout.png)
for the strip arrangement and logger positions.

The primary matched comparisons are S1 versus S2 and S3 versus S4. Twelve
Campbell Scientific dataloggers monitor Top, Middle, and Bottom positions
within the four strips. Each position has sensors at nominal depths of 6, 12,
and 18 inches. Measurements include volumetric water content, temperature, and
bulk electrical conductivity.

The application integrates these measurements with irrigation records, weather
observations, laboratory analyses, field measurements, management records,
photographs, and geospatial information.

### Research questions

The current and planned analyses are intended to help answer:

- Does biochar alter water retained in the monitored soil profile following
  irrigation or rainfall?
- Do biochar and control strips differ in soil chemistry, biological health,
  nutrient status, fertilizer needs, crop biomass, or pasture quality?
- What characteristics of the biochar used in this experiment are relevant to
  holding water, nutrients, or microorganisms?
- Are those possible pore-scale mechanisms supported directly by project
  measurements, indirectly consistent with the results, or still untested?
- What measurements and processing steps would another experiment need to make
  a meaningful comparison?

Repeated measurements and irrigation events provide valuable within-experiment
evidence, but they are not independent treatment replications. Results should
be described as matched strip contrasts and observed associations unless the
experimental design supports a stronger inference.

**Repository anchors:** [experiment configuration](../config/experiment_config.py),
[generated Experiment Design content](../markdown/outputs_md/experimentDesign.md),
and [field layout](../geospatial/field_layout/README.md).

## 3. Users and supported workflows

| User | Primary need | Current support | Important gap |
|---|---|---|---|
| Biochar researcher | Compare project results with another biochar study | Experiment description, downloads, continuous monitoring, laboratory tables, and research reports | A question-oriented synthesis across water, nutrients, microorganisms, biochar properties, and crop response |
| Researcher adapting the tools | Determine required data and reuse suitable modules | Modular calculations, configuration files, ETL commands, and tests | Supported installation, tool catalog, input contracts, and a contrasting example experiment |
| Field operator or data steward | Collect, review, and promote defensible field records | Management workbook, meter photographs, photo-review tools, and an irrigation-entry prototype | A tested offline workflow, clear review states, and integrated field observations |
| Maintainer | Keep acquisition, processing, analysis, and publication reliable | Operations guides, tests, deployment scripts, diagnostics, and generated catalogs | Explicit ownership, workload expectations, recovery procedures, and reduced coupling |
| Public visitor or data user | Understand the study, explore findings, and download data | Public dashboard, narrative pages, plots, tables, glossary, and downloads | Navigation organized more directly around research questions and data/code reuse |

The architecture should make these workflows visible without placing internal
server administration or troubleshooting details on the public website.

## 4. System boundaries and data flow

The system contains several responsibilities that currently share a repository
and, in some cases, modules:

```text
Field instruments, weather, laboratory, and management records
                         |
                         v
              acquisition and raw evidence
                         |
                         v
           validation, review, and transformation
                         |
                         v
          canonical and analysis-ready datasets
                    /             \
                   v               v
       scientific analysis     public website
```

| Responsibility | Current implementation | Intended boundary |
|---|---|---|
| Public website runtime | FastAPI, Jinja2, JavaScript, Bootstrap, Plotly, routes, tables, and downloads | Reads published or analysis-ready products; does not own field acquisition or scientific rebuilding |
| Data acquisition | [PakBus logger collection](../pakbus/core/daily_download.py), Campbell Scientific [TOA5 `.dat` files](https://help.campbellsci.com/loggernet-manual/ln_manual/campbell_scientific_file_formats/toa5.htm), [CoAgMET retrieval](../scripts/get_weather_data.py), [shared workbook retrieval](../scripts/management/update_master_workbook_snapshot.py), and [laboratory source intake](operations/lab_data_pipeline.md) | Preserves source evidence and acquisition metadata |
| Validation and ETL | `biochar_app/scripts/etl.py` and supporting builders | Produces validated, standardized, and reproducible datasets |
| Scientific analysis | Irrigation, precipitation, treatment comparison, electrical-conductivity, and geospatial workflows | Applies documented rules to decide which events or measurements can be included in each analysis and states the limits on interpretation |
| Field operations and data stewardship | Irrigation records, meter photographs, management tools, and review workbooks | Collects and reviews operational observations before promotion |
| Configuration | `biochar_app/config/` plus a legacy compatibility shim | Separates experiment facts, paths, units, thresholds, and presentation settings |
| Documentation and publication | Markdown documentation plus selected Word-authored narrative pages | Declares one designated editable source and a reproducible build path for each generated artifact |

“Remain distinguishable from the public website runtime” means that these
responsibilities should have identifiable inputs, outputs, owners, tests, and
commands. It does not mean that all files must immediately be moved into new
directories.

Two current coupling points deserve attention:

- Website startup invokes ETL when no Parquet files are present.
- The main routes module and page template each contain several otherwise
  separable visitor capabilities.

Those are candidates for staged refactoring. Before they are changed,
automated tests should confirm that routes, calculations, and analysis-inclusion
decisions continue to produce the expected results. Tests are maintained in the
[`biochar_app/tests/`](../tests/) directory; relevant examples include
[`test_custom_season_plots.py`](../tests/test_custom_season_plots.py) and
[`test_precipitation_retention.py`](../tests/test_precipitation_retention.py).

**Repository anchors:** [FastAPI entry point](../scripts/app.py),
[routes](../scripts/routes.py), [main template](../templates/index.html),
[configuration package](../config/), and
[tests](../tests/).

## 5. Data sources and collection systems

### 5.1 Dataloggers and weather

Historical logger inputs are Campbell Scientific TOA5 [`.dat`
files](https://help.campbellsci.com/loggernet-manual/ln_manual/campbell_scientific_file_formats/campbell_scientific_file_formats.htm?TocPath=Campbell+Scientific+File+Formats%7C_____0).
Beginning in 2026, the project also uses a nightly PakBus acquisition workflow;
records are added to its annual archive only after acceptance checks. The year
marks a change in acquisition method, not a statement that the 2026 dataset is
incomplete. The ETL entry point is
[`biochar_app/scripts/etl.py`](../scripts/etl.py).

The logger workflow:

1. Validates expected input files and station coverage.
2. Parses logger clock text and applies documented logger-specific corrections.
3. Interprets corrected logger time from a fixed-MST base and converts it to
   Colorado civil time.
4. Removes invalid placeholders and applies plausible-value bounds.
5. Calculates derived variables, including soil-water quantities and
   treatment-control contrasts.
6. Aggregates logger data to supported time resolutions.
7. Writes Parquet datasets, metadata, and download products.

The [Colorado Agricultural Meteorological Network
(CoAgMET)](https://www.coagmet.colostate.edu/) is a network of research-grade
weather stations managed by the Colorado Climate Center. Its
[FRT3 station](https://coagmet.colostate.edu/station_index.php), at the CSU
Fruita Experiment Station, provides environmental context for this project.
The pipeline fetches and cleans its observations, including precipitation,
then produces aligned weather datasets and downloads.

A clock-correction change requires regeneration of affected downstream
products because incorrect time alignment can produce misleading irrigation
arrival estimates.

### 5.2 Project master workbook and management data

The project master workbook is the primary collaborative record for
field-management information maintained there. A validated repository snapshot
is installed as:

`biochar_app/data-raw/biochar-data-master.xlsx`

The currently validated required-sheet list covers annual irrigation and
field-biomass worksheets. Other builders also read fertilizer recommendations
and field-level fertilization dates where available.

The workbook is not the source of all laboratory data. It supplies irrigation,
field biomass, and selected management information; Ward laboratory files and
other registered sources remain separate inputs.

The normal full ETL and the operational irrigation refresh read the workbook
directly from its shared OneDrive web location. The
[retrieval code](../scripts/management/update_master_workbook_snapshot.py)
opens the configured shared-folder link in a fresh web session, downloads the
file by its stable OneDrive identifier, validates the required worksheets and
file hashes, and replaces the repository snapshot only after those checks pass.
The user does not normally update the snapshot manually. An unchanged workbook
simply produces an “identical snapshot” result.

A local OneDrive-synchronized file remains an explicit fallback through the
standalone refresh command's `--source` option; it is not the normal input path.

**Related documentation:** [automatic irrigation refresh](operations/automatic_irrigation_refresh.md)
and [laboratory data pipeline](operations/lab_data_pipeline.md).

### 5.3 Laboratory and field-sample data

“Ward” refers to [Ward Laboratories, Inc.](https://www.wardlab.com/), the
agricultural testing laboratory that produced the project's forage, soil, and
soil-health results. NIR means near-infrared reflectance spectroscopy, a method
used here to estimate pasture and forage characteristics.

Laboratory workflows currently include:

- Ward NIR pasture-quality data from a compiled source plus explicitly
  registered supplemental files.
- Ward soil-biology data from compiled and supplemental laboratory files.
- Ward soil-chemistry data from compiled and supplemental laboratory files.
- Field biomass from master-workbook worksheets combined with a reviewed
  historical dataset.
- Biochar material-characterization reports served as configured source PDFs.

Each source has its own authority and merge rules. The website presents selected
results through the Lab-Based Results interface, but presentation in one
website menu does not imply a single upstream source.

### 5.4 Irrigation and field observations

Validated irrigation records are rebuilt from the workbook snapshot and
photo-supported quality-control sources. Photographs of the water inflow meter
at irrigation start and end provide independent evidence for reviewing
timestamps and meter readings. The photo-comparison workflow supports review;
it does not independently change logger timestamps.

The repository contains an early phone-oriented irrigation-entry interface.
Turning it into an installable, offline-capable field-observation tool is
deferred to the off-season preparation for the next field season.

### 5.5 Geospatial data, hardware, and future drone observations

The project includes field-layout information, approximate photo-derived
geometry, LiDAR terrain analysis, and directories for drone source and
processed products.

Hardware documentation should eventually identify:

- The question each instrument answers.
- Required and optional equipment.
- Calibration, placement, collection frequency, and expected service life.
- Acquisition, maintenance, and replacement costs.
- Whether the current instrument density is sufficient, redundant, or leaves
  important gaps.

A future drone pilot should begin with repeatable RGB flights and paired ground
observations. Specialized sensors should be considered only after a specific
research benefit, calibration plan, operating requirements, and maintenance
burden are established.

## 6. Processing, provenance, and publication

### 6.1 ETL responsibilities

The preferred full-pipeline command is:

```bash
python -m biochar_app.scripts.etl
```

Depending on the selected mode, ETL can:

- Retrieve and validate the master-workbook snapshot.
- Rebuild irrigation, fertilizer, field-biomass, and Ward NIR products.
- Validate and process logger observations.
- Apply clock, unit, and plausible-value rules.
- Calculate derived soil-water and treatment-comparison variables.
- Fetch and process weather data.
- Aggregate observations to 15-minute, hourly, daily, monthly, and configured
  growing-season products.
- Build website datasets, metadata, and download archives.

The command's skip and operational modes are documented in the
[repository README](../../README.md) and the relevant operations guides.

### 6.2 Designated source records and generated outputs

For each dataset, the project identifies the source file or files that should
be corrected when an observation is wrong. That designated source differs by
dataset: irrigation begins with the shared management workbook, laboratory
results begin with the applicable laboratory files, and narrative website
pages begin with selected Word documents. Merely placing a file in `data-raw/`
or `data-processed/` does not make it the controlling source.

| Artifact family | Designated source or reviewed input | Generated output | Primary consumer |
|---|---|---|---|
| Narrative website pages | Selected `markdown/docx/*.docx` files | `markdown/outputs_md/*.md` HTML fragments and generated images | Public website |
| Logger and weather data | Raw logger evidence, accepted PakBus records, clock-correction policy, and CoAgMet observations | Parquet summaries and download files | Website and research analyses |
| Management records | Shared workbook plus documented photo-supported corrections | Canonical irrigation, fertilizer, and biomass products | Website and analyses |
| Laboratory data | Compiled and registered supplemental laboratory sources | Canonical cleaned CSVs and header metadata | Laboratory tables and analyses |
| Field layout | Geometry inputs and the field-layout build script | Published maps and tabular spatial summaries | Website and spatial analyses |
| Documentation catalogs | Repository documentation files | Documentation and function catalogs | Maintainers and contributors |

One current exception is field biomass for 2023–2025. The reviewed
[`field_biomass_dry_g_2023_2025.csv`](../data-processed/lab-tests/biomass-field/csv-files/field_biomass_dry_g_2023_2025.csv)
remains the approved record for those years because some values in the current
workbook differ from values previously reviewed and published. The biomass
builder uses workbook observations to fill historical gaps and add later
sampling dates; it does not silently replace the reviewed 2023–2025 values.

Generated files must not silently become editable sources. Each workflow should
identify its input, generator, review state, output, and consumers.

### 6.3 Word-authored website content

Existing narrative website pages, such as the Experiment Design page, retain
their Word-based authoring workflow.
[`markdown_config.py`](../markdown/tools/markdown_config.py) declares which
Word documents populate application containers.
[`convert_word_to_html.py`](../markdown/tools/convert_word_to_html.py)
extracts text, headings, links, tables, images, and captions; creates web-ready
image derivatives; and writes the generated website fragments.

Technical, research-method, public data-and-code, and operational
documentation should use Markdown as their designated editable format.

## 7. Scientific analyses and findings

### 7.1 Water availability from irrigation and rainfall: contribution of biochar

**Status:** Implemented analytical workflows; scientific evaluation continues.

The principal orchestration module is
[`estimate_irrigation_holding_capacity.py`](../scripts/management/estimate_irrigation_holding_capacity.py).
Supporting modules identify water-arrival timing, evaluate sensor responses,
screen events for quality-control eligibility, estimate retained soil water,
analyze rainfall events, create figures, and produce reports.

The workflow supports matched S1/S2 and S3/S4 contrasts, with biochar values
compared with their non-biochar control under the corresponding irrigation
regime. It also combines eligible irrigation and precipitation observations to
estimate empirical upper retained volumetric water content and available
storage before irrigation.

Important interpretive limits are:

- Estimated water not retained in the monitored profile is not measured runoff.
  It can include drainage, lateral movement, measurement uncertainty, and other
  processes.
- Candidate inspection alerts are retrospective research outputs, not automated
  irrigation controls.
- Repeated events increase temporal evidence but do not create independent
  treatment replication.
- Sensor responses describe the monitored soil volumes; they do not directly
  observe water held within individual biochar pores.

**Related documentation:** [irrigation analysis pipeline](operations/irrigation_analysis_pipeline.md)
and [irrigation water-balance research](research/irrigation_flow_water_balance/README.md).

### 7.2 Soil health, nutrient needs, and fertilizer effects

The current website presents soil biology and soil chemistry tables, while
electrical-conductivity analyses provide additional context. Fertilizer
builders create recommendation-derived nutrient requirements and field-level
activity dates.

The current activity records do not by themselves establish which products,
rates, amounts, or strips were actually treated. Future treated-versus-control
analysis must separate:

- Laboratory measurements of nutrients, pH, salinity, carbon, and microbial
  communities.
- Fertilizer recommendations.
- Documented fertilizer applications.
- Sensor electrical conductivity.
- Crop nutrient removal and biomass response.

This is a priority analytical workstream, not yet a single integrated causal
analysis.

### 7.3 Crop performance and pasture quality

Field biomass and Ward NIR data provide evidence about production, forage
quality, minerals, digestibility, and related outcomes. Future analysis should
connect these measurements with treatment, water availability, soil health,
nutrient status, and management timing while preserving the sampling and
replication limitations.

Repeatable field photographs could add qualitative and spatial context, but
they require consistent locations, directions, timing, and metadata.

### 7.4 Biochar material characteristics and possible mechanisms

The public website serves configured laboratory reports that characterize the
biochar material. These measurements should be summarized alongside the field
results so that another researcher can judge comparability.

Water, nutrients, and microorganisms in biochar pores are scientifically
relevant possible mechanisms. The project should distinguish material
characteristics that were measured, field outcomes that were observed, and
mechanisms that remain hypotheses.

### 7.5 Spatial context

Field layout, terrain, LiDAR, logger influence-zone assumptions, and future
drone observations provide context for spatial differences within and among
strips. Photo-derived geometry is useful for mapping but is not automatically
survey-grade or suitable for every water-volume calculation.

**Related documentation:** [field layout](../geospatial/field_layout/README.md),
[LiDAR overview](../geospatial/lidar/README.md), and
[LiDAR pipelines](../geospatial/lidar/pipelines/README.md).

## 8. Website and public information architecture

The current website navigation is:

| Current area | Contents |
|---|---|
| Introduction | Project narrative |
| Experiment Design | Treatments, layout, and methods |
| Continuous Monitoring Results | Interactive plots, summary statistics, and custom seasons |
| Lab-Based Results | Biochar characterization, pasture quality, biomass, soil biology, and soil chemistry |
| Technical Details | Narrative technical background |
| Bulk Downloads | Downloadable datasets and README files |
| Glossary | Terms and source references |
| Acknowledgements | Contributors and support |

This structure remains operational, but it emphasizes presentation formats and
data sources. The agreed target content model is:

| Proposed area | Purpose |
|---|---|
| About the Experiment | Research questions, site, design, treatments, and limitations |
| Research Results | Water availability and biochar; soil health, nutrients, and fertilizer; crop performance; biochar material; and spatial context |
| Explore the Data | Plots, summaries, custom seasons, laboratory tables, and downloads |
| Methods and Field System | Sensors, sampling, irrigation measurement, field observations, hardware, and geospatial or drone methods |
| Use the Data and Code | Data dictionary, provenance, reproducible workflows, supported tools, minimum input requirements, adaptation examples, citation, and issue reporting |
| Reference | Glossary, acknowledgements, and related reports |

This table records the target information architecture. Navigation and page
code should change only after content ownership and labels are finalized.
Internal deployment, credentials, troubleshooting, and private field operations
remain repository-only.

## 9. Reusable tools and adaptation

The repository already contains potentially reusable components for:

- Experiment and sensor configuration.
- Logger timestamp normalization and aggregation.
- Unit conversion and variable metadata.
- Treatment-control comparisons.
- Irrigation-event response and precipitation-retention analysis.
- Laboratory source normalization.
- Plotting, table serialization, downloads, and documentation catalogs.
- Field layout and geospatial analysis.

These components are not yet a supported toolkit. The repository does not
define an installable package, and several workflows still combine
general calculations with Fruita-specific paths, strip labels, or source
formats.

Before advertising reuse, the project should:

1. Publish a tool catalog with inputs, outputs, examples, limitations, and
   stability status.
2. Define a schema for experiment configuration and required metadata.
3. Separate general calculations from project orchestration.
4. Demonstrate the workflow with a contrasting dataset, such as a rainfed maize
   experiment in Illinois.
5. Record what could be reused unchanged, configured, or rewritten.
6. Add installation, versioning, citation, and issue-reporting guidance.

## 10. Operations, maintenance, and governance

| Role | Principal responsibilities |
|---|---|
| Field operator | Collect complete observations and preserve source evidence |
| Data steward | Review corrections, resolve discrepancies, and approve promotion to canonical data |
| Scientific maintainer | Maintain analysis assumptions, eligibility rules, interpretation, and reproducibility |
| Software maintainer | Maintain dependencies, tests, deployment, monitoring, security, and recovery |
| Content maintainer | Maintain designated narrative sources, regenerate published content, and verify links |

The project should identify at least one named maintainer or steward for each
active responsibility, even when one person fills several roles.

Maintenance effort depends on:

- Frequency and variability of new data.
- Reliability and replacement rate of field hardware.
- Number of manual review exceptions.
- External service and source-format changes.
- Breadth of supported website and analysis features.
- Test coverage and release automation.
- Number of outside users and supported experiment configurations.

Operations guidance belongs in `docs/operations/`; research methods and
interpretation belong in `docs/research/`; architecture, testing, and
developer guidance belong in `docs/project/`.

## 11. Priorities, known gaps, and decisions

### Immediate: documentation and architecture

- Keep this architecture overview aligned with the implemented system.
- Maintain an accurate FastAPI application map.
- Correct operational documentation when workflow behavior changes.
- Define public content ownership before altering website navigation.
- Preserve the distinction between designated source records and generated outputs.

### Next: scientific analysis and reuse

- Develop an integrated plan for soil health, nutrient needs, fertilizer
  effects, biomass, and pasture-quality comparisons.
- Build a user-oriented catalog of supported tools.
- Identify general algorithms and Fruita-specific configuration.
- Test portability with a second experiment.
- Document maintenance effort and hardware costs.

### Off-season: preparation for the next field season

- Specify and test the irrigation and field-observation capture workflow.
- Add durable offline synchronization and provenance only after the data model
  and review process are agreed.
- Pilot repeatable ground photography and question-led RGB drone observations.

### Refactoring safeguards

Before a module or directory is moved, identify its owner, inputs, outputs,
consumers, commands, deployment role, and regression checks. Preserve data
provenance and reproducibility throughout the transition.

## 12. Repository map and detailed references

| Current location | Primary role |
|---|---|
| `biochar_app/config/` | Paths, experiment facts, units, thresholds, source definitions, and presentation configuration |
| `biochar_app/scripts/` | Mixed website runtime, ETL, analysis, management, and developer utilities |
| `biochar_app/pakbus/` | Logger communication, accepted acquisition workflows, and protocol tools |
| `biochar_app/templates/` and `static/` | Website presentation and browser behavior |
| `biochar_app/data-raw/` | Source observations and validated source snapshots |
| `biochar_app/data-processed/` | Canonical datasets, application products, downloads, and generated analysis outputs |
| `biochar_app/docs/operations/` | Maintained procedures for data updates, deployment, and troubleshooting |
| `biochar_app/docs/research/` | Scientific methods, evaluations, and reports |
| `biochar_app/docs/project/` | Architecture, testing, inventories, and maintainer guidance |
| `biochar_app/geospatial/` | Field layout, LiDAR, drone, and QGIS research assets |
| `biochar_app/markdown/` | Word-authored website sources, generated website fragments, conversion tools, and legacy notes |
| `biochar_app/tests/` | Automated verification of runtime, pipeline, analysis, and source-processing behavior |
| `biochar_app/archive/` | Historical material kept outside active imports and workflows |

**Development references:** [application structure](project/app_structure.md),
[refactoring inventory](project/refactor_inventory.md),
[developer notes and roadmap](project/developer_notes.md),
[testing guidance](project/testing.md), and
[function catalog](project/function_catalog.md).

**Operational references:** [ETL and irrigation analysis](operations/irrigation_analysis_pipeline.md),
[laboratory pipeline](operations/lab_data_pipeline.md),
[automatic irrigation refresh](operations/automatic_irrigation_refresh.md),
and [deployment](operations/deploy_to_main.md).

The architecture overview should be reviewed after material workflow or
responsibility changes. Generated catalogs may be refreshed more frequently.
