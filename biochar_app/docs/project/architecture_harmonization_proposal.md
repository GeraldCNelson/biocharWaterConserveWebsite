# Architecture, Documentation, and Website Harmonization Proposal

**Status:** Accepted documentation plan; implemented without moving code or directories

**Reviewed branch:** `etl-refactor`

**Review date:** October 15, 2026

## 1. Recommendation in brief

The architecture document, repository, and website should use the same core
vocabulary, but they should not be forced into identical structures.

They serve different purposes:

- The architecture document explains the whole system and the reasons for it.
- The repository assigns ownership to code, data, documentation, and generated
  artifacts.
- The public website helps visitors answer research and data-use questions.

The recommended approach is therefore a **hybrid structure**:

1. Organize the architecture document around users, research questions, the
   data lifecycle, and system responsibilities.
2. Give each architecture section explicit repository anchors.
3. Organize the public website around visitor tasks and scientific questions.
4. Refactor directories only after ownership boundaries and tests are agreed.

This preserves discoverability without exposing internal implementation details
as the public information architecture.

## 2. Evidence from the current application

| Area | Current evidence | Architectural implication |
|---|---|---|
| Application entry point | [`scripts/app.py`](../../scripts/app.py) creates a FastAPI application, mounts static files, includes page, API, and management routers, and serves `templates/index.html`. | [`app_structure.md`](app_structure.md) is obsolete where it describes Flask and root-level modules. |
| Runtime and ETL boundary | Application startup runs ETL when it cannot find Parquet files. | The website can initiate data preparation implicitly. The desired production boundary between serving and rebuilding data is not yet complete. |
| Backend organization | [`scripts/routes.py`](../../scripts/routes.py) is approximately 1,700 lines and contains plots, summaries, downloads, Markdown delivery, laboratory tables, reference reports, and a management page. | Future separation should follow responsibilities already visible in the routes, but only after route and calculation tests exist. |
| Browser organization | [`templates/index.html`](../../templates/index.html) is approximately 1,100 lines and contains the main navigation and most page panes. | Website navigation and presentation are concentrated in one template even though the content now represents several distinct user tasks. |
| Current public navigation | The website presents Introduction, Experiment Design, Continuous Monitoring Results, Lab-Based Results, Technical Details, Bulk Downloads, Glossary, and Acknowledgements. | Navigation describes presentation formats and data sources more clearly than it describes the principal research questions or reusable tools. |
| Data acquisition and ETL | [`scripts/etl.py`](../../scripts/etl.py) refreshes and validates the management workbook, processes logger timestamps and measurements, incorporates weather and selected laboratory data, calculates derived variables, aggregates observations, and builds publication outputs. | “Validate, transform, aggregate, and publish” should be expanded into these concrete responsibilities and linked to the entry point. |
| Experiment definition | [`config/experiment_config.py`](../../config/experiment_config.py) defines strips, treatments, irrigation regimes, treatment pairs, logger locations, and sensor depths. | Some experiment design is centralized, but a reusable toolkit must distinguish general algorithms from Fruita-specific configuration. |
| Management workbook | [`config/data_sources.py`](../../config/data_sources.py) identifies the workbook as the primary collaborative field-management record. Its required sheets currently cover annual irrigation and biomass observations. | The workbook is important but is not the source of all laboratory or research data. Its exact contents and role must be described narrowly. |
| Laboratory workflow | [`operations/lab_data_pipeline.md`](../operations/lab_data_pipeline.md) combines workbook-derived biomass with separately registered Ward NIR files. Other raw and processed laboratory directories also exist for soil biology and chemistry. | The documentation should describe several laboratory sources rather than imply that the master workbook contains all laboratory data. |
| Website narrative publishing | [`markdown/tools/markdown_config.py`](../../markdown/tools/markdown_config.py) maps selected Word documents to website containers. [`convert_word_to_html.py`](../../markdown/tools/convert_word_to_html.py) converts them into generated website fragments and images. | The source-to-generated relationship is implemented and can be linked and explained precisely. |
| Scientific analysis | Irrigation, precipitation retention, laboratory, field-biomass, fertilizer, geospatial, and other research assets span `scripts/management/`, `scripts/research/`, `data-processed/research/`, `docs/research/`, and `geospatial/`. | Current directory placement does not provide a single discoverable map of scientific questions, supported methods, and outputs. |
| Data provenance | `data-raw/`, `data-processed/`, generated website content, reviewed historical files, and canonical management outputs use different control rules. | Each dataset needs a declared editable or reviewed source record together with its generator, review state, and consumer; directory placement alone does not settle that question. |
| Reuse and installation | The repository has no supported package definition, while reusable logic and project-specific orchestration coexist under `biochar_app`. | Reuse is an intention rather than a supported installation interface. Documentation and one external-example exercise should precede packaging claims. |
| Field and geospatial tools | An irrigation-entry prototype and existing photo-review tools are present. `geospatial/drone/` and field-layout resources already provide a home for spatial work. | Field capture and drones belong in the architecture as future data-acquisition capabilities, but their implementation remains an off-season, lower-priority workstream. |

## 3. Crosswalk of the current architecture headings

| Current architecture heading | Principal repository anchors | Current website counterpart | Finding |
|---|---|---|---|
| Purpose and scope | `docs/`, documentation catalog | None | Appropriate opening, but it should identify intended readers and how to use the document. |
| Research context | Experiment configuration, field layout, generated Experiment Design content | Introduction; Experiment Design | Strong alignment, although the document should distinguish the Fruita experiment from the reusable software ambition. |
| Software architecture | `scripts/app.py`, `scripts/routes.py`, `templates/`, `static/`, `config/` | Technical Details only partially addresses this | Too broad as one section. It should present system boundaries and a data-flow map before implementation details. |
| Data acquisition and processing | `pakbus/`, `scripts/etl.py`, `scripts/lab/`, `scripts/management/`, `data-raw/`, `data-processed/` | Continuous Monitoring; Lab-Based Results; portions of Technical Details | The heading is useful, but source acquisition, validation, transformation, review, and publication should be distinguishable. |
| Scientific analysis | Management analysis, research scripts and reports, geospatial work | Continuous Monitoring and Lab-Based Results | Organized partly by mechanism rather than by the questions researchers want to answer. |
| Development priorities | Project documentation and issue tracking | Not a public website concern | Keep in the architecture document or roadmap, not in the scientific website navigation. |
| Documentation and maintenance principles | `docs/`, `markdown/`, conversion tools, deployment documentation | No “Using the Data and Code” area | Important material exists but is difficult for prospective users and maintainers to discover. |
| Review notes | Architecture and refactoring documents | None | Replace eventually with a maintained “known gaps and decisions” section. |

## 4. Users and the questions the structure should answer

### 4.1 Biochar researcher

Needs to understand:

- The biochar material used, including characteristics relevant to pore storage
  and biological interactions.
- Evidence about water, nutrients, and microorganisms associated with biochar.
- How treated and control strips compare for soil health, nutrient needs,
  fertilizer response, crop performance, and water availability.
- Whether measurements, units, timing, and experimental limitations permit a
  meaningful comparison with another study.

### 4.2 Researcher adapting the tools

Needs to understand:

- Which tools are general and which encode Fruita assumptions.
- Minimum and optional data inputs, formats, units, metadata, and sampling
  frequency.
- How to configure treatments, plots, sensors, weather, management events, and
  laboratory measurements.
- How the tools behave with a contrasting example, such as a rainfed maize
  experiment in Illinois.

### 4.3 Field operator and data steward

Needs to understand:

- How observations are collected, synchronized, reviewed, corrected, and
  promoted to a designated canonical dataset.
- Which hardware is required, useful, redundant, or optional.
- How phone photographs, meter readings, notes, repeat field photographs, and
  future drone observations retain timestamps and provenance.

### 4.4 Maintainer

Needs to understand:

- What must be run routinely and after new data arrive.
- Which generated files must not be edited directly.
- Deployment, testing, backups, credentials, monitoring, and failure recovery.
- The likely maintenance burden and which responsibilities require a named
  owner.

### 4.5 Public visitor and data user

Needs to understand:

- What was studied and why.
- What was observed, what was inferred, and what remains uncertain.
- How to explore or download data and how to cite or question the work.

## 5. Proposed structure for the architecture document

The following outline would replace the current high-level sequence after its
content has been reviewed and carried forward.

### 1. Purpose, readers, and scope

- Intended readers and navigation guidance.
- Fruita application versus reusable-toolkit ambition.
- Links to the documentation catalog and repository.

### 2. Research questions and experimental design

- Treatment layout and monitored variables.
- Biochar material characteristics.
- Questions about water, soil health, nutrients, fertilizer, microorganisms,
  and crop performance.
- Limits on replication and causal inference.

**Repository anchors:** `config/experiment_config.py`, Experiment Design source,
and `geospatial/field_layout/`.

### 3. Users and supported workflows

- Biochar comparison and interpretation.
- Data exploration and download.
- Reproduction of project analyses.
- Adaptation to another experiment.
- Field operations, review, and maintenance.

### 4. System boundaries and data flow

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

- Public website runtime.
- Acquisition and operational tools.
- ETL and publication pipeline.
- Scientific analysis.
- Shared configuration.
- Generated artifacts and archives.

This is the place to explain that these responsibilities should remain
distinguishable. It does not imply that all code must immediately move.

### 5. Data sources and collection systems

- Dataloggers and sensor hardware.
- Weather observations.
- Irrigation and other field-management records.
- Laboratory and field-sample data.
- Field photographs, future phone capture, and data stewardship.
- Field layout, LiDAR, and future drone observations.
- Hardware inventory, costs, calibration, replacement, and data sufficiency.

### 6. Processing, provenance, and publication

- The ETL entry point and its concrete stages.
- The master workbook's defined contents and authority.
- Dataset-specific sources of truth.
- Review and quality-control states.
- Generated outputs and regeneration commands.
- Word-to-website publishing and its conversion script.

### 7. Scientific analyses and findings

#### Water availability from irrigation and rainfall: contribution of biochar

Combine the current irrigation-response and precipitation-retention discussion.
Distinguish observed water application and sensor response from estimates of
storage, drainage, lateral movement, or other unmeasured losses.

#### Soil health, nutrients, and fertilizer effects

Bring together soil biology, soil chemistry, electrical conductivity, nutrient
needs, fertilizer records, and the limits of treatment comparisons.

#### Crop performance and pasture quality

Connect biomass, NIR pasture-quality metrics, field observations, and future
repeat photography.

#### Biochar material characteristics and mechanisms

Describe measured biochar characteristics and frame questions about water,
nutrients, and microorganisms in pores without claiming mechanisms not measured
by the experiment.

#### Spatial context

Explain field geometry, terrain, LiDAR, and possible drone-derived measurements.

### 8. Website and public information architecture

- Current website mapping.
- Proposed navigation and content ownership.
- Public versus internal operational information.
- Accessibility, downloads, citation, and issue reporting.

### 9. Reusable tools and adaptation

- Supported tool catalog.
- Inputs, outputs, examples, and limitations.
- Separation of general algorithms from experiment configuration.
- A second-experiment demonstration.

### 10. Operations, maintenance, and governance

- Maintainer responsibilities and recurring effort.
- Testing, deployment, monitoring, backups, and recovery.
- Data stewardship and release decisions.

### 11. Priorities, known gaps, and decisions

- Agreed sequence of documentation and code work.
- Deferred field application and drone pilot.
- Decisions that must precede directory or API changes.

### 12. Repository map and detailed references

- Concise directory ownership map.
- Links to operations, research, project, and reference documentation.
- Source-to-output table for generated artifacts.

## 6. Proposed public website organization

The public website should not mirror `scripts/`, `config/`, or the data
directories. A visitor-oriented navigation could be:

| Proposed area | Contents | Existing material to reuse |
|---|---|---|
| About the Experiment | Introduction, research questions, site, design, treatments, limitations | Introduction; Experiment Design |
| Research Results | Water availability and biochar; soil health, nutrients, and fertilizer; crop performance; biochar material characterization; spatial context | Continuous Monitoring Results; Lab-Based Results; research reports |
| Explore the Data | Interactive plots, summary statistics, custom seasons, laboratory tables, and downloads | Current monitoring, laboratory, and Bulk Downloads interfaces |
| Methods and Field System | Sensors, sampling, irrigation measurement, field observations, hardware, and geospatial or drone methods | Technical Details; field-layout and operational material selected for publication |
| Use the Data and Code | Data dictionary, provenance, reproducible workflows, reusable tools, minimum input requirements, adaptation example, citation, and issue reporting | Refactoring inventory decisions; download documentation; future tool catalog |
| Reference | Glossary, acknowledgements, and related reports | Current Glossary and Acknowledgements |

This is a content model, not yet a request to rebuild the navigation. The next
step is to agree on the labels and decide which research results are sufficiently
mature for public presentation.

## 7. Documentation changes that can precede code refactoring

The earlier review comments can be resolved through documentation work first:

1. Repair all Markdown tables and validate them in PyCharm.
2. Replace “Irrigation-response analysis” and the separate precipitation section
   with the proposed water-availability section.
3. Link “enter the ETL pipeline” directly to `scripts/etl.py` and describe the
   actual processing stages.
4. Explain the public website runtime, pipeline, research, and operations
   boundaries without implying that code has already been separated.
5. Define the master workbook by its actual irrigation and biomass sheets and
   explain its relationship to independent laboratory sources.
6. Revise the laboratory pipeline's opening instruction. The current ETL can
   download and validate the workbook snapshot; starting OneDrive is therefore
   not universally the first step.
7. Explain source-versus-generated status per artifact, including reviewed
   historical datasets that intentionally override current workbook values.
8. Link the Word publishing description to
   `markdown/tools/convert_word_to_html.py` and its mapping configuration.
9. Add repository anchors beneath architecture sections rather than making the
   headings literal directory names.

## 8. Refactoring implications, without beginning the refactor

The evidence suggests the following future seams:

- Split the web routes by visitor capability: data exploration, summaries,
  downloads, laboratory results, documents, and private management functions.
- Split the main template into navigation and domain-specific page components.
- Remove implicit ETL execution from normal website startup after deployment and
  recovery requirements are defined.
- Separate reusable calculations from Fruita orchestration and configuration.
- Reclassify meter-photo review and comparable quality-control tools as data
  stewardship or operations rather than generic developer utilities.
- Establish a supported installation and command interface before presenting
  the code as a reusable package.
- Make tests mirror the agreed boundaries before moving modules.

No directory move should begin until each affected component has:

1. An agreed owner and responsibility.
2. Identified inputs, outputs, and consumers.
3. Tests or reproducible checks that protect current behavior.
4. A migration plan for commands, imports, deployment, and documentation links.

## 9. Implementation status and remaining sequence

- [x] Approve the architecture-document outline in this proposal.
- [x] Approve and record the public website content model without rebuilding
  the navigation yet.
- [x] Revise `application_architecture_status.md` using the agreed outline and
  the earlier review comments.
- [x] Replace the obsolete `app_structure.md` with an accurate current-state
  map.
- [x] Correct the laboratory, master-workbook, and publishing workflow
  documentation.
- [ ] Define module ownership and testing requirements.
- [ ] Prepare a small, staged refactoring plan only after those requirements
  are agreed.
