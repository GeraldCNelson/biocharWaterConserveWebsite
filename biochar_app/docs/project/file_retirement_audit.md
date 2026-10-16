# File retirement audit

**Audit date:** 2026-10-16

**Branch reviewed:** `etl-refactor`

**Status:** Initial audit complete; the approved high-confidence cleanup batch
was applied on 2026-10-16. All other recommendations remain pending.

## Purpose

This audit identifies files that may no longer contribute to the current
application, data pipelines, research workflows, or operational recovery. It
is deliberately narrower than a directory refactor. Its purpose is to remove
known clutter first, reduce the risk of carrying obsolete material into a new
structure, and identify the files that require a maintainer's decision.

The audit distinguishes five outcomes:

1. **Keep active** — used by the website, a current pipeline, deployment, tests,
   or a documented supported command.
2. **Keep as a supported manual tool** — intentionally run by a researcher or
   maintainer even though nothing imports it.
3. **Delete after approval** — obsolete, empty, broken, superseded, or generated
   material with strong evidence that it is outside the current system.
4. **Review before retirement** — probably historical, but it may contain useful
   field, hardware, or recovery knowledge.
5. **Relocate or rename** — useful content stored under a misleading name or in
   the wrong part of the repository.

"Unused" cannot be inferred from imports alone. Command-line scripts, recovery
tools, and data-conversion utilities are often invoked manually. A file is
therefore a strong retirement candidate only when several kinds of evidence
agree.

## Evidence reviewed

The repository contains 674 tracked files. This initial pass reviewed:

- the current FastAPI entry point and route registration;
- README commands and operations documentation;
- systemd deployment configuration;
- Python imports and textual command references;
- tests around the application, ETL, and supported PakBus code;
- the Word-to-web publishing configuration and output directories;
- Git history dates for candidate files;
- zero-byte tracked files, files with archive/backup names, and unusually large
  tracked files; and
- relationships among the current and historical PakBus tools.

The evidence is strong enough for an initial cleanup proposal, but it does not
prove that an undocumented manual script has never been useful. Dataset files
also require a separate lineage audit before removal.

## Current system boundary

The following areas are demonstrably part of the current system and should not
be treated as retirement candidates merely because individual files have few
imports.

| Area | Evidence | Initial disposition |
|---|---|---|
| Website runtime | [`scripts/app.py`](../../scripts/app.py) creates the FastAPI application; the repository README starts it with Uvicorn. [`scripts/routes.py`](../../scripts/routes.py) registers the main website and data routes. | Keep active. |
| Website content publishing | [`markdown_config.py`](../../markdown/tools/markdown_config.py) maps Word sources to page containers. [`convert_word_to_html.py`](../../markdown/tools/convert_word_to_html.py) writes the live fragments to `markdown/outputs_md`, and the application serves that directory. | Keep the Word sources, converter, configuration, `outputs_md`, and generated web images. |
| Current PakBus acquisition | [`pakbus/core/`](../../pakbus/core/) contains the current client, archive, gateway-discovery, and daily-download implementation. [`biochar-pakbus-daily.service`](../../../deployment/systemd/biochar-pakbus-daily.service) runs `biochar_app.pakbus.core.daily_download`. | Keep active. |
| Supported operator commands | [`tools/pakbus-download`](../../../tools/pakbus-download) is documented in the README and dispatches to the current PakBus core. [`tools/sync-published-data`](../../../tools/sync-published-data) is the documented publication-data synchronizer. | Keep active. |
| Current processed web references | `data-processed/ward-html`, `static/lab_reference_media`, and the files named in [`deployment_manifest.py`](../../config/deployment_manifest.py) are part of live deployment or page rendering. | Keep active unless a separate publishing redesign replaces them. |
| Research outputs | Published research reports and figures may have no runtime imports but are human-facing scientific products. | Keep until a research-output manifest identifies source, generated product, publication status, and reproducibility command. |

## High-confidence cleanup batch — completed 2026-10-16

The approved first cleanup removed 23 obsolete, empty, or generated files from
the current branch. Git retains their histories. No files from the review or
relocation categories were removed. LaTeX build-artifact patterns were also
added to `.gitignore` to prevent the deleted compiler outputs from returning.

The batch initially also removed `scripts/wsgi.py`. The production restart
revealed that the live systemd unit imports
`biochar_app.scripts.wsgi:app`, even though the tracked deployment references
reviewed during the audit did not identify that dependency. The file was
restored immediately, and it is now an explicit Git-delivered requirement in
the deployment manifest. This incident is evidence that a deployment preflight
must validate the live process entry point, not only website content and data.

### Obsolete or broken application files

| Candidate | Evidence | Proposed action |
|---|---|---|
| `scripts/custom_app.py` | Defined a second FastAPI application on port 8001, duplicated a route already present in the main application, had no external references, and imported `biochar_app.scripts.routes_custom_gseason`, which does not exist. | Deleted. The custom-season interface remains in the main application. |
| `scripts/convert_json.py` | A hard-coded one-off conversion from `data.json` to `purple_air.csv`; no imports, commands, tests, or documentation referred to it. | Deleted. A future PurpleAir workflow should use a documented, parameterized acquisition tool. |
| `data-processed/management/initial_conditions.py` | Zero-byte Python file in a processed-data directory; no references found. | Deleted. |
| `config/initial_conditions_metadata.py` | Zero-byte configuration module; no imports or other references found. | Deleted. |
| `static/css/styles.css.backup` | Backup copy in the deployed static tree. The active stylesheet is tracked separately and Git preserves older versions. | Deleted. |
| `static/find_unused_functions.zsh` | Developer-only source audit stored under public static assets. Its role is superseded by the maintained function-catalog generator, and no workflow referred to it. | Deleted. |

### Superseded meter-photo script

`archive/clean_meter_photo_readings.py` was already identified by the irrigation
photo README as belonging to an
archived workflow. The current process compares photo evidence against the
canonical irrigation data without using this script. It was deleted after
confirming that the README retains the historical explanation.

### Obsolete publishing outputs

The live narrative path is `markdown/docx` -> converter ->
`markdown/outputs_md` plus `static/images/generated`. The following files are
outside that path and have no live route or publishing reference:

- all five files formerly in `markdown/converted_html/`;
- `markdown/outputs_html/SHA-Guide-FINAL-May.html`;
- `markdown/pdfs/SHA-Guide-FINAL-May.rtf`; and
- `markdown/pdfs/WardGuide-Master-20211118.html`.

The last two files alone occupy about 5.9 MB. Current Ward material is published
from `data-processed/ward-html` and `static/lab_reference_media`, so these old
intermediate files were deleted after confirming that the live publication path
does not use them.

### Generated LaTeX build files

[`references/waterbalance_code.tex`](../../../references/waterbalance_code.tex)
was retained as the editable source. The generated `waterbalance_code.aux` and
`waterbalance_code.synctex.gz` compiler by-products were deleted, and matching
patterns were added to `.gitignore`.

### Empty historical PakBus placeholders

The following tracked files are zero bytes and have no implementation to
preserve:

- `pakbus/scripts/fetch_table1.py`
- `pakbus/scripts/fetch_table1_live_plus.py`
- `pakbus/scripts/pakbus_time_hex.py`
- `pakbus/scripts/pakbus_time_tools.py`
- `pakbus/scripts/run_triggers_pull.py`
- `pakbus/scripts/test_triggers.py`

These six placeholders were deleted in the first cleanup. Three additional
zero-byte modules under `pakbus/utils` were retained because experimental
scripts still import them. That is evidence that those experimental commands
are incomplete; the modules and their dependents belong in the later historical
PakBus review.

## Review before retirement

### Historical PakBus experimentation

The largest concentration of probable legacy code is separate from the current
`pakbus/core` implementation:

| Area | Tracked files | Findings | Recommendation |
|---|---:|---|---|
| `pakbus/scripts` | 28 remaining (34 audited) | Mostly packet replay, protocol probing, capture decoding, and reconstructed download experiments from 2025-2026. Several depend on the empty `pakbus/utils` modules. No current deployment invokes them. | Review once for unique hardware knowledge, then delete superseded executables from the main branch. Preserve a concise engineering note, not every experiment. |
| `pakbus/examples` | 6 | Older example programs and configuration, partly derived from an external PakBus implementation. They are not used by the current core. | Confirm licensing/provenance and delete if the installed dependency and current core fully replace them. |
| `pakbus/scripts_zsh` | 4 | Session-analysis shell scripts tied to specific 2025 `.spm` files and local paths. | Convert any reusable recovery procedure into operations documentation; otherwise delete. |
| `pakbus/utils` | 4 | `crc.py`, `frame.py`, and `hex.py` are empty, although historical scripts import functions from them. | Delete together with the dependent historical scripts. Do not present those commands as usable. |
| [`scripts/CLIscripts.txt`](../../scripts/CLIscripts.txt) | 1 | An unstructured command notebook with duplicated or malformed commands, specific network addresses, commands targeting empty files, and references to a nonexistent `loop_27b_tx_pull` module. | Extract any still-valid troubleshooting knowledge into a PakBus operations page, then delete. |

The root [`tools/`](../../../tools/) directory contains a second historical
PakBus toolchain from August-September 2025. Thirteen scripts form capture,
decode, resample, and trust-check experiments. They have no current README or
deployment entry point and are superseded operationally by `tools/pakbus-download`
and `pakbus/core`. Treat them as one review batch rather than deciding file by
file. If an old protocol-decoding technique remains valuable, describe it in a
historical engineering note or turn it into a tested current diagnostic.

This review should be completed before reorganizing `pakbus/`. Moving all of
these files into a new directory would make the new structure look more
complicated without increasing capability.

### Isolated utilities with plausible manual value

| Candidate | Evidence | Decision needed |
|---|---|---|
| [`etf.py`](../../etf.py) and the export in [`biochar_app/__init__.py`](../../__init__.py) | Provides a binary Table1 decoder and is exported as a public package symbol, but no current application, test, or supported command calls it. An archived PakBus note contains the only substantive usage found. | Decide whether binary ETF decoding is a supported reusable capability. If yes, move it into the current PakBus parsing layer and test it. If no, delete both the module and public export. |
| [`scripts/logger_toa5.py`](../../scripts/logger_toa5.py) | Its documentation says it is canonical for ETL and diagnostics, but no current caller imports it. | Compare its parsing behavior with the actual ETL and diagnostic readers. Consolidate only after equivalence tests; otherwise retire it. |
| [`scripts/archive_dataset.py`](../../scripts/archive_dataset.py) | Standalone utility that copies Ward datasets into timestamped archive folders; no documented workflow calls it. | Confirm whether maintainers still run it manually. If not, delete it; if yes, document when restoration is needed and where archives are stored. |
| [`scripts/pull_leaves.py`](../../scripts/pull_leaves.py) | Older incremental collection path that calls `pakbus/scripts/fetch_leaf_records`; its own usage example points to the wrong module location. Current scheduled acquisition uses `pakbus/core/daily_download`. | Retire with the historical PakBus batch unless it supports a field workflow unavailable in `tools/pakbus-download`. |
| [`scripts/replay_collect.py`](../../scripts/replay_collect.py) | Standalone packet-replay experiment with no current references. | Preserve only if still needed for a specific logger-recovery procedure; otherwise delete. |
| [`tools/extract_management_data.py`](../../../tools/extract_management_data.py) | Extracts irrigation and fertilizer data from the master workbook, but newer dedicated builders now handle those datasets and have tests. | Compare its outputs with `build_irrigation_from_master.py` and `generate_fertilizer_clean.py`. Retire it only after confirming no unique transformation remains. |

### Reference and planning documents

The `references/` directory contains useful evidence, generated debris, and
older operational instructions together. The two LaTeX build files can be
removed immediately. The remaining files require content decisions:

- `Pycharm Virtualenv Flask Setup.txt` describes a superseded Flask setup and is
  a likely deletion candidate.
- `Biochar_Deployment_Guide.txt` should be compared with the current deployment
  documentation; retain only unique recovery knowledge.
- `Parquet Data Lake Implementation Plan.docx` may still document future data
  architecture and should remain until that plan is accepted or superseded.
- `recovery_notes_2026-02-summary-performance.md` is historical operational
  evidence; retain or summarize it according to an explicit recovery-record
  policy.
- `waterbalance_code.tex` is a possible source document and should not be
  removed merely because the current website does not load it.

## Files that need correction rather than retirement

### `.env` and `.env.txt`

Both files are tracked at the repository root, but they are different kinds of
content:

- `.env` contains application environment assignments. Values were not included
  in this audit. A local environment file normally should be ignored, with a
  documented `.env.example` containing safe placeholders if these variables are
  still required.
- `.env.txt` is not an environment file. It is a roughly 1.9 MB weather-data CSV
  whose header begins with station, date/time, air temperature, humidity,
  precipitation, wind, and soil-temperature fields. It should be renamed and
  placed under the weather source-data workflow, or removed after its provenance
  and replacement are confirmed.

This is a naming and source-control hygiene issue, not evidence that the weather
data are disposable.

### Empty `pyproject.toml`

The root `pyproject.toml` is empty. It is not operationally active, but the
architecture proposal identifies packaging reusable tools as a future goal.
Either populate it when a supported package boundary is designed, or remove it
until that work begins. Leaving an empty packaging file suggests an installation
interface that does not exist.

### Empty `scripts/experimental` directory

The directory contains only `README.md` and `__init__.py`. The README's record of
the EasyOCR meter-reading evaluation is useful. Move that short decision record
to the irrigation photo workflow documentation, then remove the empty package.

## Recommended retirement policy

Different material should leave the active tree in different ways:

| Material | Preferred treatment |
|---|---|
| Obsolete source code | Delete from the current branch after review; rely on Git history and the cleanup commit for recovery. |
| Generated files | Regenerate from named sources; remove from tracking and ignore when deployment does not require them. |
| Raw or authoritative data | Do not delete through this audit. Record origin, date range, checksum, replacement, and storage location first. |
| Historical research outputs | Keep when they are cited or represent a published result; otherwise record the generating command and decide whether reproducibility replaces versioning the output. |
| Operational incident knowledge | Summarize the durable lesson in current operations documentation, then retire command transcripts and failed experiments. |
| External reference documents | Keep only when provenance and purpose are stated; archive outside the runtime tree if long-term evidence is required. |

An `archive/` directory should not become a second unmaintained application.
Use it only for material whose evidentiary value depends on retaining the file
itself. Git history is the better archive for superseded code.

## Proposed sequence

1. **Completed: approve and apply the high-confidence cleanup batch.** The
   removals were kept in one reviewable commit containing no functional
   refactor.
2. **Completed 2026-10-16: run existing application, ETL, publishing, and
   PakBus tests.** The deployment incident led to an explicit WSGI-entry-point
   preflight, and stale irrigation-date and JavaScript-module assumptions in
   the test suite were corrected before beginning the historical-code review.
3. **Review the historical PakBus batch with field-maintenance needs in mind.**
   The evidence review is recorded in
   [`pakbus_historical_review.md`](pakbus_historical_review.md). Confirm the
   remaining direct-serial, HTTP, SPM-session, and packet-capture needs before
   applying its proposed deletion batches.
4. **Compare overlapping data builders.** Establish which command owns
   irrigation, fertilizer, TOA5 parsing, and workbook extraction before deleting
   alternate implementations.
5. **Create a data and research-output manifest.** The code audit alone is not a
   safe basis for deleting CSV, HTML report, image, or scientific-output files.
6. **Begin directory refactoring only after cleanup decisions are merged.** The
   new structure should contain current responsibilities, not renamed historical
   clutter.

## Initial conclusion

Housekeeping is the appropriate first implementation step before the larger
directory refactor. There is a small, low-risk batch of clearly obsolete or
generated files, followed by a concentrated PakBus history review and a few
manual utilities that need explicit ownership decisions. This sequence will
make the later architecture refactor smaller and easier to verify without
changing current scientific analysis or website behavior prematurely.
