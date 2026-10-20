# Biochar Water Conservation Website

Code and data-processing workflows for a research project examining the
water-holding capacity of biochar in an irrigated pasture field in western
Colorado.

## Biochar Water Conservation Dashboard

This repository contains the FastAPI-based dashboard and data-processing
pipeline for the Biochar Fruita CSU experiment.

## Repository layout

- `biochar_app/` — application code
  - `biochar_app/config/` — application, path, dataset, and external-source configuration
  - `biochar_app/scripts/` — ETL, application, analysis, and management scripts
  - `biochar_app/templates/` — Jinja website templates
  - `biochar_app/tests/` — automated tests
- `biochar_app/data-raw/` — local raw inputs and validated source snapshots
- `biochar_app/data-processed/` — processed and generated data products
  - `parquet/` — internal Parquet datasets used by the application
  - `downloads/` — prebuilt files for website download endpoints
  - `management/` — irrigation and other management-data products
- `biochar_app/docs/` — additional project and operational documentation

More specialized README files are located near the workflows they document.

## Local PakBus command

Use `tools/pakbus-download station S3B` for a targeted logger download. It
writes the records and a JSON timing report under
`biochar_app/data-raw/pakbus_manual/`. Use `tools/pakbus-download daily` to run
the complete nightly acquisition and strict acceptance checks locally;
incomplete downloads remain rejected.

The daily strategy makes one attempt per station, completes the full initial
pass, then waits five minutes before each of up to two recovery passes over
only the stations still missing. This avoids repeatedly hammering one stale
radio route while preserving the all-station acceptance requirement.

When Starlink changes the delegated IPv6 prefix, run
`tools/pakbus-download discover S3B`. The command signs into the ASUS router,
finds the Campbell client by its stable IPv6 suffix, validates port 6785,
updates the shared configuration, and tests S3B. It prompts for credentials;
they are never written to the repository.

Operational defaults are in
`biochar_app/config/pakbus_settings.json`. CLI arguments override those defaults
for one run. Set `BIOCHAR_PAKBUS_CONFIG` to use a different JSON configuration
file without changing the repository copy.

Accepted daily downloads beginning in 2026 are atomically merged into
`biochar_app/data-raw/pakbus_archive/<year>/logger_data.csv`. Rejected runs
remain in their dated diagnostic directory and are never promoted. The archive
is raw operational data, is excluded from Git, and must be backed up separately.
The year 2026 marks the introduction of this automated acquisition path; it
does not indicate that the year's observations are necessarily incomplete.

To compare a downloaded PakBus CSV with overlapping Campbell Scientific `.dat`
files on the Mac:

```bash
tools/pakbus-download compare /path/to/logger_data.csv \
  --dat-dir biochar_app/data-raw/datfiles_2026 \
  --output pakbus_comparison.json
```

To promote an older run after its diagnostic report has been verified as
`accepted` or `accepted_with_warnings`:

```bash
tools/pakbus-download promote /path/to/pakbus_daily/YYYY/MM/DD/RUN_DIRECTORY
```

The nightly job also audits the most recent seven days of the accepted archive
for internal 15-minute gaps. A gap prevents downstream publication and the
diagnostic report includes the exact station and record-number recovery command
when timestamp and record counters agree. Run the same check manually with:

```bash
tools/pakbus-download audit --lookback-days 7
```

The daily download requests 26 hours of data so consecutive runs overlap by
about two hours. Archive promotion deduplicates those overlapping rows. This
prevents a small change in the scheduler or logger response time around a
15-minute boundary from leaving one or two records between daily windows.

Explicit record requests that predate the logger's retained ring buffer are
reported immediately as unavailable, together with the oldest record number
and timestamp returned by the logger; they are not retried as communication
failures.

## Local development

Create and activate a Python virtual environment, install the requirements, and
start the development server:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip wheel
pip install -r requirements.txt
uvicorn biochar_app.scripts.app:app --reload --host 127.0.0.1 --port 8000
```

If an existing project environment is already available, activate that
environment instead of creating a new one.

### Refresh published data for local use

A Git pull updates application code but does not update the generated Parquet
datasets or the prebuilt bulk-download files. To preview and then download the
latest data currently published on the production server, run from the project
root:

```bash
tools/sync-published-data --dry-run
tools/sync-published-data
```

The command uses the `biochar-webserver` SSH alias documented in
`biochar_app/docs/operations/deploy_to_main.md`. It updates
`biochar_app/data-processed/parquet/` and
`biochar_app/data-processed/downloads/` without deleting local files. Restart
the local application afterward so its in-memory summaries are rebuilt from
the new data.

Use `--source test` to pull the test server's generated data, or `--only
parquet` / `--only downloads` to update just one data group. Run
`tools/sync-published-data --help` for all options.

## Master workbook: designated source and repository snapshot

The normal master-workbook workflow has three stages:

```text
Colleague's primary workbook in Microsoft OneDrive
    Biochar Injection Concept - Master.xlsx
                ↓ fresh anonymous download
Temporary validated copy
                ↓ atomic installation after validation
Repository snapshot used by project code
    biochar_app/data-raw/biochar-data-master.xlsx
```

A locally synchronized OneDrive file can be supplied explicitly as a fallback;
it is not required by the normal ETL workflow.

### 1. Primary online workbook

The project's designated editable master workbook is:

```text
Biochar Injection Concept - Master.xlsx
```

It is maintained by a project colleague in a shared Microsoft OneDrive folder.
The colleague periodically adds or changes irrigation, fertilizer, soil,
biomass, and other project information.

Project Python code opens the configured shared-folder link in a fresh HTTP
session, downloads the workbook by its stable OneDrive identifier, validates
it, and installs the repository snapshot atomically. It does not retain browser
cookies or Microsoft credentials.

### 2. Optional locally synchronized OneDrive file

On the currently configured Mac, Finder displays the synchronized location as:

```text
OneDrive → Data → Biochar Injection Concept - Master.xlsx
```

Its full macOS path is:

```text
/Users/gcn/Library/CloudStorage/OneDrive-Personal/Data/Biochar Injection Concept - Master.xlsx
```

The local OneDrive file is an optional synchronized cache of the colleague's
online workbook. It is not a separately maintained primary workbook.
Project scripts treat it as read-only.

Use this path only when deliberately installing a local source with
`update_master_workbook_snapshot.py --source PATH`. Before doing so:

1. Start the Microsoft OneDrive application.
2. Click the OneDrive cloud icon in the macOS menu bar.
3. Wait until OneDrive reports that synchronization is complete.
4. In Finder, open **OneDrive**, then **Data**.
5. Locate `Biochar Injection Concept - Master.xlsx`.
6. If Finder shows a cloud-download icon beside the workbook, download it or
   select **Always Keep on This Device**.
7. Confirm that any intended online workbook changes have synchronized to the
   local file.

To confirm from a terminal that OneDrive is running:

```bash
pgrep -x OneDrive
```

OneDrive is running when this command prints a process number. No output means
that OneDrive is not running.

### 3. Validated repository snapshot

The project installs the validated workbook as:

```text
biochar_app/data-raw/biochar-data-master.xlsx
```

This revision-neutral filename is the stable input used by application and
analysis code. Do not edit this file manually. A successful refresh replaces it
with a validated copy of the downloaded or explicitly supplied source.

The update process:

- never modifies the colleague's online workbook or a supplied local file;
- downloads or copies the source to a temporary file;
- confirms that the file is an Excel workbook rather than an HTML webpage;
- validates the required worksheets;
- verifies that the source and copy have identical SHA-256 hashes;
- atomically replaces the repository snapshot only after validation succeeds;
- writes an audit record to
  `biochar_app/data-raw/biochar-data-master.snapshot.json`.

The source location, destination, update method, and required worksheets are
registered as `BIOCHAR_MASTER_SOURCE` in:

```text
biochar_app/config/data_sources.py
```

The repository snapshot filename is defined by `BIOCHAR_MASTER_WORKBOOK` in:

```text
biochar_app/config/paths.py
```

## Running ETL

A normal ETL run performs these major operations:

1. Refreshes and validates the master-workbook snapshot.
2. Rebuilds irrigation and fertilizer products from the master workbook.
3. Rebuilds field biomass and Ward NIR products from their respective sources.
4. Validates and processes logger data.
5. Retrieves and processes CoAgMet weather data.
6. Writes aggregated application datasets, metadata, and download products.

Run ETL for a particular year from the repository root:

```bash
python -m biochar_app.scripts.etl --year 2026
```

For the routine 2026 logger update, merge the existing Campbell Scientific
`.dat` files with the
accepted PakBus archive and rebuild only the logger-derived Parquet outputs:

```bash
python -m biochar_app.scripts.etl --year 2026 --logger-only
```

For overlapping timestamps, the value from the `.dat` file is retained because
it has more decimal precision; newer PakBus timestamps extend the same 2026
dataset. This mode does not download weather or rebuild workbook, irrigation,
or laboratory outputs. Parquet files are written to temporary files and
replaced only after each write succeeds.

For the routine server-side publication update, rebuild logger and CoAgMet
weather products, refresh workbook-derived irrigation and fertilizer products,
and skip laboratory and legacy raw-backup stages:

```bash
python -m biochar_app.scripts.etl --year 2026 --operational-update
```

This is the ETL mode intended to follow an accepted nightly PakBus download.

The 2023-2025 Parquet datasets are frozen. ETL refuses to rebuild any of those
years unless the deliberate override `--rebuild-frozen-years` is supplied.
That override is intended only for a documented historical correction.

The ETL log and snapshot audit should identify:

- the source provider and retrieval method;
- whether the repository snapshot changed;
- the installed snapshot's SHA-256 hash;
- the number of production irrigation rows;
- the latest irrigation start timestamp;
- the number of invalid irrigation events.

Common successful snapshot results are:

- `installed_changed_snapshot` — the retrieved workbook differed
  from the previous repository snapshot, so the snapshot was replaced.
- `installed_identical_snapshot` — the retrieved workbook and repository
  snapshot were already identical. This is normal.

If the anonymous retrieval or validation fails, the existing validated
repository snapshot is not replaced. Investigate the configured sharing link,
download identifier, network access, and required-sheet validation before
publishing management-derived products.

Other ETL examples:

```bash
# Use the configured default year
python -m biochar_app.scripts.etl

# Process every configured logger and weather year
python -m biochar_app.scripts.etl --all-years

# Use the existing repository master-workbook snapshot
python -m biochar_app.scripts.etl \
    --skip-master-workbook-refresh

# Use the existing processed irrigation dataset
python -m biochar_app.scripts.etl \
    --year 2026 \
    --skip-irrigation-build

# Skip both management-data refresh stages
python -m biochar_app.scripts.etl \
    --year 2026 \
    --skip-master-workbook-refresh \
    --skip-irrigation-build
```

`--skip-master-workbook-refresh` is intended for deliberate troubleshooting or
recovery work. It produces a warning because the repository snapshot may be
stale.

`--skip-irrigation-build` prevents ETL from rebuilding
`irrigation_clean.csv`. It should also be used only deliberately because
website irrigation annotations and related analyses may be stale.

## Standalone master-workbook validation and refresh

The master workbook can be downloaded and checked separately from the complete
ETL workflow.

Download and validate the shared workbook without replacing the repository
snapshot:

```bash
python -m biochar_app.scripts.management.update_master_workbook_snapshot \
    --validate-only
```

A successful validation reports:

- `"result": "validated_not_installed"`
- `"installed": false`
- an empty `missing_required_sheets` list
- matches for the required annual irrigation and biomass worksheets
- identical `source_sha256` and `copied_sha256` values

Validate the workbook and install the repository snapshot without running the
rest of ETL:

```bash
python -m biochar_app.scripts.management.update_master_workbook_snapshot
```

To validate and install an explicitly supplied local workbook instead:

```bash
python -m biochar_app.scripts.management.update_master_workbook_snapshot \
    --source "/path/to/Biochar Injection Concept - Master.xlsx"
```

The installed destination should be:

```text
biochar_app/data-raw/biochar-data-master.xlsx
```

## Automated irrigation build

The irrigation builder reads the validated repository snapshot and converts
the irrigation worksheets into a consistent strip-level production dataset.

The required worksheets currently are:

```text
2023 IRRIGATION
2024 IRRIGATION
2025 IRRIGATION
2026 IRRIGATION
```

Worksheet-name validation tolerates harmless leading or trailing whitespace,
but worksheet names should nevertheless be kept consistent in the
primary workbook.

To validate and preview the irrigation build without replacing the production
CSV:

```bash
python biochar_app/scripts/management/build_irrigation_from_master.py \
    --dry-run
```

To build and install the production irrigation dataset:

```bash
python biochar_app/scripts/management/build_irrigation_from_master.py
```

The production output is:

```text
biochar_app/data-processed/management/irrigation/irrigation_clean.csv
```

The builder also produces audit and quality-control files in:

```text
biochar_app/data-processed/management/irrigation/
```

These files document candidate rows, invalid rows, pending events, corrections,
and the overall build result.

The command output includes:

- the number of workbook group events;
- the number of candidate strip rows;
- the number of production strip rows;
- the number of invalid group events;
- the latest irrigation start timestamp;
- the source workbook path and SHA-256 hash;
- whether the production CSV was installed.

Invalid events are not silently added to the production dataset. When the
builder reports invalid events, inspect the corresponding audit and
quality-control files before changing the validation rules or source data.

## Master-workbook processing scope

The normal ETL workflow now performs these management-data steps before
processing logger and weather data:

1. It checks that the Microsoft OneDrive desktop application is running.
2. It validates the synchronized
   `Biochar Injection Concept - Master.xlsx` workbook.
3. It installs an identical validated snapshot as
   `biochar_app/data-raw/biochar-data-master.xlsx`.
4. It reads the 2023–2026 irrigation worksheets from that snapshot.
5. It validates and converts workbook irrigation events into strip-level
   records.
6. It installs the resulting production dataset as
   `biochar_app/data-processed/management/irrigation/irrigation_clean.csv`.

The website uses `irrigation_clean.csv` for irrigation plot annotations and
related analyses. Newly synchronized irrigation events should therefore appear
after a successful ETL run without manually editing the production CSV.

The automated build currently covers the irrigation worksheets. Other
master-workbook worksheets—including fertilizer, soil, biomass, and related
project data—are available in the validated repository snapshot but are not
yet all converted into processed datasets by ETL.

Ward laboratory data are separate external sources and are not refreshed by
the master-workbook workflow.

## Manual editing policy

Do not manually edit these generated files:

```text
biochar_app/data-raw/biochar-data-master.xlsx
biochar_app/data-processed/management/irrigation/irrigation_clean.csv
```

Corrections should be made in one of the following appropriate places:

- the primary workbook, when the source information itself is incorrect;
- a reviewed corrections file, when a documented project-side correction is
  required;
- the processing code, when the conversion or validation logic is incorrect.

After making a correction, rerun the applicable build process and review its
audit output.

This policy keeps results reproducible and avoids undocumented changes that
would be lost during the next automated rebuild.

## Tests

Run the master-workbook snapshot tests from the repository root:

```bash
python -m unittest \
    biochar_app/tests/test_update_master_workbook_snapshot.py -v
```

Run the automated irrigation-build tests:

```bash
python -m unittest \
    biochar_app/tests/test_build_irrigation_from_master.py -v
```

Run both test modules together:

```bash
python -m unittest \
    biochar_app/tests/test_update_master_workbook_snapshot.py \
    biochar_app/tests/test_build_irrigation_from_master.py \
    -v
```

The tests use temporary workbooks and temporary output directories. They do
not access OneDrive, modify the synchronized source workbook, or replace the
real repository snapshot and production irrigation CSV.

The expected current combined result is ten passing tests.

## Operational sequence after master-workbook changes

When a project colleague adds or changes irrigation data:

1. Confirm that the intended changes are present in the shared workbook.
2. Run ETL for the required year. The normal workflow downloads and validates
   the shared workbook before rebuilding management products:

   ```bash
   python -m biochar_app.scripts.etl --year 2026
   ```

3. Confirm that the download, snapshot, and irrigation-build messages report
   success.
4. Review any reported invalid irrigation events.
5. Open the website and confirm that the expected irrigation annotation appears.
6. Run the relevant irrigation analyses again when the source change affects
   existing reports or scientific interpretation.

For troubleshooting, validate each stage independently:

```bash
python -m biochar_app.scripts.management.update_master_workbook_snapshot \
    --validate-only

python -m biochar_app.scripts.management.build_irrigation_from_master \
    --dry-run
```

These commands diagnose the source and irrigation-build stages without
replacing their respective production outputs.
