# Irrigation Analysis Pipeline

This is the canonical operations guide for rebuilding and interpreting the
Biochar project's irrigation-response analysis. It documents which program
owns each stage, how timestamps are interpreted, and which outputs become
stale when an upstream stage changes.

## Pipeline overview

```text
Raw Campbell .dat files
        |
        v
scripts/etl.py
        |
        +--> civil-time logger Parquet files
        |
        v
scripts/management/estimate_irrigation_holding_capacity.py
        |
        +--> irrigation_arrival_times_<year>.csv
        +--> arrival_order_diagnostics_<year>.csv
        +--> event response and holding-capacity tables
        +--> event_multidepth/<position>/*.png
        +--> irrigation_event_multidepth_plot_log_<year>.csv
        |
        v
scripts/management/irrigation_analysis/reporting.py
        |
        +--> irrigation_arrival_unexpected_<year>.docx
```

The meter-photo comparison is a related timestamp and volume quality-control
branch. It informs interpretation of recorded irrigation boundaries but does
not transform logger timestamps:

```text
Master-workbook snapshot + canonical meter-photo inventory
        |
        v
scripts/management/compare_meter_photos_to_irrigation.py
        |
        +--> meter_photo_workbook_qc.csv
        +--> meter_photo_workbook_boundary_qc.csv
        +--> meter_photo_unmatched_clean_photos.csv
```

## Timestamp policy

### Logger timestamps

Campbell logger clocks are intentionally maintained on Mountain Standard Time
(MST, UTC-7) throughout the year. Raw `.dat` timestamp text is naive and must
not be treated directly as seasonal Colorado civil time.

`scripts/etl.py` applies the timestamp policy in two stages:

1. Apply the absolute, piecewise states in `LOGGER_CLOCK_CORRECTIONS` to repair
   known logger clock changes and resets.
2. Interpret the corrected result as fixed MST and convert it to
   `America/Denver` civil time.

Consequently, exported Parquet and CSV clock values represent MST during the
winter and MDT during daylight-saving time. Time-zone information is removed
before export, but the displayed clock value remains Denver civil time.

Clock-correction evidence and explanations belong in
`LOGGER_CLOCK_CORRECTION_METADATA`. The generated
`logger_clock_corrections_audit.csv` makes those operational states reviewable.

### Irrigation and meter-photo timestamps

Workbook irrigation times are local Colorado civil times. Meter-photo EXIF
times are also local civil times; photographs taken during the growing season
are normally in MDT. They should align directly with the post-ETL logger
timestamps, not with unconverted raw logger clock text.

`compare_meter_photos_to_irrigation.py` compares camera timestamps and manually
transcribed meter counters directly with the master-workbook boundaries. It is
the canonical camera-versus-workbook QA process. Do not create a separate
photo-timestamp authority inside the arrival analysis.

## Stage responsibilities

### 1. ETL

Owner: `biochar_app/scripts/etl.py`

Relevant responsibilities:

- read raw logger `.dat` files;
- apply logger-specific clock-state corrections;
- convert normalized MST timestamps to Denver civil time;
- calculate derived variables and ratios;
- write resampled logger Parquet files used by irrigation analysis.

Run all configured years:

```bash
python biochar_app/scripts/etl.py
```

Run a selected year:

```bash
python biochar_app/scripts/etl.py --year 2026
```

Any change to logger clock corrections requires rerunning ETL and every
downstream irrigation-analysis stage.

### 2. Meter-photo and workbook QA

Owner: `biochar_app/scripts/management/compare_meter_photos_to_irrigation.py`

Inputs:

- the validated repository snapshot of the master workbook;
- `photos/photo_inventory_unique.csv`;
- manually reviewed six-digit meter readings in that inventory.

Run:

```bash
python biochar_app/scripts/management/compare_meter_photos_to_irrigation.py
```

Outputs are written under
`data-processed/management/irrigation/photos/`. Concurrent S1/S2 and S3/S4
records may share one physical meter boundary and one supporting photograph.
Interpret event-level volume totals with that shared-meter structure in mind.

### 3. Irrigation response analysis

Owner: `biochar_app/scripts/management/estimate_irrigation_holding_capacity.py`

This is the orchestration command for arrival detection, event calculations,
diagnostic tables, and event plots. Rebuilding ETL alone does not refresh these
outputs.

Run:

```bash
python biochar_app/scripts/management/estimate_irrigation_holding_capacity.py
```

The analysis reads 15-minute logger Parquet data and canonical irrigation
events. It writes year-specific diagnostics under
`data-processed/management/irrigation/analysis/diagnostics/`.

Important canonical outputs include:

- `reports/irrigation_variable_definitions.csv`: one year-independent table of
  irrigation-analysis variable definitions and calculation rules;
- `irrigation_arrival_times_<year>.csv`: depth-level detected arrival records;
- `arrival_order_diagnostics_<year>.csv`: both depth-order and logger-position
  order classifications;
- `irrigation_event_response_summary_<year>.csv`;
- `irrigation_horizontal_advance_summary_<year>.csv`;
- pre-start-response and trustworthy-event diagnostics.

### 4. Arrival diagnostics

Owner: `biochar_app/scripts/management/irrigation_analysis/diagnostics.py`

Execution: **do not run this module directly.** It is a reusable library
imported by `estimate_irrigation_holding_capacity.py`. Its diagnostic functions
run automatically during stage 3 when you execute:

```bash
python biochar_app/scripts/management/estimate_irrigation_holding_capacity.py
```

There is no separate stage-4 command. Confirm that this stage ran by checking
that `arrival_order_diagnostics_<year>.csv` and the other year-specific
diagnostic CSVs have current modification times.

`build_arrival_order_diagnostics()` performs two distinct checks:

1. **Depth order within one logger position:** expected 6 -> 12 -> 18 inches.
2. **Logger-position order at one depth:** expected top -> middle -> bottom.

The second check is stored in columns such as:

```text
arrival_6in_logger_order_class
arrival_12in_logger_order_class
arrival_18in_logger_order_class
alt_arrival_6in_logger_order_class
alt_arrival_12in_logger_order_class
alt_arrival_18in_logger_order_class
```

`arrival_order_diagnostics_<year>.csv` is the canonical source for both order
checks. New summaries and reports should reuse it instead of implementing a
second classification pipeline.

### 5. Event PNG plots

Owner: `biochar_app/scripts/management/irrigation_analysis/plotting.py`

Execution: **do not run this module directly.** It is a reusable plotting
library called automatically by `estimate_irrigation_holding_capacity.py`
during stage 3. There is no separate stage-5 command.

`estimate_irrigation_holding_capacity.py` calls
`save_irrigation_event_multidepth_plots()` once for each logger position. That
function delegates to `plot_event_multidepth_from_results()` and
`plot_event_multidepth()`.

PNG files are written under:

```text
data-processed/management/irrigation/analysis/figures/event_multidepth/
    T/
    M/
    B/
```

The corresponding `irrigation_event_multidepth_plot_log_<year>.csv` records the
event, strip, logger position, output path, and write status for each figure.
Reporting should use this log rather than reconstructing filenames.
Confirm that plotting completed by checking the plot log for `written` statuses
and inspecting representative PNG files under `event_multidepth/`.

### 6. DOCX reports

Owner: `biochar_app/scripts/management/irrigation_analysis/reporting.py`

Run after the irrigation response analysis:

```bash
python biochar_app/scripts/management/irrigation_analysis/reporting.py
```

The report builder reads:

- `arrival_order_diagnostics_<year>.csv`;
- `irrigation_arrival_times_<year>.csv`;
- `irrigation_event_multidepth_plot_log_<year>.csv`;
- the PNG files named by the plot log.

The unexpected report currently selects unexpected primary depth order,
unexpected alternate depth order, or alternate arrivals before the recorded
irrigation start. Logger-position fields are available in the canonical
diagnostic table and can be added to report selection or cross-year summaries
without a new analysis implementation.

## Arrival definitions

### Standard arrival

The first time after the recorded irrigation start that VWC exceeds its
baseline by the configured threshold (normally 0.25 percentage point) and
remains elevated for the required duration.

### Alternate arrival

The first sustained VWC step increase meeting the configured alternate
threshold (normally 0.50 percentage point). Its wider search can detect a
response before the recorded irrigation start, making it a QA signal for
timestamp or boundary problems.

An absent standard arrival does not necessarily mean the sensor failed. If the
alternate detector identifies a pre-start response, inspect the event plot and
the workbook/photo timestamp evidence.

## Rebuild order and stale outputs

Use this order after a logger timestamp correction:

```bash
python biochar_app/scripts/etl.py
python biochar_app/scripts/management/estimate_irrigation_holding_capacity.py
python biochar_app/scripts/management/irrigation_analysis/reporting.py
```

Use this order after only a reporting-format change:

```bash
python biochar_app/scripts/management/irrigation_analysis/reporting.py
```

The modification time of `irrigation_arrival_times_<year>.csv` is a useful
check when a regenerated report appears to contain old timestamps. Rebuilding a
downstream report never refreshes an upstream diagnostic CSV.

## Verification

After a full rebuild:

1. Confirm ETL completes without errors.
2. Confirm the arrival-time and arrival-order CSVs have current modification
   times.
3. Review the console arrival-order summaries.
4. Inspect representative expected, missing, reversed, and pre-start plots.
5. Run `reporting.py` and visually inspect representative DOCX pages.
6. Run the full test suite:

```bash
python -m pytest biochar_app/tests -q
```

## Known cautions

- Logger clock corrections are absolute piecewise states, not cumulative
  adjustments.
- Preserve established historical corrections unless new evidence identifies
  their exact replacement boundary.
- Logger clocks remain on MST; daylight-saving conversion belongs in ETL.
- Growing-season meter photos normally record MDT civil time.
- S1/S2 and S3/S4 can represent concurrent irrigation supported by shared
  physical meter boundaries.
- Missing arrivals, pre-start arrivals, reversed depth order, and reversed
  logger-position order are different diagnostic conditions.
- Generated outputs under `data-processed/` are evidence and products, not the
  authoritative location for workflow documentation.
