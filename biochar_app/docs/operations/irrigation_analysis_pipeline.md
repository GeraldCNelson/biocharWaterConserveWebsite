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

#### Analysis eligibility and comparisons across years

No calendar year is excluded categorically. Records from 2023 through 2026
are evaluated with the same event-level quality-control rules, and one event
may be eligible for one outcome but not another:

- `holding_capacity_eligible` requires a trustworthy event and a complete
  top/middle/bottom storage profile; it does not require applied gallons;
- `unretained_eligible` additionally requires a positive applied-water total;
- `end_of_field_unretained_eligible` additionally requires a credible
  bottom-position 6-inch response at or after irrigation starts.

Holding-capacity QC is calculated independently for the top, middle, and
bottom logger positions. The position is retained in `logger_position` (`T`,
`M`, or `B`). The separate `location` field records the fixed field side
(`west` for S1/S2 and `east` for S3/S4); it is not a logger-position field.
Bottom-only trustworthy-event outputs remain available because bottom arrival
is specifically required for downstream/end-of-field interpretation.

The end-of-field quantity is an upper-bound unretained-water proxy, not
measured runoff. Failed rows remain in the output with a reason field rather
than disappearing from the analysis.

Before pooling eligible holding-capacity observations across years, review:

```text
analysis/holding_capacity/holding_capacity_year_stability_summary.csv
```

This table reports the number and range of represented years, eligible-event
counts, annual means, annual standard deviations and coefficients of
variation, annual ranges, and event-count-weighted pooled means for each
strip, logger position, and depth. It deliberately does not impose an
automatic stable/unstable cutoff. Use those diagnostics to decide whether a
pooled estimate adequately represents the annual results.

For a compact row-by-row review of each annual estimate against its pooled
reference, use:

```text
analysis/holding_capacity/holding_capacity_annual_comparison.csv
```

It includes the annual eligible-event count, annual plateau VWC and layer
storage estimates, the corresponding event-weighted pooled estimates, each
annual value's difference from the pooled value, and the across-year
variability measures.

#### Matched biochar/control sensor comparison

The same run also writes `matched_sensor_treatment_events_<year>.csv` and
`matched_sensor_treatment_events_all_years.csv`. Each row compares the biochar
and control strip within the same irrigation event, logger position, and sensor
depth. S1 is paired with S2, and S3 is paired with S4. Both sensor records must
pass the all-position trustworthy-event QC; unmatched or failed records are
retained in the QC tables but do not enter this paired comparison.

The detail files report each value for the biochar and control strip plus an
explicit `*_difference` field calculated as biochar minus control. They retain
baseline VWC, applied gallons, and event duration so apparent treatment
differences can be reviewed against starting conditions and event exposure.

`matched_sensor_treatment_summary_<year>.csv` summarizes each year's matched
events. `matched_sensor_treatment_summary.csv` pools the eligible matched events
across years by strip pair, logger position, and depth. It reports means,
paired differences, approximate 95% confidence intervals, the fraction of
events in which biochar was higher, and separate event-to-event standard
deviations for biochar and control. These are exploratory repeated-event
comparisons, not independent randomized replicates or causal estimates.

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

## Independent precipitation retention

Owner: `biochar_app/scripts/management/analyze_precipitation_retention.py`

This complementary workflow estimates retained VWC after precipitation events
that are independent of irrigation. It must remain analytically distinct from
the irrigation-event plateau workflow: precipitation results help establish
repeated empirical upper retained VWC, while irrigation results describe the
managed application events.

Run from the repository root:

```bash
python biochar_app/scripts/management/analyze_precipitation_retention.py
```

Use `--years 2024 2025` to select years or `--no-plots` for a quick table-only
run. Precipitation defaults to 2024–2026. The combined capacity table also
uses trustworthy irrigation evidence from 2023–2026 by default; override that
window with `--irrigation-years`. Precipitation outputs are written under:

```text
analysis/holding_capacity/precipitation_events/
```

The outputs are:

- `precipitation_event_catalog.csv`: weather-station precipitation events and
  irrigation-overlap status;
- `precipitation_sensor_responses.csv`: one row per event and physical CS650
  VWC sensor, including baseline, peak, 24/48/72-hour retention, decline,
  local-temperature screening, QC and eligibility;
- `precipitation_profile_retention.csv`: complete eligible three-depth profiles
  spanning approximately 3--21 inches (18 represented inches), expressed as
  equivalent inches of water;
- `precipitation_paired_treatment_events.csv`: matched biochar-minus-control
  comparisons within S1/S2 and S3/S4;
- `precipitation_empirical_maxima.csv`: repeated upper retained VWC summaries
  by strip, logger position and depth, including the P90-based recommended
  upper retained VWC and its adoption classification;
- `precipitation_empirical_maxima_map.png`: schematic field map of P90 retained
  VWC by strip, top/middle/bottom position and sensor depth;
- `precipitation_plot_log.csv` and `figures/<year>/`: compact paired review
  plots with top, middle and bottom logger panels.

The same run writes two cross-source files one directory above
`precipitation_events/`:

- `combined_empirical_capacity_observations.csv`: the event-level precipitation
  and irrigation observations entering the combined calculation;
- `combined_empirical_capacity.csv`: one row per physical sensor with pooled
  maximum, P90, median and standard deviation, source-specific counts and
  summaries, source agreement, the event/date of the highest observation, and
  the adoption classification;
- `combined_empirical_capacity_map.png`: the pooled P90 values in the same
  strip-by-position, three-depth field layout as the precipitation-only map,
  plus a fourth panel containing equivalent profile-water inches calculated
  as the sum of the three represented depth layers;
- `combined_empirical_complete_profiles.csv`: event-level profile water for
  strips having all three eligible depths for one source, event and position;
- `combined_empirical_matched_profiles.csv`: complete profiles retained only
  where both biochar and control strips are present for the same source, event,
  treatment pair and position, with the paired difference;
- `combined_empirical_profile_bootstrap.csv`: 10,000-resample paired bootstrap
  summaries for precipitation, irrigation and pooled evidence, including mean
  and median differences, 95 percent intervals and direction classifications.
  A source/position result requires at least four matched events before it can
  receive a directional classification; smaller samples are labeled
  `insufficient_events` even when their resampled interval lies on one side of
  zero;
- `combined_empirical_profile_unmatched_bootstrap.csv`: independent-sample
  bootstrap comparisons using every complete profile available to each strip,
  without requiring the paired strip to be complete in the same event;
- `combined_empirical_profile_bootstrap_comparison.csv`: pooled matched and
  unmatched results side by side, including the change in estimated mean
  difference and whether their direction classifications agree;
- `combined_empirical_profile_best_results.csv`: concise decision table using
  the matched mean and confidence interval as the primary estimate and the
  unmatched bootstrap as a sensitivity check. `statistical_repeatability` is
  `strong` when both methods have the same directional 95-percent result,
  `moderate` when their signs agree and one method is directional, and `low`
  when both are inconclusive. `causal_confidence` remains limited because each
  irrigation regime has only one biochar and one control strip. Known S1B and
  S4 site-context concerns are retained explicitly rather than folded into the
  statistical interval.
- `combined_empirical_zone_upper_retained_water.csv`: one row per strip and
  top/middle/bottom logger influence zone. It combines the recommended P90 VWC
  at 6, 12 and 18 inches with the measured strip-specific zone geometry to
  estimate gallons of water in the represented soil profile. The three
  sensor-centered layers span approximately 3--21 inches and total 18 inches
  of represented soil; the result is therefore not a literal 0--18-inch or
  complete 0--21-inch estimate. The table includes all geometry, component
  VWC values, event support and review flags needed to audit the calculation.
- `irrigation_available_storage_comparison.csv`: one row per irrigation event
  and strip. It subtracts the trustworthy pre-event baseline from the combined
  P90 upper retained-water estimate in every 6-inch layer and logger influence
  zone, then compares the applied strip volume with the remaining available
  storage. A comparison is eligible only with all nine strip sensors. Positive
  `estimated_water_not_retained_gal_strip` is water not retained in the
  represented 3--21-inch profile; it is not measured runoff and may include
  deep percolation, lateral movement, or surface outflow.
- `irrigation_operational_alert_diagnostics.csv`: augments the available-storage
  table with workbook duration and flow fields, sustained VWC arrival times at
  top/middle/bottom loggers, modeled 50/80/90/100-percent storage timestamps,
  and a retrospective candidate inspection alert. The candidate is the later
  of the earliest sustained bottom response and the modeled 80-percent storage
  time. It also reports runtime and gallons after the candidate alert, the
  modeled full-storage stop time, and the lower average flow that would fit the
  available storage if the actual duration were retained. Modeled times use
  event-average strip flow; start/end meter readings are retained as context
  and must not be interpreted as a continuous flow record. This is an
  alert-development table, not an automated stop command.

The logger-zone areas come from
`geospatial/field_layout/logger_influence_zone_areas.csv`, generated by
`geospatial/build_fruita_field_layout.py`. They are projected nearest-logger
polygons clipped to the field boundary, not equal-width rectangles. The
equal-width calculation in `config/field_management_metadata.py` is retained
only as an explicit fallback when the generated area table is unavailable.

An irrigation observation enters the combined table only when it passes the
established trustworthy-event QC, has finite baseline and plateau VWC between
0 and 80 percent, has a plateau at or above baseline, and was not classified
as `no_peak`. The combined recommended upper retained VWC is the P90 of all
eligible event-level values, not an average of the precipitation and irrigation
P90 values. The source-specific columns must therefore be reviewed whenever
one source contributes substantially more events than the other.

The matched bootstrap resamples whole paired wetting events with replacement;
it never separates the biochar observation from its same-event control. It is
an event-repeatability analysis, not treatment replication: each irrigation
regime still contains only one biochar and one control strip, so stable paired
differences can also reflect persistent strip-specific soil or topographic
conditions.

The unmatched bootstrap is intentionally complementary, not a replacement for
the matched analysis. It gains observations but permits the biochar and control
samples to contain different events, years, water inputs and missing-data
patterns. Large matched-versus-unmatched changes are therefore diagnostic of
event selection, uneven data availability or strip-specific exposure rather
than independent confirmation of a treatment effect.

The weather station is about one-half mile from the experiment. Its
precipitation identifies candidate events, but its soil-temperature channels
do **not** determine sensor eligibility. Every VWC row is screened using the
matching local `T_<depth>_raw_<strip>_<position>` CS650 channel. A near-freezing
temperature therefore excludes only the corresponding sensor/event row, not
the entire precipitation event. Missing local temperature, incomplete VWC,
implausible VWC range, abrupt discontinuity, irrigation overlap and absence of
a material wetting response are reported as separate reasons.

The empirical maximum is operational rather than saturation: it is the upper
retained VWC observed 48–72 hours after repeated eligible **warm** wetting
events (minimum local sensor temperature at least 40 degrees F). Eligible
cold-but-unfrozen events remain in the retention and paired-treatment tables as
corroborating evidence, but do not define the empirical maximum. Near-freezing
events (minimum local temperature at or below 32.5 degrees F) are excluded.
Review event counts, the 90th percentile, the observed maximum and diagnostic
plots together before adopting a value for water-balance calculations.
The adoption classifications are `supported` for at least four warm events,
`provisional` for two or three events, `insufficient` for one event, and
`review_max_p90_gap` when an otherwise-supported observed maximum is at least
10 percent above its P90 value. A two- or three-event result with that gap is
`provisional_review_max_p90_gap`. The separate `max_p90_gap_review` flag makes
all such gaps easy to filter regardless of event count. The percentage gap
uses the observed maximum as its denominator.

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
