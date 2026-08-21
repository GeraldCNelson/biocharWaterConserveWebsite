# Laboratory data update pipeline

This document describes how laboratory and field-biomass data move from new
source files into the Lab-Based Results tab. The preferred operational entry
point is `biochar_app/scripts/etl.py`.

## Routine full update

1. Start OneDrive and wait for `Biochar Injection Concept - Master.xlsx` to
   finish synchronizing.
2. Put any new Ward NIR CSV in
   `biochar_app/data-raw/lab-tests/hay-tests/csv-files/`.
3. Register that file in `SUPPLEMENTAL_NIR_FILES` in
   `biochar_app/scripts/lab/update_ward_master_nir.py`. The filename must use
   `NIR_YYYY-MM-DD.csv`, where the date is the field sampling date.
4. Run ETL from the repository root. Use `--year YYYY` when appropriate.
5. Review the ETL log and the changed processed CSVs before committing them.

ETL performs these relevant steps in order:

```text
OneDrive master workbook
  -> validate and install biochar_app/data-raw/biochar-data-master.xlsx
  -> rebuild irrigation data
  -> rebuild field-biomass data

compiled Ward NIR master + registered supplemental Ward CSVs
  -> rebuild ward_master_nir_clean.csv

then logger and weather processing continues
```

Use `--skip-master-workbook-refresh` only when intentionally using the installed
snapshot. Use `--skip-lab-build` only when intentionally leaving biomass and
NIR products unchanged.

## Field biomass

Source worksheets are `2023 BIOMASS` through `2026 BIOMASS` in the master
workbook. `build_field_biomass_from_master.py` finds the location header and
pairs sampling dates with columns labeled `Dry` or `Dry (g)`. It ignores wet
weights, formulas, averages, ratios, and worksheet statistics.

The reviewed `field_biomass_dry_g_2023_2025.csv` is the historical authority.
This is intentional because some current workbook values differ from the values
previously reviewed and published. Workbook observations fill historical gaps
and add dates newer than the historical file; they do not overwrite reviewed
2023-2025 values.

The dashboard-ready output is:

`biochar_app/data-processed/lab-tests/biomass-field/csv-files/field_biomass_dry_g_wide_clean.csv`

Its first column is `location`; remaining columns are ISO sampling dates. Every
sampling event must have the 12 locations `S1T` through `S4B`.

When another biomass year is added, add its worksheet name to `BIOMASS_SHEETS`
and to `BIOCHAR_MASTER_SOURCE.required_sheets`, then add a layout test if its
format differs from the existing sheets.

## Ward NIR

`update_ward_master_nir.py` first cleans the compiled two-header master file,
applies the established 2024 mineral corrections, and then appends registered
one-header Ward CSV exports. Supplemental rows are standardized with the same
column mappings and strip conventions as the compiled master.

The sampling date comes from the supplemental filename, not Ward's later
`Date Received` field. Rows are de-duplicated by `(strip, nir_date)`, with the
registered supplemental file taking priority for that sampling event.

The dashboard-ready output is:

`biochar_app/data-processed/lab-tests/hay-tests/csv-files/ward_master_nir_clean.csv`

## Transitional code marked for deletion

The standalone `__main__` entry points in `build_field_biomass_from_master.py`
and `update_ward_master_nir.py` remain temporarily for troubleshooting. They
are marked `TODO(delete after ETL adoption)` and should be removed after one
full operational update has been completed through ETL. The callable builder
functions must remain because ETL imports them.
