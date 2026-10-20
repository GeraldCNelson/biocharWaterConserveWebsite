# Laboratory data update pipeline

This document describes how laboratory and field-biomass data move from their
respective source files into the Lab-Based Results tab. The project master
workbook is one source, not a container for all laboratory data.

The preferred full-pipeline entry point is
[`biochar_app/scripts/etl.py`](../../scripts/etl.py). Soil-biology and
soil-chemistry rebuilds currently remain explicit laboratory commands, as
described below.

## Source overview

| Dataset | Principal source | Normal update path |
|---|---|---|
| Field biomass | Annual `BIOMASS` worksheets in the validated project workbook, combined with a reviewed historical CSV | Full ETL |
| Pasture quality and NIR | Compiled Ward NIR source plus explicitly registered supplemental files | Full ETL |
| Soil biological health | Compiled Ward PLFA source plus explicitly listed supplemental files | Explicit soil-biology command |
| Soil chemistry | Compiled Ward soil-chemistry source plus explicitly listed supplemental files | Explicit soil-chemistry command |
| Biochar material characterization | Configured source laboratory reports | Published as reviewed source reports; not rebuilt from the project workbook |

## Routine full update

1. Confirm that any expected changes to the shared project workbook and any new
   laboratory results are ready for processing.
2. Put any new Ward NIR CSV in
   `biochar_app/data-raw/lab-tests/hay-tests/csv-files/`.
3. Register that file in `SUPPLEMENTAL_NIR_FILES` in
   `biochar_app/scripts/lab/update_ward_master_nir.py`. The filename must use
   `NIR_YYYY-MM-DD.csv`, where the date is the field sampling date.
4. Run ETL from the repository root. A normal run downloads and validates the
   shared workbook before installing the repository snapshot. Use `--year YYYY`
   when appropriate.
5. Review the ETL log, source audit, and changed processed CSVs before
   committing or publishing them.

Starting the OneDrive desktop application is not required for the normal
download workflow. When intentionally installing a locally synchronized or
otherwise supplied workbook, use the standalone snapshot tool with
`--source PATH` and review its validation report.

ETL performs these relevant steps in order:

```text
shared project master workbook
  -> download and validate
  -> install biochar_app/data-raw/biochar-data-master.xlsx
  -> rebuild irrigation data
  -> rebuild fertilizer data
  -> rebuild field-biomass data

compiled Ward NIR master + registered supplemental Ward CSVs
  -> rebuild ward_master_nir_clean.csv

then logger, weather, aggregation, metadata, and download processing continues
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

## Soil biological health

Soil-biology data do not come from the project master workbook.
`update_ward_master_soilbio.py` reads the compiled Ward PLFA source, applies the
canonical strip, date, depth, and variable mappings, merges the explicitly
listed supplemental files, and writes the cleaned master and header metadata.

Run from the repository root:

```bash
python -m biochar_app.scripts.lab.update_ward_master_soilbio
```

The canonical output is:

`biochar_app/data-processed/lab-tests/soil-tests-bio/csv-files/ward_master_soilbio_clean.csv`

Review the source-file list in the script before each update. Supplemental files
are explicit so an older observation already present in the compiled source is
not merged twice.

## Soil chemistry

Soil-chemistry data also remain independent of the project master workbook.
`update_ward_master_soilchem.py` reads the compiled Ward chemistry source,
normalizes sample identifiers, dates, depths, and variable names, optionally
merges registered supplemental files, and writes the canonical clean dataset
and header metadata.

Run from the repository root:

```bash
python -m biochar_app.scripts.lab.update_ward_master_soilchem
```

The canonical output is:

`biochar_app/data-processed/lab-tests/soil-tests-chem/csv-files/ward_master_soilchem_clean.csv`

The soil-biology and soil-chemistry commands should eventually be incorporated
into a reviewed full-pipeline interface. Until then, their separate execution
must remain visible in release and maintenance checklists.

## Transitional code marked for deletion

The standalone `__main__` entry points in `build_field_biomass_from_master.py`
and `update_ward_master_nir.py` remain temporarily for troubleshooting. They
are marked `TODO(delete after ETL adoption)` and should be removed after one
full operational update has been completed through ETL. The callable builder
functions must remain because ETL imports them.
