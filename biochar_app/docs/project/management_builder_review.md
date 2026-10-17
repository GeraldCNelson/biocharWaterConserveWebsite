# Management-data builder review

**Review date:** 2026-10-16

**Branch:** `etl-refactor`

**Status:** Review complete; the obsolete combined extractor was retired.

## Decision

`tools/extract_management_data.py` was an early combined irrigation and
fertilizer workbook extractor. It is no longer a runnable or supported command.
The maintained ETL directly invokes the dedicated, tested builders:

- [`build_irrigation_from_master.py`](../../scripts/management/build_irrigation_from_master.py)
  for canonical strip-level irrigation data; and
- [`generate_fertilizer_clean.py`](../../scripts/management/generate_fertilizer_clean.py)
  for fertilizer recommendations, field activity dates, nutrient estimates,
  and source provenance.

The combined extractor was deleted from the active tree. Git history preserves
its exact implementation. No workbook or processed dataset was changed by this
cleanup.

## Evidence

### It cannot run in the current configuration

Importing the old tool fails because it requests `IRRIGATION_CSV` and
`FERTILIZER_CSV` from `biochar_app.config.paths`; those legacy constants no
longer exist. The current configuration uses separate irrigation and fertilizer
paths matched to their dedicated workflows.

### Nothing current calls it

No application module, deployment service, test, README command, or operations
page invokes `tools/extract_management_data.py`. In contrast,
`biochar_app/scripts/etl.py` imports and calls both dedicated builders, and each
builder has focused tests.

### Its output contracts are obsolete

The old irrigation output was group-level and contained only year, timestamps,
strip group, location, gallons, and notes. The current builder additionally:

- expands paired groups to individual strips;
- records meter, group, and strip allocation;
- derives duration and flow measures;
- retains stable event identifiers;
- applies reviewed timestamp corrections;
- separates invalid and QC candidate rows; and
- validates the complete candidate before atomic installation.

The old fertilizer output reduced each annual recommendation sheet to product
pounds by strip and labeled the strip treatment. The current builder preserves
the difference between a recommendation and evidence of an application,
records field-level activity dates without inventing product-specific dates,
calculates nutrient amounts on a documented elemental basis, and retains source
sheet and cell provenance.

## Remaining builder review

This decision does not settle the separate questions around
`scripts/logger_toa5.py`, `scripts/archive_dataset.py`, or binary ETF decoding.
Those tools require their own equivalence or maintainer-use checks before
retirement.
