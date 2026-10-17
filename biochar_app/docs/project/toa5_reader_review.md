# TOA5 reader consolidation review

**Review date:** 2026-10-16

**Branch:** `etl-refactor`

**Status:** Evidence review complete; no reader code was deleted or refactored.

## Finding

[`scripts/logger_toa5.py`](../../scripts/logger_toa5.py) describes itself as
the canonical reader for Campbell Scientific TOA5 `.dat` files, but no current
application or diagnostic module imports it. The active ETL contains separate
implementations of the same timestamp normalizer and Table1 reader.

The shared utility should not be deleted. It is a sensible future module
boundary for reusable logger-file ingestion, but it is not yet the actual
owner of that behavior.

## Evidence

The shared reader and the active ETL reader were compared on eight 2026 Table1
files: S1B, S1M, S1T, S2B, S2M, S2T, S3B, and S3M. For every file they
returned identical columns and DataFrame values. Together the comparison
covered more than 800,000 data rows.

The ETL copy has one additional defensive detail: it strips a byte-order mark,
quotes, and surrounding whitespace from header names. The shared utility
should gain and test that behavior before ETL delegates to it.

Several diagnostics and the PakBus archive comparison also contain local TOA5
readers. They are not all simple duplicates:

- some scan for a TOA5 header after blank or prefatory lines;
- some accept multiple timestamp formats;
- some preserve exact header text while repairing historical files; and
- the archive comparison needs a strict, small reader for value comparison.

Those variants should not be replaced in one bulk edit without tests proving
that their deliberately broader or narrower input contracts are preserved.

## Recommended refactor

1. Add focused tests for the shared reader covering a standard Table1 file,
   cleaned quoted/BOM headers, missing headers, invalid timestamps, and gap
   reporting.
2. Add the ETL header-cleaning behavior to `scripts/logger_toa5.py`.
3. Change ETL to import the shared timestamp normalizer and Table1 reader, then
   remove only the two demonstrated duplicates from ETL.
4. Review diagnostic readers individually. Migrate a diagnostic only when its
   accepted input formats match the shared contract; otherwise retain a
   clearly named specialized reader.

This sequence makes the reusable tool real without changing current ETL output
or concealing special historical-data requirements.
