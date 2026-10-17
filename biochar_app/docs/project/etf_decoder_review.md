# Binary Table1 decoder review

**Review date:** 2026-10-16

**Branch:** `etl-refactor`

**Status:** Review complete; the obsolete top-level decoder was retired.

## Decision

The former `biochar_app/etf.py` decoded one 44-byte Campbell Table1 record and
was exported as `biochar_app.decode_row`. Nothing in the repository called the
function, no test covered it, and the project has no active installable package
contract (`pyproject.toml` is empty).

The maintained implementation is
[`pakbus/core/client.py`](../../pakbus/core/client.py). Its
`decode_table1_response` function uses the same Campbell 1990 epoch and
big-endian timestamp-plus-ten-float record layout, while also validating the
Table1 response envelope, retaining record numbers, applying the logger's
fixed-standard-time timezone, and exposing response metadata. It is exercised
by the focused PakBus client tests.

Keeping the smaller decoder as a package-level public symbol suggested two
supported binary schemas. Its field names also differed from the canonical
logger names (`BattV` rather than `BattV_Min`, for example). The duplicate
module and export were therefore removed. Git history preserves the former
implementation.

## Reusable interface implication

If binary decoding is later offered to researchers as a reusable library, the
interface should be designed around the maintained decoder and a documented
input/output contract. That packaging decision should include a populated
`pyproject.toml`, public API tests, semantic versioning expectations, and a
small example using recorded non-sensitive bytes. Restoring the untested
top-level shortcut would not provide that contract.
