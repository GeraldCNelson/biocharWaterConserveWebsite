# Thresholds and Range Checks: ETL vs Diagnostics

This project uses two different kinds of “thresholding,” with different goals:

## 1) Diagnostics: validate RAW `.dat` files (source of truth)

Diagnostics should operate ONLY on Campbell TOA5 `.dat` files (e.g. `*_Table1.dat`)
located in:

- `biochar_app/data-raw/datfiles_2023/`
- `biochar_app/data-raw/datfiles_2024/`
- `biochar_app/data-raw/datfiles_2025/`
- `biochar_app/data-raw/datfiles_2026/`

The purpose is to answer:
- “Is the raw file truth actually containing impossible values?”
- “Exactly which file / timestamp / column contains the bad value?”

**Important:** Diagnostics should NOT use parquet outputs, because parquet may already
include ETL conversions (e.g. VWC scaled to %, °C→°F), and that can confound
ETL bugs vs raw-data problems.

### Raw physical bounds (Table1 `.dat` expectations)

In the raw `.dat` files:

- `VWC_*` is expected to be a fraction: **0.0 to 1.0**
- `T_*` is expected to be soil temperature in °C: reasonable range like **-50 to 80**
- `EC_*` is expected to be in dS/m: reasonable range like **0 to 20**
- `BattV_Min` is expected to be battery voltage in V: reasonable range like **0 to 20**

Sentinel / placeholder values (e.g. -9999, 9999, 6999, 9999999) should be excluded
from diagnostics reports if ETL will mask them anyway.

Recommended tool:
- `biochar_app/diagnostics/check_dat_ranges.py`

## 2) ETL: enforce consistent processed-data constraints

ETL transforms raw data into standardized units/columns used by the app:

- VWC fractions are scaled to **percent** (×100) for downstream consistency
- soil temperatures are converted from **°C to °F**
- sentinel/placeholder values are masked to NaN
- optional bounds enforcement masks impossible values to NaN and logs examples

ETL’s bounds enforcement is centralized in:
- `biochar_app/config/thresholds.py`

This file remains in `config/` because it defines project-wide policy used by ETL.

## Recommended processing order

1. Diagnostics: run raw `.dat` checks (no conversions)
2. ETL: read/merge `.dat` files
3. ETL: mask sentinel values (|x| >= threshold)
4. ETL: apply conversions (VWC ×100, °C→°F)
5. ETL: optional bounds enforcement on the converted columns (mask to NaN + report)

## Why this split exists

- Diagnostics answers “is the raw file broken?”
- ETL answers “is the processed dataset safe/consistent for analysis and UI?”

Keeping these separate avoids hiding ETL bugs behind “raw data problems,”
and avoids blaming raw data for issues introduced during conversion.