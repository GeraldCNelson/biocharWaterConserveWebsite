#!/usr/bin/env python3
"""
etl.py

Pipeline documentation
----------------------
See ``biochar_app/docs/operations/irrigation_analysis_pipeline.md`` for this
module's role in logger timestamp normalization and downstream rebuild order.

Full ETL including growing-season (gseason) summaries:
  - Read all .dat logger files per year (in data-raw/datfiles_{year})
  - Require the macOS OneDrive application unless master refresh is skipped
  - Validate and install the synchronized authoritative master-workbook
    snapshot before downstream data processing
  - Rebuild field-biomass data from the master workbook and merge new Ward NIR
    files into the cleaned laboratory master
  - Parse raw logger timestamps as naive clock text
  - Apply per-logger clock corrections (MST/MDT jumps, resets) using LOGGER_CLOCK_CORRECTIONS
  - Convert corrected logger timestamps from a fixed MST base into America/Denver civil time
  - Backfill late-2023 BattV_Min data that lives in datfiles_2024/*_Table1.dat
      * For year=2023, also read datfiles_2024/{tag}_Table1.dat and keep rows < 2024-01-01
      * If *_late2023_withBattV.dat files exist in datfiles_2023, also read those
  - Mask extreme placeholders → NaN
  - Convert VWC fractions → percent (×100) deterministically
  - Convert soil temperature (°C → °F) deterministically
  - Apply site-specific value bounds (centralized in thresholds.py) + produce a report
  - Compute SWC cylinder volumes & logger-ratios
  - Compute ΔT (biochar − control) and ΔSWC volumes (biochar − control)
  - Resample to 15 min / hourly / daily / monthly; write Parquet + Parquet_ratios
  - Build DEFAULT gseason summaries from daily data (with cross-year support)
  - Fetch CoAgMet 5 min weather; clean precip increments; write resampled Parquet
  - Build bulk-download ZIPs for 15-min logger and weather data

Logger time policy
------------------
Raw Campbell logger timestamps are parsed as naive clock text, then interpreted
as a fixed Mountain Standard Time timeline after any manual logger-clock
corrections have been applied.

Implementation:
1. Parse raw logger timestamp text as naive datetimes.
2. Apply manual LOGGER_CLOCK_CORRECTIONS for known logger clock jumps/resets.
3. Treat the corrected timestamps as fixed MST, UTC-7 all year.
4. Convert that fixed-base timeline to America/Denver civil time.
5. Resample while timestamps are timezone-aware America/Denver timestamps.
6. Before writing Parquet/CSV outputs, remove timezone information.

Result:
Parquet and CSV logger timestamps are timezone-naive, but their clock values
represent local America/Denver civil time. During daylight-saving time, this
means the exported logger timestamps correspond to MDT local clock time.

Implication:
Irrigation start/end times recorded from field notes, meter photos, or local
Colorado clock time should align directly with logger timestamps in the parquet
outputs, provided those irrigation times are also interpreted as local
America/Denver civil time.

Usage:
python biochar_app/scripts/etl.py
python biochar_app/scripts/etl.py --year 2024
python biochar_app/scripts/etl.py --force-backup-raw
python biochar_app/scripts/etl.py --no-backup-raw
python biochar_app/scripts/etl.py --skip-master-workbook-refresh
"""

from __future__ import annotations

import csv
import logging
import math
import os
from pathlib import Path
from typing import Any, Optional, cast
import textwrap

import argparse
import json
import shutil
from datetime import datetime, timedelta

import pandas as pd
from pandas import Series

from biochar_app.config import (
    CS650_SENSING_VOLUME_CM3,
    SENSOR_DEPTH_VALUES,
)
from biochar_app.config.core import (
    COAGMET_VARIABLE_MAP,
    COAG_STATION,
    COLLECT_PERIOD,
    DEFAULT_GSEASON_PERIODS,
    DEFAULT_TIMEZONE,
    GRANULARITIES,
    LOGGER_LOCATIONS,
    STRIPS,
    YEARS,
    cylinder_volume_m3,
)

from biochar_app.config.data_sources import BIOCHAR_MASTER_SOURCE
from biochar_app.config.irrigation_config import (
    RAW_DATA_BACKUP_DIR,
    RAW_DATA_BACKUP_INTERVAL_DAYS,
    RAW_DATA_BACKUP_STATE,
    DEFAULT_ETL_YEAR
)

from biochar_app.config.paths import (
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
    LOGGER_DOWNLOADS_DIR,
    PARQUET_DIR,
    WEATHER_DOWNLOADS_DIR,
    DATASET_METADATA_PY,
)
from biochar_app.config.thresholds import (
    DEFAULT_BAD_VALUE_THRESHOLD,
    apply_value_bounds as enforce_value_bounds,
)
from biochar_app.config.units import (
    DEFAULT_UNITS,
    UNIT_CONVERSIONS,
)
from biochar_app.scripts.get_weather_data import fetch_weather_data
from biochar_app.scripts.management.build_irrigation_from_master import (
    build_and_install_irrigation,
)
from biochar_app.scripts.management.update_master_workbook_snapshot import (
    require_onedrive_desktop_app,
    update_snapshot,
)
from biochar_app.scripts.lab.build_field_biomass_from_master import (
    build_and_install_field_biomass,
)
from biochar_app.scripts.lab.update_ward_master_nir import update_ward_master_nir
from biochar_app.scripts.type_utils import NAN, NEG_INF, POS_INF, df_agg

from biochar_app.config.dataset_metadata import (
    DAILY_PRECIPITATION_MAX_IN,
    VWC_MAX_PERCENT,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

LOGGER_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
WEATHER_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

DatasetMetadata = dict[str, dict[str, float]]
METADATA_CONSTANT_NAMES = {
    "vwc_percent": (
        "VWC_MIN_PERCENT",
        "VWC_MAX_PERCENT",
    ),
    "soil_temperature_f": (
        "SOIL_TEMPERATURE_MIN_F",
        "SOIL_TEMPERATURE_MAX_F",
    ),
    "soil_ec_ds_per_m": (
        "SOIL_EC_MIN_DS_PER_M",
        "SOIL_EC_MAX_DS_PER_M",
    ),
    "air_temperature_f": (
        "AIR_TEMPERATURE_MIN_F",
        "AIR_TEMPERATURE_MAX_F",
    ),
    "daily_precipitation_in": (
        "DAILY_PRECIPITATION_MIN_IN",
        "DAILY_PRECIPITATION_MAX_IN",
    ),
}
# ---------------------------------------------------------------------------
# Logger clock corrections
# ---------------------------------------------------------------------------
# Semantics:
#   For a given logger tag, each tuple is (start_timestamp, add_minutes).
#   For rows with timestamp >= start_timestamp, we add add_minutes to the raw timestamp.
#
# These corrections are intended to stitch together known logger clock mode
# changes / resets into one consistent fixed-base timeline.


def update_dataset_metadata(
    metadata: DatasetMetadata,
    key: str,
    values: pd.Series,
) -> None:
    values = pd.to_numeric(values, errors="coerce").dropna()

    if values.empty:
        return

    series_min = float(values.min())
    series_max = float(values.max())

    if key not in metadata:
        metadata[key] = {
            "min": series_min,
            "max": series_max,
        }
        return

    metadata[key]["min"] = min(
        metadata[key]["min"],
        series_min,
    )
    metadata[key]["max"] = max(
        metadata[key]["max"],
        series_max,
    )

def write_dataset_metadata(
    metadata: DatasetMetadata,
    years: list[int],
) -> None:
    """
    Write automatically generated dataset metadata constants.
    """

    output_path = DATASET_METADATA_PY
    source_years = tuple(sorted(set(years)))
    lines = [
        '"""',
        "Automatically generated by ETL.",
        "Do not edit manually.",
        '"""',
        "",
        f"SOURCE_YEARS = {source_years!r}",
        f"SOURCE_YEAR_MIN = {min(source_years)}",
        f"SOURCE_YEAR_MAX = {max(source_years)}",
        "",
    ]

    for key, (min_name, max_name) in METADATA_CONSTANT_NAMES.items():
        if key not in metadata:
            continue

        lines.append(f"{min_name} = {metadata[key]['min']:.6f}")
        lines.append(f"{max_name} = {metadata[key]['max']:.6f}")
        lines.append("")

    output_path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    logger.info(f"✅ Wrote dataset metadata: {output_path}")

# Logger timestamps are 15-minute aggregation labels generated from each
# logger's internal wall clock. PC400 clock synchronization copied the
# laptop's current wall time into the logger, which could be either MST or
# MDT depending on the date of the field visit. Consequently, an individual
# logger file may contain multiple clock regimes.
#
# These entries reconstruct the logger's clock-state history and normalize
# raw timestamps to a fixed MST (UTC-7) base. Each offset is the complete
# number of minutes to add beginning at the listed raw timestamp; offsets
# are absolute states, not cumulative changes.
#
# After these corrections, apply_logger_seasonal_civil_time() interprets
# the normalized values as fixed MST and converts them to America/Denver
# civil time, restoring MDT where seasonally appropriate.
#
# The logger clocks and PC400 synchronization screenshots include seconds,
# but the stored .dat records are 15-minute aggregation timestamps. The
# correction map therefore represents interval-label clock states rather
# than second-level oscillator drift.

LOGGER_CLOCK_CORRECTIONS: dict[str, list[tuple[str, int]]] = {
    "S1B": [("2024-02-23 15:30:00", 60)],
    "S1M": [("2024-02-23 15:15:00", 60)],
    "S1T": [("2024-02-23 10:45:00", 60)],
    "S2B": [("2024-02-23 15:45:00", 60)],
    "S2M": [("2026-02-23 08:45:00", 60)],
    "S2T": [
        ("2024-04-02 16:00:00", -60),
        ("2026-02-18 08:45:00", 0),
    ],
    "S3B": [("2023-04-28 10:45:00", -60), ("2024-03-28 17:15:00", -120), ("2026-02-23 08:45:00", -60)],
    "S3M": [
        ("2023-09-04 10:30:00", -60),
        ("2024-07-07 06:30:00", -120),
        ("2025-01-16 23:45:00", -180),
        ("2026-02-19 15:00:00", 0),
    ],
    "S3T": [("2024-02-23 11:30:00", 60)],
    "S4B": [("2023-09-04 10:30:00", -60), ("2023-09-20 18:30:00", -120), ("2026-02-23 09:00:00", -60)],
    "S4M": [("2024-02-23 14:30:00", 60)],
    "S4T": [("2024-02-23 11:45:00", 60)],
}

# Fixed Mountain Standard Time base used before converting to civil Denver time.
# (Etc/GMT+7 is fixed UTC-7; the sign is reversed by POSIX convention.)
LOGGER_FIXED_STANDARD_TZ = "Etc/GMT+7"

# Documentation metadata for LOGGER_CLOCK_CORRECTIONS.
#
# Each correction entry represents a logger clock state change. The offset
# values in LOGGER_CLOCK_CORRECTIONS are operational values used by the ETL.
# This table documents why each correction exists and the evidence supporting it.
#
# Evidence confidence:
#   High   - direct PC400 synchronization screenshots or recorded field action
#   Medium - supported by multiple independent timestamp comparisons
#   Low    - inferred from limited evidence or unresolved ambiguity

LOGGER_CLOCK_CORRECTION_METADATA = {
    ("S1B", "2024-02-23 15:30:00"): {
        "reason": (
            "Logger clock moved backward by 45 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected BACKWARD transition."
        ),
        "details": (
            "Detected transition: 2024-02-23 16:15:00 -> "
            "2024-02-23 15:30:00 (-45.0 min)"
        ),
        "confidence": "high",
    },

    ("S1M", "2024-02-23 15:15:00"): {
        "reason": (
            "Logger clock moved backward by 45 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected BACKWARD transition."
        ),
        "details": (
            "Detected transition: 2024-02-23 16:00:00 -> "
            "2024-02-23 15:15:00 (-45.0 min)"
        ),
        "confidence": "high",
    },

    ("S1T", "2024-02-23 10:45:00"): {
        "reason": (
            "Logger clock moved backward by 45 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected BACKWARD transition."
        ),
        "details": (
            "Detected transition: 2024-02-23 11:30:00 -> "
            "2024-02-23 10:45:00 (-45.0 min)"
        ),
        "confidence": "high",
    },

    ("S2B", "2024-02-23 15:45:00"): {
        "reason": (
            "Logger clock moved backward by 45 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected BACKWARD transition."
        ),
        "details": (
            "Detected transition: 2024-02-23 16:30:00 -> "
            "2024-02-23 15:45:00 (-45.0 min)"
        ),
        "confidence": "high",
    },

    ("S2M", "2026-02-23 08:45:00"): {
        "reason": (
            "Logger synchronized during PC400 field visit. "
            "Correction represents the post-synchronization clock state."
        ),
        "evidence": (
            "PC400 before/after clock synchronization screenshots "
            "collected February 23, 2026."
        ),
        "details": (
            "Logger synchronized to laptop time during field visit."
        ),
        "confidence": "high",
    },

    ("S2T", "2024-04-02 16:00:00"): {
        "reason": (
            "Logger clock moved forward by 75 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected FORWARD transition."
        ),
        "details": (
            "Detected transition: 2024-04-02 14:45:00 -> "
            "2024-04-02 16:00:00 (+75.0 min)"
        ),
        "confidence": "high",
    },

    ("S2T", "2026-02-18 08:45:00"): {
        "reason": (
            "The S2T logger clock was synchronized to the field computer's "
            "MST clock, ending the prior manual offset state."
        ),
        "evidence": (
            "The February 18, 2026 S2T PC400 screenshot in "
            "diagnostics/logger_times_updates.docx was captured immediately "
            "before the logger clock was set."
        ),
        "details": (
            "PC400 showed S2T at 2026-02-18 08:32:41 and the adjusted server "
            "at 08:31:23. The clock was then set to the MST computer time. "
            "The first subsequent 15-minute S2T record is 08:45. Round-two "
            "screenshots on 2026-08-24 confirm S2T remained on MST; seasonal "
            "MDT conversion is applied later by ETL."
        ),
        "confidence": "high",
    },

    ("S3B", "2023-04-28 10:45:00"): {
        "reason": (
            "Logger clock moved forward by 75 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected FORWARD transition."
        ),
        "details": (
            "Detected transition: 2023-04-28 09:30:00 -> "
            "2023-04-28 10:45:00 (+75.0 min)"
        ),
        "confidence": "high",
    },

    ("S3B", "2024-03-28 17:15:00"): {
        "reason": (
            "Logger clock moved forward by 75 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected FORWARD transition."
        ),
        "details": (
            "Detected transition: 2024-03-28 16:00:00 -> "
            "2024-03-28 17:15:00 (+75.0 min)"
        ),
        "confidence": "high",
    },

    ("S3B", "2026-02-23 08:45:00"): {
        "reason": (
            "Logger synchronized during PC400 field visit."
        ),
        "evidence": (
            "PC400 before/after clock synchronization screenshots "
            "collected February 23, 2026."
        ),
        "details": (
            "Logger synchronized to laptop time during field visit."
        ),
        "confidence": "high",
    },

    ("S3M", "2023-09-04 10:30:00"): {
        "reason": (
            "Logger clock moved forward by 75 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected FORWARD transition."
        ),
        "details": (
            "Detected transition: 2023-09-04 09:15:00 -> "
            "2023-09-04 10:30:00 (+75.0 min)"
        ),
        "confidence": "high",
    },

    ("S3M", "2024-07-07 06:30:00"): {
        "reason": (
            "Logger clock moved forward by 75 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected FORWARD transition."
        ),
        "details": (
            "Detected transition: 2024-07-07 05:15:00 -> "
            "2024-07-07 06:30:00 (+75.0 min)"
        ),
        "confidence": "high",
    },

    ("S3M", "2025-01-16 23:45:00"): {
        "reason": (
            "Logger clock moved forward by 75 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected FORWARD transition."
        ),
        "details": (
            "Detected transition: 2025-01-16 22:30:00 -> "
            "2025-01-16 23:45:00 (+75.0 min)"
        ),
        "confidence": "high",
    },

    ("S3M", "2026-02-19 15:00:00"): {
        "reason": (
            "Logger clock was verified after battery replacement. "
            "Historical timestamps required a new clock-state correction "
            "starting after the field service event."
        ),
        "evidence": (
            "PC400 screenshot comparison of S3M logger time and server time "
            "after battery replacement."
        ),
        "details": (
            "Battery replaced approximately 2026-02-19 13:30 MST. "
            "Logger time and server time differed by approximately 1 second."
        ),
        "confidence": "high",
    },

    ("S3T", "2024-02-23 11:30:00"): {
        "reason": (
            "Logger clock moved backward by 45 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected BACKWARD transition."
        ),
        "details": (
            "Detected transition: 2024-02-23 12:15:00 -> "
            "2024-02-23 11:30:00 (-45.0 min)"
        ),
        "confidence": "high",
    },

    ("S4B", "2023-09-04 10:30:00"): {
        "reason": (
            "Logger clock moved forward by 75 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected FORWARD transition."
        ),
        "details": (
            "Detected transition: 2023-09-04 09:15:00 -> "
            "2023-09-04 10:30:00 (+75.0 min)"
        ),
        "confidence": "high",
    },

    ("S4B", "2023-09-20 18:30:00"): {
        "reason": (
            "Logger clock moved forward by 75 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected FORWARD transition."
        ),
        "details": (
            "Detected transition: 2023-09-20 17:15:00 -> "
            "2023-09-20 18:30:00 (+75.0 min)"
        ),
        "confidence": "high",
    },

    ("S4B", "2026-02-23 09:00:00"): {
        "reason": (
            "Logger synchronized during PC400 field visit."
        ),
        "evidence": (
            "PC400 before/after clock synchronization screenshots "
            "collected February 23, 2026."
        ),
        "details": (
            "Logger synchronized to laptop time during field visit."
        ),
        "confidence": "high",
    },

    ("S4M", "2024-02-23 14:30:00"): {
        "reason": (
            "Logger clock moved backward by 45 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected BACKWARD transition."
        ),
        "details": (
            "Detected transition: 2024-02-23 15:15:00 -> "
            "2024-02-23 14:30:00 (-45.0 min)"
        ),
        "confidence": "high",
    },

    ("S4T", "2024-02-23 11:45:00"): {
        "reason": (
            "Logger clock moved backward by 45 minutes. "
            "Correction establishes the new logger clock state."
        ),
        "evidence": (
            "scan_dat_chron_timeline.py detected BACKWARD transition."
        ),
        "details": (
            "Detected transition: 2024-02-23 12:30:00 -> "
            "2024-02-23 11:45:00 (-45.0 min)"
        ),
        "confidence": "high",
    },
}

# ---------------------------------------------------------------------------
# Timezone helpers
# ---------------------------------------------------------------------------
def tz_name(tz_like: Any) -> str:
    """
    Return a pandas-friendly timezone name string.

    Supports:
      - ZoneInfo objects (uses .key)
      - strings
      - fallback to str(...)
    """
    if hasattr(tz_like, "key"):
        key = getattr(tz_like, "key")
        if isinstance(key, str) and key:
            return key
    if isinstance(tz_like, str):
        return tz_like
    return str(tz_like)

DEFAULT_TIMEZONE_NAME = tz_name(DEFAULT_TIMEZONE)

def apply_logger_clock_corrections(ts: pd.Series, logger_tag: str) -> pd.Series:
    """
    Apply piecewise absolute clock offsets to a naive timestamp series.

    Each ``LOGGER_CLOCK_CORRECTIONS`` entry records the complete offset that
    becomes active at its start timestamp. Entries are state changes, not
    incremental adjustments. For example, successive offsets of -60 and -120
    mean that the later interval receives -120 minutes, not -180 minutes.

    Interval selection is based on the original raw logger timestamp. This is
    important because applying an earlier correction must not move a row across
    a later state-change boundary.
    """
    pts = LOGGER_CLOCK_CORRECTIONS.get(logger_tag)
    if not pts:
        return ts

    raw = pd.to_datetime(ts, errors="coerce").astype("datetime64[ns]")
    out = raw.copy()

    # Later state changes overwrite earlier ones for all subsequent raw rows.
    # Sorting also makes the behavior deterministic if configuration entries
    # are ever entered out of chronological order.
    for start_s, add_min in sorted(
        pts,
        key=lambda item: pd.Timestamp(item[0]),
    ):
        start_ts = pd.Timestamp(start_s)
        mask = raw >= start_ts
        if mask.any():
            out = out.where(
                ~mask,
                raw + pd.Timedelta(minutes=int(add_min)),
            )
    return out

def build_logger_clock_corrections_audit() -> pd.DataFrame:
    """
    Build an audit table documenting logger clock corrections.

    Combines operational correction values with provenance metadata.
    """

    rows = []

    for logger, corrections in LOGGER_CLOCK_CORRECTIONS.items():
        for start_s, offset_min in sorted(
            corrections,
            key=lambda item: pd.Timestamp(item[0]),
        ):

            metadata = LOGGER_CLOCK_CORRECTION_METADATA.get(
                (logger, start_s),
                {},
            )

            rows.append(
                {
                    "logger": logger,
                    "correction_start_raw": start_s,
                    "offset_minutes": int(offset_min),
                    "offset_hours": float(offset_min) / 60.0,
                    "reason": metadata.get(
                        "reason",
                        "No reason documented",
                    ),
                    "evidence": metadata.get(
                        "evidence",
                        "No evidence documented",
                    ),
                    "details": metadata.get(
                        "details",
                        "",
                    ),
                    "confidence": metadata.get(
                        "confidence",
                        "unknown",
                    ),
                }
            )

    return (
        pd.DataFrame(rows)
        .sort_values(
            ["logger", "correction_start_raw"]
        )
        .reset_index(drop=True)
    )

# Logger timestamp correction methodology
#
# Logger internal clocks record time with second-level resolution.
# The Campbell logger program generates 15-minute .dat records by
# averaging higher-frequency measurements internally.
#
# Historical timestamp transitions detected in .dat files may appear as
# offsets such as +75 minutes rather than exactly +60 minutes. These
# transitions represent a combination of:
#
#   1. approximately one hour of MDT/MST clock-state difference, and
#   2. alignment of the timestamp transition with the 15-minute averaged
#      .dat record boundaries.
#
# Therefore, a +75 minute discontinuity in the .dat record sequence does
# not necessarily indicate that the logger clock was manually advanced
# by exactly 75 minutes. It indicates a change in the effective timestamp
# state of the recorded data.
#
# LOGGER_CLOCK_CORRECTIONS stores absolute timestamp states, not
# cumulative adjustments.


def write_logger_clock_corrections_audit(output_path: Path) -> None:
    """
    Write logger clock correction metadata audit CSV.
    """

    df = build_logger_clock_corrections_audit()

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_csv(
        output_path,
        index=False,
    )


audit_path = (
    DATA_PROCESSED_DIR
    / "diagnostics"
    / "logger_clock_corrections_audit.csv"
)

def apply_logger_seasonal_civil_time(
    ts: pd.Series,
    *,
    fixed_tz: str = LOGGER_FIXED_STANDARD_TZ,
    local_tz: Any = DEFAULT_TIMEZONE,
) -> pd.Series:
    """
    Convert corrected logger timestamps from a fixed MST base into America/Denver
    civil time.

    Example effect:
      - spring: 02:00 local standard-base becomes 03:00 civil time
      - fall: repeated 01:00 hour is represented in the tz-aware series

    Returns a timezone-aware Series in local_tz.
    """
    s = pd.to_datetime(ts, errors="coerce")

    # First interpret corrected naive timestamps as fixed MST (UTC-7 all year)
    s_fixed = s.dt.tz_localize(fixed_tz)

    # Then convert to America/Denver civil time
    return s_fixed.dt.tz_convert(tz_name(local_tz))

# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def ts_to_iso_minute(ts_any: Any) -> str:
    if ts_any is None or pd.isna(ts_any):
        return ""
    if not isinstance(ts_any, pd.Timestamp):
        ts_any = pd.to_datetime(ts_any, errors="coerce")
        if ts_any is pd.NaT or pd.isna(ts_any):
            return ""
        if not isinstance(ts_any, pd.Timestamp):
            ts_any = pd.Timestamp(ts_any)
    return ts_any.strftime("%Y-%m-%dT%H:%M")

def ts_to_iso_date(ts_any: Any) -> str:
    if ts_any is None or pd.isna(ts_any):
        return ""
    if not isinstance(ts_any, pd.Timestamp):
        ts_any = pd.to_datetime(ts_any, errors="coerce")
        if ts_any is pd.NaT or pd.isna(ts_any):
            return ""
        if not isinstance(ts_any, pd.Timestamp):
            ts_any = pd.Timestamp(ts_any)
    return ts_any.strftime("%Y-%m-%d")

def make_timestamp_or_raise(value: str, *, context: str = "") -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError(f"Invalid timestamp {value!r}" + (f" ({context})" if context else ""))
    assert isinstance(ts, pd.Timestamp)
    return ts

def force_datetime64_ns(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    dt_nonnull = dt.dropna()
    return cast(pd.Series, dt_nonnull.astype("datetime64[ns]"))

def normalize_logger_timestamp_series(ts: Series) -> Series:
    """
    Parse raw logger timestamp text to naive datetime.

    This function does NOT do DST handling. DST handling for logger data now
    happens later via apply_logger_seasonal_civil_time().
    """
    s = ts.astype("string").str.strip()
    return pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")

def normalize_weather_timestamp_series(ts: pd.Series, tz: Any = DEFAULT_TIMEZONE) -> pd.Series:
    """
    Normalize CoAgMet timestamps:
      - parse
      - localize naive to tz, shifting DST gaps forward
      - convert any tz-aware to tz
      - drop tz info (timezone-naive)
    """
    tz_str = tz_name(tz)
    s = pd.to_datetime(ts, errors="coerce")

    if s.dt.tz is None:
        s = s.dt.tz_localize(tz_str, ambiguous="NaT", nonexistent="shift_forward")
    else:
        s = s.dt.tz_convert(tz_str)

    return s.dt.tz_localize(None)

def make_timestamp_column_naive(df_in: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    """
    If df[col] is timezone-aware, convert to DEFAULT_TIMEZONE and drop tz info.
    """
    df_out = df_in.copy()
    if col in df_out.columns:
        try:
            if isinstance(df_out[col].dtype, pd.DatetimeTZDtype):
                df_out[col] = df_out[col].dt.tz_convert(DEFAULT_TIMEZONE_NAME).dt.tz_localize(None)
        except Exception:
            pass
    return df_out

def make_datetimeindex_naive(df_in: pd.DataFrame, copy: bool = True) -> pd.DataFrame:
    """
    If df.index is a tz-aware DatetimeIndex, convert to DEFAULT_TIMEZONE and drop tz info.
    """
    df = df_in.copy() if copy else df_in
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_convert(DEFAULT_TIMEZONE_NAME).tz_localize(None)
    return df

# ---------------------------------------------------------------------------
# Strip pairing assumptions (treated vs control)
# ---------------------------------------------------------------------------
STRIP_PAIRS = [
    ("S1", "S2"),
    ("S3", "S4"),
]

# ============================= Common helpers ============================= #

def convert_soil_t_to_fahrenheit(df_in: pd.DataFrame, copy: bool = True) -> pd.DataFrame:
    df = df_in.copy() if copy else df_in
    t_cols = [c for c in df.columns if c.startswith("T_") and "_raw_" in c]
    if not t_cols:
        return df

    to_f = UNIT_CONVERSIONS["metric_to_us"]["temp"]
    for col_name in t_cols:
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce").apply(to_f)

    logger.info(f"🌡 Converted {len(t_cols)} soil-temp columns from °C to °F")
    return df

def rename_logger_columns(df: pd.DataFrame, logger_name: str) -> pd.DataFrame:
    mapping: dict[str, str] = {}
    prefix = logger_name[:2]
    loc = logger_name[2:]

    for col_name in df.columns:
        if col_name == "timestamp":
            continue

        if col_name == "BattV_Min":
            mapping[col_name] = f"BattV_Min_{prefix}_{loc}"
            continue

        if col_name.startswith(("VWC_", "T_", "EC_")):
            parts = col_name.split("_", maxsplit=2)
            if len(parts) == 3:
                var, depth, _agg = parts
                mapping[col_name] = f"{var}_{depth}_raw_{prefix}_{loc}"

    return df.rename(columns=mapping)

def _clean_col_name(s: object) -> str:
    return str(s).lstrip("\ufeff").strip().strip('"').strip("'").strip()

def _read_toa5_table1_dat(datfile: Path) -> pd.DataFrame:
    with datfile.open("r", newline="") as f:
        r = csv.reader(f)
        _meta = next(r, None)
        colnames = next(r, None)
        _units = next(r, None)
        _aggs = next(r, None)

    if not colnames:
        raise ValueError(f"{datfile.name}: missing TOA5 column-name row.")
    cols = [_clean_col_name(c) for c in colnames]
    if "TIMESTAMP" not in cols and "timestamp" not in cols:
        raise ValueError(f"{datfile.name}: TOA5 column-name row does not include TIMESTAMP.")

    return pd.read_csv(
        datfile,
        skiprows=4,
        header=None,
        names=cols,
        na_values=["", "NA", "NAN"],
        engine="python",
    )

def _candidate_logger_files(tag: str, year: int) -> list[Path]:
    """
    Resolve which .dat files should contribute to a (tag,year).

    Special case: year==2023
      - read the normal datfiles_2023/{tag}_Table1.dat (if present)
      - ALSO read datfiles_2024/{tag}_Table1.dat (if present) and keep rows < 2024-01-01
      - ALSO read datfiles_2023/{tag}_Table1_late2023_withBattV.dat (if present)
    """
    files: list[Path] = []
    base = Path(DATA_RAW_DIR)

    p_main = base / f"datfiles_{year}" / f"{tag}_Table1.dat"
    if p_main.exists():
        files.append(p_main)

    if year == 2023:
        p_next = base / "datfiles_2024" / f"{tag}_Table1.dat"
        if p_next.exists():
            files.append(p_next)

        p_late = base / "datfiles_2023" / f"{tag}_Table1_late2023_withBattV.dat"
        if p_late.exists():
            files.append(p_late)

    return files

def read_logger_data(tag: str, year: int) -> Optional[pd.DataFrame]:
    files = _candidate_logger_files(tag, year)
    if not files:
        logger.warning(f"⚠️ Not found: datfiles_{year}/{tag}_Table1.dat (and no backfill sources)")
        return None

    frames: list[pd.DataFrame] = []
    raw_ts_examples: list[str] = []

    for datfile in files:
        try:
            df = _read_toa5_table1_dat(datfile)
        except Exception as e:
            logger.error(f"❌ Failed reading TOA5 file {datfile.name}: {e}")
            continue

        if "TIMESTAMP" in df.columns and "timestamp" not in df.columns:
            df = df.rename(columns={"TIMESTAMP": "timestamp"})
        df = df.drop(columns=["RECORD"], errors="ignore")

        if df.empty or "timestamp" not in df.columns:
            continue

        raw_ts = df["timestamp"].copy()
        df["timestamp"] = normalize_logger_timestamp_series(raw_ts)

        bad_mask = df["timestamp"].isna()
        n_nat = int(bad_mask.sum())
        if n_nat:
            bad_idx = df.index[bad_mask][:10].tolist()
            for i in bad_idx:
                raw_ts_examples.append(f"{datfile.name}: row={int(i)} raw={raw_ts.iloc[i]!r}")
            df = df.loc[~bad_mask].copy()

        if df.empty:
            continue

        # Apply manual logger-specific clock stitching first (still naive).
        df["timestamp"] = apply_logger_clock_corrections(df["timestamp"], tag)

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").astype("datetime64[ns]")
        df = df.dropna(subset=["timestamp"])
        if df.empty:
            continue

        frames.append(df)

    if not frames:
        if raw_ts_examples:
            logger.warning(f"⚠️ NaT examples for {tag}: " + "; ".join(raw_ts_examples[:10]))
        return None

    df_all = pd.concat(frames, ignore_index=True)

    # Prefer later files for overlapping timestamps (e.g. 2024 copy with BattV_Min)
    # at the corrected naive timestamp stage.
    df_all = df_all.sort_values("timestamp")
    df_all = df_all.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

    # Year filtering can safely happen here because year boundaries are in standard time.
    start_ts = pd.Timestamp(year=year, month=1, day=1)
    end_ts = pd.Timestamp(year=year + 1, month=1, day=1)

    ts_vals = df_all["timestamp"].to_numpy()
    mask_year = (ts_vals >= start_ts.to_datetime64()) & (ts_vals < end_ts.to_datetime64())
    df_all = df_all.loc[mask_year].copy()
    if df_all.empty:
        return None

    # Now convert the corrected fixed-base timeline into civil America/Denver time.
    df_all["timestamp"] = apply_logger_seasonal_civil_time(df_all["timestamp"])

    return rename_logger_columns(df_all, tag)

def merge_all_loggers(year: int) -> Optional[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            tag = f"{strip}{loc}"
            df = read_logger_data(tag, year)
            if df is None or df.empty:
                continue
            df = df.set_index("timestamp")
            frames.append(df)

    if not frames:
        return None

    merged = pd.concat(frames, axis=1)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    return merged.reset_index()

def replace_bad_values(
    df_in: pd.DataFrame,
    threshold: float = DEFAULT_BAD_VALUE_THRESHOLD,
    copy: bool = True,
) -> pd.DataFrame:
    df = df_in.copy() if copy else df_in
    for col_name in df.columns:
        if col_name == "timestamp":
            continue
        s = pd.to_numeric(df[col_name], errors="coerce")
        df[col_name] = s.mask(s.abs() >= threshold, NAN)
    logger.info(f"🧹 Replaced extreme placeholders with NaN (|x| ≥ {threshold:g})")
    return df

def scale_vwc_to_percent(df_in: pd.DataFrame, *, copy: bool = True) -> pd.DataFrame:
    df = df_in.copy() if copy else df_in

    vwc_cols = [c for c in df.columns if c.startswith("VWC_") and "_raw_" in c]
    for col_name in vwc_cols:
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce") * 100.0

    return df

def add_swc_cylinder_volumes(df_in: pd.DataFrame, copy: bool = True) -> pd.DataFrame:
    """Retain legacy VWC-scaled reference-cylinder water volumes.

    The assumed 10 cm by 4 cm-radius cylinder is not the documented CS650
    sensing volume. These columns remain for backward compatibility only.
    """
    df = df_in.copy() if copy else df_in
    cyl_m3 = cylinder_volume_m3()
    cyl_l = cyl_m3 * 1000.0
    cyl_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](cyl_l)

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                vwc_col = f"VWC_{depth}_raw_{strip}_{loc}"
                if vwc_col not in df.columns:
                    continue
                frac = pd.to_numeric(df[vwc_col], errors="coerce") / 100.0
                df[f"SWC_vol_L_{strip}_{loc}_{depth}"] = frac * cyl_l
                df[f"SWC_vol_gal_{strip}_{loc}_{depth}"] = frac * cyl_gal

    logger.info("💧 Added legacy SWC reference-cylinder volumes")
    return df


def add_cs650_sensing_volume_water(
    df_in: pd.DataFrame,
    copy: bool = True,
) -> pd.DataFrame:
    """Estimate local water volume within the documented CS650 sensing volume.

    The manufacturer's 7,800 cm3 sensing volume is approximate and spatially
    weighted, with greatest sensitivity near the rods. These fields are local
    sensor diagnostics and must not be summed or treated as field-zone gallons.
    """
    df = df_in.copy() if copy else df_in
    sensing_volume_l = CS650_SENSING_VOLUME_CM3 / 1000.0
    sensing_volume_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](
        sensing_volume_l
    )

    for strip in STRIPS:
        for loc in LOGGER_LOCATIONS:
            for depth in SENSOR_DEPTH_VALUES:
                vwc_col = f"VWC_{depth}_raw_{strip}_{loc}"
                if vwc_col not in df.columns:
                    continue
                fraction = (
                    pd.to_numeric(df[vwc_col], errors="coerce") / 100.0
                )
                df[f"CS650_water_L_{strip}_{loc}_{depth}"] = (
                    fraction * sensing_volume_l
                )
                df[f"CS650_water_gal_{strip}_{loc}_{depth}"] = (
                    fraction * sensing_volume_gal
                )

    logger.info("💧 Added CS650 sensing-volume water diagnostics")
    return df

def add_temperature_differences(
    df_in: pd.DataFrame,
    *,
    copy: bool = True,
) -> pd.DataFrame:
    df = df_in.copy() if copy else df_in
    new_cols = 0

    for treated, control in STRIP_PAIRS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col_treated = f"T_{depth}_raw_{treated}_{loc}"
                col_control = f"T_{depth}_raw_{control}_{loc}"
                if col_treated not in df.columns or col_control not in df.columns:
                    continue
                diff_col = f"Tdiff_{depth}_{treated}_{control}_{loc}"
                df[diff_col] = (
                    pd.to_numeric(df[col_treated], errors="coerce")
                    - pd.to_numeric(df[col_control], errors="coerce")
                )
                new_cols += 1

    logger.info(
        f"🌡 Added {new_cols} ΔT columns (biochar − control)"
        if new_cols
        else "🌡 No ΔT columns added (required T_*_raw_* columns missing)"
    )
    return df

def add_swc_differences(
    df_in: pd.DataFrame,
    *,
    copy: bool = True,
) -> pd.DataFrame:
    df = df_in.copy() if copy else df_in
    new_cols = 0

    for treated, control in STRIP_PAIRS:
        for loc in LOGGER_LOCATIONS:
            for depth in ["1", "2", "3"]:
                col_treated_gal = f"SWC_vol_gal_{treated}_{loc}_{depth}"
                col_control_gal = f"SWC_vol_gal_{control}_{loc}_{depth}"
                col_treated_L = f"SWC_vol_L_{treated}_{loc}_{depth}"
                col_control_L = f"SWC_vol_L_{control}_{loc}_{depth}"

                if col_treated_gal in df.columns and col_control_gal in df.columns:
                    diff_col_gal = f"SWCdiff_gal_{treated}_{control}_{loc}_{depth}"
                    df[diff_col_gal] = (
                        pd.to_numeric(df[col_treated_gal], errors="coerce")
                        - pd.to_numeric(df[col_control_gal], errors="coerce")
                    )
                    new_cols += 1

                if col_treated_L in df.columns and col_control_L in df.columns:
                    diff_col_L = f"SWCdiff_L_{treated}_{control}_{loc}_{depth}"
                    df[diff_col_L] = (
                        pd.to_numeric(df[col_treated_L], errors="coerce")
                        - pd.to_numeric(df[col_control_L], errors="coerce")
                    )
                    new_cols += 1

    logger.info(
        f"💧 Added {new_cols} ΔSWC volume columns (biochar − control)"
        if new_cols
        else "💧 No ΔSWC columns added (required SWC_vol_* columns missing)"
    )
    return df

# ============================= Growing-season summary ============================= #

def unpack_gseason_period(period_code: str, period_meta: Any) -> tuple[str, str, str]:
    if isinstance(period_meta, (tuple, list)) and len(period_meta) == 2:
        return period_code, str(period_meta[0]), str(period_meta[1])

    if isinstance(period_meta, dict):
        mmdd_start = period_meta.get("start")
        mmdd_end = period_meta.get("end")
        label = period_meta.get("label", period_code)
        if mmdd_start and mmdd_end:
            return str(label), str(mmdd_start), str(mmdd_end)

    raise ValueError(
        f"DEFAULT_GSEASON_PERIODS[{period_code!r}] must be "
        f"('MM-DD','MM-DD') or {{'start':'MM-DD','end':'MM-DD','label':...}}; got {period_meta!r}"
    )

def write_gseason_summary(year: int, df_daily: pd.DataFrame) -> None:
    if "timestamp" not in df_daily.columns:
        logger.warning(f"⚠️ write_gseason_summary({year}) skipped: no 'timestamp' column")
        return

    daily_df = df_daily.copy()
    daily_df["timestamp"] = pd.to_datetime(daily_df["timestamp"], errors="coerce")
    daily_df = daily_df.dropna(subset=["timestamp"])
    if daily_df.empty:
        logger.warning(f"⚠️ write_gseason_summary({year}) skipped: empty daily frame")
        return
    daily_df["timestamp"] = daily_df["timestamp"].astype("datetime64[ns]")

    value_cols: list[str] = [c for c in daily_df.columns if c != "timestamp"]
    agg_map: dict[str, str] = {c: ("sum" if c.startswith("precip") else "mean") for c in value_cols}

    daily_dir = Path(PARQUET_DIR) / "summary" / "daily"
    prev_daily_df: Optional[pd.DataFrame] = None
    prev_loaded_year: Optional[int] = None

    rows: list[dict[str, Any]] = []

    for period_code, meta in DEFAULT_GSEASON_PERIODS.items():
        period_label, mmdd_start, mmdd_end = unpack_gseason_period(period_code, meta)

        start_month = int(mmdd_start.split("-")[0])
        end_month = int(mmdd_end.split("-")[0])
        wraps_year = start_month > end_month

        period_start_year = year - 1 if wraps_year else year
        period_end_year = year

        start_ts = make_timestamp_or_raise(f"{period_start_year}-{mmdd_start}", context=f"{period_code} start")
        end_day = make_timestamp_or_raise(f"{period_end_year}-{mmdd_end}", context=f"{period_code} end")
        end_ts = end_day + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        window_parts: list[pd.DataFrame] = []

        if wraps_year:
            prev_path = daily_dir / f"{period_start_year}_daily.parquet"
            if prev_path.exists():
                if prev_daily_df is None or prev_loaded_year != period_start_year:
                    loaded_prev = pd.read_parquet(prev_path)

                    if "timestamp" not in loaded_prev.columns:
                        logger.warning(
                            f"⚠️ {prev_path.name} missing 'timestamp'; skipping prev-year part for {period_code}."
                        )
                        prev_daily_df = None
                    else:
                        loaded_prev = loaded_prev.copy()
                        loaded_prev["timestamp"] = pd.to_datetime(loaded_prev["timestamp"], errors="coerce")
                        loaded_prev = loaded_prev.dropna(subset=["timestamp"])
                        if not loaded_prev.empty:
                            loaded_prev["timestamp"] = loaded_prev["timestamp"].astype("datetime64[ns]")
                        prev_daily_df = loaded_prev
                        prev_loaded_year = period_start_year

                if prev_daily_df is not None and not prev_daily_df.empty:
                    ts_vals_prev = prev_daily_df["timestamp"].to_numpy()
                    mask_prev = (ts_vals_prev >= start_ts.to_datetime64()) & (ts_vals_prev <= end_ts.to_datetime64())
                    window_parts.append(prev_daily_df.loc[mask_prev])
            else:
                logger.warning(
                    f"⚠️ Missing prev-year daily parquet {prev_path.name} for {period_code} ({year}); "
                    f"using only current-year component."
                )

        ts_vals_cur = daily_df["timestamp"].to_numpy()
        mask_cur = (ts_vals_cur >= start_ts.to_datetime64()) & (ts_vals_cur <= end_ts.to_datetime64())
        window_parts.append(daily_df.loc[mask_cur])

        window = pd.concat(window_parts, ignore_index=True) if window_parts else pd.DataFrame(columns=daily_df.columns)

        if window.empty:
            logger.warning(
                f"⚠️ No daily rows for gseason {period_code} in {year} "
                f"[{start_ts.date()} → {end_ts.date()}]; filling NaN."
            )
            stats: dict[str, Any] = {c: math.nan for c in value_cols}
        else:
            stats_series = window[value_cols].agg(agg_map).round(3)
            stats = stats_series.to_dict()

        rows.append(
            {
                "period_code": period_code,
                "period_label": period_label,
                "period_start": ts_to_iso_date(start_ts),
                "period_end": ts_to_iso_date(end_ts),
                **stats,
            }
        )

    out_df = pd.DataFrame(rows)
    num_cols = out_df.select_dtypes(include=["float", "int"]).columns
    if len(num_cols) > 0:
        out_df[num_cols] = out_df[num_cols].round(3)

    out_dir = Path(PARQUET_DIR) / "summary" / "gseason"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{year}_gseason.parquet"
    out_df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"✅ Summary gseason (DEFAULT periods): {out_path.name}")

# ============================= Bulk-download helpers ============================= #

def write_logger_download_zip(year: int, df_15min: pd.DataFrame) -> None:
    zip_path = LOGGER_DOWNLOADS_DIR / f"Biochar_Loggers_15min_{year}_USunits.zip"

    df = df_15min.copy()
    if "timestamp" not in df.columns:
        df = df.reset_index()
    if "timestamp" not in df.columns:
        raise ValueError("write_logger_download_zip: df_15min must have 'timestamp' as index or column")

    df = make_timestamp_column_naive(df, col="timestamp")

    readme_lines: list[str] = [
        "Biochar Fruita CSU Experiment – Logger 15-minute Data",
        f"Year: {year}",
        "",
        "Files in this ZIP",
        "-----------------",
        "  One CSV per datalogger location (e.g., S1T, S1M, ..., S4B).",
        "  Each CSV contains a 15-minute time series for all depths (1, 2, 3).",
        "",
        "CSV files:",
    ]

    from io import StringIO
    from zipfile import ZipFile

    with ZipFile(zip_path, mode="w") as zf:
        for strip in STRIPS:
            for loc in LOGGER_LOCATIONS:
                tag = f"{strip}{loc}"
                suffix = f"_{strip}_{loc}"

                cols = [c for c in df.columns if c == "timestamp" or c.endswith(suffix)]
                if len(cols) <= 1:
                    continue

                sub = df[cols].copy()
                csv_name = f"{tag}_15min_{year}_USunits.csv"

                buf = StringIO()
                sub.to_csv(buf, index=False)
                zf.writestr(csv_name, buf.getvalue())

                readme_lines.append(f"  - {csv_name}: 15-min data for logger {tag}")

        readme_lines.extend(
            [
                "",
                "Column naming convention",
                "------------------------",
                "  timestamp                         : America/Denver local civil time (timezone-naive on export)",
                "  VWC_<depth>_raw_<strip>_<loc>     : volumetric water content (%)",
                "  T_<depth>_raw_<strip>_<loc>       : soil temperature (°F)",
                "  EC_<depth>_raw_<strip>_<loc>      : electrical conductivity (dS/m)",
                "  SWC_vol_L_<strip>_<loc>_<depth>   : legacy reference-cylinder water volume (liters)",
                "  SWC_vol_gal_<strip>_<loc>_<depth> : legacy reference-cylinder water volume (gallons)",
                "  CS650_water_L_<strip>_<loc>_<depth>   : local water in approximate CS650 sensing volume (liters)",
                "  CS650_water_gal_<strip>_<loc>_<depth> : local water in approximate CS650 sensing volume (gallons)",
                "",
                "Notes",
                "-----",
                "  - Placeholder/sentinel values (e.g., -9999/9999) have been converted to NaN.",
                "  - Manual logger clock corrections may have been applied before seasonal civil-time conversion.",
                "  - Cross-strip comparison variables (ΔT, ΔSWC, ratio columns, etc.)",
                "    are not included in these per-logger CSVs.",
            ]
        )

        zf.writestr(f"README_Logger_15min_{year}.txt", "\n".join(readme_lines))

    logger.info(f"📦 Wrote logger download ZIP: {zip_path.name}")

def write_weather_download_zip(year: int, df_15min: pd.DataFrame, download_url: str = "", builder_url: str = "") -> None:
    zip_path = WEATHER_DOWNLOADS_DIR / f"Biochar_Weather_15min_{year}_USunits.zip"

    df = df_15min.copy()
    if "timestamp" not in df.columns:
        df = df.reset_index()
    if "timestamp" not in df.columns:
        raise ValueError("write_weather_download_zip: df_15min must have 'timestamp' as index or column")

    from io import StringIO
    from zipfile import ZipFile

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)

    readme_lines: list[str] = [
        f"Biochar Fruita CSU Experiment - 15-min Weather Data ({year})",
        "",
        "Source:",
        f"  - Direct CoAgMet-style download URL: {download_url or '[ADD_DOWNLOAD_URL_HERE]'}",
        f"  - CoAgMet builder page (construct custom URLs): {builder_url or 'https://coagmet.colostate.edu/data/url-builder'}",
        "",
        "Files in this ZIP",
        "-----------------",
        f"  - weather_15min_{year}_USunits.csv : 15-minute time series",
        "",
        "Notes:",
        "  - Timestamps are naive datetimes interpreted as America/Denver local time.",
        "  - Precipitation increments are clipped at 0; missing codes (-999) treated as NaN.",
    ]

    with ZipFile(zip_path, mode="w") as zf:
        zf.writestr(f"weather_15min_{year}_USunits.csv", csv_buf.getvalue())
        zf.writestr(f"README_Weather_15min_{year}.txt", "\n".join(readme_lines))

    logger.info(f"📦 Wrote weather download ZIP: {zip_path.name}")

# ============================= Aggregation (loggers) ============================= #

def aggregate_and_write(year: int, df: pd.DataFrame) -> None:
    """
    Aggregate logger data.

    Internally, logger timestamps may be timezone-aware America/Denver.
    We keep them that way through resampling, then drop tz info only when
    writing outputs.
    """
    year_dir = Path(PARQUET_DIR) / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    # Raw logger output
    df_write = make_datetimeindex_naive(df)
    raw_path = year_dir / f"{year}_raw_logger.parquet"
    ratio_path = year_dir / f"{year}_raw_logger_ratios.parquet"

    df_write.reset_index().to_parquet(raw_path, index=False, compression="snappy")
    calculate_ratios(df_write).reset_index().to_parquet(ratio_path, index=False, compression="snappy")
    logger.info(f"✅ Wrote raw & ratio: {raw_path.name}, {ratio_path.name}")

    sensor_prefixes = ("VWC_", "T_", "EC_", "SWC_", "Tdiff_", "SWCdiff_")
    sensor_cols = [c for c in df.columns if any(c.startswith(pref) for pref in sensor_prefixes)]
    summary_base = Path(PARQUET_DIR) / "summary"

    for freq, code in GRANULARITIES:
        if code is None:
            continue

        out_dir = summary_base / freq
        out_dir.mkdir(parents=True, exist_ok=True)

        agg_map = {col: "sum" if col.startswith("precip") else "mean" for col in df.columns}
        df_s = df_agg(df.resample(code), agg_map).round(3)
        df_s = df_s.dropna(subset=sensor_cols, how="all").reset_index()
        df_s = make_timestamp_column_naive(df_s, col="timestamp")

        fn_raw = f"{year}_{freq}.parquet"
        df_s.to_parquet(out_dir / fn_raw, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq}: {fn_raw}")

        if freq == "daily":
            write_gseason_summary(year, df_s)

        if freq == "15min":
            write_logger_download_zip(year, df_s.set_index("timestamp"))

        df_s_ratio = calculate_ratios(df_s.set_index("timestamp"))
        fn_ratio = f"{year}_{freq}_ratios.parquet"
        df_s_ratio.reset_index().to_parquet(out_dir / fn_ratio, index=False, compression="snappy")
        logger.info(f"✅ Summary {freq} ratios: {fn_ratio}")

# ============================= Weather (CoAgMet) ============================= #

def clean_weather_frame(dfw: pd.DataFrame) -> pd.DataFrame:
    df_copy = dfw.copy()
    df_copy["timestamp"] = normalize_weather_timestamp_series(df_copy["timestamp"])

    df_copy["precip_in"] = pd.to_numeric(df_copy["precip_in"], errors="coerce")
    df_copy.loc[df_copy["precip_in"] == -999, "precip_in"] = math.nan
    df_copy["precip_in"] = df_copy["precip_in"].fillna(0.0).clip(lower=0.0)

    spike = df_copy["precip_in"].max()
    if pd.notna(spike) and spike > 1.5:
        logger.warning(f"⚠️ CoAgMet 5 min precip spike detected: {spike:.2f} in")

    bad_mask = df_copy["timestamp"].isna()
    bad_ts = int(bad_mask.sum())
    if bad_ts:
        ex = df_copy.loc[bad_mask].head(10).copy()
        cols_to_show = [c for c in ["timestamp", "precip_in", "temp_air_degF"] if c in ex.columns]
        logger.warning(
            "⚠️ Dropping %d weather rows with invalid/ambiguous timestamps. Examples:\n%s",
            bad_ts,
            ex[cols_to_show].to_string(index=False),
        )
        df_copy = df_copy.loc[~bad_mask].copy()

    return df_copy

def validate_datfiles_for_year(year: int) -> None:
    raw_year_dir = Path(DATA_RAW_DIR) / f"datfiles_{year}"

    if not raw_year_dir.exists():
        raise FileNotFoundError(f"Missing raw dat directory: {raw_year_dir}")

    required = [
        f"{strip}{loc}_Table1.dat"
        for strip in STRIPS
        for loc in LOGGER_LOCATIONS
    ]

    missing = [
        filename
        for filename in required
        if not (raw_year_dir / filename).exists()
    ]

    if missing:
        raise FileNotFoundError(
            f"Missing required Table1 dat files for {year} in {raw_year_dir}:\n"
            + "\n".join(f"  - {name}" for name in missing)
        )

    dat_count = len(list(raw_year_dir.glob("*.dat")))
    logger.info(f"✅ Raw dat validation passed for {year}: {dat_count} .dat files found")

def _read_backup_state() -> dict[str, Any]:
    if not RAW_DATA_BACKUP_STATE.exists():
        return {}

    try:
        return json.loads(RAW_DATA_BACKUP_STATE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_backup_state(state: dict[str, Any]) -> None:
    RAW_DATA_BACKUP_STATE.parent.mkdir(parents=True, exist_ok=True)
    RAW_DATA_BACKUP_STATE.write_text(
        json.dumps(state, indent=2),
        encoding="utf-8",
    )

def maybe_backup_raw_data(force: bool = False) -> Path | None:
    RAW_DATA_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    state = _read_backup_state()
    last_backup_date = state.get("last_backup_date")

    if not force and last_backup_date:
        try:
            last_dt = datetime.fromisoformat(str(last_backup_date))
            age_days = (datetime.now() - last_dt).days

            if age_days < RAW_DATA_BACKUP_INTERVAL_DAYS:
                logger.info(
                    f"📦 Raw data backup current: last backup {age_days} days ago"
                )
                return None
        except Exception:
            pass

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_base = RAW_DATA_BACKUP_DIR / f"data_raw_{timestamp}"

    zip_path = Path(
        shutil.make_archive(
            base_name=str(archive_base),
            format="zip",
            root_dir=Path(DATA_RAW_DIR).parent,
            base_dir=Path(DATA_RAW_DIR).name,
        )
    )

    size_mb = zip_path.stat().st_size / (1024 * 1024)

    logger.info("📦 Raw data backup written:")
    logger.info(f"    File: {zip_path}")
    logger.info(f"    Size: {size_mb:.1f} MB")

    _write_backup_state(
        {
            "last_backup_date": datetime.now().isoformat(timespec="seconds"),
            "last_backup_file": str(zip_path),
            "size_mb": round(size_mb, 1),
        }
    )

    return zip_path
# ============================= Orchestration ============================= #

def generate_summaries(years: list[int]) -> None:
    dataset_metadata: DatasetMetadata = {}
    for year in years:
        logger.info(f"🌱 Starting ETL for {year}")

        df = merge_all_loggers(year)
        if df is None or df.empty:
            logger.error(f"❌ No logger .dat data for {year}, skipping logger summaries.")
        else:
            df = df.dropna(subset=["timestamp"]).copy()

            df = replace_bad_values(df, threshold=DEFAULT_BAD_VALUE_THRESHOLD)
            df = scale_vwc_to_percent(df)
            df = convert_soil_t_to_fahrenheit(df)

            df, bounds_reports = enforce_value_bounds(
                df,
                year=year,
                bad_value_threshold=None,
                collect_examples=5,
            )

            if bounds_reports:
                total_violations = sum(int(r.get("violations", 0) or 0) for r in bounds_reports)
                logger.warning(
                    f"⚠️ Bounds enforcement: {len(bounds_reports)} columns had violations "
                    f"({total_violations} total masked values). Showing up to 10 entries:"
                )

                def _fmt_ts(x: Any) -> str:
                    if x is None or pd.isna(x):
                        return ""
                    try:
                        return pd.to_datetime(x, errors="coerce").strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        return str(x)

                for r in bounds_reports[:10]:
                    logger.warning(
                        f"  - {r.get('rule')} col={r.get('column')} violations={r.get('violations')} "
                        f"min={r.get('min')} max={r.get('max')} label={r.get('label')}"
                    )

                    ex = r.get("examples") or []
                    if ex:
                        for e in ex:
                            logger.warning(
                                f"      example: ts={_fmt_ts(e.get('timestamp'))} value={e.get('value')}"
                            )

            df = add_swc_cylinder_volumes(df)
            df = add_cs650_sensing_volume_water(df)
            df = add_temperature_differences(df)
            df = add_swc_differences(df)

            df = df.set_index("timestamp").sort_index()
            #
            # Update dataset metadata from cleaned logger data.
            #
            vwc_columns = [
                c
                for c in df.columns
                if c.startswith("VWC_") and "_raw_" in c
            ]

            soil_temp_columns = [
                c
                for c in df.columns
                if c.startswith("T_") and "_raw_" in c
            ]

            ec_columns = [
                c
                for c in df.columns
                if c.startswith("EC_") and "_raw_" in c
            ]

            if vwc_columns:
                update_dataset_metadata(
                    dataset_metadata,
                    "vwc_percent",
                    cast(pd.Series, df[vwc_columns].stack(future_stack=True)),
                )

            if soil_temp_columns:
                update_dataset_metadata(
                    dataset_metadata,
                    "soil_temperature_f",
                    cast(pd.Series, df[soil_temp_columns].stack(future_stack=True)),
                )

            if ec_columns:
                update_dataset_metadata(
                    dataset_metadata,
                    "soil_ec_ds_per_m",
                    cast(pd.Series, df[ec_columns].stack(future_stack=True)),
                )
            aggregate_and_write(year, df)

        # ---------------- Weather ----------------
        try:
            dfw = fetch_weather_data(year)
        except Exception as e:
            logger.error(f"❌ fetch_weather_data({year}) failed: {e}")
            continue

        required_cols = {"timestamp", "precip_in", "temp_air_degF"}
        missing = required_cols - set(dfw.columns)
        if missing:
            logger.error(f"❌ fetch_weather_data({year}) missing columns: {sorted(missing)}")
            continue

        dfw_clean = clean_weather_frame(dfw).set_index("timestamp").sort_index()
        #
        # Update dataset metadata from cleaned weather data.
        #
        update_dataset_metadata(
            dataset_metadata,
            "air_temperature_f",
            dfw_clean["temp_air_degF"],
        )

        daily_precip = (
            pd.to_numeric(
                dfw_clean["precip_in"],
                errors="coerce",
            )
            .resample("D")
            .sum(min_count=1)
        )

        update_dataset_metadata(
            dataset_metadata,
            "daily_precipitation_in",
            daily_precip,
        )

        dfw_clean["precip_mm"] = dfw_clean["precip_in"].apply(UNIT_CONVERSIONS["us_to_metric"]["precip"])
        dfw_clean["temp_air_degC"] = dfw_clean["temp_air_degF"].apply(UNIT_CONVERSIONS["us_to_metric"]["temp"])

        weather_base = Path(PARQUET_DIR) / "summary" / "weather"
        dfw_15min_for_zip: Optional[pd.DataFrame] = None

        for freq, code in GRANULARITIES:
            if code is None:
                continue
            out_dir = weather_base / freq
            out_dir.mkdir(parents=True, exist_ok=True)

            agg_map = {col: "sum" if col.startswith("precip") else "mean" for col in dfw_clean.columns}
            dfr = dfw_clean.resample(code).agg(cast(Any, agg_map)).round(3).reset_index()
            dfr = make_timestamp_column_naive(dfr, col="timestamp")
            fn = f"{year}_{freq}.parquet"
            dfr.to_parquet(out_dir / fn, index=False, compression="snappy")
            logger.info(f"✅ Weather {freq} for {year}")

            if freq == "15min":
                dfw_15min_for_zip = dfr

        if dfw_15min_for_zip is not None:
            start_ts = pd.Timestamp(f"{year}-01-01 00:00", tz=DEFAULT_TIMEZONE_NAME)
            year_end = pd.Timestamp(f"{year}-12-31 23:59", tz=DEFAULT_TIMEZONE_NAME)
            now_ts = pd.Timestamp.now(tz=DEFAULT_TIMEZONE_NAME).floor("min")
            end_ts = min(year_end, now_ts)

            start_iso = ts_to_iso_minute(start_ts)
            end_iso = ts_to_iso_minute(end_ts)

            fields_param = ",".join(COAGMET_VARIABLE_MAP.keys())
            coag_download_url = (
                f"https://coagmet.colostate.edu/data/{COLLECT_PERIOD}/{COAG_STATION}.csv"
                f"?header=yes"
                f"&fields={fields_param}"
                f"&from={start_iso}&to={end_iso}"
                f"&tz=co&units={DEFAULT_UNITS}&dateFmt=iso"
            )
            builder_url = "https://coagmet.colostate.edu/data/url-builder"

            write_weather_download_zip(
                year,
                dfw_15min_for_zip,
                download_url=coag_download_url,
                builder_url=builder_url,
            )
    write_dataset_metadata(dataset_metadata, years)
    logger.info("🎉 ETL complete.")

def resolve_target_year(cli_year: Optional[int] = None) -> int:
    """
    Determine the year to process.

    Priority:
    1. Explicit CLI argument; eg 2024; run python etl.py --year 2024
    2. Current calendar year. python etl.py

    """
    if cli_year is not None:
        return int(cli_year)

    return DEFAULT_ETL_YEAR

def safe_series_ratio(num: pd.Series, denom: pd.Series, eps: float = 1e-3) -> pd.Series:
    """
    Compute num / denom but avoid blow-ups when denom ≈ 0.

    Any |denom| < eps becomes NaN so ratio is NaN there too.
    Also removes ±inf values if they slip through.
    """
    num_f = pd.to_numeric(num, errors="coerce").astype(float)
    denom_f = pd.to_numeric(denom, errors="coerce").astype(float)

    denom_safe = denom_f.copy()
    small_mask = denom_safe.abs() < float(eps)
    denom_safe.loc[small_mask] = NAN

    ratio = num_f / denom_safe
    ratio = ratio.replace([POS_INF, NEG_INF], NAN)
    return ratio

def calculate_ratios(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Build a ratio-only dataframe.

    Output contains:
      - the same index as df_in
      - only ratio columns (no raw logger columns)

    For VWC/EC compute (S1/S2) and (S3/S4) per depth and logger location.
    Also compute SWC ratios using SWC_vol_gal_* columns if present.
    """
    ratio_df = pd.DataFrame(index=df_in.index.copy())
    pairings = [("S1", "S2"), ("S3", "S4")]
    ratio_vars = ["VWC", "EC"]

    for var in ratio_vars:
        for s1, s2 in pairings:
            for loc in LOGGER_LOCATIONS:
                for d in SENSOR_DEPTH_VALUES:
                    c1 = f"{var}_{d}_raw_{s1}_{loc}"
                    c2 = f"{var}_{d}_raw_{s2}_{loc}"
                    out_col = f"{var}_{d}_ratio_{s1}_{s2}_{loc}"

                    if c1 in df_in.columns and c2 in df_in.columns:
                        ratio_df[out_col] = safe_series_ratio(df_in[c1], df_in[c2])
                    else:
                        ratio_df[out_col] = pd.NA

    # SWC ratios (gallons)
    for s1, s2 in pairings:
        for loc in LOGGER_LOCATIONS:
            for d in SENSOR_DEPTH_VALUES:
                c1 = f"SWC_vol_gal_{s1}_{loc}_{d}"
                c2 = f"SWC_vol_gal_{s2}_{loc}_{d}"
                out_col = f"SWC_vol_gal_{d}_ratio_{s1}_{s2}_{loc}"

                if c1 in df_in.columns and c2 in df_in.columns:
                    ratio_df[out_col] = safe_series_ratio(df_in[c1], df_in[c2])
                else:
                    ratio_df[out_col] = pd.NA

    return ratio_df

def update_plot_metadata(
    weather_frames: dict[int, pd.DataFrame],
) -> None:
    """
    Generate plot metadata from processed weather data.

    Parameters
    ----------
    weather_frames
        Dictionary keyed by year containing the cleaned weather
        DataFrame (timestamp index, precip_in column).

    Writes
    ------
    biochar_app/config/dataset_metadata.py

    This file is automatically regenerated by ETL and should not be
    edited manually.
    """

    if not weather_frames:
        logger.warning("No weather data available for plot metadata.")
        return

    max_daily_precip_in = 0.0

    for year, df in weather_frames.items():

        if df.empty or "precip_in" not in df.columns:
            continue

        daily = (
            df[["precip_in"]]
            .resample("D")
            .sum(min_count=1)
        )

        if daily.empty:
            continue

        daily_max = float(
            pd.to_numeric(
                daily["precip_in"],
                errors="coerce",
            ).max()
        )

        if math.isnan(daily_max):
            continue

        max_daily_precip_in = max(
            max_daily_precip_in,
            daily_max,
        )

    if max_daily_precip_in <= 0:
        logger.warning(
            "Unable to determine maximum daily precipitation."
        )
        return

    #
    # Round upward to the nearest 0.05 inch
    #
    max_daily_precip_in = (
        math.ceil(max_daily_precip_in * 20.0)
        / 20.0
    )

    max_daily_precip_mm = round(
        UNIT_CONVERSIONS["us_to_metric"]["precip"](
            max_daily_precip_in
        ),
        1,
    )

    plot_metadata_path = (
        Path(__file__).resolve().parents[1]
        / "config"
        / "dataset_metadata.py"
    )

    contents = textwrap.dedent(
        f'''\
        """
        Automatically generated by etl.py

        Do not edit manually.
        """

        PLOT_METADATA = {{
            "precipitation": {{
                "max_daily_in": {max_daily_precip_in:.2f},
                "max_daily_mm": {max_daily_precip_mm:.1f},
            }},
        }}
        '''
    )

    plot_metadata_path.write_text(
        contents,
        encoding="utf-8",
    )

    logger.info(
        "📈 Updated plot metadata: "
        f"max daily precipitation = "
        f"{max_daily_precip_in:.2f} in"
    )


def refresh_master_workbook_snapshot() -> dict[str, Any]:
    """
    Validate and install the latest locally synchronized master workbook.

    Microsoft OneDrive performs cloud synchronization. This function verifies
    that the desktop application is running and then installs a validated,
    byte-identical repository snapshot for application and analysis code.
    """
    require_onedrive_desktop_app()

    source = BIOCHAR_MASTER_SOURCE.synced_source_path
    logger.info("Checking synchronized master workbook: %s", source)
    logger.info(
        "Synchronized workbook modification time: %s",
        datetime.fromtimestamp(source.stat().st_mtime).astimezone().isoformat()
        if source.exists()
        else "unavailable",
    )

    audit = update_snapshot(
        source=source,
        destination=BIOCHAR_MASTER_SOURCE.local_path,
        required_sheets=BIOCHAR_MASTER_SOURCE.required_sheets,
        audit_path=BIOCHAR_MASTER_SOURCE.local_path.with_suffix(
            ".snapshot.json"
        ),
        validate_only=False,
    )

    logger.info(
        "Master workbook snapshot: result=%s changed=%s sha256=%s",
        audit["result"],
        audit["changed"],
        audit["installed_sha256"],
    )
    return audit


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--all-years", action="store_true")
    parser.add_argument("--no-backup-raw", action="store_true")
    parser.add_argument("--force-backup-raw", action="store_true")
    parser.add_argument(
        "--skip-master-workbook-refresh",
        action="store_true",
        help=(
            "Use the existing repository snapshot without checking OneDrive. "
            "Management data may be stale."
        ),
    )
    parser.add_argument(
        "--skip-irrigation-build",
        action="store_true",
        help=(
            "Do not rebuild irrigation_clean.csv from the validated master "
            "workbook snapshot."
        ),
    )
    parser.add_argument(
        "--skip-lab-build",
        action="store_true",
        help=(
            "Do not rebuild field biomass or merge supplemental Ward NIR "
            "data into the cleaned laboratory outputs."
        ),
    )
    args = parser.parse_args()

    os.makedirs(PARQUET_DIR, exist_ok=True)
    write_logger_clock_corrections_audit(audit_path)

    if args.skip_master_workbook_refresh:
        logger.warning(
            "Master-workbook refresh was skipped. Management data may be "
            "based on an older repository snapshot."
        )
    else:
        refresh_master_workbook_snapshot()

    if args.skip_irrigation_build:
        logger.warning(
            "Irrigation build was skipped. Plot annotations and irrigation "
            "analysis may be stale."
        )
    else:
        irrigation_audit = build_and_install_irrigation()
        logger.info(
            "Irrigation data rebuilt: rows=%s latest_start=%s "
            "invalid_group_events=%s",
            irrigation_audit["production_strip_rows"],
            irrigation_audit["latest_irrigation_start"],
            irrigation_audit["invalid_group_events"],
        )

    if args.skip_lab_build:
        logger.warning(
            "Laboratory-data builds were skipped. Biomass and NIR dashboard "
            "data may be stale."
        )
    else:
        biomass_audit = build_and_install_field_biomass()
        logger.info(
            "Field biomass rebuilt: rows=%s sampling_dates=%s latest=%s",
            biomass_audit["rows"],
            biomass_audit["sampling_dates"],
            biomass_audit["latest_sampling_date"],
        )
        update_ward_master_nir()
        logger.info("Ward NIR clean master rebuilt with supplemental files.")

    years = list(YEARS) if args.all_years else [resolve_target_year(args.year)]

    for year in years:
        validate_datfiles_for_year(year)

    if not args.no_backup_raw:
        maybe_backup_raw_data(force=args.force_backup_raw)

    generate_summaries(years)

if __name__ == "__main__":
    main()
