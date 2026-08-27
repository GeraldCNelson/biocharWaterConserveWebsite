#!/usr/bin/env python3
"""
Compare irrigation meter photographs directly with the master workbook.

Pipeline documentation
----------------------
See ``biochar_app/docs/operations/irrigation_analysis_pipeline.md`` for this
camera-versus-workbook QA stage and its relationship to logger civil time.

Camera-first QC model
---------------------
The camera EXIF timestamp and the manually transcribed meter reading are treated
as independent observations.

For each irrigation event in the master workbook, this script compares:

1. Workbook TIME ON against the timestamp of the likely start photograph.
2. Workbook start totalizer against the manually read start-photo counter.
3. Workbook TIME OFF against the timestamp of the likely end photograph.
4. Workbook end totalizer against the manually read end-photo counter.
5. Workbook GAL. USED against the volume derived from the two photographs.

The central question is:

    Which workbook value disagrees with the camera?

The script does not assume that the workbook time or totalizer value is correct.
It also does not use irrigation_clean.csv as the matching authority.

Concurrent irrigation events
----------------------------
When S1/S2 and S3/S4 have the same workbook timestamp and totalizer reading,
they represent one physical meter boundary. A single photograph may therefore
legitimately support both workbook events.

Meter units
-----------
The photographed six-digit reading and workbook totalizer columns are meter
counter-display units.

One counter unit represents 100 gallons.

Inputs
------
- photos/photo_inventory_unique.csv
- the validated repository snapshot of the master workbook
- worksheets named like:
    2023 IRRIGATION
    2024 IRRIGATION
    2025 IRRIGATION
    2026 IRRIGATION

Outputs
-------
- meter_photo_workbook_qc.csv
    One row per workbook irrigation event.

- meter_photo_workbook_boundary_qc.csv
    One row per physical workbook start/end boundary.

- meter_photo_unmatched_clean_photos.csv
    Clean timestamped photographs not selected for any workbook boundary.
"""

from __future__ import annotations

import argparse
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from biochar_app.config.paths import (
    BIOCHAR_MASTER_WORKBOOK,
    IRRIGATION_DIR,
)


# ============================================================
# Paths and constants
# ============================================================
PHOTO_DIR = IRRIGATION_DIR / "photos"

DEFAULT_PHOTO_CSV = (
    PHOTO_DIR / "photo_inventory_unique.csv"
)


DEFAULT_EVENT_OUTPUT_CSV = (
    PHOTO_DIR
    / "meter_photo_workbook_qc.csv"
)

DEFAULT_BOUNDARY_OUTPUT_CSV = (
    PHOTO_DIR
    / "meter_photo_workbook_boundary_qc.csv"
)

DEFAULT_UNMATCHED_PHOTO_CSV = (
    PHOTO_DIR
    / "meter_photo_unmatched_clean_photos.csv"
)

METER_MULTIPLIER_GALLONS = 100
GALLONS_PER_ACRE_FOOT = 325_851.0

DEFAULT_MAX_MATCH_HOURS = 24.0
DEFAULT_TIME_TOLERANCE_MINUTES = 15.0
DEFAULT_COUNTER_TOLERANCE_UNITS = 10
DEFAULT_VOLUME_TOLERANCE_GALLONS = 2_000.0

IRRIGATION_SHEET_PATTERN = re.compile(
    r"^\s*(20\d{2})\s+IRRIGATION\s*$",
    flags=re.IGNORECASE,
)


# ============================================================
# Command line
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare meter-photo EXIF timestamps and manually read counters "
            "directly with irrigation records in the master workbook."
        )
    )

    parser.add_argument(
        "--photo-csv",
        type=Path,
        default=DEFAULT_PHOTO_CSV,
        help=(
            "Canonical unique-photo inventory CSV. "
            f"Default: {DEFAULT_PHOTO_CSV}"
        ),
    )

    parser.add_argument(
        "--workbook",
        type=Path,
        default=BIOCHAR_MASTER_WORKBOOK,
        help=(
            "Master project workbook. "
            f"Default: {BIOCHAR_MASTER_WORKBOOK}"
        ),
    )

    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_EVENT_OUTPUT_CSV,
        help=(
            "Event-level QC output. "
            f"Default: {DEFAULT_EVENT_OUTPUT_CSV}"
        ),
    )

    parser.add_argument(
        "--boundary-output-csv",
        type=Path,
        default=DEFAULT_BOUNDARY_OUTPUT_CSV,
        help=(
            "Physical-boundary QC output. "
            f"Default: {DEFAULT_BOUNDARY_OUTPUT_CSV}"
        ),
    )

    parser.add_argument(
        "--unmatched-photo-csv",
        type=Path,
        default=DEFAULT_UNMATCHED_PHOTO_CSV,
        help=(
            "Output containing clean photographs not used by any boundary. "
            f"Default: {DEFAULT_UNMATCHED_PHOTO_CSV}"
        ),
    )

    parser.add_argument(
        "--max-match-hours",
        type=float,
        default=DEFAULT_MAX_MATCH_HOURS,
        help=(
            "Maximum absolute time separation allowed when considering a "
            "photo for a workbook boundary. "
            f"Default: {DEFAULT_MAX_MATCH_HOURS:g} hours."
        ),
    )

    parser.add_argument(
        "--time-tolerance-minutes",
        type=float,
        default=DEFAULT_TIME_TOLERANCE_MINUTES,
        help=(
            "Maximum absolute camera-versus-workbook time difference treated "
            "as agreement. "
            f"Default: {DEFAULT_TIME_TOLERANCE_MINUTES:g} minutes."
        ),
    )

    parser.add_argument(
        "--counter-tolerance-units",
        type=int,
        default=DEFAULT_COUNTER_TOLERANCE_UNITS,
        help=(
            "Maximum absolute photo-versus-workbook totalizer difference "
            "treated as agreement, in meter counter units. "
            f"Default: {DEFAULT_COUNTER_TOLERANCE_UNITS} units "
            f"({DEFAULT_COUNTER_TOLERANCE_UNITS * METER_MULTIPLIER_GALLONS:,} "
            "gallons)."
        ),
    )

    parser.add_argument(
        "--volume-tolerance-gallons",
        type=float,
        default=DEFAULT_VOLUME_TOLERANCE_GALLONS,
        help=(
            "Maximum absolute photo-derived versus workbook reported-volume "
            "difference treated as agreement. "
            f"Default: {DEFAULT_VOLUME_TOLERANCE_GALLONS:,.0f} gallons."
        ),
    )

    return parser.parse_args()


# ============================================================
# General helpers
# ============================================================

def clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip()

    if text.lower() in {
        "",
        "nan",
        "none",
        "<na>",
        "nat",
    }:
        return ""

    return text


def normalize_column_name(value: object) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out.columns = [
        normalize_column_name(column)
        for column in out.columns
    ]

    return out


def require_columns(
    df: pd.DataFrame,
    required: tuple[str, ...],
    source_name: str,
) -> None:
    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise KeyError(
            f"{source_name} is missing required columns: {missing}\n"
            f"Available columns: {df.columns.tolist()}"
        )


def find_required_column(
    columns: list[str],
    candidates: tuple[str, ...],
    display_name: str,
    source_name: str,
) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate

    raise KeyError(
        f"{source_name!r} does not contain the expected "
        f"{display_name} column.\n"
        f"Expected one of: {candidates}\n"
        f"Available columns: {columns}"
    )


def parse_datetime_series(
    series: pd.Series,
) -> pd.Series:
    text = (
        series
        .astype("string")
        .str.strip()
    )

    return pd.to_datetime(
        text,
        format="mixed",
        errors="coerce",
    )


def parse_local_wall_datetime_series(
    series: pd.Series,
) -> pd.Series:
    """Parse local observations and discard offsets without shifting time."""

    def parse_one(value: object) -> pd.Timestamp:
        text = clean_text(value)
        if not text:
            return pd.NaT

        try:
            timestamp = pd.Timestamp(text)
        except (TypeError, ValueError):
            return pd.NaT

        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_localize(None)

        return timestamp

    return pd.Series(
        (parse_one(value) for value in series),
        index=series.index,
        dtype="datetime64[ns]",
    )


def parse_numeric_series(
    series: pd.Series,
) -> pd.Series:
    return pd.to_numeric(
        series,
        errors="coerce",
    )


def join_unique_text(
    values: pd.Series,
) -> str:
    cleaned = sorted(
        {
            clean_text(value)
            for value in values
            if clean_text(value)
        }
    )

    return " | ".join(cleaned)


def unique_numeric_values(
    values: pd.Series,
) -> list[float]:
    numeric = pd.to_numeric(
        values,
        errors="coerce",
    ).dropna()

    return sorted(
        {
            float(value)
            for value in numeric
        }
    )


def nullable_float(
    value: object,
) -> float | None:
    parsed = pd.to_numeric(
        pd.Series([value]),
        errors="coerce",
    ).iloc[0]

    if pd.isna(parsed):
        return None

    return float(parsed)


def normalize_strip_group(
    value: object,
) -> str | None:
    text = clean_text(value).upper()

    compact = (
        text
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )

    if compact in {
        "1&2",
        "1AND2",
        "12",
        "S1S2",
        "S1&S2",
        "S1ANDS2",
    }:
        return "S1_S2"

    if compact in {
        "3&4",
        "3AND4",
        "34",
        "S3S4",
        "S3&S4",
        "S3ANDS4",
    }:
        return "S3_S4"

    return None


def normalize_location(
    value: object,
) -> str | None:
    text = clean_text(value).lower()

    if text in {"west", "w"}:
        return "west"

    if text in {"east", "e"}:
        return "east"

    return text or None


def infer_location_from_strip_group(
    strip_group: str | None,
) -> str | None:
    if strip_group == "S1_S2":
        return "west"

    if strip_group == "S3_S4":
        return "east"

    return None


def parse_excel_date(
    value: object,
) -> date | None:
    if value is None or pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.date()

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    parsed = pd.to_datetime(
        value,
        errors="coerce",
    )

    if pd.isna(parsed):
        return None

    return pd.Timestamp(parsed).date()


def parse_excel_time(
    value: object,
) -> time | None:
    if value is None or pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.time().replace(
            microsecond=0
        )

    if isinstance(value, datetime):
        return value.time().replace(
            microsecond=0
        )

    if isinstance(value, time):
        return value.replace(
            microsecond=0
        )

    if isinstance(value, timedelta):
        total_seconds = int(
            value.total_seconds()
        ) % 86_400

        return time(
            hour=total_seconds // 3_600,
            minute=(total_seconds % 3_600) // 60,
            second=total_seconds % 60,
        )

    if isinstance(value, (int, float, np.number)):
        numeric = float(value)

        if 0 <= numeric < 1:
            total_seconds = int(
                round(numeric * 86_400)
            ) % 86_400

            return time(
                hour=total_seconds // 3_600,
                minute=(total_seconds % 3_600) // 60,
                second=total_seconds % 60,
            )

    parsed = pd.to_datetime(
        str(value),
        errors="coerce",
    )

    if pd.isna(parsed):
        return None

    return pd.Timestamp(parsed).time().replace(
        microsecond=0
    )


def combine_excel_date_and_time(
    date_value: object,
    time_value: object,
) -> pd.Timestamp:
    parsed_date = parse_excel_date(
        date_value
    )

    parsed_time = parse_excel_time(
        time_value
    )

    if (
        parsed_date is None
        or parsed_time is None
    ):
        return pd.NaT

    return pd.Timestamp(
        datetime.combine(
            parsed_date,
            parsed_time,
        )
    )


def correct_overnight_end_times(
    start: pd.Series,
    end: pd.Series,
) -> pd.Series:
    corrected = end.copy()

    overnight = (
        start.notna()
        & corrected.notna()
        & corrected.lt(start)
    )

    corrected.loc[overnight] = (
        corrected.loc[overnight]
        + pd.Timedelta(days=1)
    )

    return corrected


# ============================================================
# Photo readings
# ============================================================

def load_photo_readings(
    path: Path,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Meter-photo CSV does not exist: {path}"
        )

    df = pd.read_csv(path)
    df = normalize_columns(df)

    canonical_columns = {
        "filename",
        "sha256",
        "effective_datetime",
        "meter_reading",
        "review_status",
    }

    if canonical_columns.issubset(df.columns):
        include = (
            df["include"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"true", "1", "yes", "y"})
            if "include" in df.columns
            else pd.Series(True, index=df.index)
        )
        review_status = (
            df["review_status"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
        )
        counter = parse_numeric_series(df["meter_reading"])
        valid_counter = (
            counter.notna()
            & counter.between(100_000, 999_999)
            & counter.mod(1).eq(0)
        )

        out = pd.DataFrame(
            {
                "photo_id": df["sha256"].map(clean_text),
                "photo_datetime": parse_local_wall_datetime_series(
                    df["effective_datetime"]
                ),
                "photo_counter_value": counter,
                "photo_qc_status": "clean",
                "photo_filename": df["filename"].map(clean_text),
                "photo_notes": (
                    df["notes"].map(clean_text)
                    if "notes" in df.columns
                    else ""
                ),
            }
        )
        out = out.loc[
            include
            & review_status.eq("readable")
            & valid_counter
        ].copy()
    else:
        require_columns(
            df,
            (
                "photo_id",
                "photo_datetime",
                "meter_counter_value",
                "qc_status",
            ),
            source_name="Meter-photo CSV",
        )

        filename_column = (
            "preferred_filename"
            if "preferred_filename" in df.columns
            else (
                "filename"
                if "filename" in df.columns
                else None
            )
        )

        out = pd.DataFrame(
            {
                "photo_id": df["photo_id"].map(clean_text),
                "photo_datetime": parse_datetime_series(
                    df["photo_datetime"]
                ),
                "photo_counter_value": parse_numeric_series(
                    df["meter_counter_value"]
                ),
                "photo_qc_status": df["qc_status"].map(clean_text),
                "photo_filename": (
                    df[filename_column].map(clean_text)
                    if filename_column is not None
                    else ""
                ),
                "photo_notes": (
                    df["notes"].map(clean_text)
                    if "notes" in df.columns
                    else ""
                ),
            }
        )

    out = out.loc[
        out["photo_qc_status"]
        .str.lower()
        .eq("clean")
    ].copy()

    out = out.dropna(
        subset=[
            "photo_datetime",
            "photo_counter_value",
        ]
    )

    out["photo_counter_value"] = (
        out["photo_counter_value"]
        .round()
        .astype("int64")
    )

    out = (
        out.sort_values(
            [
                "photo_datetime",
                "photo_id",
            ],
            kind="stable",
        )
        .drop_duplicates(
            subset=["photo_id"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    if out.empty:
        raise ValueError(
            "No clean, timestamped meter-photo readings were available."
        )

    return out


# ============================================================
# Master-workbook irrigation events
# ============================================================

def find_irrigation_sheets(
    workbook_path: Path,
) -> list[tuple[int, str]]:
    if not workbook_path.exists():
        raise FileNotFoundError(
            f"Master workbook does not exist: {workbook_path}"
        )

    xls = pd.ExcelFile(
        workbook_path,
        engine="openpyxl",
    )

    found: list[tuple[int, str]] = []

    for sheet_name in xls.sheet_names:
        match = IRRIGATION_SHEET_PATTERN.match(
            str(sheet_name)
        )

        if match is None:
            continue

        found.append(
            (
                int(match.group(1)),
                str(sheet_name),
            )
        )

    if not found:
        raise ValueError(
            f"No year-specific IRRIGATION worksheets were found in "
            f"{workbook_path}"
        )

    return sorted(
        found,
        key=lambda item: item[0],
    )


def load_master_irrigation_sheet(
    workbook_path: Path,
    sheet_name: str,
    year: int,
) -> pd.DataFrame:
    raw = pd.read_excel(
        workbook_path,
        sheet_name=sheet_name,
        engine="openpyxl",
    )

    raw = normalize_columns(raw)
    columns = raw.columns.tolist()

    date_col = find_required_column(
        columns,
        ("date",),
        "DATE",
        sheet_name,
    )

    strip_col = find_required_column(
        columns,
        (
            "strip_i_d",
            "strip_id",
        ),
        "STRIP I.D.",
        sheet_name,
    )

    location_col = (
        "location"
        if "location" in columns
        else None
    )

    time_on_col = find_required_column(
        columns,
        ("time_on",),
        "TIME ON",
        sheet_name,
    )

    time_off_col = find_required_column(
        columns,
        ("time_off",),
        "TIME OFF",
        sheet_name,
    )

    start_totalizer_col = find_required_column(
        columns,
        (
            "meter_gal_start_gal_x_100",
            "meter_gal_start_gal_x100",
        ),
        "METER GAL. START",
        sheet_name,
    )

    end_totalizer_col = find_required_column(
        columns,
        (
            "meter_gal_end_gal_x_100",
            "meter_gal_end_gal_x100",
        ),
        "METER GAL. END",
        sheet_name,
    )

    gallons_col = find_required_column(
        columns,
        (
            "gal_used_x_100",
            "gal_used_x100",
        ),
        "GAL. USED",
        sheet_name,
    )

    notes_col = find_required_column(
        columns,
        ("notes",),
        "NOTES",
        sheet_name,
    )

    acre_ft_col = next(
        (
            candidate
            for candidate in (
                "acre_ft_used",
                "acre_feet_used",
            )
            if candidate in columns
        ),
        None,
    )

    gal_min_col = next(
        (
            candidate
            for candidate in (
                "gal_min",
                "gallons_min",
            )
            if candidate in columns
        ),
        None,
    )

    start_flow_col = next(
        (
            candidate
            for candidate in (
                "flow_rate_start",
            )
            if candidate in columns
        ),
        None,
    )

    end_flow_col = next(
        (
            candidate
            for candidate in (
                "flow_rate_end",
            )
            if candidate in columns
        ),
        None,
    )

    start_timestamp = pd.Series(
        [
            combine_excel_date_and_time(
                date_value,
                time_value,
            )
            for date_value, time_value in zip(
                raw[date_col],
                raw[time_on_col],
                strict=False,
            )
        ],
        index=raw.index,
        dtype="datetime64[ns]",
    )

    end_timestamp = pd.Series(
        [
            combine_excel_date_and_time(
                date_value,
                time_value,
            )
            for date_value, time_value in zip(
                raw[date_col],
                raw[time_off_col],
                strict=False,
            )
        ],
        index=raw.index,
        dtype="datetime64[ns]",
    )

    end_timestamp = correct_overnight_end_times(
        start=start_timestamp,
        end=end_timestamp,
    )

    strip_group = raw[
        strip_col
    ].map(normalize_strip_group)

    if location_col is not None:
        location = raw[
            location_col
        ].map(normalize_location)
    else:
        location = strip_group.map(
            infer_location_from_strip_group
        )

    missing_location = location.isna()

    location.loc[missing_location] = (
        strip_group.loc[missing_location]
        .map(infer_location_from_strip_group)
    )

    out = pd.DataFrame(
        {
            "year": year,

            "workbook_date": pd.to_datetime(
                raw[date_col],
                errors="coerce",
            ),

            "strip_group": strip_group,

            "location": location,

            "workbook_start_timestamp": (
                start_timestamp
            ),

            "workbook_end_timestamp": (
                end_timestamp
            ),

            "workbook_start_counter": (
                parse_numeric_series(
                    raw[start_totalizer_col]
                )
            ),

            "workbook_end_counter": (
                parse_numeric_series(
                    raw[end_totalizer_col]
                )
            ),

            "workbook_reported_gallons": (
                parse_numeric_series(
                    raw[gallons_col]
                )
            ),

            "workbook_reported_acre_ft": (
                parse_numeric_series(
                    raw[acre_ft_col]
                )
                if acre_ft_col is not None
                else np.nan
            ),

            "workbook_reported_gpm": (
                parse_numeric_series(
                    raw[gal_min_col]
                )
                if gal_min_col is not None
                else np.nan
            ),

            "workbook_start_flow_gpm": (
                parse_numeric_series(
                    raw[start_flow_col]
                )
                if start_flow_col is not None
                else np.nan
            ),

            "workbook_end_flow_gpm": (
                parse_numeric_series(
                    raw[end_flow_col]
                )
                if end_flow_col is not None
                else np.nan
            ),

            "workbook_notes": raw[
                notes_col
            ].map(clean_text),

            "source_sheet": sheet_name,

            "source_row": (
                raw.index + 2
            ),

            "strip_id_raw": raw[
                strip_col
            ].map(clean_text),

            "location_raw": (
                raw[location_col].map(clean_text)
                if location_col is not None
                else ""
            ),
        }
    )

    event_like = (
        out["workbook_date"].notna()
        | out["workbook_start_timestamp"].notna()
        | out["workbook_end_timestamp"].notna()
        | out["strip_id_raw"].ne("")
        | out["location_raw"].ne("")
    )

    out = out.loc[
        event_like
    ].copy()

    # Rows without an identifiable irrigation group are footer, summary,
    # or malformed records rather than comparable irrigation events.
    out = out.loc[
        out["strip_group"].notna()
    ].copy()

    # Rows intentionally showing no irrigation and no start/end time should
    # not be treated as actual irrigation events.
    intentional_non_event = (
        out["workbook_start_timestamp"].isna()
        & out["workbook_end_timestamp"].isna()
        & out["workbook_reported_gallons"]
        .fillna(0)
        .eq(0)
    )

    out = out.loc[
        ~intentional_non_event
    ].copy()

    out["workbook_event_duration_hours"] = (
        out["workbook_end_timestamp"]
        - out["workbook_start_timestamp"]
    ).dt.total_seconds() / 3600.0

    out["workbook_totalizer_derived_gallons"] = (
        out["workbook_end_counter"]
        - out["workbook_start_counter"]
    ) * METER_MULTIPLIER_GALLONS

    out["workbook_reported_minus_totalizer_gallons"] = (
        out["workbook_reported_gallons"]
        - out["workbook_totalizer_derived_gallons"]
    )

    out["workbook_calculated_acre_ft"] = (
        out["workbook_reported_gallons"]
        / GALLONS_PER_ACRE_FOOT
    )

    out["workbook_calculated_gpm"] = (
        out["workbook_reported_gallons"]
        / (
            out["workbook_event_duration_hours"]
            * 60.0
        )
    )

    invalid_duration = (
        out["workbook_event_duration_hours"].isna()
        | out["workbook_event_duration_hours"].le(0)
    )

    out.loc[
        invalid_duration,
        "workbook_calculated_gpm",
    ] = np.nan

    out["event_key"] = (
        out["workbook_start_timestamp"]
        .dt.strftime("%Y-%m-%d_%H%M%S")
        .fillna("missing_start")
        + "_"
        + out["strip_group"].fillna("missing_group")
        + "_row"
        + out["source_row"].astype(str)
    )

    return out.reset_index(drop=True)


def load_master_irrigation_events(
    workbook_path: Path,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for year, sheet_name in find_irrigation_sheets(
        workbook_path
    ):
        frames.append(
            load_master_irrigation_sheet(
                workbook_path=workbook_path,
                sheet_name=sheet_name,
                year=year,
            )
        )

    events = pd.concat(
        frames,
        ignore_index=True,
    )

    events = events.sort_values(
        [
            "workbook_start_timestamp",
            "strip_group",
            "source_sheet",
            "source_row",
        ],
        na_position="last",
        kind="stable",
    ).reset_index(drop=True)

    if events.empty:
        raise ValueError(
            "No irrigation-event records were loaded from the master workbook."
        )

    return events


# ============================================================
# Physical workbook boundary table
# ============================================================

def build_boundary_table(
    events: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for _, event in events.iterrows():
        common = {
            "event_key": clean_text(
                event["event_key"]
            ),
            "year": event["year"],
            "strip_group": clean_text(
                event["strip_group"]
            ),
            "location": clean_text(
                event["location"]
            ),
            "source_sheet": clean_text(
                event["source_sheet"]
            ),
            "source_row": int(
                event["source_row"]
            ),
            "workbook_reported_gallons": event[
                "workbook_reported_gallons"
            ],
            "workbook_notes": clean_text(
                event["workbook_notes"]
            ),
        }

        if pd.notna(
            event["workbook_start_timestamp"]
        ):
            rows.append(
                {
                    **common,
                    "boundary_type": "start",
                    "workbook_boundary_datetime": pd.Timestamp(
                        event[
                            "workbook_start_timestamp"
                        ]
                    ),
                    "workbook_counter_value": event[
                        "workbook_start_counter"
                    ],
                }
            )

        if pd.notna(
            event["workbook_end_timestamp"]
        ):
            rows.append(
                {
                    **common,
                    "boundary_type": "end",
                    "workbook_boundary_datetime": pd.Timestamp(
                        event[
                            "workbook_end_timestamp"
                        ]
                    ),
                    "workbook_counter_value": event[
                        "workbook_end_counter"
                    ],
                }
            )

    raw_boundaries = pd.DataFrame(
        rows
    )

    if raw_boundaries.empty:
        raise ValueError(
            "No usable workbook start or end timestamps were found."
        )

    # Events irrigated simultaneously can share one physical meter photograph.
    # Group records having the same type, timestamp, and counter.
    group_columns = [
        "boundary_type",
        "workbook_boundary_datetime",
        "workbook_counter_value",
    ]

    grouped_rows: list[dict[str, object]] = []

    for keys, group in raw_boundaries.groupby(
        group_columns,
        sort=True,
        dropna=False,
    ):
        (
            boundary_type,
            boundary_datetime,
            counter_value,
        ) = keys

        grouped_rows.append(
            {
                "physical_boundary_id": (
                    f"{pd.Timestamp(boundary_datetime):%Y%m%d_%H%M%S}_"
                    f"{boundary_type}_"
                    f"{clean_text(counter_value) or 'missing_counter'}"
                ),

                "boundary_type": boundary_type,

                "workbook_boundary_datetime": pd.Timestamp(
                    boundary_datetime
                ),

                "workbook_counter_value": (
                    float(counter_value)
                    if pd.notna(counter_value)
                    else np.nan
                ),

                "event_keys": join_unique_text(
                    group["event_key"]
                ),

                "years": join_unique_text(
                    group["year"].astype("string")
                ),

                "strip_groups": join_unique_text(
                    group["strip_group"]
                ),

                "locations": join_unique_text(
                    group["location"]
                ),

                "source_sheets": join_unique_text(
                    group["source_sheet"]
                ),

                "source_rows": " | ".join(
                    str(int(value))
                    for value in sorted(
                        pd.to_numeric(
                            group["source_row"],
                            errors="coerce",
                        )
                        .dropna()
                        .unique()
                    )
                ),

                "boundary_event_count": int(
                    group["event_key"].nunique()
                ),

                "workbook_reported_gallons_values": (
                    " | ".join(
                        f"{value:g}"
                        for value in unique_numeric_values(
                            group[
                                "workbook_reported_gallons"
                            ]
                        )
                    )
                ),

                "workbook_notes": join_unique_text(
                    group["workbook_notes"]
                ),
            }
        )

    return (
        pd.DataFrame(grouped_rows)
        .sort_values(
            [
                "workbook_boundary_datetime",
                "boundary_type",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


# ============================================================
# Camera-first boundary matching
# ============================================================

def candidate_photos_for_boundary(
    boundary: pd.Series,
    photos: pd.DataFrame,
    max_match_hours: float,
) -> pd.DataFrame:
    workbook_datetime = pd.Timestamp(
        boundary[
            "workbook_boundary_datetime"
        ]
    )

    candidates = photos.copy()

    candidates["photo_minus_workbook_minutes"] = (
        candidates["photo_datetime"]
        - workbook_datetime
    ).dt.total_seconds() / 60.0

    candidates["absolute_time_difference_minutes"] = (
        candidates[
            "photo_minus_workbook_minutes"
        ].abs()
    )

    candidates = candidates.loc[
        candidates[
            "absolute_time_difference_minutes"
        ].le(
            max_match_hours * 60.0
        )
    ].copy()

    workbook_counter = nullable_float(
        boundary["workbook_counter_value"]
    )

    if workbook_counter is None:
        candidates[
            "photo_minus_workbook_counter_units"
        ] = np.nan

        candidates[
            "absolute_counter_difference_units"
        ] = np.nan
    else:
        candidates[
            "photo_minus_workbook_counter_units"
        ] = (
            candidates["photo_counter_value"]
            - workbook_counter
        )

        candidates[
            "absolute_counter_difference_units"
        ] = candidates[
            "photo_minus_workbook_counter_units"
        ].abs()

    return candidates


def select_best_photo(
    boundary: pd.Series,
    photos: pd.DataFrame,
    max_match_hours: float,
    time_tolerance_minutes: float,
    counter_tolerance_units: int,
) -> pd.Series | None:
    candidates = candidate_photos_for_boundary(
        boundary=boundary,
        photos=photos,
        max_match_hours=max_match_hours,
    )

    if candidates.empty:
        return None

    # Matching uses both independent camera observations:
    # timestamp and counter value.
    #
    # A one-tolerance timing difference and a one-tolerance counter
    # difference contribute equally to the score.
    time_scale = max(
        float(time_tolerance_minutes),
        1.0,
    )

    counter_scale = max(
        float(counter_tolerance_units),
        1.0,
    )

    candidates["time_score"] = (
        candidates[
            "absolute_time_difference_minutes"
        ]
        / time_scale
    )

    if candidates[
        "absolute_counter_difference_units"
    ].notna().any():
        candidates["counter_score"] = (
            candidates[
                "absolute_counter_difference_units"
            ]
            .fillna(counter_scale * 10.0)
            / counter_scale
        )
    else:
        candidates["counter_score"] = 0.0

    candidates["combined_match_score"] = (
        candidates["time_score"]
        + candidates["counter_score"]
    )

    candidates = candidates.sort_values(
        [
            "combined_match_score",
            "absolute_time_difference_minutes",
            "absolute_counter_difference_units",
            "photo_datetime",
            "photo_id",
        ],
        na_position="last",
        kind="stable",
    )

    return candidates.iloc[0]


def classify_boundary_comparison(
    photo_found: bool,
    within_time_tolerance: bool,
    workbook_counter_present: bool,
    within_counter_tolerance: bool,
) -> tuple[str, str]:
    if not photo_found:
        return (
            "no_camera_match",
            "no clean meter photograph was found within the matching window",
        )

    if not workbook_counter_present:
        if within_time_tolerance:
            return (
                "workbook_counter_missing",
                (
                    "camera timestamp agrees with workbook time, but the "
                    "workbook totalizer value is missing"
                ),
            )

        return (
            "workbook_time_disagrees_counter_missing",
            (
                "camera timestamp differs from workbook time, and the "
                "workbook totalizer value is missing"
            ),
        )

    if (
        within_time_tolerance
        and within_counter_tolerance
    ):
        return (
            "camera_workbook_agreement",
            "",
        )

    if (
        not within_time_tolerance
        and within_counter_tolerance
    ):
        return (
            "workbook_time_disagrees",
            (
                "camera counter agrees with workbook totalizer, but the "
                "camera timestamp differs from the workbook time"
            ),
        )

    if (
        within_time_tolerance
        and not within_counter_tolerance
    ):
        return (
            "workbook_counter_disagrees",
            (
                "camera timestamp agrees with workbook time, but the "
                "camera counter differs from the workbook totalizer"
            ),
        )

    return (
        "workbook_time_and_counter_disagree",
        (
            "both the camera timestamp and camera counter differ from the "
            "workbook boundary values"
        ),
    )


def compare_boundary_to_camera(
    boundary: pd.Series,
    photos: pd.DataFrame,
    max_match_hours: float,
    time_tolerance_minutes: float,
    counter_tolerance_units: int,
) -> dict[str, object]:
    photo = select_best_photo(
        boundary=boundary,
        photos=photos,
        max_match_hours=max_match_hours,
        time_tolerance_minutes=(
            time_tolerance_minutes
        ),
        counter_tolerance_units=(
            counter_tolerance_units
        ),
    )

    base = {
        "physical_boundary_id": clean_text(
            boundary["physical_boundary_id"]
        ),
        "boundary_type": clean_text(
            boundary["boundary_type"]
        ),
        "workbook_boundary_datetime": pd.Timestamp(
            boundary[
                "workbook_boundary_datetime"
            ]
        ),
        "workbook_counter_value": boundary[
            "workbook_counter_value"
        ],
        "event_keys": clean_text(
            boundary["event_keys"]
        ),
        "years": clean_text(
            boundary["years"]
        ),
        "strip_groups": clean_text(
            boundary["strip_groups"]
        ),
        "locations": clean_text(
            boundary["locations"]
        ),
        "source_sheets": clean_text(
            boundary["source_sheets"]
        ),
        "source_rows": clean_text(
            boundary["source_rows"]
        ),
        "boundary_event_count": int(
            boundary["boundary_event_count"]
        ),
        "workbook_reported_gallons_values": clean_text(
            boundary[
                "workbook_reported_gallons_values"
            ]
        ),
        "workbook_notes": clean_text(
            boundary["workbook_notes"]
        ),
    }

    if photo is None:
        status, reason = classify_boundary_comparison(
            photo_found=False,
            within_time_tolerance=False,
            workbook_counter_present=pd.notna(
                boundary["workbook_counter_value"]
            ),
            within_counter_tolerance=False,
        )

        return {
            **base,
            "photo_id": "",
            "photo_filename": "",
            "photo_datetime": pd.NaT,
            "photo_counter_value": np.nan,
            "photo_notes": "",
            "photo_minus_workbook_minutes": np.nan,
            "absolute_time_difference_minutes": np.nan,
            "photo_minus_workbook_counter_units": np.nan,
            "photo_minus_workbook_gallons": np.nan,
            "absolute_counter_difference_units": np.nan,
            "absolute_counter_difference_gallons": np.nan,
            "within_time_tolerance": False,
            "within_counter_tolerance": False,
            "combined_match_score": np.nan,
            "boundary_qc_status": status,
            "boundary_review_required": True,
            "boundary_review_reason": reason,
        }

    photo_datetime = pd.Timestamp(
        photo["photo_datetime"]
    )

    photo_counter = int(
        photo["photo_counter_value"]
    )

    workbook_datetime = pd.Timestamp(
        boundary[
            "workbook_boundary_datetime"
        ]
    )

    workbook_counter = nullable_float(
        boundary["workbook_counter_value"]
    )

    photo_minus_workbook_minutes = (
        photo_datetime
        - workbook_datetime
    ).total_seconds() / 60.0

    absolute_time_difference_minutes = abs(
        photo_minus_workbook_minutes
    )

    within_time_tolerance = (
        absolute_time_difference_minutes
        <= time_tolerance_minutes
    )

    if workbook_counter is None:
        counter_difference_units = np.nan
        absolute_counter_difference_units = np.nan
        counter_difference_gallons = np.nan
        absolute_counter_difference_gallons = np.nan
        within_counter_tolerance = False
    else:
        counter_difference_units = (
            photo_counter
            - workbook_counter
        )

        absolute_counter_difference_units = abs(
            counter_difference_units
        )

        counter_difference_gallons = (
            counter_difference_units
            * METER_MULTIPLIER_GALLONS
        )

        absolute_counter_difference_gallons = abs(
            counter_difference_gallons
        )

        within_counter_tolerance = (
            absolute_counter_difference_units
            <= counter_tolerance_units
        )

    status, reason = classify_boundary_comparison(
        photo_found=True,
        within_time_tolerance=(
            within_time_tolerance
        ),
        workbook_counter_present=(
            workbook_counter is not None
        ),
        within_counter_tolerance=(
            within_counter_tolerance
        ),
    )

    return {
        **base,

        "photo_id": clean_text(
            photo["photo_id"]
        ),

        "photo_filename": clean_text(
            photo["photo_filename"]
        ),

        "photo_datetime": photo_datetime,

        "photo_counter_value": photo_counter,

        "photo_notes": clean_text(
            photo["photo_notes"]
        ),

        # Positive means the photo was taken after the workbook boundary.
        "photo_minus_workbook_minutes": (
            photo_minus_workbook_minutes
        ),

        "absolute_time_difference_minutes": (
            absolute_time_difference_minutes
        ),

        # Positive means the photo counter exceeds the workbook counter.
        "photo_minus_workbook_counter_units": (
            counter_difference_units
        ),

        "photo_minus_workbook_gallons": (
            counter_difference_gallons
        ),

        "absolute_counter_difference_units": (
            absolute_counter_difference_units
        ),

        "absolute_counter_difference_gallons": (
            absolute_counter_difference_gallons
        ),

        "within_time_tolerance": bool(
            within_time_tolerance
        ),

        "within_counter_tolerance": bool(
            within_counter_tolerance
        ),

        "combined_match_score": float(
            photo["combined_match_score"]
        ),

        "boundary_qc_status": status,

        "boundary_review_required": (
            status != "camera_workbook_agreement"
        ),

        "boundary_review_reason": reason,
    }


def build_boundary_comparison(
    boundaries: pd.DataFrame,
    photos: pd.DataFrame,
    max_match_hours: float,
    time_tolerance_minutes: float,
    counter_tolerance_units: int,
) -> pd.DataFrame:
    comparison = pd.DataFrame(
        [
            compare_boundary_to_camera(
                boundary=boundary,
                photos=photos,
                max_match_hours=max_match_hours,
                time_tolerance_minutes=(
                    time_tolerance_minutes
                ),
                counter_tolerance_units=(
                    counter_tolerance_units
                ),
            )
            for _, boundary in boundaries.iterrows()
        ]
    )

    return comparison.sort_values(
        [
            "workbook_boundary_datetime",
            "boundary_type",
            "physical_boundary_id",
        ],
        kind="stable",
    ).reset_index(drop=True)


# ============================================================
# Event-level QC
# ============================================================

def boundary_record_for_event(
    event_key: str,
    boundary_type: str,
    boundary_qc: pd.DataFrame,
) -> pd.Series | None:
    event_pattern = re.compile(
        rf"(?:^|\s\|\s){re.escape(event_key)}(?:$|\s\|\s)"
    )

    mask = (
        boundary_qc["boundary_type"]
        .eq(boundary_type)
        & boundary_qc["event_keys"]
        .fillna("")
        .str.contains(
            event_pattern,
            regex=True,
        )
    )

    matches = boundary_qc.loc[
        mask
    ]

    if matches.empty:
        return None

    return matches.iloc[0]


def prefixed_boundary_fields(
    prefix: str,
    row: pd.Series | None,
) -> dict[str, object]:
    if row is None:
        return {
            f"{prefix}_photo_id": "",
            f"{prefix}_photo_filename": "",
            f"{prefix}_photo_datetime": pd.NaT,
            f"{prefix}_photo_counter": np.nan,
            f"{prefix}_photo_notes": "",
            f"{prefix}_photo_minus_workbook_minutes": np.nan,
            f"{prefix}_absolute_time_difference_minutes": np.nan,
            f"{prefix}_photo_minus_workbook_counter_units": np.nan,
            f"{prefix}_photo_minus_workbook_gallons": np.nan,
            f"{prefix}_absolute_counter_difference_units": np.nan,
            f"{prefix}_absolute_counter_difference_gallons": np.nan,
            f"{prefix}_within_time_tolerance": False,
            f"{prefix}_within_counter_tolerance": False,
            f"{prefix}_qc_status": "missing_boundary_comparison",
            f"{prefix}_review_reason": (
                "no physical boundary comparison was available"
            ),
        }

    return {
        f"{prefix}_photo_id": clean_text(
            row["photo_id"]
        ),

        f"{prefix}_photo_filename": clean_text(
            row["photo_filename"]
        ),

        f"{prefix}_photo_datetime": row[
            "photo_datetime"
        ],

        f"{prefix}_photo_counter": row[
            "photo_counter_value"
        ],

        f"{prefix}_photo_notes": clean_text(
            row["photo_notes"]
        ),

        f"{prefix}_photo_minus_workbook_minutes": row[
            "photo_minus_workbook_minutes"
        ],

        f"{prefix}_absolute_time_difference_minutes": row[
            "absolute_time_difference_minutes"
        ],

        f"{prefix}_photo_minus_workbook_counter_units": row[
            "photo_minus_workbook_counter_units"
        ],

        f"{prefix}_photo_minus_workbook_gallons": row[
            "photo_minus_workbook_gallons"
        ],

        f"{prefix}_absolute_counter_difference_units": row[
            "absolute_counter_difference_units"
        ],

        f"{prefix}_absolute_counter_difference_gallons": row[
            "absolute_counter_difference_gallons"
        ],

        f"{prefix}_within_time_tolerance": bool(
            row["within_time_tolerance"]
        ),

        f"{prefix}_within_counter_tolerance": bool(
            row["within_counter_tolerance"]
        ),

        f"{prefix}_qc_status": clean_text(
            row["boundary_qc_status"]
        ),

        f"{prefix}_review_reason": clean_text(
            row["boundary_review_reason"]
        ),
    }


def classify_event_qc(
    start_status: str,
    end_status: str,
    volume_difference: float | None,
    volume_tolerance_gallons: float,
) -> tuple[str, str]:
    reasons: list[str] = []

    start_agrees = (
        start_status
        == "camera_workbook_agreement"
    )

    end_agrees = (
        end_status
        == "camera_workbook_agreement"
    )

    volume_available = (
        volume_difference is not None
    )

    volume_agrees = (
        volume_available
        and abs(volume_difference)
        <= volume_tolerance_gallons
    )

    if not start_agrees:
        reasons.append(
            f"start boundary: {start_status}"
        )

    if not end_agrees:
        reasons.append(
            f"end boundary: {end_status}"
        )

    if not volume_available:
        reasons.append(
            "photo-derived event volume is unavailable"
        )
    elif not volume_agrees:
        reasons.append(
            "photo-derived and workbook reported gallons differ beyond tolerance"
        )

    if (
        start_agrees
        and end_agrees
        and volume_agrees
    ):
        return (
            "camera_workbook_agreement",
            "",
        )

    if (
        start_agrees
        and end_agrees
        and not volume_agrees
    ):
        return (
            "workbook_volume_disagrees",
            "; ".join(reasons),
        )

    if (
        not start_agrees
        and end_agrees
    ):
        return (
            "start_boundary_review",
            "; ".join(reasons),
        )

    if (
        start_agrees
        and not end_agrees
    ):
        return (
            "end_boundary_review",
            "; ".join(reasons),
        )

    return (
        "multiple_workbook_fields_review",
        "; ".join(reasons),
    )


def build_event_qc(
    events: pd.DataFrame,
    boundary_qc: pd.DataFrame,
    volume_tolerance_gallons: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for _, event in events.iterrows():
        event_key = clean_text(
            event["event_key"]
        )

        start_boundary = boundary_record_for_event(
            event_key=event_key,
            boundary_type="start",
            boundary_qc=boundary_qc,
        )

        end_boundary = boundary_record_for_event(
            event_key=event_key,
            boundary_type="end",
            boundary_qc=boundary_qc,
        )

        start_fields = prefixed_boundary_fields(
            prefix="start",
            row=start_boundary,
        )

        end_fields = prefixed_boundary_fields(
            prefix="end",
            row=end_boundary,
        )

        start_photo_counter = nullable_float(
            start_fields[
                "start_photo_counter"
            ]
        )

        end_photo_counter = nullable_float(
            end_fields[
                "end_photo_counter"
            ]
        )

        if (
            start_photo_counter is not None
            and end_photo_counter is not None
        ):
            photo_derived_gallons = (
                end_photo_counter
                - start_photo_counter
            ) * METER_MULTIPLIER_GALLONS
        else:
            photo_derived_gallons = None

        workbook_reported_gallons = nullable_float(
            event[
                "workbook_reported_gallons"
            ]
        )

        workbook_totalizer_gallons = nullable_float(
            event[
                "workbook_totalizer_derived_gallons"
            ]
        )

        if (
            photo_derived_gallons is not None
            and workbook_reported_gallons is not None
        ):
            photo_minus_reported_gallons = (
                photo_derived_gallons
                - workbook_reported_gallons
            )
        else:
            photo_minus_reported_gallons = None

        if (
            photo_derived_gallons is not None
            and workbook_totalizer_gallons is not None
        ):
            photo_minus_workbook_totalizer_gallons = (
                photo_derived_gallons
                - workbook_totalizer_gallons
            )
        else:
            photo_minus_workbook_totalizer_gallons = None

        event_status, event_reason = (
            classify_event_qc(
                start_status=clean_text(
                    start_fields[
                        "start_qc_status"
                    ]
                ),
                end_status=clean_text(
                    end_fields[
                        "end_qc_status"
                    ]
                ),
                volume_difference=(
                    photo_minus_reported_gallons
                ),
                volume_tolerance_gallons=(
                    volume_tolerance_gallons
                ),
            )
        )

        detailed_reasons = [
            clean_text(
                start_fields[
                    "start_review_reason"
                ]
            ),
            clean_text(
                end_fields[
                    "end_review_reason"
                ]
            ),
            event_reason,
        ]

        rows.append(
            {
                "event_key": event_key,
                "year": event["year"],
                "strip_group": event[
                    "strip_group"
                ],
                "location": event[
                    "location"
                ],
                "source_sheet": event[
                    "source_sheet"
                ],
                "source_row": event[
                    "source_row"
                ],

                "workbook_start_timestamp": event[
                    "workbook_start_timestamp"
                ],
                "workbook_end_timestamp": event[
                    "workbook_end_timestamp"
                ],
                "workbook_event_duration_hours": event[
                    "workbook_event_duration_hours"
                ],

                "workbook_start_counter": event[
                    "workbook_start_counter"
                ],
                "workbook_end_counter": event[
                    "workbook_end_counter"
                ],

                "workbook_reported_gallons": event[
                    "workbook_reported_gallons"
                ],

                "workbook_totalizer_derived_gallons": event[
                    "workbook_totalizer_derived_gallons"
                ],

                "workbook_reported_minus_totalizer_gallons": event[
                    "workbook_reported_minus_totalizer_gallons"
                ],

                "workbook_reported_acre_ft": event[
                    "workbook_reported_acre_ft"
                ],

                "workbook_calculated_acre_ft": event[
                    "workbook_calculated_acre_ft"
                ],

                "workbook_reported_gpm": event[
                    "workbook_reported_gpm"
                ],

                "workbook_calculated_gpm": event[
                    "workbook_calculated_gpm"
                ],

                "workbook_start_flow_gpm": event[
                    "workbook_start_flow_gpm"
                ],

                "workbook_end_flow_gpm": event[
                    "workbook_end_flow_gpm"
                ],

                **start_fields,
                **end_fields,

                "photo_derived_gallons": (
                    photo_derived_gallons
                ),

                "photo_derived_minus_workbook_reported_gallons": (
                    photo_minus_reported_gallons
                ),

                "photo_derived_minus_workbook_totalizer_gallons": (
                    photo_minus_workbook_totalizer_gallons
                ),

                "volume_within_tolerance": (
                    photo_minus_reported_gallons
                    is not None
                    and abs(
                        photo_minus_reported_gallons
                    )
                    <= volume_tolerance_gallons
                ),

                "event_qc_status": event_status,

                "event_review_required": (
                    event_status
                    != "camera_workbook_agreement"
                ),

                "event_review_reason": "; ".join(
                    dict.fromkeys(
                        reason
                        for reason in detailed_reasons
                        if reason
                    )
                ),

                "workbook_notes": clean_text(
                    event["workbook_notes"]
                ),
            }
        )

    return (
        pd.DataFrame(rows)
        .sort_values(
            [
                "workbook_start_timestamp",
                "strip_group",
                "source_row",
            ],
            na_position="last",
            kind="stable",
        )
        .reset_index(drop=True)
    )


# ============================================================
# Unmatched photographs
# ============================================================

def build_unmatched_photo_table(
    photos: pd.DataFrame,
    boundary_qc: pd.DataFrame,
) -> pd.DataFrame:
    used_photo_ids = {
        clean_text(value)
        for value in boundary_qc[
            "photo_id"
        ]
        if clean_text(value)
    }

    unmatched = photos.loc[
        ~photos["photo_id"].isin(
            used_photo_ids
        )
    ].copy()

    return unmatched.sort_values(
        [
            "photo_datetime",
            "photo_id",
        ],
        kind="stable",
    ).reset_index(drop=True)


# ============================================================
# Output formatting and console audit
# ============================================================

def round_numeric_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    out = df.copy()

    three_decimal_columns = [
        column
        for column in out.columns
        if (
            column.endswith("_minutes")
            or column.endswith("_hours")
            or column.endswith("_gpm")
            or column == "combined_match_score"
        )
    ]

    one_decimal_columns = [
        column
        for column in out.columns
        if column.endswith("_gallons")
    ]

    for column in three_decimal_columns:
        out[column] = pd.to_numeric(
            out[column],
            errors="coerce",
        ).round(3)

    for column in one_decimal_columns:
        out[column] = pd.to_numeric(
            out[column],
            errors="coerce",
        ).round(1)

    return out


def write_csv(
    df: pd.DataFrame,
    path: Path,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_csv(
        path,
        index=False,
        date_format="%Y-%m-%d %H:%M:%S",
    )


def print_console_summary(
    photos: pd.DataFrame,
    events: pd.DataFrame,
    boundaries: pd.DataFrame,
    boundary_qc: pd.DataFrame,
    event_qc: pd.DataFrame,
    unmatched_photos: pd.DataFrame,
) -> None:
    print(
        "\n--- Camera-first irrigation QC ---"
    )

    print(
        f"Clean timestamped photographs: "
        f"{len(photos):,}"
    )

    print(
        f"Workbook irrigation events: "
        f"{len(events):,}"
    )

    print(
        f"Physical workbook boundaries: "
        f"{len(boundaries):,}"
    )

    print(
        f"Unmatched clean photographs: "
        f"{len(unmatched_photos):,}"
    )

    print(
        "\nBoundary QC status counts:"
    )

    print(
        boundary_qc[
            "boundary_qc_status"
        ]
        .value_counts(
            dropna=False
        )
        .to_string()
    )

    print(
        "\nEvent QC status counts:"
    )

    print(
        event_qc[
            "event_qc_status"
        ]
        .value_counts(
            dropna=False
        )
        .to_string()
    )

    timing_values = (
        boundary_qc[
            "absolute_time_difference_minutes"
        ]
        .dropna()
    )

    if not timing_values.empty:
        print(
            "\nAbsolute camera-versus-workbook "
            "time differences, minutes:"
        )

        print(
            timing_values
            .describe()
            .to_string()
        )

    counter_values = (
        boundary_qc[
            "absolute_counter_difference_units"
        ]
        .dropna()
    )

    if not counter_values.empty:
        print(
            "\nAbsolute camera-versus-workbook "
            "counter differences:"
        )

        print(
            counter_values
            .describe()
            .to_string()
        )

    review = event_qc.loc[
        event_qc[
            "event_review_required"
        ]
        .fillna(True)
    ].copy()

    print(
        f"\nEvents requiring review: "
        f"{len(review):,}"
    )

    if not review.empty:
        review_columns = [
            "year",
            "strip_group",
            "workbook_start_timestamp",
            "workbook_end_timestamp",
            "start_photo_datetime",
            "start_photo_minus_workbook_minutes",
            "start_photo_minus_workbook_counter_units",
            "end_photo_datetime",
            "end_photo_minus_workbook_minutes",
            "end_photo_minus_workbook_counter_units",
            "workbook_reported_gallons",
            "photo_derived_gallons",
            "photo_derived_minus_workbook_reported_gallons",
            "event_qc_status",
            "event_review_reason",
        ]

        print()
        print(
            review[
                review_columns
            ]
            .head(100)
            .to_string(
                index=False
            )
        )


# ============================================================
# Main
# ============================================================

def main() -> None:
    args = parse_args()

    photos = load_photo_readings(
        args.photo_csv.resolve()
    )

    events = load_master_irrigation_events(
        args.workbook.resolve()
    )

    boundaries = build_boundary_table(
        events
    )

    boundary_qc = build_boundary_comparison(
        boundaries=boundaries,
        photos=photos,
        max_match_hours=(
            args.max_match_hours
        ),
        time_tolerance_minutes=(
            args.time_tolerance_minutes
        ),
        counter_tolerance_units=(
            args.counter_tolerance_units
        ),
    )

    event_qc = build_event_qc(
        events=events,
        boundary_qc=boundary_qc,
        volume_tolerance_gallons=(
            args.volume_tolerance_gallons
        ),
    )

    unmatched_photos = build_unmatched_photo_table(
        photos=photos,
        boundary_qc=boundary_qc,
    )

    boundary_qc = round_numeric_columns(
        boundary_qc
    )

    event_qc = round_numeric_columns(
        event_qc
    )

    write_csv(
        event_qc,
        args.output_csv.resolve(),
    )

    write_csv(
        boundary_qc,
        args.boundary_output_csv.resolve(),
    )

    write_csv(
        unmatched_photos,
        args.unmatched_photo_csv.resolve(),
    )

    print(
        f"Wrote event QC: "
        f"{args.output_csv.resolve()}"
    )

    print(
        f"Wrote boundary QC: "
        f"{args.boundary_output_csv.resolve()}"
    )

    print(
        f"Wrote unmatched photos: "
        f"{args.unmatched_photo_csv.resolve()}"
    )

    print_console_summary(
        photos=photos,
        events=events,
        boundaries=boundaries,
        boundary_qc=boundary_qc,
        event_qc=event_qc,
        unmatched_photos=unmatched_photos,
    )


if __name__ == "__main__":
    main()
