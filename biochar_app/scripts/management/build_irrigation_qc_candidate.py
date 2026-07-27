#!/usr/bin/env python3
"""
build_irrigation_qc_candidate.py

Build a camera-QC version of the canonical irrigation dataset without changing
the live irrigation_clean.csv file.

The input is the master-workbook-derived candidate:

    irrigation_clean_candidate.csv

The principal output is:

    irrigation_clean_qc_candidate.csv

The script applies only corrections supported by independently reviewed meter
photographs and their EXIF timestamps.

Current corrections
-------------------
1. 2025-05-03, S3_S4
   Both start and end photographs are one calendar day earlier than the
   workbook values. The photo counters agree with the workbook totalizers.

2. 2025-05-17, S1_S2
   Both start and end photographs are approximately one calendar day earlier
   than the workbook values. The photo counters agree with the workbook
   totalizers.

3. 2023-06-01, S1_S2
   The start photograph is approximately one hour earlier than the workbook
   start time. The photo counter agrees with the workbook start totalizer.
   The workbook end time is retained.

Shared-meter classifications
----------------------------
The following irrigation periods supplied water to both strip groups
simultaneously:

- 2025-05-28
- 2026-06-20
- 2026-07-24

The meter photographed the combined volume supplied to all four strips, while
the workbook divided that volume between S1_S2 and S3_S4. These records are
tagged as shared-meter events, but their group and strip volumes are not
changed.

Pending event
-------------
The 2026-06-27 event is not added to the principal QC candidate because its
volume remains unresolved. Its camera-supported timestamps are written to a
separate pending-event report.

This script never overwrites irrigation_clean.csv.
"""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pandas as pd

from biochar_app.config.paths import IRRIGATION_DIR


DEFAULT_INPUT_CSV: Final[Path] = (
    IRRIGATION_DIR
    / "irrigation_clean_candidate.csv"
)

DEFAULT_OUTPUT_CSV: Final[Path] = (
    IRRIGATION_DIR
    / "irrigation_clean_qc_candidate.csv"
)

DEFAULT_CORRECTION_AUDIT_CSV: Final[Path] = (
    IRRIGATION_DIR
    / "irrigation_clean_qc_candidate_corrections.csv"
)

DEFAULT_PENDING_INPUT_CSV: Final[Path] = (
    IRRIGATION_DIR
    / "irrigation_master_invalid_rows.csv"
)

DEFAULT_PENDING_OUTPUT_CSV: Final[Path] = (
    IRRIGATION_DIR
    / "irrigation_qc_pending_events.csv"
)


@dataclass(frozen=True)
class TimestampCorrection:
    source_date: str
    strip_group: str
    corrected_start_timestamp: str | None
    corrected_end_timestamp: str | None
    correction_code: str
    correction_reason: str


IRRIGATION_QC_CORRECTIONS: Final[tuple[TimestampCorrection, ...]] = (
    TimestampCorrection(
        source_date="2025-05-03",
        strip_group="S3_S4",
        corrected_start_timestamp="2025-05-02 19:10:02",
        corrected_end_timestamp="2025-05-03 08:21:02",
        correction_code="camera_date_preferred",
        correction_reason=(
            "Start and end photo counters agree with the workbook totalizers, "
            "but both EXIF timestamps are one calendar day earlier than the "
            "workbook timestamps."
        ),
    ),
    TimestampCorrection(
        source_date="2025-05-17",
        strip_group="S1_S2",
        corrected_start_timestamp="2025-05-16 15:36:41",
        corrected_end_timestamp="2025-05-17 07:37:42",
        correction_code="camera_date_preferred",
        correction_reason=(
            "Start and end photo counters agree with the workbook totalizers, "
            "but both EXIF timestamps are approximately one calendar day "
            "earlier than the workbook timestamps."
        ),
    ),
    TimestampCorrection(
        source_date="2023-06-01",
        strip_group="S1_S2",
        corrected_start_timestamp="2023-06-01 11:16:08",
        corrected_end_timestamp=None,
        correction_code="camera_start_time_preferred",
        correction_reason=(
            "The start photo counter agrees with the workbook start totalizer, "
            "but the EXIF timestamp is approximately one hour earlier than the "
            "workbook start time. The workbook end time is retained."
        ),
    ),
)


SHARED_METER_EVENTS: Final[tuple[tuple[str, str], ...]] = (
    ("2025-05-28", "S1_S2"),
    ("2025-05-28", "S3_S4"),
    ("2026-06-20", "S1_S2"),
    ("2026-06-20", "S3_S4"),
    ("2026-07-24", "S1_S2"),
    ("2026-07-24", "S3_S4"),
)


OUTPUT_COLUMN_ORDER: Final[list[str]] = [
    "year",
    "date",
    "start_timestamp",
    "end_timestamp",
    "strip_group",
    "location",
    "strip",
    "total_meter_gallons",
    "flow_allocation_fraction",
    "strip_allocation_fraction",
    "gallons_group",
    "gallons_strip",
    "avg_flow_gpm_group",
    "avg_flow_gpm_strip",
    "avg_flow_gph_strip",
    "event_duration_hours",
    "start_flow_gpm",
    "end_flow_gpm",
    "start_totalizer_gal_x100",
    "end_totalizer_gal_x100",
    "entered_by",
    "event_id",
    "notes",
    "original_event_id",
    "original_date",
    "original_start_timestamp",
    "original_end_timestamp",
    "timestamp_correction_applied",
    "correction_code",
    "correction_reason",
    "meter_volume_shared_between_groups",
    "meter_volume_allocation_method",
    "qc_candidate_source",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a camera-corrected irrigation candidate without changing "
            "the live irrigation_clean.csv file."
        )
    )

    parser.add_argument(
        "--input-csv",
        type=Path,
        default=DEFAULT_INPUT_CSV,
    )

    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
    )

    parser.add_argument(
        "--correction-audit-csv",
        type=Path,
        default=DEFAULT_CORRECTION_AUDIT_CSV,
    )

    parser.add_argument(
        "--pending-input-csv",
        type=Path,
        default=DEFAULT_PENDING_INPUT_CSV,
    )

    parser.add_argument(
        "--pending-output-csv",
        type=Path,
        default=DEFAULT_PENDING_OUTPUT_CSV,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    return parser.parse_args()


def clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip()

    if text.lower() in {
        "",
        "nan",
        "none",
        "<na>",
    }:
        return ""

    return text


def normalize_strip_group(value: object) -> str:
    text = clean_text(value).upper()
    compact = (
        text
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )

    if compact in {
        "S1S2",
        "1&2",
        "1AND2",
        "S1&S2",
    }:
        return "S1_S2"

    if compact in {
        "S3S4",
        "3&4",
        "3AND4",
        "S3&S4",
    }:
        return "S3_S4"

    return text


def parse_datetime_column(
    series: pd.Series,
) -> pd.Series:
    return pd.to_datetime(
        series.astype("string").str.strip(),
        format="mixed",
        errors="coerce",
    )


def require_columns(
    df: pd.DataFrame,
    required: list[str],
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


def make_event_id(
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    strip_group: str,
) -> str:
    """
    Create an event ID from the corrected physical event identity.

    Corrected dates intentionally produce corrected IDs. Historical event IDs
    are retained separately in original_event_id.
    """
    date_text = start_timestamp.strftime("%Y-%m-%d")
    start_text = start_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    end_text = end_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    key_text = "|".join(
        [
            date_text,
            start_text,
            end_text,
            strip_group,
        ]
    )

    suffix = hashlib.sha1(
        key_text.encode("utf-8")
    ).hexdigest()[:8]

    return (
        f"{date_text}_"
        f"{strip_group}_"
        f"{suffix}"
    )


def initialize_qc_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    out = df.copy()

    out["original_event_id"] = (
        out["event_id"]
        .fillna("")
        .astype("string")
    )

    out["original_date"] = (
        out["date"]
        .fillna("")
        .astype("string")
    )

    out["original_start_timestamp"] = (
        out["start_timestamp"]
    )

    out["original_end_timestamp"] = (
        out["end_timestamp"]
    )

    out["timestamp_correction_applied"] = False
    out["correction_code"] = ""
    out["correction_reason"] = ""

    out["meter_volume_shared_between_groups"] = False
    out["meter_volume_allocation_method"] = ""

    out["qc_candidate_source"] = (
        "master_workbook_candidate_plus_camera_qc"
    )

    return out


def match_correction_rows(
    df: pd.DataFrame,
    correction: TimestampCorrection,
) -> pd.Series:
    date_values = pd.to_datetime(
        df["date"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    return (
        date_values.eq(correction.source_date)
        & df["strip_group"].eq(
            correction.strip_group
        )
    )


def apply_timestamp_corrections(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = df.copy()
    audit_rows: list[dict[str, object]] = []

    for correction in IRRIGATION_QC_CORRECTIONS:
        mask = match_correction_rows(
            out,
            correction,
        )

        matched = out.loc[mask].copy()

        if matched.empty:
            raise ValueError(
                "No candidate rows matched timestamp correction:\n"
                f"date={correction.source_date}, "
                f"strip_group={correction.strip_group}"
            )

        expected_strips = {
            "S1",
            "S2",
        }

        if correction.strip_group == "S3_S4":
            expected_strips = {
                "S3",
                "S4",
            }

        matched_strips = set(
            matched["strip"]
            .dropna()
            .astype(str)
        )

        if matched_strips != expected_strips:
            raise ValueError(
                "Timestamp correction did not match the expected two strip "
                "rows.\n"
                f"date={correction.source_date}, "
                f"strip_group={correction.strip_group}\n"
                f"expected strips={sorted(expected_strips)}\n"
                f"matched strips={sorted(matched_strips)}"
            )

        original_start_values = (
            matched["start_timestamp"]
            .dropna()
            .drop_duplicates()
        )

        original_end_values = (
            matched["end_timestamp"]
            .dropna()
            .drop_duplicates()
        )

        if len(original_start_values) != 1:
            raise ValueError(
                "Correction source rows contain conflicting start timestamps:\n"
                f"{matched.to_string(index=False)}"
            )

        if len(original_end_values) != 1:
            raise ValueError(
                "Correction source rows contain conflicting end timestamps:\n"
                f"{matched.to_string(index=False)}"
            )

        original_start = pd.Timestamp(
            original_start_values.iloc[0]
        )

        original_end = pd.Timestamp(
            original_end_values.iloc[0]
        )

        corrected_start = (
            pd.Timestamp(
                correction.corrected_start_timestamp
            )
            if correction.corrected_start_timestamp
            else original_start
        )

        corrected_end = (
            pd.Timestamp(
                correction.corrected_end_timestamp
            )
            if correction.corrected_end_timestamp
            else original_end
        )

        if corrected_end < corrected_start:
            raise ValueError(
                "Corrected irrigation end occurs before corrected start:\n"
                f"{correction}"
            )

        corrected_event_id = make_event_id(
            start_timestamp=corrected_start,
            end_timestamp=corrected_end,
            strip_group=correction.strip_group,
        )

        duration_hours = (
            corrected_end
            - corrected_start
        ).total_seconds() / 3600.0

        out.loc[
            mask,
            "start_timestamp",
        ] = corrected_start

        out.loc[
            mask,
            "end_timestamp",
        ] = corrected_end

        out.loc[
            mask,
            "date",
        ] = corrected_start.strftime(
            "%Y-%m-%d"
        )

        out.loc[
            mask,
            "year",
        ] = corrected_start.year

        out.loc[
            mask,
            "event_duration_hours",
        ] = duration_hours

        out.loc[
            mask,
            "event_id",
        ] = corrected_event_id

        out.loc[
            mask,
            "timestamp_correction_applied",
        ] = True

        out.loc[
            mask,
            "correction_code",
        ] = correction.correction_code

        out.loc[
            mask,
            "correction_reason",
        ] = correction.correction_reason

        gallons_strip = pd.to_numeric(
            out.loc[mask, "gallons_strip"],
            errors="coerce",
        )

        if duration_hours > 0:
            out.loc[
                mask,
                "avg_flow_gph_strip",
            ] = gallons_strip / duration_hours

        audit_rows.append(
            {
                "source_date": correction.source_date,
                "strip_group": correction.strip_group,
                "original_event_ids": " | ".join(
                    sorted(
                        {
                            clean_text(value)
                            for value in matched["event_id"]
                            if clean_text(value)
                        }
                    )
                ),
                "corrected_event_id": corrected_event_id,
                "matched_strip_rows": len(matched),
                "original_start_timestamp": original_start,
                "corrected_start_timestamp": corrected_start,
                "start_change_minutes": (
                    corrected_start
                    - original_start
                ).total_seconds() / 60.0,
                "original_end_timestamp": original_end,
                "corrected_end_timestamp": corrected_end,
                "end_change_minutes": (
                    corrected_end
                    - original_end
                ).total_seconds() / 60.0,
                "original_duration_hours": (
                    original_end
                    - original_start
                ).total_seconds() / 3600.0,
                "corrected_duration_hours": duration_hours,
                "correction_code": correction.correction_code,
                "correction_reason": correction.correction_reason,
            }
        )

    audit = pd.DataFrame(
        audit_rows
    )

    return out, audit


def apply_shared_meter_classifications(
    df: pd.DataFrame,
) -> pd.DataFrame:
    out = df.copy()

    normalized_dates = pd.to_datetime(
        out["date"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    for event_date, strip_group in SHARED_METER_EVENTS:
        mask = (
            normalized_dates.eq(event_date)
            & out["strip_group"].eq(
                strip_group
            )
        )

        if not mask.any():
            raise ValueError(
                "No candidate rows matched shared-meter classification:\n"
                f"date={event_date}, "
                f"strip_group={strip_group}"
            )

        out.loc[
            mask,
            "meter_volume_shared_between_groups",
        ] = True

        out.loc[
            mask,
            "meter_volume_allocation_method",
        ] = "equal_split_between_active_groups"

    return out


def validate_corrected_events(
    df: pd.DataFrame,
) -> None:
    corrected = df.loc[
        df["timestamp_correction_applied"]
        .fillna(False)
    ].copy()

    if corrected.empty:
        raise ValueError(
            "No timestamp corrections were applied."
        )

    event_check = (
        corrected.groupby(
            "event_id",
            dropna=False,
        )
        .agg(
            strip_rows=(
                "strip",
                "size",
            ),
            unique_start_times=(
                "start_timestamp",
                "nunique",
            ),
            unique_end_times=(
                "end_timestamp",
                "nunique",
            ),
            unique_strip_groups=(
                "strip_group",
                "nunique",
            ),
        )
        .reset_index()
    )

    bad = event_check.loc[
        event_check["strip_rows"].ne(2)
        | event_check["unique_start_times"].ne(1)
        | event_check["unique_end_times"].ne(1)
        | event_check["unique_strip_groups"].ne(1)
    ]

    if not bad.empty:
        raise ValueError(
            "Corrected event validation failed:\n"
            f"{bad.to_string(index=False)}"
        )

    duplicated_strip_rows = df.duplicated(
        subset=[
            "event_id",
            "strip",
        ],
        keep=False,
    )

    if duplicated_strip_rows.any():
        bad_rows = df.loc[
            duplicated_strip_rows,
            [
                "event_id",
                "strip",
                "start_timestamp",
                "end_timestamp",
                "gallons_strip",
            ],
        ]

        raise ValueError(
            "Duplicate event_id/strip rows were found:\n"
            f"{bad_rows.to_string(index=False)}"
        )


def format_output(
    df: pd.DataFrame,
) -> pd.DataFrame:
    out = df.copy()

    out["start_timestamp"] = pd.to_datetime(
        out["start_timestamp"],
        errors="coerce",
    )

    out["end_timestamp"] = pd.to_datetime(
        out["end_timestamp"],
        errors="coerce",
    )

    out["original_start_timestamp"] = pd.to_datetime(
        out["original_start_timestamp"],
        errors="coerce",
    )

    out["original_end_timestamp"] = pd.to_datetime(
        out["original_end_timestamp"],
        errors="coerce",
    )

    out["date"] = (
        out["start_timestamp"]
        .dt.strftime("%Y-%m-%d")
    )

    out["year"] = (
        out["start_timestamp"]
        .dt.year
        .astype("Int64")
    )

    out["event_duration_hours"] = (
        (
            out["end_timestamp"]
            - out["start_timestamp"]
        )
        .dt.total_seconds()
        .div(3600.0)
        .round(4)
    )

    numeric_rounding = {
        "total_meter_gallons": 0,
        "flow_allocation_fraction": 3,
        "strip_allocation_fraction": 3,
        "gallons_group": 0,
        "gallons_strip": 0,
        "avg_flow_gpm_group": 3,
        "avg_flow_gpm_strip": 3,
        "avg_flow_gph_strip": 3,
        "event_duration_hours": 4,
        "start_flow_gpm": 3,
        "end_flow_gpm": 3,
        "start_totalizer_gal_x100": 1,
        "end_totalizer_gal_x100": 1,
    }

    for column, digits in numeric_rounding.items():
        if column in out.columns:
            out[column] = (
                pd.to_numeric(
                    out[column],
                    errors="coerce",
                )
                .round(digits)
            )

    for column in OUTPUT_COLUMN_ORDER:
        if column not in out.columns:
            out[column] = pd.NA

    additional_columns = [
        column
        for column in out.columns
        if column not in OUTPUT_COLUMN_ORDER
    ]

    out = out[
        OUTPUT_COLUMN_ORDER
        + additional_columns
    ]

    return (
        out.sort_values(
            [
                "start_timestamp",
                "end_timestamp",
                "strip_group",
                "strip",
            ],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def build_pending_events(
    input_path: Path,
) -> pd.DataFrame:
    if not input_path.exists():
        return pd.DataFrame(
            [
                {
                    "year": 2026,
                    "date": "2026-06-27",
                    "strip_group": "S1_S2",
                    "workbook_start_timestamp": "2026-06-27 11:05:00",
                    "camera_start_timestamp": "2026-06-27 11:59:26",
                    "workbook_end_timestamp": "2026-06-27 16:34:00",
                    "camera_end_timestamp": "2026-06-27 16:34:43",
                    "start_camera_minus_workbook_minutes": 54.433,
                    "end_camera_minus_workbook_minutes": 0.717,
                    "volume_status": "unresolved",
                    "candidate_inclusion_status": "excluded_pending_volume",
                    "reason": (
                        "Flowmeter was not turning because of gravel. "
                        "Irrigation occurred, but a defensible event volume "
                        "is not yet available."
                    ),
                },
                {
                    "year": 2026,
                    "date": "2026-06-27",
                    "strip_group": "S3_S4",
                    "workbook_start_timestamp": "2026-06-27 11:05:00",
                    "camera_start_timestamp": "2026-06-27 11:59:26",
                    "workbook_end_timestamp": "2026-06-27 16:34:00",
                    "camera_end_timestamp": "2026-06-27 16:34:43",
                    "start_camera_minus_workbook_minutes": 54.433,
                    "end_camera_minus_workbook_minutes": 0.717,
                    "volume_status": "unresolved",
                    "candidate_inclusion_status": "excluded_pending_volume",
                    "reason": (
                        "Flowmeter was not turning because of gravel. "
                        "Irrigation occurred, but a defensible event volume "
                        "is not yet available."
                    ),
                },
            ]
        )

    source = pd.read_csv(
        input_path
    )

    require_columns(
        source,
        [
            "date",
            "strip_group",
        ],
        source_name="Invalid-row report",
    )

    source_dates = pd.to_datetime(
        source["date"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    normalized_groups = source[
        "strip_group"
    ].map(normalize_strip_group)

    mask = (
        source_dates.eq("2026-06-27")
        & normalized_groups.isin(
            [
                "S1_S2",
                "S3_S4",
            ]
        )
    )

    pending = source.loc[
        mask
    ].copy()

    if pending.empty:
        raise ValueError(
            "The invalid-row report did not contain the expected "
            "2026-06-27 S1_S2 and S3_S4 rows."
        )

    pending["year"] = 2026
    pending["date"] = "2026-06-27"
    pending["strip_group"] = normalized_groups.loc[mask]

    pending[
        "workbook_start_timestamp"
    ] = pd.Timestamp(
        "2026-06-27 11:05:00"
    )

    pending[
        "camera_start_timestamp"
    ] = pd.Timestamp(
        "2026-06-27 11:59:26"
    )

    pending[
        "workbook_end_timestamp"
    ] = pd.Timestamp(
        "2026-06-27 16:34:00"
    )

    pending[
        "camera_end_timestamp"
    ] = pd.Timestamp(
        "2026-06-27 16:34:43"
    )

    pending[
        "start_camera_minus_workbook_minutes"
    ] = 54.433

    pending[
        "end_camera_minus_workbook_minutes"
    ] = 0.717

    pending["volume_status"] = "unresolved"

    pending[
        "candidate_inclusion_status"
    ] = "excluded_pending_volume"

    pending["reason"] = (
        "Flowmeter was not turning because of gravel. Irrigation occurred, "
        "but a defensible event volume is not yet available."
    )

    preferred_columns = [
        "year",
        "date",
        "strip_group",
        "workbook_start_timestamp",
        "camera_start_timestamp",
        "start_camera_minus_workbook_minutes",
        "workbook_end_timestamp",
        "camera_end_timestamp",
        "end_camera_minus_workbook_minutes",
        "reported_gallons_group",
        "volume_status",
        "candidate_inclusion_status",
        "exception_code",
        "exception_action",
        "review_reason",
        "reason",
        "source_sheet",
        "source_row",
        "notes",
    ]

    columns = [
        column
        for column in preferred_columns
        if column in pending.columns
    ]

    return (
        pending[columns]
        .sort_values(
            [
                "date",
                "strip_group",
            ]
        )
        .reset_index(drop=True)
    )


def print_summary(
    input_df: pd.DataFrame,
    output_df: pd.DataFrame,
    audit: pd.DataFrame,
    pending: pd.DataFrame,
    output_path: Path,
) -> None:
    print(
        "\n--- Irrigation camera-QC candidate ---"
    )

    print(
        f"Input rows: {len(input_df):,}"
    )

    print(
        f"Output rows: {len(output_df):,}"
    )

    print(
        "Timestamp-corrected strip rows: "
        f"{int(output_df['timestamp_correction_applied'].sum()):,}"
    )

    corrected_event_count = (
        output_df.loc[
            output_df[
                "timestamp_correction_applied"
            ]
        ]["event_id"]
        .nunique()
    )

    print(
        "Timestamp-corrected physical events: "
        f"{corrected_event_count:,}"
    )

    print(
        "Shared-meter strip rows: "
        f"{int(output_df['meter_volume_shared_between_groups'].sum()):,}"
    )

    shared_event_count = (
        output_df.loc[
            output_df[
                "meter_volume_shared_between_groups"
            ]
        ]["event_id"]
        .nunique()
    )

    print(
        "Shared-meter physical events: "
        f"{shared_event_count:,}"
    )

    print(
        f"Pending unresolved events: {len(pending):,}"
    )

    print(
        f"QC candidate path: {output_path}"
    )

    if not audit.empty:
        print(
            "\nApplied timestamp corrections:"
        )

        display_columns = [
            "source_date",
            "strip_group",
            "original_event_ids",
            "corrected_event_id",
            "original_start_timestamp",
            "corrected_start_timestamp",
            "start_change_minutes",
            "original_end_timestamp",
            "corrected_end_timestamp",
            "end_change_minutes",
            "correction_code",
        ]

        print(
            audit[
                display_columns
            ].to_string(index=False)
        )

    shared = output_df.loc[
        output_df[
            "meter_volume_shared_between_groups"
        ]
    ]

    if not shared.empty:
        print(
            "\nShared-meter classifications:"
        )

        display = (
            shared[
                [
                    "date",
                    "strip_group",
                    "event_id",
                    "gallons_group",
                    "gallons_strip",
                    "meter_volume_allocation_method",
                ]
            ]
            .drop_duplicates(
                subset=[
                    "event_id",
                    "strip_group",
                ]
            )
            .sort_values(
                [
                    "date",
                    "strip_group",
                ]
            )
        )

        print(
            display.to_string(index=False)
        )


def main() -> int:
    args = parse_args()

    input_path = args.input_csv.resolve()
    output_path = args.output_csv.resolve()
    correction_audit_path = (
        args.correction_audit_csv.resolve()
    )
    pending_input_path = (
        args.pending_input_csv.resolve()
    )
    pending_output_path = (
        args.pending_output_csv.resolve()
    )

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input candidate does not exist: {input_path}"
        )

    source = pd.read_csv(
        input_path
    )

    require_columns(
        source,
        [
            "year",
            "date",
            "start_timestamp",
            "end_timestamp",
            "strip_group",
            "strip",
            "gallons_group",
            "gallons_strip",
            "event_duration_hours",
            "event_id",
        ],
        source_name="Irrigation candidate",
    )

    source["strip_group"] = source[
        "strip_group"
    ].map(normalize_strip_group)

    source["start_timestamp"] = (
        parse_datetime_column(
            source["start_timestamp"]
        )
    )

    source["end_timestamp"] = (
        parse_datetime_column(
            source["end_timestamp"]
        )
    )

    invalid_timestamp = (
        source["start_timestamp"].isna()
        | source["end_timestamp"].isna()
    )

    if invalid_timestamp.any():
        raise ValueError(
            "Input candidate contains invalid timestamps:\n"
            f"{source.loc[invalid_timestamp].head(20).to_string(index=False)}"
        )

    qc_candidate = initialize_qc_columns(
        source
    )

    qc_candidate, correction_audit = (
        apply_timestamp_corrections(
            qc_candidate
        )
    )

    qc_candidate = (
        apply_shared_meter_classifications(
            qc_candidate
        )
    )

    validate_corrected_events(
        qc_candidate
    )

    qc_candidate = format_output(
        qc_candidate
    )

    pending = build_pending_events(
        pending_input_path
    )

    print_summary(
        input_df=source,
        output_df=qc_candidate,
        audit=correction_audit,
        pending=pending,
        output_path=output_path,
    )

    if args.dry_run:
        print(
            "\nDry run complete. No files were written."
        )
        return 0

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    correction_audit_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    pending_output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    qc_candidate.to_csv(
        output_path,
        index=False,
        date_format="%Y-%m-%d %H:%M:%S",
    )

    correction_audit.to_csv(
        correction_audit_path,
        index=False,
        date_format="%Y-%m-%d %H:%M:%S",
    )

    pending.to_csv(
        pending_output_path,
        index=False,
        date_format="%Y-%m-%d %H:%M:%S",
    )

    print(
        f"\nWrote QC candidate: {output_path}"
    )

    print(
        "Wrote correction audit: "
        f"{correction_audit_path}"
    )

    print(
        f"Wrote pending events: {pending_output_path}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )