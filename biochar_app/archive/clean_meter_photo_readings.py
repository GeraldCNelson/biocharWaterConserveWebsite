#!/usr/bin/env python3
"""
Archived cleaner for the former ``photos_Lobato_phone`` workflow.

The active meter-photo workflow uses
``photos/photo_inventory_unique.csv`` directly. This script is retained only
to document and reproduce the earlier Lobato-phone review process.

Inputs
------
1. Completed Excel meter-review workbook containing manually entered readings.
2. Original photo-metadata CSV containing filename and datetime_original.

Processing
----------
- Restores photo timestamps from the metadata CSV by exact filename.
- Extracts the underlying photo UUID from each filename.
- Groups HEIC/JPEG and resized variants of the same original photo.
- Checks whether duplicate copies have matching readings and timestamps.
- Selects one preferred filename per photo UUID.
- Produces one combined QC CSV with both clean and unresolved records.

Output
------
meter_photo_readings_clean.csv

The output contains one row per underlying photo UUID. QC fields indicate
whether the row is ready for comparison with irrigation_clean.csv.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]

DEFAULT_PHOTO_DIR = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "management"
    / "irrigation"
    / "photos_Lobato_phone"
)

DEFAULT_WORKBOOK = (
    DEFAULT_PHOTO_DIR
    / "meter_photo_review_Lobato_phone.xlsx"
)

DEFAULT_METADATA_CSV = (
    DEFAULT_PHOTO_DIR
    / "meter_photo_dates_Lobato_phone.csv"
)

DEFAULT_OUTPUT_CSV = (
    DEFAULT_PHOTO_DIR
    / "meter_photo_readings_clean.csv"
)

EXPECTED_READING_DIGITS = 6
METER_MULTIPLIER_GALLONS = 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collapse duplicate irrigation-meter photos and create one "
            "quality-controlled meter-reading CSV."
        )
    )

    parser.add_argument(
        "--workbook",
        type=Path,
        default=DEFAULT_WORKBOOK,
        help=f"Completed review workbook. Default: {DEFAULT_WORKBOOK}",
    )

    parser.add_argument(
        "--metadata-csv",
        type=Path,
        default=DEFAULT_METADATA_CSV,
        help=f"Original photo metadata CSV. Default: {DEFAULT_METADATA_CSV}",
    )

    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help=f"Clean combined output CSV. Default: {DEFAULT_OUTPUT_CSV}",
    )

    parser.add_argument(
        "--sheet-name",
        type=str,
        default=0,
        help=(
            "Workbook sheet name or zero-based sheet index. "
            "Default: first sheet."
        ),
    )

    return parser.parse_args()


def normalize_column_name(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [
        normalize_column_name(column)
        for column in out.columns
    ]
    return out


def first_existing_column(
    df: pd.DataFrame,
    candidates: tuple[str, ...],
    *,
    required: bool = True,
) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate

    if required:
        raise KeyError(
            "None of the expected columns were found: "
            f"{candidates}. Available columns: {df.columns.tolist()}"
        )

    return None


def clean_text(value: object) -> str:
    if pd.isna(value):
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


def normalize_filename(value: object) -> str:
    return Path(clean_text(value)).name


def extract_photo_id(filename: str) -> str:
    """
    Extract the original phone-photo UUID.

    Examples
    --------
    UUID_1_102_o.jpeg   -> UUID
    UUID_4_5005_c.jpeg  -> UUID
    UUID.heic           -> UUID
    """
    stem = Path(filename).stem
    return stem.split("_", 1)[0]


def normalize_manual_reading(value: object) -> str:
    """
    Convert a workbook reading into a digit string.

    Handles Excel numeric cells such as 169684.0 without producing
    the text '169684.0'.
    """
    if pd.isna(value):
        return ""

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if pd.isna(value):
            return ""

        if value.is_integer():
            return str(int(value))

        return str(value).strip()

    text = str(value).strip()

    # Remove a trailing decimal introduced by spreadsheet numeric formatting.
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]

    return text


def unique_nonblank(values: pd.Series) -> list[str]:
    cleaned = {
        clean_text(value)
        for value in values
        if clean_text(value)
    }

    return sorted(cleaned)


def unique_datetimes(values: pd.Series) -> list[pd.Timestamp]:
    parsed = pd.to_datetime(
        values,
        errors="coerce",
    ).dropna()

    if parsed.empty:
        return []

    unique_values = sorted(
        {
            pd.Timestamp(value)
            for value in parsed
        }
    )

    return unique_values


def choose_preferred_filename(filenames: list[str]) -> str:
    """
    Prefer a standard full-size JPEG, then another JPEG, then HEIC.
    """

    def preference_key(filename: str) -> tuple[int, str]:
        lower = filename.lower()

        if lower.endswith("_4_5005_c.jpeg"):
            rank = 0
        elif lower.endswith("_4_5005_c.jpg"):
            rank = 1
        elif lower.endswith((".jpeg", ".jpg")):
            rank = 2
        elif lower.endswith((".heic", ".heif")):
            rank = 3
        else:
            rank = 4

        return rank, lower

    if not filenames:
        return ""

    return min(
        filenames,
        key=preference_key,
    )


def notes_indicate_nonreading(notes: list[str], statuses: list[str]) -> bool:
    combined = " | ".join(
        [*notes, *statuses]
    ).lower()

    phrases = (
        "meter not visible",
        "not visible",
        "can't see meter",
        "cannot see meter",
        "screenshot",
        "not meter",
        "not a meter",
    )

    return any(
        phrase in combined
        for phrase in phrases
    )


def classify_group(
    readings: list[str],
    datetimes: list[pd.Timestamp],
    notes: list[str],
    statuses: list[str],
) -> tuple[str, str, bool]:
    """
    Return qc_status, unresolved_reason, review_required.
    """
    reasons: list[str] = []

    nonreading = notes_indicate_nonreading(
        notes=notes,
        statuses=statuses,
    )

    if nonreading:
        reasons.append(
            "photo does not provide a usable meter reading"
        )

    if not readings:
        reasons.append(
            "manual reading is missing"
        )
    elif len(readings) > 1:
        reasons.append(
            "duplicate copies have different manual readings"
        )
    elif not readings[0].isdigit():
        reasons.append(
            "manual reading contains non-digit characters"
        )
    elif len(readings[0]) != EXPECTED_READING_DIGITS:
        reasons.append(
            f"manual reading is not {EXPECTED_READING_DIGITS} digits"
        )

    if not datetimes:
        reasons.append(
            "photo datetime is missing"
        )
    elif len(datetimes) > 1:
        reasons.append(
            "duplicate copies have different photo datetimes"
        )

    if nonreading:
        qc_status = "not_usable_as_meter_reading"
    elif not reasons:
        qc_status = "clean"
    elif len(readings) > 1:
        qc_status = "duplicate_reading_mismatch"
    elif not readings:
        qc_status = "missing_reading"
    elif not readings[0].isdigit():
        qc_status = "invalid_reading"
    elif len(readings[0]) != EXPECTED_READING_DIGITS:
        qc_status = "invalid_reading_length"
    elif not datetimes:
        qc_status = "missing_datetime"
    elif len(datetimes) > 1:
        qc_status = "duplicate_datetime_mismatch"
    else:
        qc_status = "review_required"

    unresolved_reason = "; ".join(reasons)
    review_required = qc_status != "clean"

    return (
        qc_status,
        unresolved_reason,
        review_required,
    )


def load_review_workbook(
    path: Path,
    sheet_name: str | int,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Review workbook does not exist: {path}"
        )

    df = pd.read_excel(
        path,
        sheet_name=sheet_name,
        engine="openpyxl",
    )

    df = normalize_columns(df)

    filename_col = first_existing_column(
        df,
        (
            "filename",
            "file_name",
        ),
    )

    reading_col = first_existing_column(
        df,
        (
            "manual_reading",
            "meter_reading",
            "reading",
        ),
    )

    status_col = first_existing_column(
        df,
        ("status",),
        required=False,
    )

    notes_col = first_existing_column(
        df,
        ("notes", "note"),
        required=False,
    )

    out = pd.DataFrame(
        {
            "filename": df[filename_col].map(
                normalize_filename
            ),
            "manual_reading": df[reading_col].map(
                normalize_manual_reading
            ),
            "status": (
                df[status_col].map(clean_text)
                if status_col
                else ""
            ),
            "notes": (
                df[notes_col].map(clean_text)
                if notes_col
                else ""
            ),
        }
    )

    out = out.loc[
        out["filename"].ne("")
    ].copy()

    return out


def load_metadata(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Metadata CSV does not exist: {path}"
        )

    df = pd.read_csv(path)
    df = normalize_columns(df)

    filename_col = first_existing_column(
        df,
        (
            "filename",
            "file_name",
        ),
    )

    datetime_col = first_existing_column(
        df,
        (
            "datetime_original",
            "photo_datetime",
            "datetime",
            "date_time_original",
        ),
    )

    datetime_text = (
        df[datetime_col]
        .astype("string")
        .str.strip()
    )

    # First try the EXIF timestamp format:
    # YYYY:MM:DD HH:MM:SS
    photo_datetime = pd.to_datetime(
        datetime_text,
        format="%Y:%m:%d %H:%M:%S",
        errors="coerce",
    )

    # Fall back for any ISO or other conventional timestamp strings.
    missing_mask = photo_datetime.isna()

    if missing_mask.any():
        photo_datetime.loc[missing_mask] = pd.to_datetime(
            datetime_text.loc[missing_mask],
            format="mixed",
            errors="coerce",
        )

    out = pd.DataFrame(
        {
            "filename": df[filename_col].map(
                normalize_filename
            ),
            "photo_datetime": photo_datetime,
        }
    )

    out = (
        out.sort_values(
            ["filename", "photo_datetime"],
            na_position="last",
        )
        .drop_duplicates(
            subset=["filename"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    return out


def combine_group(
    photo_id: str,
    group: pd.DataFrame,
) -> dict[str, Any]:
    filenames = sorted(
        {
            normalize_filename(value)
            for value in group["filename"]
            if normalize_filename(value)
        }
    )

    readings = unique_nonblank(
        group["manual_reading"]
    )

    statuses = unique_nonblank(
        group["status"]
    )

    notes = unique_nonblank(
        group["notes"]
    )

    datetimes = unique_datetimes(
        group["photo_datetime"]
    )

    qc_status, unresolved_reason, review_required = (
        classify_group(
            readings=readings,
            datetimes=datetimes,
            notes=notes,
            statuses=statuses,
        )
    )

    reading_consistent = len(readings) <= 1
    datetime_consistent = len(datetimes) <= 1

    authoritative_reading = (
        readings[0]
        if len(readings) == 1
        and readings[0].isdigit()
        and len(readings[0]) == EXPECTED_READING_DIGITS
        else ""
    )

    meter_counter_value: int | pd._libs.missing.NAType

    if authoritative_reading:
        meter_counter_value = int(
            authoritative_reading
        )
    else:
        meter_counter_value = pd.NA

    meter_gallons: int | pd._libs.missing.NAType

    if authoritative_reading:
        meter_gallons = (
            int(authoritative_reading)
            * METER_MULTIPLIER_GALLONS
        )
    else:
        meter_gallons = pd.NA

    photo_datetime: pd.Timestamp | pd.NaT

    if len(datetimes) == 1:
        photo_datetime = datetimes[0]
    else:
        photo_datetime = pd.NaT

    return {
        "photo_id": photo_id,
        "photo_datetime": photo_datetime,
        "manual_reading": authoritative_reading,
        "meter_counter_value": meter_counter_value,
        "meter_gallons": meter_gallons,
        "preferred_filename": choose_preferred_filename(
            filenames
        ),
        "all_filenames": " | ".join(filenames),
        "source_row_count": int(len(group)),
        "duplicate_count": max(
            0,
            int(len(filenames) - 1),
        ),
        "all_manual_readings": " | ".join(readings),
        "reading_consistent_across_duplicates": (
            reading_consistent
        ),
        "all_photo_datetimes": " | ".join(
            value.isoformat(sep=" ")
            for value in datetimes
        ),
        "datetime_consistent_across_duplicates": (
            datetime_consistent
        ),
        "status": " | ".join(statuses),
        "notes": " | ".join(notes),
        "qc_status": qc_status,
        "review_required": review_required,
        "unresolved_reason": unresolved_reason,
    }


def main() -> None:
    args = parse_args()

    review = load_review_workbook(
        path=args.workbook,
        sheet_name=args.sheet_name,
    )

    metadata = load_metadata(
        args.metadata_csv
    )

    merged = review.merge(
        metadata,
        on="filename",
        how="left",
        validate="many_to_one",
    )

    merged["photo_id"] = merged[
        "filename"
    ].map(extract_photo_id)

    grouped_rows = [
        combine_group(
            photo_id=str(photo_id),
            group=group,
        )
        for photo_id, group in merged.groupby(
            "photo_id",
            sort=True,
            dropna=False,
        )
    ]

    output = pd.DataFrame(
        grouped_rows
    )

    output = output.sort_values(
        [
            "photo_datetime",
            "photo_id",
        ],
        na_position="last",
    ).reset_index(drop=True)

    args.output_csv.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output.to_csv(
        args.output_csv,
        index=False,
        date_format="%Y-%m-%d %H:%M:%S",
    )

    print(
        f"Wrote: {args.output_csv}"
    )

    print(
        f"Workbook rows read: {len(review):,}"
    )

    print(
        f"Unique photo IDs: {len(output):,}"
    )

    print(
        f"Collapsed duplicate rows: "
        f"{len(review) - len(output):,}"
    )

    print("\nQC status counts:")
    print(
        output["qc_status"]
        .value_counts(dropna=False)
        .to_string()
    )

    review_rows = output.loc[
        output["review_required"].fillna(True)
    ]

    print(
        f"\nRows requiring review: "
        f"{len(review_rows):,}"
    )

    if not review_rows.empty:
        columns = [
            "photo_id",
            "photo_datetime",
            "all_manual_readings",
            "all_filenames",
            "qc_status",
            "unresolved_reason",
        ]

        print(
            review_rows[columns]
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
