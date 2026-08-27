#!/usr/bin/env python3
"""
build_meter_photo_inventory.py
Build an inventory of original irrigation-meter photographs.

The inventory records:

- file identity and location
- file size and SHA-256 hash
- image dimensions and orientation
- EXIF timestamps and timezone offset
- selected, manual, and effective timestamps
- timestamp source, confidence, review status, and effective year
- camera make and model
- GPS coordinates
- exact-duplicate groups
- duplicate cleanup actions

No OCR or meter-reading extraction is performed.

Default input
-------------
biochar_app/data-processed/management/irrigation/photos/originals/

Default output
--------------
biochar_app/data-processed/management/irrigation/photos/photo_inventory.csv

Timestamp selection
-------------------
The preferred automatic timestamp order is:

1. DateTimeOriginal
2. CreateDate
3. MediaCreateDate
4. TrackCreateDate
5. FileModifyDate

FileModifyDate is retained only as a low-confidence fallback because it may
reflect copying or exporting rather than when the photograph was taken.

A manually entered manual_datetime overrides selected_datetime. Downstream
processing should use effective_datetime and effective_year.

Existing manual columns
-----------------------
When the output CSV already exists, columns listed in MANUAL_COLUMNS are
preserved by relative_path. This allows the inventory to be regenerated
without overwriting manual review notes.

Duplicate handling
------------------
Exact duplicates are identified by SHA-256.

For duplicate rows whose duplicate_action is blank, the script automatically
assigns:

- KEEP to the preferred copy
- DELETE to the other exact copies

Preference is given to recognizable camera filenames, date-based filenames,
and descriptive filenames over UUID-style export filenames.

Existing duplicate_action values are preserved for rows that remain members
of an exact-duplicate group. Actions are cleared automatically when a file is
no longer part of a duplicate group.

Requirements
------------
Install ExifTool once on macOS:

    brew install exiftool

Run from any directory:

    python biochar_app/scripts/management/build_meter_photo_inventory.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[3]

DEFAULT_PHOTO_DIR = (
    REPO_ROOT
    / "biochar_app"
    / "data-processed"
    / "management"
    / "irrigation"
    / "photos"
    / "originals"
)

DEFAULT_OUTPUT_CSV = (
    REPO_ROOT
    / "biochar_app"
    / "data-processed"
    / "management"
    / "irrigation"
    / "photos"
    / "photo_inventory.csv"
)

SUPPORTED_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".heic",
    ".heif",
    ".tif",
    ".tiff",
    ".webp",
}

MANUAL_COLUMNS = (
    "include",
    "review_status",
    "manual_datetime",
    "timestamp_review",
    "duplicate_action",
    "source_collection",
    "meter_reading",
    "notes",
)

TIMESTAMP_PRIORITY = (
    (
        "datetime_original",
        "DateTimeOriginal",
        "high",
    ),
    (
        "create_date",
        "CreateDate",
        "medium",
    ),
    (
        "media_create_date",
        "MediaCreateDate",
        "medium",
    ),
    (
        "track_create_date",
        "TrackCreateDate",
        "medium",
    ),
    (
        "file_modify_date",
        "FileModifyDate",
        "low",
    ),
)

VALID_DUPLICATE_ACTIONS = {
    "",
    "KEEP",
    "DELETE",
    "REVIEW",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a metadata and duplicate inventory for original "
            "irrigation-meter photographs."
        )
    )

    parser.add_argument(
        "--photo-dir",
        type=Path,
        default=DEFAULT_PHOTO_DIR,
        help=(
            "Directory containing original meter photographs. "
            f"Default: {DEFAULT_PHOTO_DIR}"
        ),
    )

    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help=(
            "CSV file to create or refresh. "
            f"Default: {DEFAULT_OUTPUT_CSV}"
        ),
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help=(
            "Print progress after this many files. "
            "Use 0 to suppress progress messages."
        ),
    )

    parser.add_argument(
        "--no-preserve-manual",
        action="store_true",
        help=(
            "Do not preserve manual columns from an existing inventory CSV."
        ),
    )

    parser.add_argument(
        "--exiftool",
        type=Path,
        default=None,
        help="Optional explicit path to the ExifTool executable.",
    )

    return parser.parse_args()


def resolve_exiftool(explicit_path: Path | None) -> str:
    if explicit_path is not None:
        resolved = explicit_path.expanduser().resolve()

        if not resolved.exists():
            raise FileNotFoundError(
                f"ExifTool executable does not exist: {resolved}"
            )

        if not resolved.is_file():
            raise FileNotFoundError(
                f"ExifTool path is not a file: {resolved}"
            )

        return str(resolved)

    discovered = shutil.which("exiftool")

    if discovered is None:
        raise RuntimeError(
            "ExifTool was not found on PATH.\n\n"
            "Install it on macOS with:\n\n"
            "    brew install exiftool"
        )

    return discovered


def find_photos(photo_dir: Path) -> list[Path]:
    if not photo_dir.exists():
        raise FileNotFoundError(
            f"Photo directory does not exist: {photo_dir}"
        )

    if not photo_dir.is_dir():
        raise NotADirectoryError(
            f"Photo path is not a directory: {photo_dir}"
        )

    photos = [
        path
        for path in photo_dir.rglob("*")
        if (
            path.is_file()
            and path.suffix.lower() in SUPPORTED_EXTENSIONS
        )
    ]

    return sorted(
        photos,
        key=lambda path: (
            path.relative_to(photo_dir)
            .as_posix()
            .lower()
        ),
    )


def calculate_sha256(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file_handle:
        while chunk := file_handle.read(chunk_size):
            digest.update(chunk)

    return digest.hexdigest()


def read_exif_batch(
    exiftool: str,
    photo_dir: Path,
) -> dict[str, dict[str, Any]]:
    command = [
        exiftool,
        "-json",
        "-r",
        "-n",
        "-FileName",
        "-Directory",
        "-FileType",
        "-FileSize#",
        "-ImageWidth",
        "-ImageHeight",
        "-Orientation#",
        "-Orientation",
        "-DateTimeOriginal",
        "-CreateDate",
        "-MediaCreateDate",
        "-TrackCreateDate",
        "-ModifyDate",
        "-FileModifyDate",
        "-OffsetTime",
        "-OffsetTimeOriginal",
        "-OffsetTimeDigitized",
        "-GPSLatitude",
        "-GPSLongitude",
        "-GPSAltitude",
        "-Make",
        "-Model",
        "-LensModel",
        "-Software",
        str(photo_dir),
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "ExifTool failed.\n\n"
            f"Command:\n{' '.join(command)}\n\n"
            f"Standard error:\n{result.stderr.strip()}"
        )

    try:
        loaded_records: object = json.loads(
            result.stdout
        )
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "ExifTool returned invalid JSON."
        ) from exc

    if not isinstance(loaded_records, list):
        raise RuntimeError(
            "ExifTool JSON output was not a list of records."
        )

    metadata_by_path: dict[str, dict[str, Any]] = {}

    for raw_record in loaded_records:
        if not isinstance(raw_record, dict):
            continue

        record: dict[str, Any] = {
            str(key): value
            for key, value in raw_record.items()
        }

        source_file = record.get("SourceFile")

        if not source_file:
            continue

        resolved = str(
            Path(str(source_file))
            .expanduser()
            .resolve()
        )

        metadata_by_path[resolved] = record

    return metadata_by_path


def clean_text(value: object) -> str:
    if value is None:
        return ""

    text = str(value).strip()

    if text.lower() in {
        "",
        "none",
        "nan",
        "<na>",
        "nat",
    }:
        return ""

    return text


def optional_number(value: object) -> object:
    if value is None:
        return pd.NA

    if isinstance(value, (str, int, float)):
        try:
            return float(value)
        except ValueError:
            return pd.NA

    return pd.NA


def optional_integer(value: object) -> object:
    if value is None:
        return pd.NA

    if isinstance(value, (str, int, float)):
        try:
            return int(float(value))
        except ValueError:
            return pd.NA

    return pd.NA


def normalize_exif_datetime(value: object) -> str:
    """
    Convert common EXIF date formatting into an ISO-like string.

    Example:
        2023:07:18 11:42:03

    becomes:
        2023-07-18 11:42:03
    """
    text = clean_text(value)

    if not text:
        return ""

    if (
        len(text) >= 10
        and text[4:5] == ":"
        and text[7:8] == ":"
    ):
        text = (
            f"{text[:4]}-"
            f"{text[5:7]}-"
            f"{text[8:]}"
        )

    return text


def select_timezone_offset(
    metadata: dict[str, Any],
) -> tuple[str, str]:
    candidates = (
        (
            "OffsetTimeOriginal",
            "offset_time_original",
        ),
        (
            "OffsetTimeDigitized",
            "offset_time_digitized",
        ),
        (
            "OffsetTime",
            "offset_time",
        ),
    )

    for exif_key, source_name in candidates:
        value = clean_text(
            metadata.get(exif_key)
        )

        if value:
            return value, source_name

    return "", ""


def select_timestamp(
    metadata: dict[str, Any],
) -> tuple[str, str, str]:
    for output_name, exif_key, confidence in TIMESTAMP_PRIORITY:
        value = normalize_exif_datetime(
            metadata.get(exif_key)
        )

        if value:
            return (
                value,
                output_name,
                confidence,
            )

    return "", "", "missing"


def derive_year(timestamp: object) -> object:
    """
    Derive a four-digit year from one timestamp value.

    The value is parsed individually rather than as part of an entire pandas
    Series. This avoids problems caused by mixing timezone-aware FileModifyDate
    values with timezone-naive camera EXIF timestamps.
    """
    text = clean_text(timestamp)

    if not text:
        return pd.NA

    parsed = pd.to_datetime(
        text,
        errors="coerce",
    )

    if pd.isna(parsed):
        return pd.NA

    year = getattr(
        parsed,
        "year",
        None,
    )

    if year is None:
        return pd.NA

    return int(year)


def infer_source_collection(
    relative_path: Path,
) -> str:
    """
    Infer provenance when originals are stored in source subdirectories.

    Files directly inside originals/ return an empty value so a manual source
    can be entered later if needed.
    """
    if len(relative_path.parts) <= 1:
        return ""

    return relative_path.parts[0]


def load_manual_values(
    output_csv: Path,
) -> dict[str, dict[str, str]]:
    if not output_csv.exists():
        return {}

    try:
        existing = pd.read_csv(
            output_csv,
            dtype=str,
        ).fillna("")
    except Exception as exc:
        raise RuntimeError(
            f"Could not read existing inventory: {output_csv}"
        ) from exc

    if "relative_path" not in existing.columns:
        print(
            "Warning: existing inventory has no relative_path column; "
            "manual values cannot be preserved.",
            file=sys.stderr,
        )
        return {}

    available_manual_columns = [
        column
        for column in MANUAL_COLUMNS
        if column in existing.columns
    ]

    preserved: dict[str, dict[str, str]] = {}

    for _, row in existing.iterrows():
        relative_path = clean_text(
            row.get("relative_path")
        )

        if not relative_path:
            continue

        preserved[relative_path] = {
            column: clean_text(
                row.get(column)
            )
            for column in available_manual_columns
        }

    return preserved


def build_inventory_rows(
    photos: list[Path],
    photo_dir: Path,
    metadata_by_path: dict[str, dict[str, Any]],
    preserved_manual: dict[str, dict[str, str]],
    progress_every: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    total = len(photos)

    for position, photo in enumerate(
        photos,
        start=1,
    ):
        resolved_photo = str(
            photo.resolve()
        )

        metadata = metadata_by_path.get(
            resolved_photo,
            {},
        )

        relative_path = photo.relative_to(
            photo_dir
        )

        relative_path_text = (
            relative_path.as_posix()
        )

        (
            selected_datetime,
            timestamp_source,
            timestamp_confidence,
        ) = select_timestamp(metadata)

        (
            timezone_offset,
            timezone_offset_source,
        ) = select_timezone_offset(metadata)

        stat = photo.stat()

        manual = preserved_manual.get(
            relative_path_text,
            {},
        )

        inferred_source = infer_source_collection(
            relative_path
        )

        source_collection = (
            manual.get("source_collection")
            or inferred_source
        )

        orientation_value = metadata.get(
            "Orientation"
        )

        row: dict[str, object] = {
            "filename": photo.name,
            "relative_path": relative_path_text,
            "absolute_path": str(
                photo.resolve()
            ),
            "extension": photo.suffix.lower(),
            "file_type": clean_text(
                metadata.get("FileType")
            ),
            "file_size_bytes": int(
                stat.st_size
            ),
            "sha256": calculate_sha256(
                photo
            ),
            "image_width": optional_integer(
                metadata.get("ImageWidth")
            ),
            "image_height": optional_integer(
                metadata.get("ImageHeight")
            ),
            "orientation_code": optional_integer(
                orientation_value
                if isinstance(
                    orientation_value,
                    (int, float),
                )
                else None
            ),
            "orientation_description": clean_text(
                orientation_value
                if isinstance(
                    orientation_value,
                    str,
                )
                else ""
            ),
            "datetime_original": normalize_exif_datetime(
                metadata.get("DateTimeOriginal")
            ),
            "create_date": normalize_exif_datetime(
                metadata.get("CreateDate")
            ),
            "media_create_date": normalize_exif_datetime(
                metadata.get("MediaCreateDate")
            ),
            "track_create_date": normalize_exif_datetime(
                metadata.get("TrackCreateDate")
            ),
            "modify_date": normalize_exif_datetime(
                metadata.get("ModifyDate")
            ),
            "file_modify_date": normalize_exif_datetime(
                metadata.get("FileModifyDate")
            ),
            "timezone_offset": timezone_offset,
            "timezone_offset_source": (
                timezone_offset_source
            ),
            "selected_datetime": selected_datetime,
            "manual_datetime": manual.get(
                "manual_datetime",
                "",
            ),
            "effective_datetime": "",
            "timestamp_source": timestamp_source,
            "timestamp_confidence": (
                timestamp_confidence
            ),
            "timestamp_review": manual.get(
                "timestamp_review",
                "",
            ),
            "duplicate_action": manual.get(
                "duplicate_action",
                "",
            ),
            "year": derive_year(
                selected_datetime
            ),
            "camera_make": clean_text(
                metadata.get("Make")
            ),
            "camera_model": clean_text(
                metadata.get("Model")
            ),
            "lens_model": clean_text(
                metadata.get("LensModel")
            ),
            "software": clean_text(
                metadata.get("Software")
            ),
            "gps_latitude": optional_number(
                metadata.get("GPSLatitude")
            ),
            "gps_longitude": optional_number(
                metadata.get("GPSLongitude")
            ),
            "gps_altitude_m": optional_number(
                metadata.get("GPSAltitude")
            ),
            "has_gps": bool(
                metadata.get("GPSLatitude") is not None
                and metadata.get("GPSLongitude") is not None
            ),
            "source_collection": source_collection,
            "include": manual.get(
                "include",
                "TRUE",
            ),
            "review_status": manual.get(
                "review_status",
                "",
            ),
            "meter_reading": manual.get(
                "meter_reading",
                "",
            ),
            "notes": manual.get(
                "notes",
                "",
            ),
        }

        rows.append(row)

        if (
            progress_every > 0
            and (
                position % progress_every == 0
                or position == total
            )
        ):
            print(
                f"Processed {position:,} of {total:,} photos..."
            )

    return rows


def apply_manual_datetime(
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build effective datetime and year using an optional manual timestamp.

    Priority:
        manual_datetime
        selected_datetime

    Datetime values remain normalized strings in the inventory. Year parsing
    is performed one value at a time so timezone-aware FileModifyDate values
    and timezone-naive camera EXIF values can coexist safely.
    """
    out = inventory.copy()

    selected_datetime = (
        out["selected_datetime"]
        .fillna("")
        .astype(str)
        .map(clean_text)
    )

    manual_datetime = (
        out["manual_datetime"]
        .fillna("")
        .astype(str)
        .map(normalize_exif_datetime)
    )

    out["selected_datetime"] = (
        selected_datetime
    )

    out["manual_datetime"] = (
        manual_datetime
    )

    manual_mask = manual_datetime.ne("")

    out["effective_datetime"] = (
        selected_datetime.copy()
    )

    out.loc[
        manual_mask,
        "effective_datetime",
    ] = manual_datetime.loc[
        manual_mask
    ]

    out["effective_year"] = (
        out["effective_datetime"]
        .map(derive_year)
        .astype("Int64")
    )

    out["year_source"] = "metadata"

    out.loc[
        manual_mask,
        "year_source",
    ] = "manual"

    out.loc[
        out["effective_datetime"].eq(""),
        "year_source",
    ] = "missing"

    return out


def duplicate_filename_preference(
    filename: str,
) -> tuple[int, int, str]:
    """
    Return a deterministic archival-preference key.

    Lower values are preferred.

    Preference order:

    1. Camera-style filenames such as IMG_7599.JPEG
    2. Date-based filenames such as 2026-04-21_...
    3. Other descriptive, non-UUID filenames
    4. UUID exports ending in _1_102_o
    5. UUID exports ending in _1_105_c
    6. UUID exports ending in _4_5005_c
    7. Other UUID-style filenames
    """
    name = Path(filename).name
    stem = Path(filename).stem
    upper_name = name.upper()
    lower_stem = stem.lower()

    is_uuid_name = bool(
        re.match(
            r"^[0-9A-Fa-f]{8}-"
            r"[0-9A-Fa-f]{4}-"
            r"[0-9A-Fa-f]{4}-"
            r"[0-9A-Fa-f]{4}-"
            r"[0-9A-Fa-f]{12}",
            stem,
        )
    )

    if re.match(
        r"^IMG[_-]?\d+",
        upper_name,
    ):
        category = 0

    elif re.match(
        r"^\d{4}-\d{2}-\d{2}",
        name,
    ):
        category = 1

    elif not is_uuid_name:
        category = 2

    elif lower_stem.endswith(
        "_1_102_o"
    ):
        category = 3

    elif lower_stem.endswith(
        "_1_105_c"
    ):
        category = 4

    elif lower_stem.endswith(
        "_4_5005_c"
    ):
        category = 5

    else:
        category = 6

    return (
        category,
        len(name),
        name.lower(),
    )


def add_duplicate_information(
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add duplicate-group information and choose one preferred archival copy.

    For duplicate rows whose duplicate_action is blank:

    - preferred copy -> KEEP
    - other copies   -> DELETE

    Existing actions are preserved while a row remains part of an exact
    duplicate group. Actions are cleared when a row is not a duplicate.
    """
    out = inventory.copy()

    out["sha256"] = (
        out["sha256"]
        .fillna("")
        .astype(str)
    )

    hash_values: list[str] = (
        out["sha256"].tolist()
    )

    hash_counts: Counter[str] = Counter(
        hash_values
    )

    out["exact_duplicate_count"] = (
        out["sha256"]
        .map(
            lambda value: hash_counts[
                str(value)
            ]
        )
        .astype("Int64")
    )

    out["is_exact_duplicate"] = (
        out["exact_duplicate_count"] > 1
    )

    duplicate_hashes: list[str] = sorted(
        [
            hash_value
            for hash_value, count in hash_counts.items()
            if (
                count > 1
                and hash_value
            )
        ]
    )

    duplicate_group_map: dict[str, str] = {
        hash_value: f"DUP-{position:04d}"
        for position, hash_value in enumerate(
            duplicate_hashes,
            start=1,
        )
    }

    out["duplicate_group"] = (
        out["sha256"]
        .map(duplicate_group_map)
        .fillna("")
    )

    out["preferred_duplicate_copy"] = False

    if "duplicate_action" not in out.columns:
        out["duplicate_action"] = ""

    out["duplicate_action"] = (
        out["duplicate_action"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    invalid_action_mask = (
        ~out["duplicate_action"].isin(
            VALID_DUPLICATE_ACTIONS
        )
    )

    if invalid_action_mask.any():
        invalid_actions = sorted(
            set(
                out.loc[
                    invalid_action_mask,
                    "duplicate_action",
                ].tolist()
            )
        )

        raise RuntimeError(
            "Invalid duplicate_action values found: "
            + ", ".join(
                repr(value)
                for value in invalid_actions
            )
            + ". Allowed values are KEEP, DELETE, REVIEW, or blank."
        )

    # A duplicate action has no meaning once a row is no longer duplicated.
    out.loc[
        ~out["is_exact_duplicate"],
        "duplicate_action",
    ] = ""

    duplicate_rows = out.loc[
        out["is_exact_duplicate"]
    ].copy()

    if not duplicate_rows.empty:
        preference_keys = (
            duplicate_rows["filename"]
            .astype(str)
            .map(
                duplicate_filename_preference
            )
        )

        duplicate_rows[
            "_filename_category"
        ] = preference_keys.map(
            lambda value: value[0]
        )

        duplicate_rows[
            "_filename_length"
        ] = preference_keys.map(
            lambda value: value[1]
        )

        duplicate_rows[
            "_filename_sort"
        ] = preference_keys.map(
            lambda value: value[2]
        )

        duplicate_rows[
            "_format_preference"
        ] = (
            duplicate_rows["extension"]
            .astype(str)
            .str.lower()
            .map(
                {
                    ".jpg": 0,
                    ".jpeg": 0,
                    ".png": 1,
                    ".heic": 2,
                    ".heif": 2,
                    ".tif": 3,
                    ".tiff": 3,
                    ".webp": 4,
                }
            )
            .fillna(9)
        )

        preferred_indices = (
            duplicate_rows
            .sort_values(
                [
                    "duplicate_group",
                    "_filename_category",
                    "_format_preference",
                    "_filename_length",
                    "_filename_sort",
                    "relative_path",
                ]
            )
            .groupby(
                "duplicate_group",
                sort=False,
            )
            .head(1)
            .index
        )

        out.loc[
            preferred_indices,
            "preferred_duplicate_copy",
        ] = True

    blank_action = (
        out["duplicate_action"].eq("")
    )

    out.loc[
        (
            out["is_exact_duplicate"]
            & out["preferred_duplicate_copy"]
            & blank_action
        ),
        "duplicate_action",
    ] = "KEEP"

    out.loc[
        (
            out["is_exact_duplicate"]
            & ~out["preferred_duplicate_copy"]
            & blank_action
        ),
        "duplicate_action",
    ] = "DELETE"

    return out


def order_columns(
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    column_order = [
        "filename",
        "relative_path",
        "source_collection",
        "extension",
        "file_type",
        "file_size_bytes",
        "sha256",
        "exact_duplicate_count",
        "is_exact_duplicate",
        "duplicate_group",
        "preferred_duplicate_copy",
        "duplicate_action",
        "image_width",
        "image_height",
        "orientation_code",
        "orientation_description",
        "datetime_original",
        "create_date",
        "media_create_date",
        "track_create_date",
        "modify_date",
        "file_modify_date",
        "timezone_offset",
        "timezone_offset_source",
        "selected_datetime",
        "manual_datetime",
        "effective_datetime",
        "timestamp_source",
        "timestamp_confidence",
        "timestamp_review",
        "year",
        "effective_year",
        "year_source",
        "camera_make",
        "camera_model",
        "lens_model",
        "software",
        "gps_latitude",
        "gps_longitude",
        "gps_altitude_m",
        "has_gps",
        "include",
        "review_status",
        "meter_reading",
        "notes",
        "absolute_path",
    ]

    existing_columns = [
        column
        for column in column_order
        if column in inventory.columns
    ]

    extra_columns = [
        column
        for column in inventory.columns
        if column not in existing_columns
    ]

    return inventory[
        existing_columns + extra_columns
    ]


def print_summary(
    inventory: pd.DataFrame,
    output_csv: Path,
) -> None:
    total = len(inventory)

    exact_duplicate_files = int(
        inventory["is_exact_duplicate"].sum()
    )

    duplicate_groups = int(
        inventory.loc[
            inventory["duplicate_group"].ne(""),
            "duplicate_group",
        ].nunique()
    )

    missing_selected_timestamps = int(
        inventory["selected_datetime"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("")
        .sum()
    )

    missing_effective_timestamps = int(
        inventory["effective_datetime"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("")
        .sum()
    )

    low_confidence_timestamps = int(
        inventory["timestamp_confidence"]
        .eq("low")
        .sum()
    )

    gps_count = int(
        inventory["has_gps"].sum()
    )

    keep_count = int(
        inventory["duplicate_action"]
        .eq("KEEP")
        .sum()
    )

    delete_count = int(
        inventory["duplicate_action"]
        .eq("DELETE")
        .sum()
    )

    review_count = int(
        inventory["duplicate_action"]
        .eq("REVIEW")
        .sum()
    )

    print("\nPhoto inventory summary")
    print("-----------------------")
    print(
        f"Photos inventoried          : {total:,}"
    )
    print(
        f"Exact duplicate files       : {exact_duplicate_files:,}"
    )
    print(
        f"Exact duplicate groups      : {duplicate_groups:,}"
    )
    print(
        f"Missing selected timestamps : "
        f"{missing_selected_timestamps:,}"
    )
    print(
        f"Missing effective timestamps: "
        f"{missing_effective_timestamps:,}"
    )
    print(
        f"Low-confidence timestamps   : "
        f"{low_confidence_timestamps:,}"
    )
    print(
        f"Photos with GPS             : {gps_count:,}"
    )

    if duplicate_groups:
        print("\nDuplicate actions:")
        print(
            f"  KEEP  : {keep_count:,}"
        )
        print(
            f"  DELETE: {delete_count:,}"
        )
        print(
            f"  REVIEW: {review_count:,}"
        )

    years = (
        inventory["effective_year"]
        .dropna()
        .astype(int)
        .value_counts()
        .sort_index()
    )

    if not years.empty:
        print("\nPhotos by effective year:")

        for year, count in years.items():
            print(
                f"  {year}: {count:,}"
            )

    timestamp_sources = Counter(
        clean_text(value) or "missing"
        for value in inventory[
            "timestamp_source"
        ]
    )

    print("\nTimestamp sources:")

    for source, count in sorted(
        timestamp_sources.items()
    ):
        print(
            f"  {source}: {count:,}"
        )

    print(
        f"\nWrote inventory: {output_csv}"
    )


def main() -> int:
    args = parse_args()

    photo_dir = (
        args.photo_dir
        .expanduser()
        .resolve()
    )

    output_csv = (
        args.output_csv
        .expanduser()
        .resolve()
    )

    try:
        exiftool = resolve_exiftool(
            args.exiftool
        )

        photos = find_photos(
            photo_dir
        )

        if not photos:
            print(
                f"No supported images found in: {photo_dir}",
                file=sys.stderr,
            )
            return 1

        print(
            f"Photo directory : {photo_dir}"
        )
        print(
            f"Photos found    : {len(photos):,}"
        )
        print(
            f"ExifTool        : {exiftool}"
        )

        print(
            "\nReading EXIF metadata..."
        )

        metadata_by_path = read_exif_batch(
            exiftool=exiftool,
            photo_dir=photo_dir,
        )

        preserved_manual: dict[
            str,
            dict[str, str],
        ] = {}

        if not args.no_preserve_manual:
            preserved_manual = load_manual_values(
                output_csv
            )

            if preserved_manual:
                print(
                    "Preserving manual fields for "
                    f"{len(preserved_manual):,} existing rows."
                )

        print(
            "\nBuilding inventory..."
        )

        rows = build_inventory_rows(
            photos=photos,
            photo_dir=photo_dir,
            metadata_by_path=metadata_by_path,
            preserved_manual=preserved_manual,
            progress_every=max(
                0,
                args.progress_every,
            ),
        )

        inventory = pd.DataFrame(
            rows
        )

        inventory = add_duplicate_information(
            inventory
        )

        inventory = apply_manual_datetime(
            inventory
        )

        inventory = inventory.sort_values(
            [
                "effective_datetime",
                "relative_path",
            ],
            na_position="last",
        ).reset_index(
            drop=True
        )

        inventory = order_columns(
            inventory
        )

        output_csv.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        inventory.to_csv(
            output_csv,
            index=False,
        )

        print_summary(
            inventory=inventory,
            output_csv=output_csv,
        )

    except (
        FileNotFoundError,
        NotADirectoryError,
        RuntimeError,
        subprocess.SubprocessError,
        OSError,
    ) as exc:
        print(
            f"Error: {exc}",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())