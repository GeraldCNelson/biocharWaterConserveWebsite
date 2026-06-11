#!/usr/bin/env python3
"""
extract_irrigation_photo_events.py

Read irrigation meter photos/videos, extract capture timestamps, sort them,
suggest start/end event pairs, and compute gallons from manually entered
meter readings.

Important:
- Meter readings are entered as displayed on the meter: Gallons x 100.
- Computed gallons = (end_reading_x100 - start_reading_x100) * 100
- EXIF timestamps are treated as local field time, not timezone-converted.

Workflow:
1. Put photos/videos in:
   biochar_app/data-processed/management/photos/irrigation/photos_2023

2. Run once to create a review CSV:
   python biochar_app/scripts/extract_irrigation_photo_events.py --year 2023

3. Open:
   biochar_app/data-processed/management/photos/irrigation/photo_review_2023.csv

4. Fill in meter_reading_x100 for each usable image.
   Optional: set exclude = TRUE for bad/unusable photos.

5. Run again:
   python biochar_app/scripts/extract_irrigation_photo_events.py --year 2023 --use-review

6. Review suggested event pairs:
   biochar_app/data-processed/management/photos/irrigation/suggested_irrigation_events_2023.csv
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from PIL import Image
from PIL.ExifTags import TAGS


from biochar_app.config.paths import DATA_PROCESSED_DIR

DEFAULT_PHOTO_DIR = (
        DATA_PROCESSED_DIR
        / "management"
        / "photos"
        / "irrigation"
        / "photos_2023"
)

SUPPORTED_IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".heic",
    ".heif",
}

SUPPORTED_VIDEO_EXTENSIONS = {
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".m4v",
}


@dataclass(frozen=True)
class PhotoRecord:
    filename: str
    path: str
    file_type: str
    timestamp: str | None
    timestamp_source: str
    meter_reading_x100: float | None
    exclude: bool
    notes: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract irrigation photo metadata and suggest start/end irrigation pairs."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="Year used for output filenames.",
    )
    parser.add_argument(
        "--photo-dir",
        type=Path,
        default=DEFAULT_PHOTO_DIR,
        help="Directory containing irrigation meter photos/videos.",
    )
    parser.add_argument(
        "--use-review",
        action="store_true",
        help="Use an existing photo_review_YEAR.csv with manually entered meter readings.",
    )
    parser.add_argument(
        "--max-event-hours",
        type=float,
        default=24.0,
        help="Maximum duration allowed for an automatically suggested start/end pair.",
    )
    parser.add_argument(
        "--min-gallons",
        type=float,
        default=1_000.0,
        help="Minimum positive gallons needed to suggest a start/end pair.",
    )
    parser.add_argument(
        "--irrigation-csv",
        type=Path,
        default=DATA_PROCESSED_DIR / "management" / "irrigation_2023.csv",
        help="Clean irrigation CSV used to match photos to known irrigation events.",
    )

    parser.add_argument(
        "--match-photos",
        action="store_true",
        help="Attach likely irrigation event matches to the photo review CSV.",
    )

    parser.add_argument(
        "--max-match-minutes",
        type=float,
        default=90.0,
        help="Maximum minutes from event start/end to consider a photo matched.",
    )
    return parser.parse_args()


def get_exif_datetime_image(path: Path) -> tuple[str | None, str]:
    """
    Extract DateTimeOriginal/DateTimeDigitized/DateTime from image EXIF.

    Returns:
        (timestamp_iso, source)
    """
    try:
        img = Image.open(path)
        exif = img.getexif()
    except Exception:
        return None, "image_open_failed"

    if not exif:
        return None, "no_exif"

    exif_data: dict[str, Any] = {
        TAGS.get(tag, str(tag)): value
        for tag, value in exif.items()
    }

    for key in ["DateTimeOriginal", "DateTimeDigitized", "DateTime"]:
        raw = exif_data.get(key)
        if raw:
            parsed = parse_exif_datetime(str(raw))
            if parsed:
                return parsed, key

    return None, "no_datetime_tag"


def parse_exif_datetime(raw: str) -> str | None:
    raw = raw.strip()
    if not raw:
        return None

    formats = [
        "%Y:%m:%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(raw[:19], fmt)
            return dt.strftime("%Y-%m-%dT%H:%M")
        except ValueError:
            pass

    return None


def get_video_metadata_with_exiftool(path: Path) -> tuple[str | None, str]:
    """
    Try to read video capture metadata using exiftool if available.

    This is optional. If exiftool is not installed, falls back to file modified time.
    """
    try:
        result = subprocess.run(
            ["exiftool", "-j", str(path)],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None, "video_exiftool_not_installed"

    if result.returncode != 0 or not result.stdout.strip():
        return None, "video_exiftool_failed"

    try:
        data = json.loads(result.stdout)[0]
    except Exception:
        return None, "video_exiftool_json_failed"

    candidate_keys = [
        "CreationDate",
        "CreateDate",
        "MediaCreateDate",
        "TrackCreateDate",
        "FileModifyDate",
    ]

    for key in candidate_keys:
        raw = data.get(key)
        parsed = parse_video_datetime(str(raw)) if raw else None
        if parsed:
            return parsed, key

    return None, "video_no_datetime_tag"


def parse_video_datetime(raw: str) -> str | None:
    """
    Parse common exiftool video date strings.

    Examples:
    - 2023:06:15 10:12:34-06:00
    - 2023:06:15 10:12:34
    - 2023-06-15T10:12:34
    """
    raw = raw.strip()
    if not raw:
        return None

    # Strip timezone suffix; for this project we want local naive field time.
    raw_no_tz = raw.replace("Z", "")
    if len(raw_no_tz) >= 25 and raw_no_tz[-3] == ":" and raw_no_tz[-6] in ["+", "-"]:
        raw_no_tz = raw_no_tz[:-6]

    raw_no_tz = raw_no_tz[:19]

    for fmt in ["%Y:%m:%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
        try:
            dt = datetime.strptime(raw_no_tz, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M")
        except ValueError:
            pass

    return None


def get_file_modified_timestamp(path: Path) -> tuple[str, str]:
    dt = datetime.fromtimestamp(path.stat().st_mtime)
    return dt.strftime("%Y-%m-%dT%H:%M"), "file_modified_time_fallback"


def get_capture_timestamp(path: Path) -> tuple[str | None, str]:
    ext = path.suffix.lower()

    if ext in SUPPORTED_IMAGE_EXTENSIONS:
        ts, source = get_exif_datetime_image(path)
        if ts:
            return ts, source

    if ext in SUPPORTED_VIDEO_EXTENSIONS:
        ts, source = get_video_metadata_with_exiftool(path)
        if ts:
            return ts, source

    # Last-resort fallback. Useful, but less authoritative than EXIF.
    ts, source = get_file_modified_timestamp(path)
    return ts, source


def list_media_files(photo_dir: Path) -> list[Path]:
    allowed = SUPPORTED_IMAGE_EXTENSIONS | SUPPORTED_VIDEO_EXTENSIONS
    return sorted(
        p for p in photo_dir.iterdir()
        if p.is_file() and p.suffix.lower() in allowed
    )


def build_initial_photo_index(photo_dir: Path) -> pd.DataFrame:
    rows: list[PhotoRecord] = []

    for path in list_media_files(photo_dir):
        ext = path.suffix.lower()
        file_type = "video" if ext in SUPPORTED_VIDEO_EXTENSIONS else "image"
        ts, source = get_capture_timestamp(path)

        rows.append(
            PhotoRecord(
                filename=path.name,
                path=str(path),
                file_type=file_type,
                timestamp=ts,
                timestamp_source=source,
                meter_reading_x100=None,
                exclude=False,
                notes="",
            )
        )

    df = pd.DataFrame([r.__dict__ for r in rows])

    if df.empty:
        return pd.DataFrame(
            columns=[
                "filename",
                "path",
                "file_type",
                "timestamp",
                "timestamp_source",
                "meter_reading_x100",
                "exclude",
                "notes",
            ]
        )

    df["timestamp_sort"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values(["timestamp_sort", "filename"]).drop(columns=["timestamp_sort"])
    return df.reset_index(drop=True)


def load_or_create_review_csv(photo_dir: Path, year: int, use_review: bool) -> pd.DataFrame:
    review_path = photo_dir.parent / f"photo_review_{year}.csv"

    if use_review:
        if not review_path.exists():
            raise FileNotFoundError(
                f"Review CSV not found: {review_path}\n"
                f"Run without --use-review first to create it."
            )
        return pd.read_csv(review_path)

    df = build_initial_photo_index(photo_dir)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(review_path, index=False)

    print(f"✅ Wrote review CSV: {review_path}")
    print("   Fill in meter_reading_x100, then rerun with --use-review.")
    return df


def normalize_review_df(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "filename",
        "path",
        "file_type",
        "timestamp",
        "timestamp_source",
        "meter_reading_x100",
        "exclude",
        "notes",
    ]

    for col in required:
        if col not in df.columns:
            df[col] = pd.NA

    out = df[required].copy()

    out["timestamp_dt"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out["meter_reading_x100"] = pd.to_numeric(out["meter_reading_x100"], errors="coerce")

    exclude_raw = out["exclude"].astype("string").fillna("")

    out["exclude"] = (
        exclude_raw
        .str.strip()
        .str.lower()
        .isin(["true", "t", "1", "yes", "y"])
    )
    out["notes"] = out["notes"].fillna("").astype(str)

    out = out.loc[~out["exclude"]].copy()
    out = out.loc[out["timestamp_dt"].notna()].copy()
    out = out.sort_values(["timestamp_dt", "filename"]).reset_index(drop=True)

    return out


def suggest_pairs(df: pd.DataFrame, max_event_hours: float, min_gallons: float) -> pd.DataFrame:
    """
    Greedy pairing:
    - Sort by timestamp.
    - Treat each reading as a possible start.
    - Pair with the next later reading that increases by at least min_gallons
      and is within max_event_hours.
    - Skip readings already consumed by a pair.

    This is intentionally conservative and reviewable.
    """
    usable = df.loc[df["meter_reading_x100"].notna()].copy()
    usable = usable.sort_values(["timestamp_dt", "filename"]).reset_index(drop=True)

    pairs: list[dict[str, Any]] = []
    used: set[int] = set()
    event_num = 1

    for i, start in usable.iterrows():
        if i in used:
            continue

        best_j: int | None = None
        best_score: float | None = None

        for j in range(i + 1, len(usable)):
            if j in used:
                continue

            end = usable.iloc[j]
            hours = (end["timestamp_dt"] - start["timestamp_dt"]).total_seconds() / 3600

            if hours <= 0:
                continue
            if hours > max_event_hours:
                break

            gallons = (end["meter_reading_x100"] - start["meter_reading_x100"]) * 100.0

            if gallons < min_gallons:
                continue

            # Prefer plausible longer irrigation periods with positive gallons.
            # This score favors closer pairs but still allows multi-hour irrigations.
            score = hours

            if best_score is None or score < best_score:
                best_score = score
                best_j = j

        if best_j is None:
            continue

        end = usable.iloc[best_j]
        duration_hours = (end["timestamp_dt"] - start["timestamp_dt"]).total_seconds() / 3600
        gallons = (end["meter_reading_x100"] - start["meter_reading_x100"]) * 100.0

        pairs.append(
            {
                "suggested_event_id": f"photo_event_{event_num:03d}",
                "start_timestamp": start["timestamp"],
                "end_timestamp": end["timestamp"],
                "duration_hours": round(duration_hours, 3),
                "start_reading_x100": start["meter_reading_x100"],
                "end_reading_x100": end["meter_reading_x100"],
                "computed_gallons": gallons,
                "start_photo": start["path"],
                "end_photo": end["path"],
                "start_filename": start["filename"],
                "end_filename": end["filename"],
                "start_timestamp_source": start["timestamp_source"],
                "end_timestamp_source": end["timestamp_source"],
                "review_status": "suggested",
                "notes": "",
            }
        )

        used.add(i)
        used.add(best_j)
        event_num += 1

    return pd.DataFrame(pairs)

def enrich_with_irrigation_comparison(events_df: pd.DataFrame, irrigation_csv: Path):
    irrigation = pd.read_csv(irrigation_csv).copy()

    irrigation["start_dt"] = pd.to_datetime(irrigation["start_timestamp"], errors="coerce", format="mixed")
    irrigation["end_dt"] = pd.to_datetime(irrigation["end_timestamp"], errors="coerce", format="mixed")

    irrigation = irrigation.dropna(subset=["start_dt", "end_dt"])

    out_rows = []

    for _, ev in events_df.iterrows():
        start_dt = pd.to_datetime(ev["start_timestamp"])
        end_dt = pd.to_datetime(ev["end_timestamp"])

        best = None

        for _, ir in irrigation.iterrows():
            # compare midpoint times
            ev_mid = start_dt + (end_dt - start_dt) / 2
            ir_mid = ir["start_dt"] + (ir["end_dt"] - ir["start_dt"]) / 2

            time_diff = abs((ev_mid - ir_mid).total_seconds() / 3600)

            if best is None or time_diff < best["time_diff_hours"]:
                best = {
                    "matched_irrigation_start": ir["start_dt"].strftime("%Y-%m-%dT%H:%M"),
                    "matched_irrigation_end": ir["end_dt"].strftime("%Y-%m-%dT%H:%M"),
                    "reported_gallons": ir.get("gallons"),
                    "time_diff_hours": round(time_diff, 2),
                    "matched_strip_group": ir.get("strip_group", ""),
                    "matched_location": ir.get("location", ""),
                }

        row = ev.to_dict()

        if best:
            row.update(best)

            if pd.notna(row["computed_gallons"]) and pd.notna(best["reported_gallons"]):
                row["gallons_difference"] = row["computed_gallons"] - best["reported_gallons"]
                row["gallons_ratio"] = (
                    round(row["computed_gallons"] / best["reported_gallons"], 3)
                    if best["reported_gallons"]
                    else None
                )
        else:
            row.update({
                "matched_irrigation_start": "",
                "matched_irrigation_end": "",
                "reported_gallons": None,
                "time_diff_hours": None,
                "gallons_difference": None,
                "gallons_ratio": None,
            })

        out_rows.append(row)

    return pd.DataFrame(out_rows)

def suggest_pairs_from_known_irrigation_events(
        photo_df: pd.DataFrame,
        irrigation_csv: Path,
        max_start_window_hours: float = 4.0,
        max_end_window_hours: float = 4.0,
) -> pd.DataFrame:
    """
    Event-driven matching.

    For each known irrigation row, find the photo start/end pair that best matches:
    - known irrigation start/end timestamps
    - reported gallons
    - increasing meter reading

    This avoids overlapping photo-driven pairs.
    """
    irrigation = pd.read_csv(irrigation_csv).copy()

    irrigation["start_dt"] = pd.to_datetime(
        irrigation["start_timestamp"],
        errors="coerce",
        format="mixed",
    )
    irrigation["end_dt"] = pd.to_datetime(
        irrigation["end_timestamp"],
        errors="coerce",
        format="mixed",
    )
    irrigation["gallons"] = pd.to_numeric(irrigation["gallons"], errors="coerce")

    irrigation = irrigation.loc[
        irrigation["start_dt"].notna()
        & irrigation["end_dt"].notna()
        & irrigation["gallons"].notna()
        ].copy()

    photos = photo_df.copy()
    photos["timestamp_dt"] = pd.to_datetime(
        photos["timestamp"],
        errors="coerce",
        format="mixed",
    )
    photos["meter_reading_x100"] = pd.to_numeric(
        photos["meter_reading_x100"],
        errors="coerce",
    )

    photos = photos.loc[
        photos["timestamp_dt"].notna()
        & photos["meter_reading_x100"].notna()
        ].copy()

    rows: list[dict[str, Any]] = []

    for idx, ev in irrigation.reset_index(drop=True).iterrows():
        ev_start = ev["start_dt"]
        ev_end = ev["end_dt"]
        reported_gallons = float(ev["gallons"])

        start_candidates = photos.loc[
            (photos["timestamp_dt"] >= ev_start - pd.Timedelta(hours=max_start_window_hours))
            & (photos["timestamp_dt"] <= ev_start + pd.Timedelta(hours=max_start_window_hours))
            ].copy()

        end_candidates = photos.loc[
            (photos["timestamp_dt"] >= ev_end - pd.Timedelta(hours=max_end_window_hours))
            & (photos["timestamp_dt"] <= ev_end + pd.Timedelta(hours=max_end_window_hours))
            ].copy()

        best: dict[str, Any] | None = None

        for _, start_photo in start_candidates.iterrows():
            for _, end_photo in end_candidates.iterrows():
                if end_photo["timestamp_dt"] <= start_photo["timestamp_dt"]:
                    continue

                computed_gallons = (
                                           end_photo["meter_reading_x100"] - start_photo["meter_reading_x100"]
                                   ) * 100.0

                if computed_gallons <= 0:
                    continue

                gallons_difference = computed_gallons - reported_gallons
                gallons_ratio = computed_gallons / reported_gallons if reported_gallons else None

                start_diff_hours = abs(
                    (start_photo["timestamp_dt"] - ev_start).total_seconds() / 3600
                )
                end_diff_hours = abs(
                    (end_photo["timestamp_dt"] - ev_end).total_seconds() / 3600
                )

                # Lower score is better.
                # Gallons match matters most; timestamp closeness is secondary.
                score = abs(gallons_difference) + (start_diff_hours + end_diff_hours) * 1000

                candidate = {
                    "matched_strip_group": ev.get("strip_group", ""),
                    "matched_location": ev.get("location", ""),
                    "suggested_event_id": f"known_event_{idx + 1:03d}",
                    "start_timestamp": start_photo["timestamp"],
                    "end_timestamp": end_photo["timestamp"],
                    "duration_hours": round(
                        (end_photo["timestamp_dt"] - start_photo["timestamp_dt"]).total_seconds() / 3600,
                        3,
                        ),
                    "start_reading_x100": start_photo["meter_reading_x100"],
                    "end_reading_x100": end_photo["meter_reading_x100"],
                    "computed_gallons": computed_gallons,
                    "reported_gallons": reported_gallons,
                    "gallons_difference": round(gallons_difference, 0),
                    "gallons_ratio": round(gallons_ratio, 3) if gallons_ratio is not None else None,
                    "time_diff_hours": round((start_diff_hours + end_diff_hours) / 2, 2),
                    "start_time_diff_hours": round(start_diff_hours, 2),
                    "end_time_diff_hours": round(end_diff_hours, 2),
                    "matched_irrigation_start": ev_start.strftime("%Y-%m-%dT%H:%M"),
                    "matched_irrigation_end": ev_end.strftime("%Y-%m-%dT%H:%M"),
                    "start_photo": start_photo["path"],
                    "end_photo": end_photo["path"],
                    "start_filename": start_photo["filename"],
                    "end_filename": end_photo["filename"],
                    "start_timestamp_source": start_photo["timestamp_source"],
                    "end_timestamp_source": end_photo["timestamp_source"],
                    "review_status": "event_driven_suggested",
                    "notes": "",
                    "_score": score,
                }

                if best is None or candidate["_score"] < best["_score"]:
                    best = candidate

        if best is not None:
            best.pop("_score", None)
            rows.append(best)
        else:
            rows.append({
                "matched_strip_group": ev.get("strip_group", ""),
                "matched_location": ev.get("location", ""),
                "suggested_event_id": f"known_event_{idx + 1:03d}",
                "start_timestamp": "",
                "end_timestamp": "",
                "duration_hours": "",
                "start_reading_x100": "",
                "end_reading_x100": "",
                "computed_gallons": "",
                "reported_gallons": reported_gallons,
                "gallons_difference": "",
                "gallons_ratio": "",
                "time_diff_hours": "",
                "start_time_diff_hours": "",
                "end_time_diff_hours": "",
                "matched_irrigation_start": ev_start.strftime("%Y-%m-%dT%H:%M"),
                "matched_irrigation_end": ev_end.strftime("%Y-%m-%dT%H:%M"),
                "start_photo": "",
                "end_photo": "",
                "start_filename": "",
                "end_filename": "",
                "start_timestamp_source": "",
                "end_timestamp_source": "",
                "review_status": "no_photo_pair_found",
                "notes": "No valid start/end photo pair found within search windows.",
            })

    return pd.DataFrame(rows)


def append_unmatched_rows(events_df: pd.DataFrame, photo_df: pd.DataFrame) -> pd.DataFrame:
    events_df = events_df.copy()

    if "start_filename" in events_df.columns:
        start_files = set(events_df["start_filename"].dropna())
    else:
        start_files = set()

    if "end_filename" in events_df.columns:
        end_files = set(events_df["end_filename"].dropna())
    else:
        end_files = set()

    used_files = start_files | end_files
    unmatched = photo_df[~photo_df["filename"].isin(used_files)].copy()

    for col in [
        "photo_timestamp",
        "photo_meter_reading_x100",
        "photo_filename",
        "photo_path",
        "photo_timestamp_source",
        "photo_file_type",
    ]:
        if col not in events_df.columns:
            events_df[col] = ""

    if unmatched.empty:
        return events_df

    rows = []
    for _, row in unmatched.iterrows():
        file_type = str(row.get("file_type", "") or "").strip().lower()
        timestamp_source = str(row.get("timestamp_source", "") or "").strip()

        is_video = file_type == "video"

        if is_video:
            review_status = "video_unmatched"
            notes = (
                "video; possible mid-event/test; "
                f"timestamp source: {timestamp_source or 'unknown'}"
            )
        else:
            review_status = "unmatched_photo"
            notes = ""

        rows.append({
            "matched_strip_group": "",
            "matched_location": "",
            "suggested_event_id": "",
            "start_timestamp": "",
            "end_timestamp": "",
            "duration_hours": "",
            "start_reading_x100": "",
            "end_reading_x100": "",
            "computed_gallons": "",
            "reported_gallons": "",
            "gallons_difference": "",
            "gallons_ratio": "",
            "time_diff_hours": "",
            "matched_irrigation_start": "",
            "matched_irrigation_end": "",
            "start_photo": "",
            "end_photo": "",
            "start_filename": "",
            "end_filename": "",
            "start_timestamp_source": "",
            "end_timestamp_source": "",
            "review_status": review_status,
            "notes": notes,

            # Photo-specific review fields
            "photo_timestamp": row.get("timestamp", ""),
            "photo_meter_reading_x100": row.get("meter_reading_x100", ""),
            "photo_filename": row.get("filename", ""),
            "photo_path": row.get("path", ""),
            "photo_timestamp_source": timestamp_source,
            "photo_file_type": file_type,
        })

    unmatched_df = pd.DataFrame(rows)
    return pd.concat([events_df, unmatched_df], ignore_index=True)

def attach_irrigation_matches(
        photo_df: pd.DataFrame,
        irrigation_csv: Path,
        max_match_minutes: float = 90.0,
) -> pd.DataFrame:
    irrigation = pd.read_csv(irrigation_csv).copy()

    irrigation["start_dt"] = pd.to_datetime(
        irrigation["start_timestamp"],
        errors="coerce",
        format="mixed",
    )
    irrigation["end_dt"] = pd.to_datetime(
        irrigation["end_timestamp"],
        errors="coerce",
        format="mixed",
    )

    irrigation = irrigation.loc[
        irrigation["start_dt"].notna() & irrigation["end_dt"].notna()
        ].copy()

    irrigation["event_row_id"] = [
        f"{int(row.year)}_{row.date}_{row.strip_group}_{i:03d}"
        for i, row in irrigation.reset_index(drop=True).iterrows()
    ]

    photo_df = photo_df.copy()
    photo_df["photo_dt"] = pd.to_datetime(
        photo_df["timestamp"],
        errors="coerce",
        format="mixed",
    )

    rows = []

    for _, photo in photo_df.iterrows():
        best = None

        for _, ev in irrigation.iterrows():
            if pd.isna(photo["photo_dt"]):
                continue

            start_diff = abs((photo["photo_dt"] - ev["start_dt"]).total_seconds() / 60)
            end_diff = abs((photo["photo_dt"] - ev["end_dt"]).total_seconds() / 60)

            if start_diff <= end_diff:
                role = "start"
                diff = start_diff
                matched_time = ev["start_dt"]
            else:
                role = "end"
                diff = end_diff
                matched_time = ev["end_dt"]

            if best is None or diff < best["matched_time_diff_minutes"]:
                best = {
                    "matched_event_id": ev["event_row_id"],
                    "matched_strip_group": ev.get("strip_group", ""),
                    "matched_location": ev.get("location", ""),
                    "matched_role": role,
                    "matched_time_diff_minutes": round(float(diff), 2),
                    "event_start_timestamp": ev["start_dt"].strftime("%Y-%m-%dT%H:%M"),
                    "event_end_timestamp": ev["end_dt"].strftime("%Y-%m-%dT%H:%M"),
                    "event_gallons": ev.get("gallons", pd.NA),
                    "expected_reading_delta_x100": (
                            pd.to_numeric(ev.get("gallons"), errors="coerce") / 100.0
                    ),
                    "matched_event_time": matched_time.strftime("%Y-%m-%dT%H:%M"),
                }

        row = photo.to_dict()

        if best is None or best["matched_time_diff_minutes"] > max_match_minutes:
            row.update(
                {
                    "matched_event_id": "",
                    "matched_strip_group": "",
                    "matched_location": "",
                    "matched_role": "",
                    "matched_time_diff_minutes": pd.NA,
                    "event_start_timestamp": "",
                    "event_end_timestamp": "",
                    "event_gallons": pd.NA,
                    "expected_reading_delta_x100": pd.NA,
                    "matched_event_time": "",
                    "has_photo_evidence": False,
                    "review_needed": True,
                }
            )
        else:
            row.update(best)
            row["has_photo_evidence"] = True
            row["review_needed"] = best["matched_time_diff_minutes"] > 15

        rows.append(row)

    out = pd.DataFrame(rows)

    if "photo_dt" in out.columns:
        out = out.drop(columns=["photo_dt"])

    return out


def main() -> int:
    args = parse_args()

    photo_dir: Path = args.photo_dir
    if not photo_dir.exists():
        raise FileNotFoundError(f"Photo directory not found: {photo_dir}")

    df_review = load_or_create_review_csv(
        photo_dir=photo_dir,
        year=args.year,
        use_review=args.use_review,
    )

    if args.match_photos:
        df_review = attach_irrigation_matches(
            photo_df=df_review,
            irrigation_csv=args.irrigation_csv,
            max_match_minutes=args.max_match_minutes,
        )

        review_path = photo_dir.parent / f"photo_review_{args.year}.csv"
        df_review.to_csv(review_path, index=False)
        print(f"✅ Updated review CSV with irrigation matches: {review_path}")

    if not args.use_review:
        return 0

    df_norm = normalize_review_df(df_review)

    index_out = photo_dir.parent / f"photo_index_{args.year}.csv"
    df_norm.drop(columns=["timestamp_dt"]).to_csv(index_out, index=False)
    print(f"✅ Wrote normalized photo index: {index_out}")

    pairs = suggest_pairs_from_known_irrigation_events(
        photo_df=df_norm,
        irrigation_csv=args.irrigation_csv,
    )

    pairs = append_unmatched_rows(pairs, df_norm)

    pairs_out = photo_dir.parent / f"suggested_irrigation_events_{args.year}.csv"
    preferred_order = [
        "matched_strip_group",
        "matched_location",
        "suggested_event_id",
        "start_timestamp",
        "end_timestamp",
        "duration_hours",
        "start_reading_x100",
        "end_reading_x100",
        "computed_gallons",
        "reported_gallons",
        "gallons_difference",
        "gallons_ratio",
        "time_diff_hours",
        "matched_irrigation_start",
        "matched_irrigation_end",
        ...
    ]

    # Keep only columns that exist, preserve extras at end
    cols = [c for c in preferred_order if c in pairs.columns] + \
           [c for c in pairs.columns if c not in preferred_order]

    pairs = pairs[cols]

    pairs.to_csv(pairs_out, index=False)

    print(f"✅ Wrote suggested event pairs: {pairs_out}")
    n_suggested = int((pairs["review_status"] == "event_driven_suggested").sum())
    n_unmatched = int((pairs["review_status"] == "unmatched_photo").sum())
    n_video = int((pairs["review_status"] == "video_unmatched").sum())

    print(f"Suggested event pairs: {n_suggested}")
    print(f"Unmatched photos: {n_unmatched}")
    print(f"Unmatched videos: {n_video}")
    print(f"Total review rows: {len(pairs)}")

    if pairs.empty:
        print(
            "\nNo pairs suggested. Check that meter_reading_x100 values are filled in "
            "and that max_event_hours/min_gallons are reasonable."
        )

    return 0


def detect_irrigation_events_from_meter(df: pd.DataFrame,
                                        min_event_gallons: float = 20000,
                                        max_gap_minutes: float = 180) -> pd.DataFrame:
    """
    Detect irrigation events based purely on meter reading increases.

    Parameters:
    - min_event_gallons: minimum gallons to count as irrigation
    - max_gap_minutes: max time gap to group readings into same event
    """

    df = df.copy()

    df["timestamp_dt"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp_dt").reset_index(drop=True)

    df["reading"] = pd.to_numeric(df["meter_reading_x100"], errors="coerce")
    df = df[df["reading"].notna()].copy()

    df["delta_x100"] = df["reading"].diff()
    df["delta_gallons"] = df["delta_x100"] * 100

    events = []
    current_event = None

    for i, row in df.iterrows():
        if pd.isna(row["delta_gallons"]):
            continue

        if row["delta_gallons"] > 0:
            # Potential irrigation activity

            if current_event is None:
                # start new event
                current_event = {
                    "start_idx": i - 1,
                    "end_idx": i,
                    "total_gallons": row["delta_gallons"],
                }
            else:
                # check time gap
                prev_time = df.loc[current_event["end_idx"], "timestamp_dt"]
                gap_minutes = (row["timestamp_dt"] - prev_time).total_seconds() / 60

                if gap_minutes <= max_gap_minutes:
                    # continue same event
                    current_event["end_idx"] = i
                    current_event["total_gallons"] += row["delta_gallons"]
                else:
                    # close previous event
                    if current_event["total_gallons"] >= min_event_gallons:
                        events.append(current_event)

                    # start new event
                    current_event = {
                        "start_idx": i - 1,
                        "end_idx": i,
                        "total_gallons": row["delta_gallons"],
                    }

    # finalize last event
    if current_event and current_event["total_gallons"] >= min_event_gallons:
        events.append(current_event)

    # build output
    rows = []

    for idx, ev in enumerate(events, 1):
        start_row = df.loc[ev["start_idx"]]
        end_row = df.loc[ev["end_idx"]]

        rows.append({
            "event_id": f"meter_event_{idx:03d}",
            "start_timestamp": start_row["timestamp"],
            "end_timestamp": end_row["timestamp"],
            "start_reading_x100": start_row["reading"],
            "end_reading_x100": end_row["reading"],
            "computed_gallons": ev["total_gallons"],
            "start_photo": start_row["filename"],
            "end_photo": end_row["filename"],
        })

    return pd.DataFrame(rows)

if __name__ == "__main__":
    raise SystemExit(main())