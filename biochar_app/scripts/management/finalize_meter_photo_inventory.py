#!/usr/bin/env python3
"""Merge reviewed meter readings and build the unique photo inventory.

Script metadata
---------------
Purpose:
    Reproducibly convert reviewed meter-photo workbooks plus the canonical
    metadata inventory into ``photo_inventory_unique.csv``.
Primary inputs:
    ``photo_inventory.csv``, the main meter-review workbook, the
    supplemental flat review workbook, the full-resolution follow-up workbook,
    and an optional corrections ledger.
Primary outputs:
    Unique inventory CSV, keep/exclude manifest CSV, and audit JSON.
Authority order:
    Explicit correction -> readable exact main-workbook entry -> unambiguous
    exact supplemental-workbook entry -> unambiguous readable main-workbook
    photo-family consensus -> unambiguous readable main-workbook capture-time
    consensus -> full-resolution follow-up -> existing inventory value.
Safety:
    Input files are read-only. The optional unique-photo directory must not
    already exist, preventing accidental mixing with an earlier run.
Introduced:
    2026-07-23, following recovery and review of irrigation meter photographs.
Maintainer:
    Biochar Water Conservation project (Gerald Nelson).
Version:
    0.1.0
Example:
    Run from the repository root (replace workbook names or paths if needed)::

        python biochar_app/scripts/management/finalize_meter_photo_inventory.py \
          --inventory-csv biochar_app/data-processed/management/irrigation/photos/photo_inventory.csv \
          --main-workbook biochar_app/data-processed/management/irrigation/photos/meter_photo_review.xlsx \
          --supplemental-workbook biochar_app/data-processed/management/irrigation/photos/meter_photo_unresolved_review.xlsx \
          --corrections-csv biochar_app/data-processed/management/irrigation/photos/meter_photo_reading_corrections.csv \
          --output-csv biochar_app/data-processed/management/irrigation/photos/photo_inventory_unique.csv \
          --manifest-csv biochar_app/data-processed/management/irrigation/photos/photo_inventory_unique_manifest.csv \
          --transfer-audit-csv biochar_app/data-processed/management/irrigation/photos/photo_inventory_reading_transfer_audit.csv \
          --audit-json biochar_app/data-processed/management/irrigation/photos/photo_inventory_unique_audit.json

This is the reproducible bridge between:

* photo_inventory.csv (metadata for every archived photo),
* the main meter-review workbook,
* the supplemental flat review workbook,
* the full-resolution follow-up workbook, and
* an explicit correction ledger.

The script never edits its inputs. It writes a unique inventory, a complete
selection manifest, and a machine-readable audit report.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from openpyxl import load_workbook


__version__ = "0.1.0"

EXPECTED_READING_DIGITS = 6
READABLE_STATUSES = {"readable"}
RENDITION_SUFFIX = re.compile(r"_(?:1_102_o|1_102_a|1_105_c|4_5005_c)$", re.I)


@dataclass(frozen=True)
class ReviewReading:
    sha256: str
    filename: str
    reading: str
    status: str
    notes: str
    source: str
    photo_datetime: str = ""


def clean(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "<na>"}:
        return ""
    return text


def normalize_reading(value: object) -> str:
    text = clean(value)
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    return text


def normalize_photo_datetime(value: object) -> str:
    """Return a capture timestamp normalized to second precision.

    Excel cells may arrive as ``datetime`` objects, while the inventory uses
    ISO-like strings and ExifTool may use ``YYYY:MM:DD``.  Time-zone suffixes
    are intentionally omitted because the review workbook stores local field
    time without a zone.  The transfer rule only uses exact local-second
    agreement and rejects targets explicitly marked low confidence.
    """

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d 00:00:00")

    text = clean(value).replace("T", " ")
    match = re.match(
        r"^(\d{4})[-:](\d{2})[-:](\d{2})[ ](\d{2}):(\d{2}):(\d{2})",
        text,
    )
    if not match:
        return ""
    year, month, day, hour, minute, second = match.groups()
    return f"{year}-{month}-{day} {hour}:{minute}:{second}"


def valid_reading(value: object) -> bool:
    return bool(re.fullmatch(rf"\d{{{EXPECTED_READING_DIGITS}}}", normalize_reading(value)))


def photo_family(filename: str) -> str:
    stem = Path(clean(filename)).stem
    return RENDITION_SUFFIX.sub("", stem).casefold()


def integer(value: object) -> int:
    try:
        return int(float(clean(value)))
    except (TypeError, ValueError):
        return 0


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), [dict(row) for row in reader]


def write_csv_rows(path: Path, fieldnames: list[str], rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def workbook_rows(
    path: Path,
    sheet_name: str,
    *,
    header_row: int = 1,
) -> list[dict[str, object]]:
    workbook = load_workbook(path, data_only=True, read_only=True)
    if sheet_name not in workbook.sheetnames:
        raise KeyError(f"{path.name} does not contain sheet {sheet_name!r}")
    sheet = workbook[sheet_name]
    values = sheet.iter_rows(min_row=header_row, values_only=True)
    headers = [clean(value) for value in next(values)]
    return [dict(zip(headers, row)) for row in values]


def read_main_review(path: Path) -> list[ReviewReading]:
    rows = workbook_rows(path, "Meter Review")
    required = {"Filename", "SHA-256", "Manual Reading", "Status", "Notes"}
    missing = required.difference(rows[0] if rows else {})
    if missing:
        raise KeyError(f"Main review workbook is missing columns: {sorted(missing)}")
    return [
        ReviewReading(
            sha256=clean(row["SHA-256"]),
            filename=clean(row["Filename"]),
            reading=normalize_reading(row["Manual Reading"]),
            status=clean(row["Status"]).casefold(),
            notes=clean(row["Notes"]),
            source="main_review",
            photo_datetime=normalize_photo_datetime(row.get("Photo Date/Time")),
        )
        for row in rows
        if clean(row["SHA-256"])
    ]


def read_supplemental_review(path: Path) -> list[ReviewReading]:
    """Read the compact unresolved-photo review workbook.

    The workbook places its tabular header on row 6 because rows 1-4 contain
    the title, instructions, and completion counter. Exact SHA-256 matches are
    required; no filename or timestamp propagation is performed from this
    supplemental source.
    """

    rows = workbook_rows(path, "Review", header_row=6)
    required = {"Filename", "SHA-256", "Manual reading", "Status", "Notes"}
    missing = required.difference(rows[0] if rows else {})
    if missing:
        raise KeyError(
            f"Supplemental review workbook is missing columns: {sorted(missing)}"
        )

    result: list[ReviewReading] = []
    allowed_statuses = READABLE_STATUSES | {
        "uncertain", "unreadable", "not_meter_reading",
    }
    for row in rows:
        sha256 = clean(row.get("SHA-256"))
        if not sha256:
            continue
        status = clean(row.get("Status")).casefold()
        reading = normalize_reading(row.get("Manual reading"))
        if status not in allowed_statuses:
            raise ValueError(
                f"Invalid supplemental status for {row.get('Filename')}: {status!r}"
            )
        if status in READABLE_STATUSES and not valid_reading(reading):
            raise ValueError(
                f"Readable supplemental row does not have a six-digit reading "
                f"for {row.get('Filename')}: {reading!r}"
            )
        result.append(
            ReviewReading(
                sha256=sha256,
                filename=clean(row.get("Filename")),
                reading=reading,
                status=status,
                notes=clean(row.get("Notes")),
                source="supplemental_review",
                photo_datetime=normalize_photo_datetime(row.get("Photo date/time")),
            )
        )
    return result


def read_full_resolution_review(path: Path) -> list[ReviewReading]:
    workbook = load_workbook(path, data_only=True, read_only=True)
    rows: list[ReviewReading] = []
    for sheet_name in workbook.sheetnames:
        if not sheet_name.startswith("Photo "):
            continue
        sheet = workbook[sheet_name]
        sha256 = clean(sheet["B6"].value)
        if not sha256:
            continue
        rows.append(
            ReviewReading(
                sha256=sha256,
                filename=clean(sheet["B5"].value),
                reading=normalize_reading(sheet["B11"].value),
                status=clean(sheet["B12"].value).casefold(),
                notes=clean(sheet["B13"].value),
                source="full_resolution_followup",
            )
        )
    return rows


def read_corrections(path: Path | None) -> dict[str, ReviewReading]:
    if path is None or not path.exists():
        return {}
    _, rows = read_csv_rows(path)
    required = {"sha256", "filename", "corrected_reading", "reason"}
    missing = required.difference(rows[0] if rows else {})
    if missing:
        raise KeyError(f"Correction ledger is missing columns: {sorted(missing)}")
    result: dict[str, ReviewReading] = {}
    for row in rows:
        reading = normalize_reading(row["corrected_reading"])
        if not valid_reading(reading):
            raise ValueError(f"Invalid correction reading for {row['filename']}: {reading!r}")
        result[clean(row["sha256"])] = ReviewReading(
            sha256=clean(row["sha256"]),
            filename=clean(row["filename"]),
            reading=reading,
            status="readable",
            notes=clean(row["reason"]),
            source="explicit_correction",
        )
    return result


def main_family_consensus(
    main_rows: list[ReviewReading],
) -> tuple[dict[str, str], dict[str, list[str]]]:
    readings: dict[str, set[str]] = defaultdict(set)
    for row in main_rows:
        if row.status in READABLE_STATUSES and valid_reading(row.reading):
            readings[photo_family(row.filename)].add(row.reading)
    ambiguous = {
        family: sorted(values)
        for family, values in readings.items()
        if len(values) > 1
    }
    consensus = {
        family: next(iter(values))
        for family, values in readings.items()
        if len(values) == 1
    }
    return consensus, ambiguous


def main_timestamp_consensus(
    main_rows: list[ReviewReading],
) -> tuple[
    dict[str, str],
    dict[str, list[str]],
    dict[str, list[dict[str, str]]],
]:
    """Return unambiguous reviewed readings keyed by exact capture second."""

    readings: dict[str, set[str]] = defaultdict(set)
    evidence: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in main_rows:
        timestamp = normalize_photo_datetime(row.photo_datetime)
        if (
            timestamp
            and row.status in READABLE_STATUSES
            and valid_reading(row.reading)
        ):
            readings[timestamp].add(row.reading)
            evidence[timestamp].append(
                {
                    "sha256": row.sha256,
                    "filename": row.filename,
                    "reading": row.reading,
                }
            )

    ambiguous = {
        timestamp: sorted(values)
        for timestamp, values in readings.items()
        if len(values) > 1
    }
    consensus = {
        timestamp: next(iter(values))
        for timestamp, values in readings.items()
        if len(values) == 1
    }
    consensus_evidence = {
        timestamp: evidence[timestamp]
        for timestamp in consensus
    }
    return consensus, ambiguous, consensus_evidence


def choose_reading(
    inventory_row: dict[str, str],
    main_by_sha: dict[str, ReviewReading],
    supplemental_by_sha: dict[str, ReviewReading],
    followup_by_sha: dict[str, ReviewReading],
    corrections: dict[str, ReviewReading],
    family_consensus: dict[str, str],
    timestamp_consensus: dict[str, str],
) -> tuple[str, str, str, list[dict[str, str]]]:
    sha256 = clean(inventory_row.get("sha256"))
    filename = clean(inventory_row.get("filename"))
    family = photo_family(filename)
    timestamp = normalize_photo_datetime(inventory_row.get("effective_datetime"))
    timestamp_confidence = clean(
        inventory_row.get("timestamp_confidence")
    ).casefold()
    main_row = main_by_sha.get(sha256)
    supplemental_row = supplemental_by_sha.get(sha256)
    followup_row = followup_by_sha.get(sha256)
    correction = corrections.get(sha256)
    conflicts: list[dict[str, str]] = []

    if correction is not None:
        return correction.reading, "readable", correction.source, conflicts

    if main_row and main_row.status in READABLE_STATUSES and valid_reading(main_row.reading):
        selected = main_row
    elif supplemental_row is not None:
        if supplemental_row.status in READABLE_STATUSES:
            selected = supplemental_row
        else:
            return "", supplemental_row.status, supplemental_row.source, conflicts
    elif family in family_consensus:
        selected = ReviewReading(
            sha256=sha256,
            filename=filename,
            reading=family_consensus[family],
            status="readable",
            notes="Propagated from readable main-workbook rendition",
            source="main_review_family_consensus",
        )
    elif (
        timestamp
        and timestamp_confidence != "low"
        and timestamp in timestamp_consensus
    ):
        selected = ReviewReading(
            sha256=sha256,
            filename=filename,
            reading=timestamp_consensus[timestamp],
            status="readable",
            notes="Propagated from exact capture-time consensus",
            source="main_review_timestamp_consensus",
            photo_datetime=timestamp,
        )
    elif followup_row and valid_reading(followup_row.reading):
        selected = followup_row
    elif main_row and valid_reading(main_row.reading):
        selected = main_row
    else:
        existing = normalize_reading(inventory_row.get("meter_reading"))
        status = clean(inventory_row.get("review_status")).casefold()
        return existing, status, "existing_inventory", conflicts

    if (
        followup_row
        and valid_reading(followup_row.reading)
        and followup_row.reading != selected.reading
        and selected.source != "full_resolution_followup"
    ):
        conflicts.append(
            {
                "sha256": sha256,
                "filename": filename,
                "selected_reading": selected.reading,
                "selected_source": selected.source,
                "suppressed_followup_reading": followup_row.reading,
            }
        )
    return selected.reading, selected.status or "readable", selected.source, conflicts


def preferred_record(rows: list[dict[str, str]]) -> dict[str, str]:
    return sorted(
        rows,
        key=lambda row: (
            -(integer(row.get("image_width")) * integer(row.get("image_height"))),
            -integer(row.get("file_size_bytes")),
            duplicate_filename_preference(clean(row.get("filename"))),
            clean(row.get("filename")).casefold(),
        ),
    )[0]


def duplicate_filename_preference(filename: str) -> int:
    """Prefer camera/date/descriptive names over UUID rendition names."""
    name = Path(filename).name
    stem = Path(filename).stem
    if re.match(r"^IMG[_-]?\d+", name, re.I):
        return 0
    if re.match(r"^\d{4}-\d{2}-\d{2}", name):
        return 1
    if not re.match(
        r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-"
        r"[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}",
        stem,
    ):
        return 2
    if stem.casefold().endswith("_1_102_o"):
        return 3
    if stem.casefold().endswith("_1_105_c"):
        return 4
    if stem.casefold().endswith("_4_5005_c"):
        return 5
    return 6


def finalize_inventory(
    inventory_csv: Path,
    main_workbook: Path,
    full_resolution_workbook: Path | None,
    corrections_csv: Path | None,
    output_csv: Path,
    manifest_csv: Path,
    audit_json: Path,
    supplemental_workbook: Path | None = None,
    transfer_audit_csv: Path | None = None,
    copy_unique_dir: Path | None = None,
    originals_dir: Path | None = None,
) -> dict[str, object]:
    fieldnames, inventory = read_csv_rows(inventory_csv)
    required = {
        "filename", "sha256", "effective_datetime", "meter_reading",
        "review_status", "image_width", "image_height", "file_size_bytes",
    }
    missing = required.difference(fieldnames)
    if missing:
        raise KeyError(f"Inventory is missing columns: {sorted(missing)}")

    main_rows = read_main_review(main_workbook)
    supplemental_rows = (
        read_supplemental_review(supplemental_workbook)
        if supplemental_workbook is not None
        else []
    )
    followup_rows = (
        read_full_resolution_review(full_resolution_workbook)
        if full_resolution_workbook is not None
        else []
    )
    main_by_sha = {row.sha256: row for row in main_rows}
    supplemental_by_sha = {row.sha256: row for row in supplemental_rows}
    followup_by_sha = {row.sha256: row for row in followup_rows}
    corrections = read_corrections(corrections_csv)
    family_consensus, ambiguous_main_families = main_family_consensus(main_rows)
    (
        timestamp_consensus,
        ambiguous_main_timestamps,
        timestamp_consensus_evidence,
    ) = main_timestamp_consensus(main_rows)

    source_counts: dict[str, int] = defaultdict(int)
    suppressed_conflicts: list[dict[str, str]] = []
    timestamp_transfers: list[dict[str, object]] = []
    merged: list[dict[str, str]] = []
    for source_row in inventory:
        row = dict(source_row)
        reading, status, source, conflicts = choose_reading(
            row,
            main_by_sha,
            supplemental_by_sha,
            followup_by_sha,
            corrections,
            family_consensus,
            timestamp_consensus,
        )
        row["meter_reading"] = reading
        row["review_status"] = status
        row["meter_reading_source"] = source
        if "meter_reading_source" not in fieldnames:
            fieldnames.append("meter_reading_source")
        source_counts[source] += 1
        if source == "main_review_timestamp_consensus":
            timestamp = normalize_photo_datetime(row.get("effective_datetime"))
            timestamp_transfers.append(
                {
                    "target_sha256": clean(row.get("sha256")),
                    "target_filename": clean(row.get("filename")),
                    "target_timestamp": timestamp,
                    "transferred_reading": reading,
                    "target_timestamp_source": clean(row.get("timestamp_source")),
                    "target_timestamp_confidence": clean(
                        row.get("timestamp_confidence")
                    ),
                    "review_evidence": timestamp_consensus_evidence[timestamp],
                }
            )
        suppressed_conflicts.extend(conflicts)
        merged.append(row)

    duplicate_groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in merged:
        timestamp = clean(row.get("effective_datetime"))
        reading = normalize_reading(row.get("meter_reading"))
        if timestamp and valid_reading(reading):
            duplicate_groups[(timestamp, reading)].append(row)

    # Select individual rows, not checksums. Exact-byte duplicate files share a
    # checksum, so checksum selection would inadvertently keep every copy.
    selected_row_ids: set[int] = set()
    group_details: dict[int, dict[str, str]] = {}
    group_number = 0
    for key, rows in sorted(duplicate_groups.items()):
        if len(rows) == 1:
            selected_row_ids.add(id(rows[0]))
            continue
        group_number += 1
        selected = preferred_record(rows)
        selected_row_ids.add(id(selected))
        group_id = f"DUP-{group_number:03d}"
        for row in rows:
            group_details[id(row)] = {
                "duplicate_group": group_id,
                "selected_filename": clean(selected["filename"]),
                "action": "kept" if row is selected else "excluded_from_unique",
            }

    for row in merged:
        reading = normalize_reading(row.get("meter_reading"))
        timestamp = clean(row.get("effective_datetime"))
        if not (timestamp and valid_reading(reading)):
            selected_row_ids.add(id(row))

    selected = [row for row in merged if id(row) in selected_row_ids]
    manifest_fields = [
        "action", "duplicate_group", "selected_filename",
        "effective_datetime", "meter_reading", "meter_reading_source",
        "filename", "sha256", "image_width", "image_height", "file_size_bytes",
    ]
    manifest = []
    for row in merged:
        detail = group_details.get(id(row), {})
        manifest.append(
            {
                "action": detail.get("action", "kept"),
                "duplicate_group": detail.get("duplicate_group", ""),
                "selected_filename": detail.get("selected_filename", clean(row["filename"])),
                **row,
            }
        )

    write_csv_rows(output_csv, fieldnames, selected)
    write_csv_rows(manifest_csv, manifest_fields, manifest)

    transfer_audit_fields = [
        "target_filename",
        "target_sha256",
        "target_timestamp",
        "transferred_reading",
        "target_timestamp_source",
        "target_timestamp_confidence",
        "review_evidence_count",
        "review_filenames",
        "review_sha256s",
    ]
    transfer_audit_rows: list[dict[str, object]] = []
    for transfer in timestamp_transfers:
        evidence = transfer["review_evidence"]
        transfer_audit_rows.append(
            {
                **transfer,
                "review_evidence_count": len(evidence),
                "review_filenames": " | ".join(
                    clean(item["filename"]) for item in evidence
                ),
                "review_sha256s": " | ".join(
                    clean(item["sha256"]) for item in evidence
                ),
            }
        )
    if transfer_audit_csv is not None:
        write_csv_rows(
            transfer_audit_csv,
            transfer_audit_fields,
            transfer_audit_rows,
        )

    copied = 0
    if copy_unique_dir is not None:
        if originals_dir is None:
            raise ValueError("--copy-unique-dir requires --originals-dir")
        copy_unique_dir.mkdir(parents=True, exist_ok=False)
        for row in selected:
            shutil.copy2(originals_dir / row["filename"], copy_unique_dir / row["filename"])
            copied += 1

    audit = {
        "inputs": {
            "inventory_csv": str(inventory_csv),
            "main_workbook": str(main_workbook),
            "supplemental_workbook": (
                str(supplemental_workbook)
                if supplemental_workbook is not None
                else None
            ),
            "full_resolution_workbook": (
                str(full_resolution_workbook)
                if full_resolution_workbook is not None
                else None
            ),
            "corrections_csv": str(corrections_csv) if corrections_csv else None,
        },
        "outputs": {
            "unique_inventory_csv": str(output_csv),
            "manifest_csv": str(manifest_csv),
            "transfer_audit_csv": (
                str(transfer_audit_csv) if transfer_audit_csv is not None else None
            ),
            "copy_unique_dir": str(copy_unique_dir) if copy_unique_dir else None,
        },
        "inventory_rows": len(inventory),
        "selected_unique_rows": len(selected),
        "excluded_duplicate_rows": len(inventory) - len(selected),
        "duplicate_groups": sum(1 for rows in duplicate_groups.values() if len(rows) > 1),
        "six_digit_unique_readings": sum(valid_reading(row["meter_reading"]) for row in selected),
        "blank_unique_readings": sum(not clean(row["meter_reading"]) for row in selected),
        "reading_source_counts": dict(sorted(source_counts.items())),
        "explicit_correction_count": len(corrections),
        "supplemental_review_count": len(supplemental_rows),
        "ambiguous_main_workbook_families": ambiguous_main_families,
        "main_workbook_timestamp_consensus_count": len(timestamp_consensus),
        "ambiguous_main_workbook_timestamps": ambiguous_main_timestamps,
        "timestamp_reading_transfer_count": len(timestamp_transfers),
        "timestamp_reading_transfers": timestamp_transfers,
        "suppressed_followup_conflicts": suppressed_conflicts,
        "copied_unique_files": copied,
        "output_sha256": hashlib.sha256(output_csv.read_bytes()).hexdigest(),
    }
    audit_json.parent.mkdir(parents=True, exist_ok=True)
    audit_json.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    return audit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory-csv", type=Path, required=True)
    parser.add_argument("--main-workbook", type=Path, required=True)
    parser.add_argument("--supplemental-workbook", type=Path)
    parser.add_argument("--full-resolution-workbook", type=Path)
    parser.add_argument("--corrections-csv", type=Path)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--manifest-csv", type=Path, required=True)
    parser.add_argument("--audit-json", type=Path, required=True)
    parser.add_argument("--transfer-audit-csv", type=Path)
    parser.add_argument("--copy-unique-dir", type=Path)
    parser.add_argument("--originals-dir", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit = finalize_inventory(
        inventory_csv=args.inventory_csv,
        main_workbook=args.main_workbook,
        full_resolution_workbook=args.full_resolution_workbook,
        corrections_csv=args.corrections_csv,
        output_csv=args.output_csv,
        manifest_csv=args.manifest_csv,
        audit_json=args.audit_json,
        supplemental_workbook=args.supplemental_workbook,
        transfer_audit_csv=args.transfer_audit_csv,
        copy_unique_dir=args.copy_unique_dir,
        originals_dir=args.originals_dir,
    )
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
