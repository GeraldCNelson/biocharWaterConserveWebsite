#!/usr/bin/env python3
"""
Build and install canonical irrigation data from the master workbook.

Purpose
-------
Convert annual irrigation worksheets in the validated repository snapshot into
the strip-level schema used by plots and irrigation analysis. Apply established
photo-supported timestamp corrections, validate the complete result, and only
then replace ``irrigation_clean.csv``.

The Microsoft OneDrive workbook is never read directly here. ETL first creates
the validated ``biochar-data-master.xlsx`` repository snapshot.

Run from the repository root
----------------------------

Preview and validate without writing files::

    python biochar_app/scripts/management/build_irrigation_from_master.py \
        --dry-run

Build candidates and install the production CSV::

    python biochar_app/scripts/management/build_irrigation_from_master.py

Introduced
----------
2026-07-25

Maintainer
----------
Biochar Water Conservation project (Gerald Nelson).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

import pandas as pd

from biochar_app.config.paths import (
    BIOCHAR_MASTER_WORKBOOK,
    IRRIGATION_DIR,
    IRRIGATION_PRODUCTION_CSV,
    IRRIGATION_QC_CSV,
)
from biochar_app.scripts.management.build_irrigation_qc_candidate import (
    apply_shared_meter_classifications,
    apply_timestamp_corrections,
    format_output,
    initialize_qc_columns,
)
from biochar_app.scripts.management.compare_meter_photos_to_irrigation import (
    load_master_irrigation_events,
)
from biochar_app.scripts.management.update_master_workbook_snapshot import (
    sha256_file,
)


GALLONS_PER_ACRE_FOOT: Final[float] = 325_851.0

DEFAULT_CANDIDATE_CSV: Final[Path] = (
    IRRIGATION_DIR / "irrigation_clean_candidate.csv"
)
DEFAULT_INVALID_ROWS_CSV: Final[Path] = (
    IRRIGATION_DIR / "irrigation_master_invalid_rows.csv"
)
DEFAULT_AUDIT_JSON: Final[Path] = (
    IRRIGATION_DIR / "irrigation_build_audit.json"
)

STRIPS_BY_GROUP: Final[dict[str, tuple[str, str]]] = {
    "S1_S2": ("S1", "S2"),
    "S3_S4": ("S3", "S4"),
}

PRODUCTION_COLUMNS: Final[list[str]] = [
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
]

CANDIDATE_COLUMNS: Final[list[str]] = [
    *PRODUCTION_COLUMNS[:11],
    "gallons_source",
    "gallons_estimated",
    *PRODUCTION_COLUMNS[11:],
    "reported_gallons_group",
    "reported_acre_ft_used",
    "reported_avg_flow_gpm_group",
    "calculated_gallons_from_totalizer",
    "calculated_acre_ft_used",
    "calculated_avg_flow_gpm_group",
    "calculated_total_meter_gallons",
    "calculated_group_gallons_from_totalizer",
    "concurrent_group_count",
    "reported_minus_totalizer_gallons",
    "reported_minus_calculated_acre_ft",
    "reported_minus_calculated_gpm",
    "source_workbook",
    "source_sheet",
    "source_row",
]


def stable_event_id(
    date_value: object,
    start_value: object,
    strip_group: str,
) -> str:
    """Create a deterministic ID for a newly encountered workbook event."""
    date_text = pd.Timestamp(date_value).strftime("%Y-%m-%d")
    start_text = pd.Timestamp(start_value).strftime("%Y-%m-%d %H:%M:%S")
    payload = f"{date_text}|{start_text}|{strip_group}".encode("utf-8")
    suffix = hashlib.sha256(payload).hexdigest()[:8]
    return f"{date_text}_{strip_group}_{suffix}"


def existing_event_ids(path: Path) -> dict[tuple[str, str, str], str]:
    """Read prior event IDs so unchanged events retain stable identifiers."""
    if not path.exists():
        return {}

    prior = pd.read_csv(path)
    required = {
        "date",
        "start_timestamp",
        "strip_group",
        "event_id",
    }
    if not required.issubset(prior.columns):
        return {}

    prior_dates = pd.to_datetime(
        prior["date"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    prior_starts = pd.to_datetime(
        prior["start_timestamp"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    result: dict[tuple[str, str, str], str] = {}
    for index, row in prior.iterrows():
        key = (
            prior_dates.loc[index],
            prior_starts.loc[index],
            str(row["strip_group"]).strip(),
        )
        event_id = str(row["event_id"]).strip()
        if all(key) and event_id:
            result.setdefault(key, event_id)
    return result


def concurrent_group_counts(events: pd.DataFrame) -> pd.Series:
    """Count strip groups sharing one physical meter interval."""
    keys = [
        "workbook_start_timestamp",
        "workbook_end_timestamp",
        "workbook_start_counter",
        "workbook_end_counter",
    ]
    return events.groupby(keys, dropna=False)["strip_group"].transform("nunique")


def build_candidate_from_events(
    events: pd.DataFrame,
    *,
    prior_event_ids: dict[tuple[str, str, str], str] | None = None,
    source_workbook_name: str = "biochar-data-master.xlsx",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Expand valid group-level workbook events into canonical strip-level rows.

    Events lacking a positive reported volume are returned separately instead
    of being silently included with invented water volumes.
    """
    prior_event_ids = prior_event_ids or {}
    work = events.copy()

    work["workbook_reported_gallons"] = pd.to_numeric(
        work["workbook_reported_gallons"],
        errors="coerce",
    )

    valid_mask = (
        work["workbook_reported_gallons"].notna()
        & (work["workbook_reported_gallons"] > 0)
    )
    invalid = work.loc[~valid_mask].copy()
    valid = work.loc[valid_mask].copy()

    if valid.empty:
        raise ValueError("No positive-volume irrigation events were found.")

    valid["concurrent_group_count"] = concurrent_group_counts(valid)
    rows: list[dict[str, Any]] = []

    for event in valid.to_dict(orient="records"):
        strip_group = str(event["strip_group"])
        if strip_group not in STRIPS_BY_GROUP:
            raise ValueError(f"Unknown strip group: {strip_group!r}")

        start = pd.Timestamp(event["workbook_start_timestamp"])
        end = pd.Timestamp(event["workbook_end_timestamp"])
        date_text = pd.Timestamp(event["workbook_date"]).strftime("%Y-%m-%d")
        start_text = start.strftime("%Y-%m-%d %H:%M:%S")
        key = (date_text, start_text, strip_group)
        event_id = prior_event_ids.get(
            key,
            stable_event_id(date_text, start, strip_group),
        )

        gallons_group = float(event["workbook_reported_gallons"])
        duration_hours = float(event["workbook_event_duration_hours"])
        gallons_strip = gallons_group / 2.0
        avg_group = gallons_group / (duration_hours * 60.0)
        avg_strip = avg_group / 2.0

        calculated_gallons = event.get(
            "workbook_totalizer_derived_gallons"
        )
        calculated_gallons = (
            float(calculated_gallons)
            if pd.notna(calculated_gallons)
            else pd.NA
        )

        concurrent_count = int(event["concurrent_group_count"])
        flow_allocation_fraction = 1.0 / concurrent_count
        total_meter_gallons = (
            calculated_gallons
            if calculated_gallons is not pd.NA
            else gallons_group / flow_allocation_fraction
        )
        calculated_group = (
            calculated_gallons / concurrent_count
            if calculated_gallons is not pd.NA
            else pd.NA
        )

        for strip in STRIPS_BY_GROUP[strip_group]:
            rows.append(
                {
                    "year": int(event["year"]),
                    "date": date_text,
                    "start_timestamp": start,
                    "end_timestamp": end,
                    "strip_group": strip_group,
                    "location": event["location"],
                    "strip": strip,
                    "total_meter_gallons": total_meter_gallons,
                    "flow_allocation_fraction": (
                        flow_allocation_fraction
                    ),
                    "strip_allocation_fraction": 0.5,
                    "gallons_group": gallons_group,
                    "gallons_source": "reported_gal_used",
                    "gallons_estimated": False,
                    "gallons_strip": gallons_strip,
                    "avg_flow_gpm_group": avg_group,
                    "avg_flow_gpm_strip": avg_strip,
                    "avg_flow_gph_strip": avg_strip * 60.0,
                    "event_duration_hours": duration_hours,
                    "start_flow_gpm": event["workbook_start_flow_gpm"],
                    "end_flow_gpm": event["workbook_end_flow_gpm"],
                    "start_totalizer_gal_x100": event[
                        "workbook_start_counter"
                    ],
                    "end_totalizer_gal_x100": event[
                        "workbook_end_counter"
                    ],
                    "entered_by": "master_workbook",
                    "event_id": event_id,
                    "notes": event["workbook_notes"],
                    "reported_gallons_group": gallons_group,
                    "reported_acre_ft_used": event[
                        "workbook_reported_acre_ft"
                    ],
                    "reported_avg_flow_gpm_group": event[
                        "workbook_reported_gpm"
                    ],
                    "calculated_gallons_from_totalizer": calculated_gallons,
                    "calculated_acre_ft_used": (
                        calculated_group / GALLONS_PER_ACRE_FOOT
                        if calculated_group is not pd.NA
                        else pd.NA
                    ),
                    "calculated_avg_flow_gpm_group": (
                        calculated_group / (duration_hours * 60.0)
                        if calculated_group is not pd.NA
                        else pd.NA
                    ),
                    "calculated_total_meter_gallons": calculated_gallons,
                    "calculated_group_gallons_from_totalizer": (
                        calculated_group
                    ),
                    "concurrent_group_count": concurrent_count,
                    "reported_minus_totalizer_gallons": (
                        gallons_group - calculated_group
                        if calculated_group is not pd.NA
                        else pd.NA
                    ),
                    "reported_minus_calculated_acre_ft": (
                        float(event["workbook_reported_acre_ft"])
                        - calculated_group / GALLONS_PER_ACRE_FOOT
                        if (
                            pd.notna(event["workbook_reported_acre_ft"])
                            and calculated_group is not pd.NA
                        )
                        else pd.NA
                    ),
                    "reported_minus_calculated_gpm": (
                        float(event["workbook_reported_gpm"])
                        - calculated_group / (duration_hours * 60.0)
                        if (
                            pd.notna(event["workbook_reported_gpm"])
                            and calculated_group is not pd.NA
                        )
                        else pd.NA
                    ),
                    "source_workbook": source_workbook_name,
                    "source_sheet": event["source_sheet"],
                    "source_row": int(event["source_row"]),
                }
            )

    candidate = pd.DataFrame(rows)[CANDIDATE_COLUMNS]
    candidate = candidate.sort_values(
        ["start_timestamp", "strip_group", "strip"],
        kind="stable",
    ).reset_index(drop=True)

    numeric_rounding = {
        "avg_flow_gpm_group": 1,
        "avg_flow_gpm_strip": 1,
        "avg_flow_gph_strip": 1,
        "event_duration_hours": 2,
        "reported_acre_ft_used": 5,
        "reported_avg_flow_gpm_group": 1,
        "calculated_acre_ft_used": 5,
        "calculated_avg_flow_gpm_group": 1,
        "reported_minus_calculated_acre_ft": 5,
        "reported_minus_calculated_gpm": 1,
    }
    for column, digits in numeric_rounding.items():
        candidate[column] = pd.to_numeric(
            candidate[column],
            errors="coerce",
        ).round(digits)

    invalid = invalid.rename(
        columns={
            "workbook_date": "date",
            "workbook_start_timestamp": "start_timestamp",
            "workbook_end_timestamp": "end_timestamp",
            "workbook_reported_gallons": "reported_gallons_group",
            "workbook_notes": "notes",
        }
    )
    return candidate, invalid


def build_qc_candidate(candidate: pd.DataFrame) -> pd.DataFrame:
    """Apply established photo-supported corrections and validate them."""
    qc = initialize_qc_columns(candidate)
    qc, _audit = apply_timestamp_corrections(qc)
    qc = apply_shared_meter_classifications(qc)

    shared_mask = (
        pd.to_numeric(
            qc["concurrent_group_count"],
            errors="coerce",
        ).fillna(1) > 1
    )
    qc.loc[
        shared_mask,
        "meter_volume_shared_between_groups",
    ] = True
    qc.loc[
        shared_mask,
        "meter_volume_allocation_method",
    ] = "equal_split_between_active_groups"

    if not qc["timestamp_correction_applied"].fillna(False).any():
        raise ValueError(
            "No established photo-supported timestamp corrections were applied."
        )

    return format_output(qc)


def validate_production(
    production: pd.DataFrame,
    *,
    previous_production: pd.DataFrame | None = None,
) -> None:
    """Validate schema, uniqueness, volumes, and accidental row loss."""
    missing = sorted(set(PRODUCTION_COLUMNS) - set(production.columns))
    if missing:
        raise ValueError(f"Production irrigation data missing columns: {missing}")

    if production.empty:
        raise ValueError("Production irrigation data is empty.")

    duplicate = production.duplicated(
        subset=["event_id", "strip"],
        keep=False,
    )
    if duplicate.any():
        raise ValueError(
            "Duplicate event_id/strip rows:\n"
            f"{production.loc[duplicate, ['event_id', 'strip']]}"
        )

    if (
        pd.to_numeric(production["gallons_strip"], errors="coerce").isna().any()
        or (pd.to_numeric(production["gallons_strip"]) <= 0).any()
    ):
        raise ValueError("Every production row must have positive gallons_strip.")

    event_sizes = production.groupby("event_id")["strip"].nunique()
    if not event_sizes.eq(2).all():
        raise ValueError("Every irrigation event must contain exactly two strips.")

    if previous_production is not None and len(production) < len(previous_production):
        raise ValueError(
            "Refusing to replace irrigation_clean.csv because the rebuilt "
            f"dataset has fewer rows ({len(production)}) than the existing "
            f"production dataset ({len(previous_production)})."
        )


def atomic_write_csv(path: Path, dataframe: pd.DataFrame) -> None:
    """Write one CSV through a temporary file and atomically replace it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".csv",
        prefix=f".{path.stem}-",
        dir=path.parent,
        delete=False,
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)
        dataframe.to_csv(
            temporary_file,
            index=False,
            date_format="%Y-%m-%d %H:%M:%S",
        )
    try:
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write one JSON audit through a temporary file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".json",
        prefix=f".{path.stem}-",
        dir=path.parent,
        delete=False,
    ) as temporary_file:
        temporary_path = Path(temporary_file.name)
        json.dump(payload, temporary_file, indent=2, sort_keys=True)
        temporary_file.write("\n")
    try:
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def build_and_install_irrigation(
    *,
    workbook_path: Path = BIOCHAR_MASTER_WORKBOOK,
    candidate_path: Path = DEFAULT_CANDIDATE_CSV,
    qc_candidate_path: Path = IRRIGATION_QC_CSV,
    production_path: Path = IRRIGATION_PRODUCTION_CSV,
    invalid_rows_path: Path = DEFAULT_INVALID_ROWS_CSV,
    audit_path: Path = DEFAULT_AUDIT_JSON,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Build, validate, and optionally install all irrigation products."""
    events = load_master_irrigation_events(workbook_path)
    ids = existing_event_ids(candidate_path)
    candidate, invalid = build_candidate_from_events(
        events,
        prior_event_ids=ids,
        source_workbook_name=workbook_path.name,
    )
    qc_candidate = build_qc_candidate(candidate)
    production = qc_candidate[PRODUCTION_COLUMNS].copy()

    previous = (
        pd.read_csv(production_path)
        if production_path.exists()
        else None
    )
    validate_production(
        production,
        previous_production=previous,
    )

    latest = pd.to_datetime(
        production["start_timestamp"],
        errors="raise",
    ).max()
    audit = {
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "workbook_path": str(workbook_path.resolve()),
        "workbook_sha256": sha256_file(workbook_path),
        "workbook_group_events": int(len(events)),
        "candidate_strip_rows": int(len(candidate)),
        "qc_candidate_strip_rows": int(len(qc_candidate)),
        "production_strip_rows": int(len(production)),
        "invalid_group_events": int(len(invalid)),
        "latest_irrigation_start": latest.isoformat(),
        "dry_run": dry_run,
        "installed": not dry_run,
    }

    if not dry_run:
        atomic_write_csv(candidate_path, candidate)
        atomic_write_csv(qc_candidate_path, qc_candidate)
        atomic_write_csv(invalid_rows_path, invalid)
        atomic_write_csv(production_path, production)
        atomic_write_json(audit_path, audit)

    return audit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build validated strip-level irrigation data from the master "
            "workbook snapshot."
        )
    )
    parser.add_argument(
        "--workbook",
        type=Path,
        default=BIOCHAR_MASTER_WORKBOOK,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate without writing any files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit = build_and_install_irrigation(
        workbook_path=args.workbook,
        dry_run=args.dry_run,
    )
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
