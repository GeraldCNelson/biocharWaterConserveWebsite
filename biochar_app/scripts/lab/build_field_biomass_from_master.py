#!/usr/bin/env python3
"""Build the dashboard field-biomass CSV from the master workbook.

The workbook sheets have changed layout over time.  This builder extracts only
field location, sampling date, and dry weight.  The reviewed 2023-2025 CSV is
kept as the historical authority; workbook values fill gaps and supply newer
sampling dates without rewriting established historical observations.
"""

from __future__ import annotations

import argparse
import os
import re
import tempfile
import warnings
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook

from biochar_app.config.paths import (
    BIOCHAR_MASTER_WORKBOOK,
    BIOMASS_FIELD_CSV,
    BIOMASS_HISTORICAL_CSV,
)

BIOMASS_SHEETS = tuple(f"{year} BIOMASS" for year in range(2023, 2027))
FIELD_ACTIVITY_SHEET = "FIELD ACTIVITY DATES"
LOCATION_RE = re.compile(r"^S[1-4][TMB]$")
DATE_RE = re.compile(r"(?P<month>\d{1,2})[/-](?P<day>\d{1,2})[/-](?P<year>\d{2,4})")
HAY_SAMPLE_RE = re.compile(r"^hay\s+sample\s+(?P<number>\d+)$", re.IGNORECASE)
MAX_DATE_DISAGREEMENT_DAYS = 45


class BiomassDateWarning(UserWarning):
    """Warn that a biomass-sheet date disagrees with the activity calendar."""


def _normalize_location(value: object) -> str | None:
    location = str(value).strip().upper() if value is not None else ""
    return location if LOCATION_RE.fullmatch(location) else None


def _parse_date(value: object) -> str | None:
    if isinstance(value, (datetime, date)):
        return (
            value.date().isoformat()
            if isinstance(value, datetime)
            else value.isoformat()
        )
    match = DATE_RE.search(str(value)) if value is not None else None
    if not match:
        return None
    year = int(match.group("year"))
    if year < 100:
        year += 2000
    return date(year, int(match.group("month")), int(match.group("day"))).isoformat()


def _read_hay_sample_dates(workbook_path: Path) -> dict[int, dict[int, str | None]]:
    """Return authoritative year/event dates from ``FIELD ACTIVITY DATES``."""
    workbook = load_workbook(workbook_path, read_only=True, data_only=True)
    try:
        if FIELD_ACTIVITY_SHEET not in workbook.sheetnames:
            raise ValueError(
                f"Workbook must contain the authoritative {FIELD_ACTIVITY_SHEET!r} worksheet"
            )
        rows = list(workbook[FIELD_ACTIVITY_SHEET].iter_rows(values_only=True))
    finally:
        workbook.close()

    if not rows:
        raise ValueError(f"Worksheet {FIELD_ACTIVITY_SHEET!r} is empty")

    years_by_column: dict[int, int] = {}
    for column_index, value in enumerate(rows[0][1:], start=1):
        try:
            year = int(value)
        except (TypeError, ValueError):
            continue
        if 2000 <= year <= 2100:
            years_by_column[column_index] = year

    dates: dict[int, dict[int, str | None]] = defaultdict(dict)
    for row in rows[1:]:
        label = str(row[0]).strip() if row and row[0] is not None else ""
        match = HAY_SAMPLE_RE.fullmatch(label)
        if not match:
            continue
        sample_number = int(match.group("number"))
        for column_index, year in years_by_column.items():
            value = row[column_index] if column_index < len(row) else None
            dates[year][sample_number] = _parse_date(value)

    if not dates:
        raise ValueError(
            f"Worksheet {FIELD_ACTIVITY_SHEET!r} contains no Hay Sample rows"
        )
    return dict(dates)


def _canonical_date_for_source(
    *,
    source_date: str,
    year: int,
    source_name: str,
    canonical_dates: dict[int, dict[int, str | None]],
    used_sample_numbers: set[int],
    has_dry_values: bool,
) -> tuple[str, int] | None:
    """Match a source heading to the nearest unused authoritative hay event."""
    candidates = [
        (sample_number, sampling_date)
        for sample_number, sampling_date in canonical_dates.get(year, {}).items()
        if sampling_date is not None and sample_number not in used_sample_numbers
    ]
    if not candidates:
        message = (
            f"{source_name}: no resolved unused date exists in {FIELD_ACTIVITY_SHEET!r} "
            f"for a {year} dry-biomass event headed {source_date}"
        )
        if has_dry_values:
            raise ValueError(
                f"{message}; refusing to publish completed dry-weight values"
            )
        warnings.warn(
            f"{message}; empty dry-weight column skipped",
            BiomassDateWarning,
            stacklevel=2,
        )
        return None

    source_day = date.fromisoformat(source_date)
    ranked = sorted(
        (
            abs((date.fromisoformat(sampling_date) - source_day).days),
            sample_number,
            sampling_date,
        )
        for sample_number, sampling_date in candidates
    )
    distance, sample_number, canonical_date = ranked[0]
    if distance > MAX_DATE_DISAGREEMENT_DAYS:
        message = (
            f"{source_name}: date {source_date} cannot be matched safely to an unused "
            f"{FIELD_ACTIVITY_SHEET!r} hay date for {year}; nearest is {canonical_date} "
            f"({distance} days away)"
        )
        if has_dry_values:
            raise ValueError(
                f"{message}; refusing to publish completed dry-weight values"
            )
        warnings.warn(
            f"{message}; empty dry-weight column skipped",
            BiomassDateWarning,
            stacklevel=2,
        )
        return None

    if source_date != canonical_date:
        warnings.warn(
            f"{source_name}: date {source_date} disagrees with authoritative "
            f"{FIELD_ACTIVITY_SHEET!r} Hay Sample {sample_number} date {canonical_date}; "
            f"using {canonical_date}",
            BiomassDateWarning,
            stacklevel=2,
        )
    return canonical_date, sample_number


def extract_biomass_sheet(
    workbook_path: Path,
    sheet_name: str,
    *,
    canonical_dates: dict[int, dict[int, str | None]] | None = None,
) -> pd.DataFrame:
    """Return location/sampling_date/dry_g rows from one biomass worksheet."""
    if canonical_dates is None:
        canonical_dates = _read_hay_sample_dates(workbook_path)
    workbook = load_workbook(workbook_path, read_only=True, data_only=True)
    try:
        worksheet = workbook[sheet_name]
        rows = list(worksheet.iter_rows(values_only=True))
    finally:
        workbook.close()

    location_header_row = next(
        i
        for i, row in enumerate(rows)
        if row and str(row[0]).strip().casefold() == "location"
    )
    date_row = rows[location_header_row - 1]
    header_row = rows[location_header_row]
    primary_section_end = next(
        (
            column_index
            for column_index, header in enumerate(header_row[1:], start=1)
            if str(header).strip().casefold() == "location"
        ),
        len(header_row),
    )

    observations: list[dict[str, Any]] = []
    active_date: str | None = None
    dry_columns: list[tuple[int, str]] = []
    for column_index, header in enumerate(header_row[:primary_section_end]):
        parsed_date = _parse_date(
            date_row[column_index] if column_index < len(date_row) else None
        )
        if parsed_date:
            active_date = parsed_date
        header_text = str(header).strip().casefold() if header is not None else ""
        if active_date and header_text in {"dry", "dry (g)"}:
            dry_columns.append((column_index, active_date))

    if not dry_columns:
        raise ValueError(
            f"No dated dry-weight columns found in worksheet {sheet_name!r}"
        )

    year = int(sheet_name.split()[0])
    used_sample_numbers: set[int] = set()
    for column_index, source_date in dry_columns:
        dry_values = [
            pd.to_numeric(row[column_index], errors="coerce")
            for row in rows[location_header_row + 1 :]
            if row
            and _normalize_location(row[0]) is not None
            and column_index < len(row)
        ]
        has_dry_values = any(pd.notna(value) for value in dry_values)
        match = _canonical_date_for_source(
            source_date=source_date,
            year=year,
            source_name=f"worksheet {sheet_name!r}",
            canonical_dates=canonical_dates,
            used_sample_numbers=used_sample_numbers,
            has_dry_values=has_dry_values,
        )
        if match is None:
            continue
        sampling_date, sample_number = match
        used_sample_numbers.add(sample_number)
        for row in rows[location_header_row + 1 :]:
            location = _normalize_location(row[0] if row else None)
            if location is None:
                continue
            value = row[column_index] if column_index < len(row) else None
            dry_g = pd.to_numeric(value, errors="coerce")
            if pd.notna(dry_g):
                observations.append(
                    {
                        "location": location,
                        "sampling_date": sampling_date,
                        "dry_g": float(dry_g),
                    }
                )

    result = pd.DataFrame(observations, columns=["location", "sampling_date", "dry_g"])
    if result.duplicated(["location", "sampling_date"]).any():
        raise ValueError(
            f"Duplicate location/date observations in worksheet {sheet_name!r}"
        )
    return result


def read_historical_biomass(
    path: Path,
    *,
    canonical_dates: dict[int, dict[int, str | None]] | None = None,
) -> pd.DataFrame:
    """Normalize the reviewed two-header 2023-2025 CSV to wide form."""
    raw = pd.read_csv(path, header=[0, 1])
    location_column = raw.columns[0]
    normalized = pd.DataFrame(
        {"location": raw[location_column].map(_normalize_location)}
    )
    used_sample_numbers: dict[int, set[int]] = defaultdict(set)
    for column in raw.columns[1:]:
        source_date = _parse_date(column[0])
        if not source_date:
            continue
        sampling_date = source_date
        if canonical_dates is not None:
            year = date.fromisoformat(source_date).year
            values = pd.to_numeric(raw[column], errors="coerce")
            match = _canonical_date_for_source(
                source_date=source_date,
                year=year,
                source_name=f"historical file {path.name!r}",
                canonical_dates=canonical_dates,
                used_sample_numbers=used_sample_numbers[year],
                has_dry_values=values.notna().any(),
            )
            if match is None:
                continue
            sampling_date, sample_number = match
            used_sample_numbers[year].add(sample_number)
        normalized[sampling_date] = pd.to_numeric(raw[column], errors="coerce")
    return normalized[normalized["location"].notna()].reset_index(drop=True)


def _observations_to_wide(observations: pd.DataFrame) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame(columns=["location"])
    wide = observations.pivot(index="location", columns="sampling_date", values="dry_g")
    return wide.reset_index().rename_axis(columns=None)


def build_field_biomass(
    *,
    workbook_path: Path = BIOCHAR_MASTER_WORKBOOK,
    historical_path: Path = BIOMASS_HISTORICAL_CSV,
) -> pd.DataFrame:
    """Build wide biomass data, with reviewed historical values taking priority."""
    canonical_dates = _read_hay_sample_dates(workbook_path)
    historical = read_historical_biomass(
        historical_path,
        canonical_dates=canonical_dates,
    )
    extracted = pd.concat(
        [
            extract_biomass_sheet(
                workbook_path,
                sheet,
                canonical_dates=canonical_dates,
            )
            for sheet in BIOMASS_SHEETS
        ],
        ignore_index=True,
    )
    workbook_wide = _observations_to_wide(extracted)
    historical_dates = [column for column in historical.columns if column != "location"]
    historical_latest = max(historical_dates)
    allowed_workbook_dates = [
        column
        for column in workbook_wide.columns
        if column == "location"
        or column in historical_dates
        or column > historical_latest
    ]
    workbook_wide = workbook_wide[allowed_workbook_dates]
    combined = historical.set_index("location").combine_first(
        workbook_wide.set_index("location")
    )
    combined = combined.reindex(sorted(combined.columns), axis=1)
    location_order = [
        f"S{strip}{position}" for strip in range(1, 5) for position in "TMB"
    ]
    combined = combined.reindex(location_order).round(10).reset_index()
    expected_locations = {
        f"S{strip}{position}" for strip in range(1, 5) for position in "TMB"
    }
    if set(combined["location"]) != expected_locations:
        raise ValueError(
            "Biomass output must contain exactly the 12 expected field locations"
        )
    return combined


def _atomic_write_csv(dataframe: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", dir=path.parent, delete=False
    ) as handle:
        temporary_path = Path(handle.name)
        dataframe.to_csv(handle, index=False)
    try:
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def build_and_install_field_biomass(
    *,
    workbook_path: Path = BIOCHAR_MASTER_WORKBOOK,
    historical_path: Path = BIOMASS_HISTORICAL_CSV,
    output_path: Path = BIOMASS_FIELD_CSV,
) -> dict[str, Any]:
    data = build_field_biomass(
        workbook_path=workbook_path, historical_path=historical_path
    )
    _atomic_write_csv(data, output_path)
    return {
        "built_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": len(data),
        "sampling_dates": len(data.columns) - 1,
        "latest_sampling_date": data.columns[-1],
        "output_path": str(output_path),
    }


def main() -> None:
    """Compatibility entry point; ETL is now the preferred orchestrator."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--workbook", type=Path, default=BIOCHAR_MASTER_WORKBOOK)
    args = parser.parse_args()
    print(build_and_install_field_biomass(workbook_path=args.workbook))


if __name__ == "__main__":
    # TODO(delete after ETL adoption): remove this standalone entry point once
    # operations have used biochar_app/scripts/etl.py for a full update cycle.
    main()
