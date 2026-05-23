#!/usr/bin/env python3
"""
update_irrigation_clean.py

Update the live irrigation_clean.csv file from raw irrigation event CSV files.

Purpose
-------
This script converts raw/group-level irrigation records into the clean,
app-facing irrigation event table used by plotting and irrigation analysis code.

Important terminology
---------------------
gallons_group
    Water assigned to the active strip pair/group, such as S1_S2 or S3_S4.
    For older raw files, the input column `gallons` is interpreted as
    gallons_group.

gallons_strip
    Estimated water assigned to one individual strip after splitting the group
    total between the two strips.

total_meter_gallons
    Total water measured at the meter. For older files this may be the same as
    gallons_group.

flow_allocation_fraction
    Fraction of total_meter_gallons assigned to the active strip group.

strip_allocation_fraction
    Fraction of gallons_group assigned to an individual strip. Default is 0.5.

The clean output intentionally does NOT include a generic `gallons` column,
because that name is ambiguous.

Usage
-----
Run from repo root:

    python tools/update_irrigation_clean.py

Dry run:

    python tools/update_irrigation_clean.py --dry-run

Optional custom paths:

    python tools/update_irrigation_clean.py \
      --clean biochar_app/data-processed/management/irrigation/irrigation_clean.csv \
      --new biochar_app/data-processed/management/irrigation/irrigation_2026_raw.csv
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final, cast

import pandas as pd

from biochar_app.config.paths import (
    IRRIGATION_CSV,
    IRRIGATION_DIR,
)

# ------------------------------------------------------------------

# Standardize numeric precision

# ------------------------------------------------------------------
rounding_map = {
    "total_meter_gallons": 0,
    "flow_allocation_fraction": 3,
    "strip_allocation_fraction": 3,
    "gallons_group": 0,
    "gallons_strip": 0,
    "avg_flow_gpm_group": 1,
    "avg_flow_gpm_strip": 1,
    "avg_flow_gph_strip": 1,
    "event_duration_hours": 2,
    "start_flow_gpm": 1,
    "end_flow_gpm": 1,
    "start_totalizer_gal_x100": 1,
    "end_totalizer_gal_x100": 1,
}


CORE_REQUIRED_COLUMNS: Final[list[str]] = [
    "year",
    "date",
    "start_timestamp",
    "end_timestamp",
    "strip_group",
    "location",
    "notes",
]

STRIP_GROUP_TO_STRIPS: Final[dict[str, list[str]]] = {
    "S1_S2": ["S1", "S2"],
    "S3_S4": ["S3", "S4"],
}

DEFAULT_DUP_KEY: Final[list[str]] = [
    "start_timestamp",
    "end_timestamp",
    "strip_group",
    "strip",
    "gallons_strip",
]

CLEAN_OUTPUT_COLUMNS: Final[list[str]] = [
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

@dataclass(frozen=True)
class UpdatePaths:
    clean_csv: Path
    new_csv: Path
    backup_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update irrigation_clean.csv with strip-level irrigation events."
    )
    parser.add_argument("--clean", type=Path, default=IRRIGATION_CSV)
    parser.add_argument("--new", type=Path, default=IRRIGATION_DIR / "irrigation_2026_raw.csv")
    parser.add_argument("--backup-dir", type=Path, default=IRRIGATION_DIR / "backups")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return pd.read_csv(path)


def validate_input_columns(df: pd.DataFrame, name: str) -> None:
    missing = [c for c in CORE_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")

    has_volume = any(c in df.columns for c in ["gallons_group", "gallons_strip", "gallons"])
    if not has_volume:
        raise ValueError(
            f"{name} must contain one of: gallons_group, gallons_strip, gallons"
        )


def normalize_strip_group(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip().upper()
    compact = s.replace(" ", "").replace("_", "").replace("-", "")

    if compact in {"S1S2", "1&2", "1AND2", "S1&S2", "S1ANDS2"}:
        return "S1_S2"
    if compact in {"S3S4", "3&4", "3AND4", "S3&S4", "S3ANDS4"}:
        return "S3_S4"

    return s if s else None


def normalize_strip(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip().upper().replace(" ", "").replace("_", "")
    if s in {"S1", "1", "STRIP1"}:
        return "S1"
    if s in {"S2", "2", "STRIP2"}:
        return "S2"
    if s in {"S3", "3", "STRIP3"}:
        return "S3"
    if s in {"S4", "4", "STRIP4"}:
        return "S4"

    return None


def normalize_location(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip().lower()
    if not s:
        return None
    if s in {"west", "w"}:
        return "west"
    if s in {"east", "e"}:
        return "east"
    return s


def normalize_notes(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def numeric_column(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="Float64")
    return pd.to_numeric(df[col], errors="coerce")


def string_column(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series("", index=df.index, dtype="string")
    return df[col].fillna("").astype("string")


def normalize_base_events(df: pd.DataFrame, name: str) -> pd.DataFrame:
    validate_input_columns(df, name)

    out = df.copy()

    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["start_timestamp"] = pd.to_datetime(out["start_timestamp"], errors="coerce")
    out["end_timestamp"] = pd.to_datetime(out["end_timestamp"], errors="coerce")
    out["date"] = out["start_timestamp"].dt.date.astype(str)

    out["strip_group"] = out["strip_group"].map(normalize_strip_group)
    out["location"] = out["location"].map(normalize_location)
    out["notes"] = out["notes"].map(normalize_notes)

    if "gallons_group" in out.columns:
        out["gallons_group"] = numeric_column(out, "gallons_group")
    elif "gallons" in out.columns:
        out["gallons_group"] = numeric_column(out, "gallons")
    else:
        out["gallons_group"] = pd.Series(pd.NA, index=out.index, dtype="Float64")

    if "gallons_strip" in out.columns:
        out["gallons_strip"] = numeric_column(out, "gallons_strip")
    else:
        out["gallons_strip"] = pd.Series(pd.NA, index=out.index, dtype="Float64")

    if "total_meter_gallons" in out.columns:
        out["total_meter_gallons"] = numeric_column(out, "total_meter_gallons")
    else:
        out["total_meter_gallons"] = out["gallons_group"]

    if "flow_allocation_fraction" in out.columns:
        out["flow_allocation_fraction"] = numeric_column(out, "flow_allocation_fraction")
    else:
        out["flow_allocation_fraction"] = out["gallons_group"] / out["total_meter_gallons"]

    out.loc[out["flow_allocation_fraction"].isna(), "flow_allocation_fraction"] = 1.0

    if "avg_flow_gpm_group" in out.columns:
        out["avg_flow_gpm_group"] = numeric_column(out, "avg_flow_gpm_group")
    elif "avg_flow_gpm" in out.columns:
        out["avg_flow_gpm_group"] = numeric_column(out, "avg_flow_gpm")
    else:
        out["avg_flow_gpm_group"] = pd.Series(pd.NA, index=out.index, dtype="Float64")

    for col in [
        "start_flow_gpm",
        "end_flow_gpm",
        "start_totalizer_gal_x100",
        "end_totalizer_gal_x100",
    ]:
        out[col] = numeric_column(out, col)

    out["entered_by"] = string_column(out, "entered_by")
    out["event_id"] = string_column(out, "event_id")

    if "strip" in out.columns:
        out["strip"] = out["strip"].map(normalize_strip)
    else:
        out["strip"] = pd.Series(pd.NA, index=out.index, dtype="object")

    if "strip_allocation_fraction" in out.columns:
        out["strip_allocation_fraction"] = numeric_column(out, "strip_allocation_fraction")
    else:
        out["strip_allocation_fraction"] = pd.Series(pd.NA, index=out.index, dtype="Float64")

    _validate_base(out, name)
    return out


def _validate_base(out: pd.DataFrame, name: str) -> None:
    checks = {
        "start_timestamp": out["start_timestamp"].isna(),
        "end_timestamp": out["end_timestamp"].isna(),
        "year": out["year"].isna(),
        "strip_group": out["strip_group"].isna(),
        "location": out["location"].isna(),
    }

    for label, mask in checks.items():
        if mask.any():
            bad = out.loc[mask]
            raise ValueError(
                f"{name} has rows with invalid {label} values.\n"
                f"First few bad rows:\n{bad.head().to_string(index=False)}"
            )

    no_volume = out["gallons_group"].isna() & out["gallons_strip"].isna()
    if no_volume.any():
        bad = out.loc[no_volume]
        raise ValueError(
            f"{name} has rows with no usable gallons_group or gallons_strip.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )

    bad_duration = out["end_timestamp"] < out["start_timestamp"]
    if bad_duration.any():
        bad = out.loc[bad_duration]
        raise ValueError(
            f"{name} has rows where end_timestamp < start_timestamp.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )


def expand_to_strip_level(base: pd.DataFrame, name: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for _, row in base.iterrows():
        strip_group = str(row["strip_group"])
        group_strips = STRIP_GROUP_TO_STRIPS.get(strip_group)

        if group_strips is None:
            raise ValueError(f"{name} has unsupported strip_group: {strip_group!r}")

        existing_strip = normalize_strip(row.get("strip"))
        existing_gallons_strip = row.get("gallons_strip")
        existing_fraction = row.get("strip_allocation_fraction")

        already_strip_level = existing_strip in group_strips and pd.notna(existing_gallons_strip)

        if already_strip_level:
            if existing_strip is None:
                raise ValueError(f"{name} has strip-level row with missing strip: {row.to_dict()}")
            strips_to_write: list[str] = [existing_strip]
        else:
            strips_to_write = list(group_strips)

        for strip in strips_to_write:
            if already_strip_level and pd.notna(existing_fraction):
                strip_fraction = float(existing_fraction)
            else:
                strip_fraction = 1.0 / float(len(group_strips))

            if pd.notna(row.get("gallons_group")):
                gallons_group = float(row["gallons_group"])

            elif already_strip_level:
                if existing_gallons_strip is None or pd.isna(existing_gallons_strip):
                    raise ValueError(
                        f"{name} has strip-level row with missing gallons_strip: {row.to_dict()}"
                    )
                gallons_group = float(cast(float, existing_gallons_strip)) / strip_fraction
            else:
                raise ValueError(f"{name} has no gallons_group for row: {row.to_dict()}")

            if already_strip_level:
                if existing_gallons_strip is None or pd.isna(existing_gallons_strip):
                    raise ValueError(f"{name} has strip-level row with missing gallons_strip: {row.to_dict()}")
                gallons_strip = float(cast(float, existing_gallons_strip))
            else:
                gallons_strip = gallons_group * strip_fraction

            avg_flow_gpm_group = row.get("avg_flow_gpm_group")
            avg_flow_gpm_strip: float | None = None
            if pd.notna(avg_flow_gpm_group):
                avg_flow_gpm_strip = float(avg_flow_gpm_group) * strip_fraction

            start_ts = pd.Timestamp(row["start_timestamp"])
            end_ts = pd.Timestamp(row["end_timestamp"])
            duration_hours = (end_ts - start_ts).total_seconds() / 3600.0

            avg_flow_gph_strip: float | None = None
            if duration_hours > 0:
                avg_flow_gph_strip = gallons_strip / duration_hours

            rows.append(
                {
                    "year": int(row["year"]),
                    "date": str(row["date"]),
                    "start_timestamp": start_ts,
                    "end_timestamp": end_ts,
                    "strip_group": strip_group,
                    "location": row["location"],
                    "strip": strip,
                    "total_meter_gallons": row["total_meter_gallons"],
                    "flow_allocation_fraction": row["flow_allocation_fraction"],
                    "strip_allocation_fraction": strip_fraction,
                    "gallons_group": gallons_group,
                    "gallons_strip": gallons_strip,
                    "avg_flow_gpm_group": avg_flow_gpm_group,
                    "avg_flow_gpm_strip": avg_flow_gpm_strip,
                    "avg_flow_gph_strip": avg_flow_gph_strip,
                    "event_duration_hours": duration_hours,
                    "start_flow_gpm": row["start_flow_gpm"],
                    "end_flow_gpm": row["end_flow_gpm"],
                    "start_totalizer_gal_x100": row["start_totalizer_gal_x100"],
                    "end_totalizer_gal_x100": row["end_totalizer_gal_x100"],
                    "entered_by": row["entered_by"],
                    "event_id": row["event_id"],
                    "notes": row["notes"],
                }
            )

    out = pd.DataFrame(rows)

    for col in CLEAN_OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    out = out[CLEAN_OUTPUT_COLUMNS].copy()

    out["date"] = pd.to_datetime(out["start_timestamp"], errors="coerce").dt.date.astype(str)
    out["start_timestamp"] = pd.to_datetime(out["start_timestamp"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    out["end_timestamp"] = pd.to_datetime(out["end_timestamp"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    numeric_cols = [
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
    ]

    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype(int)
    for col, digits in rounding_map.items():

        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(digits)
    return out


def normalize_irrigation_df(df: pd.DataFrame, name: str) -> pd.DataFrame:
    base = normalize_base_events(df, name)
    return expand_to_strip_level(base, name)


def make_backup(clean_csv: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{clean_csv.stem}_{stamp}{clean_csv.suffix}"
    backup_path.write_bytes(clean_csv.read_bytes())
    return backup_path


def make_duplicate_key(df: pd.DataFrame) -> pd.Series:
    key_df = df[DEFAULT_DUP_KEY].copy()

    for col in ["start_timestamp", "end_timestamp", "strip_group", "strip"]:
        key_df[col] = key_df[col].fillna("").astype(str).str.strip()

    key_df["gallons_strip"] = (
        pd.to_numeric(key_df["gallons_strip"], errors="coerce")
        .round(3)
        .astype(str)
    )

    return key_df.astype(str).agg("|".join, axis=1)


def sort_clean_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["_sort_start"] = pd.to_datetime(out["start_timestamp"], errors="coerce")
    out["_sort_end"] = pd.to_datetime(out["end_timestamp"], errors="coerce")

    out = (
        out.sort_values(
            ["_sort_start", "_sort_end", "strip_group", "strip"],
            kind="stable",
        )
        .drop(columns=["_sort_start", "_sort_end"])
        .reset_index(drop=True)
    )

    return out


def update_irrigation_clean(paths: UpdatePaths, dry_run: bool = False) -> None:
    current_raw = load_csv(paths.clean_csv, "Live irrigation_clean.csv")
    new_raw = load_csv(paths.new_csv, "New irrigation CSV")

    current_df = normalize_irrigation_df(current_raw, "Live irrigation_clean.csv")
    new_df = normalize_irrigation_df(new_raw, "New irrigation CSV")

    current_keys = make_duplicate_key(current_df)
    new_keys = make_duplicate_key(new_df)

    is_new = ~new_keys.isin(set(current_keys))
    new_unique = new_df.loc[is_new].copy()

    combined = pd.concat([current_df, new_unique], ignore_index=True)
    combined = sort_clean_df(combined)

    print("\n--- Irrigation update summary ---")
    print(f"Live strip-level rows before update : {len(current_df)}")
    print(f"Incoming strip-level rows           : {len(new_df)}")
    print(f"New unique strip-level rows         : {len(new_unique)}")
    print(f"Rows after update                   : {len(combined)}")

    if not new_unique.empty:
        print("\nNew rows to append:")
        display_cols = [
            "year",
            "date",
            "start_timestamp",
            "end_timestamp",
            "strip_group",
            "strip",
            "gallons_group",
            "gallons_strip",
            "avg_flow_gpm_group",
            "avg_flow_gpm_strip",
            "notes",
        ]
        print(new_unique[display_cols].to_string(index=False))
    else:
        print("\nNo new irrigation events found.")

    if dry_run:
        print("\nDry run only. No files were written.")
        return

    backup_path = make_backup(paths.clean_csv, paths.backup_dir)
    combined.to_csv(paths.clean_csv, index=False)

    print(f"\nBackup written to: {backup_path}")
    print(f"Updated live file : {paths.clean_csv}")


def main() -> int:
    args = parse_args()

    paths = UpdatePaths(
        clean_csv=args.clean.resolve(),
        new_csv=args.new.resolve(),
        backup_dir=args.backup_dir.resolve(),
    )

    update_irrigation_clean(paths, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())