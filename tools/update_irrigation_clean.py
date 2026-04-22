#!/usr/bin/env python3
"""
update_irrigation_clean.py

Append new irrigation events into the live irrigation_clean.csv file.

Recommended workflow
--------------------
1. Keep historical cleaned file:
   - irrigation_clean.csv  -> live cumulative file used by the app
   - irrigation_clean_2023_2025.csv -> frozen historical backup

2. Put new incoming 2026 events in:
   - irrigation_2026_raw.csv

3. Run this script to:
   - validate schemas
   - normalize timestamps and text fields
   - append only new events
   - create a timestamped backup of irrigation_clean.csv
   - rewrite irrigation_clean.csv sorted by start_timestamp

Expected columns
----------------
year,date,start_timestamp,end_timestamp,strip_group,location,gallons,notes

Default duplicate key
---------------------
start_timestamp + end_timestamp + strip_group + gallons

Usage
-----
python tools/update_irrigation_clean.py

Optional custom paths
---------------------
python tools/update_irrigation_clean.py \
  --clean biochar_app/data-processed/management/irrigation_clean.csv \
  --new biochar_app/data-raw/management/irrigation_2026_raw.csv
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final

import pandas as pd


REQUIRED_COLUMNS: Final[list[str]] = [
    "year",
    "date",
    "start_timestamp",
    "end_timestamp",
    "strip_group",
    "location",
    "gallons",
    "notes",
]

DEFAULT_DUP_KEY: Final[list[str]] = [
    "start_timestamp",
    "end_timestamp",
    "strip_group",
    "gallons",
]


@dataclass(frozen=True)
class UpdatePaths:
    clean_csv: Path
    new_csv: Path
    backup_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update irrigation_clean.csv with new events.")
    parser.add_argument(
        "--clean",
        type=Path,
        default=Path("biochar_app/data-processed/management/irrigation_clean.csv"),
        help="Path to the live cumulative irrigation_clean.csv",
    )
    parser.add_argument(
        "--new",
        type=Path,
        default=Path("biochar_app/data-raw/management/irrigation_2026_raw.csv"),
        help="Path to the new raw irrigation CSV to append",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=Path("biochar_app/data-processed/management/backups"),
        help="Directory for timestamped backups of irrigation_clean.csv",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report changes without writing files.",
    )
    return parser.parse_args()


def validate_columns(df: pd.DataFrame, name: str) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def normalize_strip_group(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    s = str(value).strip().upper()
    compact = s.replace(" ", "").replace("_", "").replace("-", "")

    if compact in {"S1S2", "1&2", "1AND2", "S1&S2", "S1ANDS2"}:
        return "S1_S2"
    if compact in {"S3S4", "3&4", "3AND4", "S3&S4", "S3ANDS4"}:
        return "S3_S4"

    return s if s else None


def normalize_location(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
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
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def normalize_datetime_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, format="ISO8601", errors="coerce")


def normalize_irrigation_df(df: pd.DataFrame, name: str) -> pd.DataFrame:
    validate_columns(df, name)

    out = df.copy()

    # Keep only expected columns, preserve order
    out = out[REQUIRED_COLUMNS].copy()

    out["start_timestamp"] = normalize_datetime_series(out["start_timestamp"])
    out["end_timestamp"] = normalize_datetime_series(out["end_timestamp"])

    if out["start_timestamp"].isna().any():
        bad = out.loc[out["start_timestamp"].isna(), REQUIRED_COLUMNS]
        raise ValueError(
            f"{name} has rows with invalid start_timestamp values.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )

    if out["end_timestamp"].isna().any():
        bad = out.loc[out["end_timestamp"].isna(), REQUIRED_COLUMNS]
        raise ValueError(
            f"{name} has rows with invalid end_timestamp values.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )

    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    if out["year"].isna().any():
        bad = out.loc[out["year"].isna(), REQUIRED_COLUMNS]
        raise ValueError(
            f"{name} has rows with invalid year values.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )

    out["date"] = out["start_timestamp"].dt.date.astype(str)
    out["strip_group"] = out["strip_group"].map(normalize_strip_group)
    out["location"] = out["location"].map(normalize_location)
    out["notes"] = out["notes"].map(normalize_notes)

    out["gallons"] = pd.to_numeric(out["gallons"], errors="coerce")
    if out["gallons"].isna().any():
        bad = out.loc[out["gallons"].isna(), REQUIRED_COLUMNS]
        raise ValueError(
            f"{name} has rows with invalid gallons values.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )

    if out["strip_group"].isna().any():
        bad = out.loc[out["strip_group"].isna(), REQUIRED_COLUMNS]
        raise ValueError(
            f"{name} has rows with invalid strip_group values.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )

    if out["location"].isna().any():
        bad = out.loc[out["location"].isna(), REQUIRED_COLUMNS]
        raise ValueError(
            f"{name} has rows with invalid location values.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )

    if (out["end_timestamp"] < out["start_timestamp"]).any():
        bad = out.loc[out["end_timestamp"] < out["start_timestamp"], REQUIRED_COLUMNS]
        raise ValueError(
            f"{name} has rows where end_timestamp < start_timestamp.\n"
            f"First few bad rows:\n{bad.head().to_string(index=False)}"
        )

    # Standard display/storage format
    out["start_timestamp"] = out["start_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out["end_timestamp"] = out["end_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out["year"] = out["year"].astype(int)

    return out


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return pd.read_csv(path)


def make_backup(clean_csv: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{clean_csv.stem}_{stamp}{clean_csv.suffix}"
    backup_path.write_bytes(clean_csv.read_bytes())
    return backup_path


def update_irrigation_clean(paths: UpdatePaths, dry_run: bool = False) -> None:
    current_raw = load_csv(paths.clean_csv, "Live irrigation_clean.csv")
    new_raw = load_csv(paths.new_csv, "New irrigation CSV")

    current_df = normalize_irrigation_df(current_raw, "Live irrigation_clean.csv")
    new_df = normalize_irrigation_df(new_raw, "New irrigation CSV")

    current_keys = current_df[DEFAULT_DUP_KEY].astype(str).agg("|".join, axis=1)
    new_keys = new_df[DEFAULT_DUP_KEY].astype(str).agg("|".join, axis=1)

    is_new = ~new_keys.isin(set(current_keys))
    new_unique = new_df.loc[is_new].copy()

    combined = pd.concat([current_df, new_unique], ignore_index=True)
    combined = combined.sort_values(["start_timestamp", "end_timestamp", "strip_group"]).reset_index(drop=True)

    print("\n--- Irrigation update summary ---")
    print(f"Live rows before update : {len(current_df)}")
    print(f"Incoming rows           : {len(new_df)}")
    print(f"New unique rows         : {len(new_unique)}")
    print(f"Rows after update       : {len(combined)}")

    if new_unique.empty:
        print("\nNo new irrigation events found. Nothing to append.")
        return

    print("\nNew rows to append:")
    print(new_unique.to_string(index=False))

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