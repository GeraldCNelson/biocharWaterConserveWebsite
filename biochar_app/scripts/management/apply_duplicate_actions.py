#!/usr/bin/env python3
"""
Apply duplicate-photo cleanup decisions from photo_inventory.csv.

Rows with duplicate_action == DELETE are removed from the working
photos/originals directory.

Safety features
---------------
- Dry-run is the default.
- Actual deletion requires --apply.
- Only exact duplicate rows can be deleted.
- Every affected duplicate group must retain exactly one KEEP row.
- SHA-256 is recalculated before deletion.
- The current file hash must match the inventory.
- Paths must resolve inside the configured originals directory.
- Rows marked KEEP, REVIEW, or blank are never deleted.

Because photos/originals is a working copy assembled from source collections,
the script deletes files directly rather than creating another duplicate
archive.

Examples
--------
Preview:

    python biochar_app/scripts/management/apply_duplicate_actions.py

Delete approved duplicate copies:

    python biochar_app/scripts/management/apply_duplicate_actions.py --apply
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

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

DEFAULT_INVENTORY_CSV = (
    REPO_ROOT
    / "biochar_app"
    / "data-processed"
    / "management"
    / "irrigation"
    / "photos"
    / "photo_inventory.csv"
)

VALID_ACTIONS = {
    "",
    "KEEP",
    "DELETE",
    "REVIEW",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete exact duplicate photographs marked DELETE "
            "in photo_inventory.csv."
        )
    )

    parser.add_argument(
        "--photo-dir",
        type=Path,
        default=DEFAULT_PHOTO_DIR,
        help=(
            "Working directory containing original photos. "
            f"Default: {DEFAULT_PHOTO_DIR}"
        ),
    )

    parser.add_argument(
        "--inventory-csv",
        type=Path,
        default=DEFAULT_INVENTORY_CSV,
        help=(
            "Photo inventory CSV. "
            f"Default: {DEFAULT_INVENTORY_CSV}"
        ),
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually delete files. Without this flag, the script "
            "performs validation and prints a dry-run preview."
        ),
    )

    return parser.parse_args()


def calculate_sha256(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file_handle:
        while chunk := file_handle.read(chunk_size):
            digest.update(chunk)

    return digest.hexdigest()


def normalize_boolean_series(
    values: pd.Series,
) -> pd.Series:
    return (
        values
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(
            {
                "true",
                "1",
                "yes",
                "y",
            }
        )
    )


def load_inventory(
    inventory_csv: Path,
) -> pd.DataFrame:
    if not inventory_csv.exists():
        raise FileNotFoundError(
            f"Inventory CSV does not exist: {inventory_csv}"
        )

    inventory = pd.read_csv(
        inventory_csv,
        dtype=str,
    ).fillna("")

    required_columns = {
        "relative_path",
        "sha256",
        "is_exact_duplicate",
        "duplicate_group",
        "duplicate_action",
    }

    missing_columns = sorted(
        required_columns - set(inventory.columns)
    )

    if missing_columns:
        raise RuntimeError(
            "Inventory is missing required columns: "
            + ", ".join(missing_columns)
        )

    inventory["duplicate_action"] = (
        inventory["duplicate_action"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    inventory["is_exact_duplicate_bool"] = (
        normalize_boolean_series(
            inventory["is_exact_duplicate"]
        )
    )

    invalid_actions = sorted(
        set(inventory["duplicate_action"]) - VALID_ACTIONS
    )

    if invalid_actions:
        raise RuntimeError(
            "Invalid duplicate_action values found: "
            + ", ".join(repr(value) for value in invalid_actions)
            + ". Allowed values are KEEP, DELETE, REVIEW, or blank."
        )

    return inventory


def validate_duplicate_decisions(
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    delete_rows = inventory.loc[
        inventory["duplicate_action"].eq("DELETE")
    ].copy()

    if delete_rows.empty:
        return delete_rows

    invalid_delete_rows = delete_rows.loc[
        ~delete_rows["is_exact_duplicate_bool"]
        | delete_rows["duplicate_group"].eq("")
    ]

    if not invalid_delete_rows.empty:
        paths = invalid_delete_rows[
            "relative_path"
        ].tolist()

        raise RuntimeError(
            "DELETE is permitted only for confirmed exact duplicates. "
            "Invalid rows:\n  "
            + "\n  ".join(paths)
        )

    affected_groups = sorted(
        delete_rows["duplicate_group"].unique()
    )

    errors: list[str] = []

    for group_name in affected_groups:
        group = inventory.loc[
            inventory["duplicate_group"].eq(group_name)
        ]

        keep_count = int(
            group["duplicate_action"].eq("KEEP").sum()
        )

        delete_count = int(
            group["duplicate_action"].eq("DELETE").sum()
        )

        unresolved_count = int(
            group["duplicate_action"]
            .isin({"", "REVIEW"})
            .sum()
        )

        if keep_count != 1:
            errors.append(
                f"{group_name}: expected exactly one KEEP, "
                f"found {keep_count}"
            )

        if delete_count < 1:
            errors.append(
                f"{group_name}: no DELETE rows found"
            )

        if unresolved_count:
            errors.append(
                f"{group_name}: contains {unresolved_count} "
                "blank or REVIEW rows"
            )

    if errors:
        raise RuntimeError(
            "Duplicate-action validation failed:\n  "
            + "\n  ".join(errors)
        )

    return delete_rows.sort_values(
        [
            "duplicate_group",
            "relative_path",
        ]
    )


def resolve_source_path(
    photo_dir: Path,
    relative_path: str,
) -> Path:
    clean_relative_path = str(
        relative_path
    ).strip()

    if not clean_relative_path:
        raise RuntimeError(
            "Inventory contains a blank relative_path."
        )

    relative = Path(clean_relative_path)

    if relative.is_absolute():
        raise RuntimeError(
            f"Inventory relative_path is absolute: {relative}"
        )

    source = (
        photo_dir
        / relative
    ).resolve()

    try:
        source.relative_to(photo_dir)
    except ValueError as exc:
        raise RuntimeError(
            f"Path escapes the originals directory: {relative}"
        ) from exc

    return source


def validate_files(
    delete_rows: pd.DataFrame,
    photo_dir: Path,
) -> list[tuple[str, Path]]:
    validated: list[tuple[str, Path]] = []
    errors: list[str] = []

    for _, row in delete_rows.iterrows():
        relative_path = str(
            row["relative_path"]
        ).strip()

        source = resolve_source_path(
            photo_dir=photo_dir,
            relative_path=relative_path,
        )

        if not source.exists():
            errors.append(
                f"Missing file: {relative_path}"
            )
            continue

        if not source.is_file():
            errors.append(
                f"Not a regular file: {relative_path}"
            )
            continue

        inventory_hash = str(
            row["sha256"]
        ).strip().lower()

        current_hash = calculate_sha256(
            source
        ).lower()

        if current_hash != inventory_hash:
            errors.append(
                f"SHA-256 mismatch: {relative_path}"
            )
            continue

        validated.append(
            (
                str(row["duplicate_group"]),
                source,
            )
        )

    if errors:
        raise RuntimeError(
            "File validation failed. Nothing was deleted:\n  "
            + "\n  ".join(errors)
        )

    return validated


def print_plan(
    validated_files: list[tuple[str, Path]],
    photo_dir: Path,
    apply_changes: bool,
) -> None:
    mode = (
        "DELETE"
        if apply_changes
        else "DRY RUN"
    )

    print(f"\nDuplicate cleanup plan — {mode}")
    print("-" * (27 + len(mode)))

    for duplicate_group, source in validated_files:
        relative_path = source.relative_to(
            photo_dir
        )

        print(
            f"{duplicate_group}: {relative_path}"
        )

    print(
        f"\nFiles marked DELETE: {len(validated_files):,}"
    )


def main() -> int:
    args = parse_args()

    photo_dir = (
        args.photo_dir
        .expanduser()
        .resolve()
    )

    inventory_csv = (
        args.inventory_csv
        .expanduser()
        .resolve()
    )

    try:
        if not photo_dir.exists():
            raise FileNotFoundError(
                f"Photo directory does not exist: {photo_dir}"
            )

        inventory = load_inventory(
            inventory_csv
        )

        delete_rows = validate_duplicate_decisions(
            inventory
        )

        if delete_rows.empty:
            print(
                "No inventory rows are marked DELETE."
            )
            return 0

        affected_groups = set(
            delete_rows["duplicate_group"]
        )
        keep_rows = inventory.loc[
            inventory["duplicate_group"].isin(affected_groups)
            & inventory["duplicate_action"].eq("KEEP")
        ].copy()

        # Validate the retained copy as well as every deletion candidate.
        # A stale inventory must never authorize removal when the sole KEEP
        # file is missing or no longer matches its recorded hash.
        validate_files(
            delete_rows=keep_rows,
            photo_dir=photo_dir,
        )

        validated_files = validate_files(
            delete_rows=delete_rows,
            photo_dir=photo_dir,
        )

        print_plan(
            validated_files=validated_files,
            photo_dir=photo_dir,
            apply_changes=args.apply,
        )

        if not args.apply:
            print(
                "\nDry run only. No files were deleted."
            )
            print(
                "Run again with --apply to perform the cleanup."
            )
            return 0

        deleted_count = 0

        for _, source in validated_files:
            source.unlink()
            deleted_count += 1

        print("\nDuplicate cleanup summary")
        print("-------------------------")
        print(f"Files deleted: {deleted_count:,}")
        print(f"Files missing: 0")
        print(f"Hash errors  : 0")

        print(
            "\nRebuild photo_inventory.csv to record "
            "the cleaned working collection."
        )

    except (
        FileNotFoundError,
        RuntimeError,
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
