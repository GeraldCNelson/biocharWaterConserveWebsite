"""Safety tests for exact-duplicate photo cleanup."""

from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from biochar_app.scripts.management.apply_duplicate_actions import (
    load_inventory,
    validate_duplicate_decisions,
    validate_files,
)


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def inventory_row(
    relative_path: str,
    action: str,
    digest: str,
    *,
    group: str = "duplicate_0001",
    exact: str = "TRUE",
) -> dict[str, str]:
    return {
        "relative_path": relative_path,
        "sha256": digest,
        "is_exact_duplicate": exact,
        "duplicate_group": group,
        "duplicate_action": action,
    }


def write_inventory(tmp_path, rows: list[dict[str, str]]) -> pd.DataFrame:
    path = tmp_path / "photo_inventory.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return load_inventory(path)


def test_valid_group_verifies_keep_and_delete_files(tmp_path) -> None:
    content = b"same photograph"
    digest = sha256(content)
    (tmp_path / "keep.jpg").write_bytes(content)
    (tmp_path / "delete.jpg").write_bytes(content)
    inventory = write_inventory(
        tmp_path,
        [
            inventory_row("keep.jpg", "KEEP", digest),
            inventory_row("delete.jpg", "DELETE", digest),
        ],
    )

    delete_rows = validate_duplicate_decisions(inventory)
    keep_rows = inventory.loc[inventory["duplicate_action"].eq("KEEP")]

    retained = validate_files(keep_rows, tmp_path)
    deletions = validate_files(delete_rows, tmp_path)

    assert retained == [("duplicate_0001", tmp_path / "keep.jpg")]
    assert deletions == [("duplicate_0001", tmp_path / "delete.jpg")]
    assert (tmp_path / "delete.jpg").exists()


def test_rejects_group_without_exactly_one_keep(tmp_path) -> None:
    digest = sha256(b"same")
    inventory = write_inventory(
        tmp_path,
        [
            inventory_row("first.jpg", "DELETE", digest),
            inventory_row("second.jpg", "DELETE", digest),
        ],
    )

    with pytest.raises(RuntimeError, match="expected exactly one KEEP"):
        validate_duplicate_decisions(inventory)


def test_rejects_path_that_escapes_photo_directory(tmp_path) -> None:
    rows = pd.DataFrame(
        [inventory_row("../outside.jpg", "DELETE", sha256(b"outside"))]
    )

    with pytest.raises(RuntimeError, match="escapes the originals directory"):
        validate_files(rows, tmp_path)


def test_rejects_hash_mismatch(tmp_path) -> None:
    (tmp_path / "delete.jpg").write_bytes(b"changed")
    rows = pd.DataFrame(
        [inventory_row("delete.jpg", "DELETE", sha256(b"inventory version"))]
    )

    with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
        validate_files(rows, tmp_path)


def test_rejects_missing_retained_copy(tmp_path) -> None:
    rows = pd.DataFrame(
        [inventory_row("keep.jpg", "KEEP", sha256(b"same"))]
    )

    with pytest.raises(RuntimeError, match="Missing file: keep.jpg"):
        validate_files(rows, tmp_path)
