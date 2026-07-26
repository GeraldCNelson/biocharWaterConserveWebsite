#!/usr/bin/env python3
"""
Tests for update_master_workbook_snapshot.py.

Purpose
-------
Verify workbook validation and safe snapshot installation without reading from
OneDrive or changing the project's real master-workbook snapshot.

Run from the repository root
----------------------------

    python -m unittest \
        biochar_app/tests/test_update_master_workbook_snapshot.py -v

Introduced
----------
2026-07-25

Maintainer
----------
Biochar Water Conservation project (Gerald Nelson).
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from openpyxl import Workbook

from biochar_app.scripts.management.update_master_workbook_snapshot import (
    require_onedrive_desktop_app,
    sha256_file,
    update_snapshot,
    validate_workbook,
)


REQUIRED_SHEETS = (
    "2023 IRRIGATION",
    "2024 IRRIGATION",
    "2025 IRRIGATION",
    "2026 IRRIGATION",
)


def create_workbook(path: Path, sheet_names: tuple[str, ...]) -> None:
    """Create a small workbook containing exactly ``sheet_names``."""
    workbook = Workbook()
    first_sheet = workbook.active
    first_sheet.title = sheet_names[0]

    for sheet_name in sheet_names[1:]:
        workbook.create_sheet(sheet_name)

    workbook.save(path)
    workbook.close()


class MasterWorkbookSnapshotTests(unittest.TestCase):
    """Exercise validation, dry-run, and installation behavior."""

    @patch(
        "biochar_app.scripts.management."
        "update_master_workbook_snapshot.subprocess.run"
    )
    @patch(
        "biochar_app.scripts.management."
        "update_master_workbook_snapshot.sys.platform",
        "darwin",
    )
    def test_onedrive_process_check_accepts_running_app(
        self,
        run_mock: Mock,
    ) -> None:
        run_mock.return_value = Mock(returncode=0)

        require_onedrive_desktop_app()

        run_mock.assert_called_once_with(
            ["pgrep", "-x", "OneDrive"],
            capture_output=True,
            text=True,
            check=False,
        )

    @patch(
        "biochar_app.scripts.management."
        "update_master_workbook_snapshot.subprocess.run"
    )
    @patch(
        "biochar_app.scripts.management."
        "update_master_workbook_snapshot.sys.platform",
        "darwin",
    )
    def test_onedrive_process_check_rejects_stopped_app(
        self,
        run_mock: Mock,
    ) -> None:
        run_mock.return_value = Mock(returncode=1)

        with self.assertRaisesRegex(
            RuntimeError,
            "Microsoft OneDrive is not running",
        ):
            require_onedrive_desktop_app()

    @patch(
        "biochar_app.scripts.management."
        "update_master_workbook_snapshot.sys.platform",
        "linux",
    )
    def test_onedrive_process_check_rejects_non_macos(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "requires.*macOS"):
            require_onedrive_desktop_app()

    def test_validation_accepts_harmless_sheet_name_whitespace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            workbook_path = Path(temporary_directory) / "source.xlsx"
            create_workbook(
                workbook_path,
                (
                    "2023 IRRIGATION ",
                    "2024 IRRIGATION",
                    "2025 IRRIGATION",
                    "2026 IRRIGATION",
                ),
            )

            result = validate_workbook(workbook_path, REQUIRED_SHEETS)

            self.assertEqual(result["missing_required_sheets"], [])
            self.assertEqual(
                result["required_sheet_matches"]["2023 IRRIGATION"],
                "2023 IRRIGATION ",
            )

    def test_validation_rejects_a_missing_required_sheet(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            workbook_path = Path(temporary_directory) / "source.xlsx"
            create_workbook(
                workbook_path,
                REQUIRED_SHEETS[:-1],
            )

            with self.assertRaisesRegex(
                ValueError,
                "2026 IRRIGATION",
            ):
                validate_workbook(workbook_path, REQUIRED_SHEETS)

    def test_validate_only_does_not_replace_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            source = directory / "source.xlsx"
            destination = directory / "destination.xlsx"
            audit_path = directory / "snapshot.json"

            create_workbook(source, REQUIRED_SHEETS)
            destination.write_bytes(b"existing snapshot")
            original_destination = destination.read_bytes()

            audit = update_snapshot(
                source=source,
                destination=destination,
                required_sheets=REQUIRED_SHEETS,
                audit_path=audit_path,
                validate_only=True,
            )

            self.assertEqual(destination.read_bytes(), original_destination)
            self.assertFalse(audit["installed"])
            self.assertTrue(audit["changed"])
            self.assertEqual(audit["result"], "validated_not_installed")
            self.assertEqual(
                json.loads(audit_path.read_text(encoding="utf-8"))["result"],
                "validated_not_installed",
            )

    def test_install_replaces_destination_with_identical_copy(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            source = directory / "source.xlsx"
            destination = directory / "destination.xlsx"
            audit_path = directory / "snapshot.json"

            create_workbook(source, REQUIRED_SHEETS)
            destination.write_bytes(b"obsolete snapshot")
            source_sha256 = sha256_file(source)

            audit = update_snapshot(
                source=source,
                destination=destination,
                required_sheets=REQUIRED_SHEETS,
                audit_path=audit_path,
                validate_only=False,
            )

            self.assertTrue(audit["installed"])
            self.assertTrue(audit["changed"])
            self.assertEqual(
                audit["result"],
                "installed_changed_snapshot",
            )
            self.assertEqual(sha256_file(source), source_sha256)
            self.assertEqual(sha256_file(destination), source_sha256)
            self.assertEqual(audit["installed_sha256"], source_sha256)


if __name__ == "__main__":
    unittest.main()
