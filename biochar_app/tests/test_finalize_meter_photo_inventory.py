from __future__ import annotations

import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook


SCRIPT = (
    Path(__file__).parents[1]
    / "scripts/management/finalize_meter_photo_inventory.py"
)
SPEC = importlib.util.spec_from_file_location("finalize_meter_photo_inventory", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def make_main(path: Path, *, conflicting_timestamp: bool = False) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Meter Review"
    sheet.append([
        "Photo Date/Time", "Filename", "SHA-256", "Manual Reading", "Status", "Notes"
    ])
    sheet.append([
        "2023-05-23 19:19:29", "UUID.heic", "sha-original", "162023", "readable", ""
    ])
    sheet.append([
        "2023-05-23 19:19:29", "UUID_4_5005_c.jpeg", "sha-small", "", "unreadable", ""
    ])
    sheet.append([
        "2023-05-23 19:19:29", "IMG_7515.JPEG", "sha-img", "", "unreadable", ""
    ])
    if conflicting_timestamp:
        sheet.append([
            "2023-05-23 19:19:29", "conflict.jpeg", "sha-conflict", "999999", "readable", ""
        ])
    workbook.save(path)


def make_followup(path: Path) -> None:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for index, values in enumerate(
        [
            ("UUID_4_5005_c.jpeg", "sha-small", "182023"),
            ("IMG_7515.JPEG", "sha-img", "182023"),
        ],
        start=1,
    ):
        sheet = workbook.create_sheet(f"Photo {index:02d}")
        sheet["B5"], sheet["B6"], sheet["B11"], sheet["B12"] = (
            values[0], values[1], values[2], "readable"
        )
    workbook.save(path)


def make_supplemental(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Review"
    for _ in range(5):
        sheet.append([])
    sheet.append([
        "Photo", "Photo date/time", "Manual reading", "Status", "Notes",
        "Filename", "Timestamp confidence", "Timestamp source", "SHA-256",
        "Crop method", "Review check",
    ])
    sheet.append([
        "", "2026-08-14 13:47:54", "240191", "readable", "confirmed",
        "IMG_8795.HEIC", "high", "datetime_original", "sha-supplemental",
        "circle_meter_head", "Complete",
    ])
    sheet.append([
        "", "2023-08-10 14:00:12", "", "not_meter_reading", "no meter",
        "not-a-meter.jpg", "low", "file_modify_date", "sha-no-meter",
        "circle_meter_head", "Complete",
    ])
    workbook.save(path)


class FinalizeInventoryTests(unittest.TestCase):
    def test_supplemental_review_imports_reading_and_non_meter_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            inventory = tmp_path / "photo_inventory.csv"
            main = tmp_path / "main.xlsx"
            supplemental = tmp_path / "supplemental.xlsx"
            output = tmp_path / "photo_inventory_unique.csv"
            manifest = tmp_path / "manifest.csv"
            audit = tmp_path / "audit.json"

            write_csv(
                inventory,
                [
                    {
                        "filename": "IMG_8795.HEIC", "sha256": "sha-supplemental",
                        "effective_datetime": "2026-08-14 13:47:54",
                        "meter_reading": "", "review_status": "",
                        "image_width": "4032", "image_height": "3024",
                        "file_size_bytes": "2000000", "timestamp_confidence": "high",
                    },
                    {
                        "filename": "not-a-meter.jpg", "sha256": "sha-no-meter",
                        "effective_datetime": "2023-08-10 14:00:12",
                        "meter_reading": "", "review_status": "",
                        "image_width": "4032", "image_height": "3024",
                        "file_size_bytes": "2000000", "timestamp_confidence": "low",
                    },
                ],
            )
            make_main(main)
            make_supplemental(supplemental)

            result = MODULE.finalize_inventory(
                inventory, main, None, None, output, manifest, audit,
                supplemental_workbook=supplemental,
            )

            _, selected = MODULE.read_csv_rows(output)
            by_sha = {row["sha256"]: row for row in selected}
            self.assertEqual(by_sha["sha-supplemental"]["meter_reading"], "240191")
            self.assertEqual(by_sha["sha-supplemental"]["review_status"], "readable")
            self.assertEqual(
                by_sha["sha-supplemental"]["meter_reading_source"],
                "supplemental_review",
            )
            self.assertEqual(by_sha["sha-no-meter"]["meter_reading"], "")
            self.assertEqual(
                by_sha["sha-no-meter"]["review_status"], "not_meter_reading"
            )
            self.assertEqual(result["supplemental_review_count"], 2)

    def test_precedence_and_unique_selection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            inventory = tmp_path / "photo_inventory.csv"
            main = tmp_path / "main.xlsx"
            followup = tmp_path / "followup.xlsx"
            corrections = tmp_path / "corrections.csv"
            output = tmp_path / "photo_inventory_unique.csv"
            manifest = tmp_path / "manifest.csv"
            audit = tmp_path / "audit.json"

            rows = [
                {
                    "filename": "UUID.heic", "sha256": "sha-original",
                    "effective_datetime": "2023-05-23 19:19:29", "meter_reading": "",
                    "review_status": "", "image_width": "4032", "image_height": "3024",
                    "file_size_bytes": "2000000", "timestamp_confidence": "high",
                },
                {
                    "filename": "UUID_4_5005_c.jpeg", "sha256": "sha-small",
                    "effective_datetime": "2023-05-23 19:19:29", "meter_reading": "",
                    "review_status": "", "image_width": "360", "image_height": "480",
                    "file_size_bytes": "70000", "timestamp_confidence": "high",
                },
                {
                    "filename": "IMG_7515.JPEG", "sha256": "sha-img",
                    "effective_datetime": "2023-05-23 19:19:29", "meter_reading": "",
                    "review_status": "", "image_width": "1536", "image_height": "2048",
                    "file_size_bytes": "700000", "timestamp_confidence": "high",
                },
            ]
            write_csv(inventory, rows)
            make_main(main)
            make_followup(followup)
            write_csv(
                corrections,
                [{
                    "sha256": "sha-img", "filename": "IMG_7515.JPEG",
                    "corrected_reading": "162023", "reason": "confirmed",
                }],
            )

            result = MODULE.finalize_inventory(
                inventory, main, followup, corrections, output, manifest, audit
            )

            _, selected = MODULE.read_csv_rows(output)
            self.assertEqual(len(selected), 1)
            self.assertEqual(selected[0]["filename"], "UUID.heic")
            self.assertEqual(selected[0]["meter_reading"], "162023")
            self.assertEqual(
                result["suppressed_followup_conflicts"][0]["selected_reading"],
                "162023",
            )
            self.assertEqual(
                json.loads(audit.read_text())["explicit_correction_count"],
                1,
            )

    def test_exact_timestamp_transfers_reviewed_reading_to_new_original(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            inventory = tmp_path / "photo_inventory.csv"
            main = tmp_path / "main.xlsx"
            output = tmp_path / "photo_inventory_unique.csv"
            manifest = tmp_path / "manifest.csv"
            audit = tmp_path / "audit.json"
            transfer_audit = tmp_path / "transfer_audit.csv"

            write_csv(
                inventory,
                [{
                    "filename": "IMG_7515.HEIC", "sha256": "sha-new-original",
                    "effective_datetime": "2023-05-23 19:19:29",
                    "timestamp_source": "datetime_original",
                    "timestamp_confidence": "high", "meter_reading": "",
                    "review_status": "", "image_width": "4032",
                    "image_height": "3024", "file_size_bytes": "2000000",
                }],
            )
            make_main(main)

            result = MODULE.finalize_inventory(
                inventory, main, None, None, output, manifest, audit,
                transfer_audit_csv=transfer_audit,
            )

            _, selected = MODULE.read_csv_rows(output)
            self.assertEqual(selected[0]["meter_reading"], "162023")
            self.assertEqual(
                selected[0]["meter_reading_source"],
                "main_review_timestamp_consensus",
            )
            self.assertEqual(result["timestamp_reading_transfer_count"], 1)
            self.assertEqual(
                result["timestamp_reading_transfers"][0]["target_filename"],
                "IMG_7515.HEIC",
            )
            _, transfer_rows = MODULE.read_csv_rows(transfer_audit)
            self.assertEqual(len(transfer_rows), 1)
            self.assertEqual(transfer_rows[0]["review_evidence_count"], "1")
            self.assertEqual(transfer_rows[0]["review_filenames"], "UUID.heic")

    def test_low_confidence_or_ambiguous_timestamp_is_not_transferred(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            inventory = tmp_path / "photo_inventory.csv"
            main = tmp_path / "main.xlsx"
            output = tmp_path / "photo_inventory_unique.csv"
            manifest = tmp_path / "manifest.csv"
            audit = tmp_path / "audit.json"

            rows = [
                {
                    "filename": "low.jpeg", "sha256": "sha-low",
                    "effective_datetime": "2023-05-23 19:19:29-06:00",
                    "timestamp_source": "file_modify_date",
                    "timestamp_confidence": "low", "meter_reading": "",
                    "review_status": "", "image_width": "360",
                    "image_height": "480", "file_size_bytes": "70000",
                },
                {
                    "filename": "ambiguous.HEIC", "sha256": "sha-ambiguous",
                    "effective_datetime": "2023-05-23 19:19:29",
                    "timestamp_source": "datetime_original",
                    "timestamp_confidence": "high", "meter_reading": "",
                    "review_status": "", "image_width": "4032",
                    "image_height": "3024", "file_size_bytes": "2000000",
                },
            ]
            write_csv(inventory, rows)
            make_main(main, conflicting_timestamp=True)

            result = MODULE.finalize_inventory(
                inventory, main, None, None, output, manifest, audit
            )

            _, selected = MODULE.read_csv_rows(output)
            self.assertTrue(all(not row["meter_reading"] for row in selected))
            self.assertEqual(result["timestamp_reading_transfer_count"], 0)
            self.assertEqual(
                result["ambiguous_main_workbook_timestamps"]
                ["2023-05-23 19:19:29"],
                ["162023", "999999"],
            )

    def test_exact_byte_duplicate_files_select_only_one_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            inventory = tmp_path / "photo_inventory.csv"
            main = tmp_path / "main.xlsx"
            output = tmp_path / "photo_inventory_unique.csv"
            manifest = tmp_path / "manifest.csv"
            audit = tmp_path / "audit.json"

            common = {
                "sha256": "same-sha",
                "effective_datetime": "2023-05-23 19:19:29",
                "meter_reading": "162023", "review_status": "readable",
                "image_width": "4032", "image_height": "3024",
                "file_size_bytes": "2000000", "timestamp_confidence": "high",
            }
            write_csv(
                inventory,
                [
                    {"filename": "a1b2c3d4-1111-2222-3333-444444444444.heic", **common},
                    {"filename": "IMG_7515.HEIC", **common},
                ],
            )
            make_main(main)

            result = MODULE.finalize_inventory(
                inventory, main, None, None, output, manifest, audit
            )

            _, selected = MODULE.read_csv_rows(output)
            _, manifest_rows = MODULE.read_csv_rows(manifest)
            self.assertEqual(len(selected), 1)
            self.assertEqual(selected[0]["filename"], "IMG_7515.HEIC")
            self.assertEqual(result["excluded_duplicate_rows"], 1)
            self.assertEqual(
                [row["action"] for row in manifest_rows].count("excluded_from_unique"),
                1,
            )

    def test_conflicting_readable_main_family_is_not_propagated(self) -> None:
        rows = [
            MODULE.ReviewReading("a", "UUID.heic", "162023", "readable", "", "main"),
            MODULE.ReviewReading("b", "UUID_4_5005_c.jpeg", "182023", "readable", "", "main"),
        ]
        consensus, ambiguous = MODULE.main_family_consensus(rows)
        self.assertNotIn("uuid", consensus)
        self.assertEqual(ambiguous["uuid"], ["162023", "182023"])


if __name__ == "__main__":
    unittest.main()
