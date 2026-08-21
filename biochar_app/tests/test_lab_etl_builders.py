"""Tests for field-biomass and supplemental NIR ETL builders."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

from biochar_app.config.paths import WARD_MASTER_NIR_CSV
from biochar_app.scripts.lab.build_field_biomass_from_master import build_field_biomass
from biochar_app.scripts.lab.clean_ward_master_common import read_ward_two_header_csv
from biochar_app.scripts.lab.update_ward_master_nir import (
    IN_MASTER_CSV,
    _read_supplemental_nir_csv,
)
from biochar_app.scripts.tables.tables_nir import build_nir_set1_table


LOCATIONS = [f"S{strip}{position}" for strip in range(1, 5) for position in "TMB"]


def _add_sheet(workbook: Workbook, name: str, year: int, old_layout: bool = False) -> None:
    worksheet = workbook.create_sheet(name)
    if old_layout:
        worksheet.append(["6-12-23 Biomass in grams", None, None, None, "7/25/23", None])
        worksheet.append(["Location", "Wet", "24 hours", "48 hours", "Wet", "Dry"])
        for index, location in enumerate(LOCATIONS, start=1):
            worksheet.append([location, 100 + index, 50 + index, 10 + index, 70 + index, 20 + index])
    else:
        worksheet.append([None, datetime(year, 7, 28), None])
        worksheet.append(["LOCATION", "WET (g)", "DRY (g)"])
        for index, location in enumerate(LOCATIONS, start=1):
            worksheet.append([location, 100 + index, year - 1900 + index])


class LabEtlBuilderTests(unittest.TestCase):
    def test_biomass_preserves_history_and_adds_only_new_dates(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            workbook_path = directory / "master.xlsx"
            historical_path = directory / "historical.csv"
            workbook = Workbook()
            workbook.remove(workbook.active)
            _add_sheet(workbook, "2023 BIOMASS", 2023, old_layout=True)
            _add_sheet(workbook, "2024 BIOMASS", 2024)
            _add_sheet(workbook, "2025 BIOMASS", 2025)
            _add_sheet(workbook, "2026 BIOMASS", 2026)
            workbook.save(workbook_path)
            workbook.close()

            historical_path.write_text(
                "unnamed: 0,6/12/23,7/28/25\n"
                "Location,dry (g),dry (g)\n"
                + "\n".join(f"{location},{index},{100 + index}" for index, location in enumerate(LOCATIONS, 1))
                + "\n",
                encoding="utf-8",
            )

            result = build_field_biomass(
                workbook_path=workbook_path,
                historical_path=historical_path,
            )

            self.assertEqual(result.columns.tolist(), ["location", "2023-06-12", "2025-07-28", "2026-07-28"])
            self.assertEqual(result["location"].tolist(), LOCATIONS)
            self.assertEqual(result.loc[0, "2023-06-12"], 1)
            self.assertEqual(result.loc[0, "2026-07-28"], 127)

    def test_2026_nir_file_uses_filename_sampling_date(self) -> None:
        _, header_map = read_ward_two_header_csv(IN_MASTER_CSV)
        source = Path("biochar_app/data-raw/lab-tests/hay-tests/csv-files/NIR_2026-07-28.csv")

        result = _read_supplemental_nir_csv(source, header_map)

        self.assertEqual(result["strip"].tolist(), ["strip_1", "strip_2", "strip_3", "strip_4"])
        self.assertEqual(result["nir_date"].unique().tolist(), ["2026-07-28"])
        self.assertTrue(pd.to_numeric(result["crude_protein_pct_db"]).notna().all())

    def test_nir_table_includes_latest_year_from_clean_master(self) -> None:
        payload = build_nir_set1_table(WARD_MASTER_NIR_CSV)

        period_keys = [period["key"] for period in payload["periods"]]
        self.assertIn("2026-07-28", period_keys)
        self.assertEqual(
            payload["data"]["crude_protein_pct_db"]["STRIP 1"]["2026-07-28"],
            10.6,
        )


if __name__ == "__main__":
    unittest.main()
