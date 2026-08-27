"""Regression tests for current biomass/hay and irrigation downloads."""

from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from biochar_app.scripts.bulk_downloads import (
    _build_biomass_hay_files,
    bulk_download,
)
from biochar_app.scripts.plot_components import load_irrigation_events


class BulkDownloadCurrentDataTests(unittest.TestCase):
    def test_biomass_hay_bundle_contains_two_distinct_datasets(self) -> None:
        files = _build_biomass_hay_files()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            for name, content in files:
                archive.writestr(name, content)

        with zipfile.ZipFile(io.BytesIO(buffer.getvalue())) as archive:
            self.assertEqual(
                set(archive.namelist()),
                {
                    "biochar_field_biomass_all_years.csv",
                    "biochar_hay_nir_all_years.csv",
                    "README.txt",
                },
            )
            biomass = pd.read_csv(archive.open("biochar_field_biomass_all_years.csv"))
            nir = pd.read_csv(archive.open("biochar_hay_nir_all_years.csv"))

        self.assertIn("2026-07-28", biomass.columns)
        self.assertIn("dry_matter_pct", nir.columns)
        self.assertNotIn("dry_matter_pct", biomass.columns)

    def test_plot_irrigation_source_includes_august_14_event(self) -> None:
        for strip in ("S1", "S2", "S3", "S4"):
            events = load_irrigation_events(strip, 2026)
            self.assertEqual(events["start"].max().date().isoformat(), "2026-08-14")


class LoggerRatioBulkDownloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_logger_bundle_contains_ratio_csv_when_parquet_exists(self) -> None:
        logger_frame = pd.DataFrame(
            {"timestamp": ["2026-01-01 00:00:00"], "VWC_1_raw_S1_T": [25.0]}
        )
        ratio_frame = pd.DataFrame(
            {"timestamp": ["2026-01-01 00:00:00"], "VWC_ratio_S1_S2_T_1": [1.23456]}
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            ratio_path = Path(temp_dir) / "2026_daily_ratios.parquet"
            ratio_path.touch()

            with (
                patch(
                    "biochar_app.scripts.bulk_downloads._load_logger_download_df",
                    return_value=logger_frame,
                ),
                patch(
                    "biochar_app.scripts.bulk_downloads._logger_ratios_parquet_path",
                    return_value=ratio_path,
                ),
                patch(
                    "biochar_app.scripts.bulk_downloads._read_parquet_df",
                    return_value=ratio_frame,
                ),
                patch(
                    "biochar_app.scripts.bulk_downloads.build_timeseries_yearly_readme",
                    return_value="Ratios included as separate file: yes",
                ),
            ):
                response = await bulk_download({"key": "loggers_2026_daily"})

            response_chunks = [
                chunk async for chunk in response.body_iterator
            ]
            zip_bytes = b"".join(response_chunks)

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            self.assertEqual(
                set(archive.namelist()),
                {
                    "biochar_loggers_2026_daily.csv",
                    "biochar_loggers_2026_daily_ratios.csv",
                    "README.txt",
                },
            )
            ratios = pd.read_csv(
                archive.open("biochar_loggers_2026_daily_ratios.csv")
            )
            readme = archive.read("README.txt").decode("utf-8")

        self.assertEqual(ratios.loc[0, "VWC_ratio_S1_S2_T_1"], 1.235)
        self.assertIn("Ratios included as separate file: yes", readme)


if __name__ == "__main__":
    unittest.main()
