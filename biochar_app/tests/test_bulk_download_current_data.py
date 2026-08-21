"""Regression tests for current biomass/hay and irrigation downloads."""

from __future__ import annotations

import io
import unittest
import zipfile

import pandas as pd

from biochar_app.scripts.bulk_downloads import _build_biomass_hay_files
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


if __name__ == "__main__":
    unittest.main()
