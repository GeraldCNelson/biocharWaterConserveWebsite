"""Tests for irrigation figure cleanup and depth-profile context."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from biochar_app.scripts.management.estimate_irrigation_holding_capacity import (
    concat_nonempty_informative_frames,
    logger_order_flag_summary,
    prune_stale_multidepth_figures,
)
from biochar_app.scripts.management.irrigation_analysis.diagnostics import (
    build_arrival_order_diagnostics,
)


class IrrigationFigureCleanupTests(unittest.TestCase):
    def test_successful_build_removes_only_stale_year_figures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bottom = root / "B"
            bottom.mkdir()

            current = bottom / "2025-current.png"
            stale = bottom / "2025-stale.png"
            other_year = bottom / "2024-keep.png"
            for path in (current, stale, other_year):
                path.write_bytes(b"png")

            log = pd.DataFrame(
                [{"output_file": str(current), "status": "written"}]
            )

            removed = prune_stale_multidepth_figures(
                year=2025,
                plot_log=log,
                multidepth_plot_dir=root,
            )

            self.assertEqual(removed, [stale])
            self.assertTrue(current.exists())
            self.assertFalse(stale.exists())
            self.assertTrue(other_year.exists())

    def test_failed_build_does_not_remove_stale_figures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bottom = root / "B"
            bottom.mkdir()
            stale = bottom / "2025-stale.png"
            stale.write_bytes(b"png")

            log = pd.DataFrame(
                [{"output_file": str(bottom / "new.png"), "status": "failed"}]
            )

            removed = prune_stale_multidepth_figures(
                year=2025,
                plot_log=log,
                multidepth_plot_dir=root,
            )

            self.assertEqual(removed, [])
            self.assertTrue(stale.exists())


class ElevatedDepthContextTests(unittest.TestCase):
    def test_elevated_18in_baseline_is_context_not_order_override(self) -> None:
        rows = []
        for depth, arrival, baseline in (
            (6, 10.0, 20.0),
            (12, 20.0, 18.0),
            (18, 30.0, 35.0),
        ):
            rows.append(
                {
                    "year": 2026,
                    "event_id": "event",
                    "strip": "S3",
                    "logger_position": "B",
                    "depth_inches": depth,
                    "baseline_vwc": baseline,
                    "arrival_minutes_after_irrigation_start": arrival,
                    "alt_arrival_minutes_after_irrigation_start": arrival,
                    "alt_arrival_before_irrigation_start": False,
                }
            )

        result = build_arrival_order_diagnostics(pd.DataFrame(rows))
        row = result.iloc[0]

        self.assertEqual(row["order_class"], "expected")
        self.assertTrue(row["elevated_18in_baseline_context"])
        self.assertIn("not an anomaly", row["depth_profile_interpretation"])


class CombinedOutputTests(unittest.TestCase):
    def test_concat_drops_per_frame_all_na_columns_but_keeps_real_values(
        self,
    ) -> None:
        first = pd.DataFrame(
            {"year": [2025], "optional": [pd.NA], "value": [1.0]}
        )
        second = pd.DataFrame(
            {"year": [2026], "optional": ["present"], "value": [2.0]}
        )

        result = concat_nonempty_informative_frames([first, second])

        self.assertEqual(result["year"].tolist(), [2025, 2026])
        self.assertEqual(result["value"].tolist(), [1.0, 2.0])
        self.assertTrue(pd.isna(result.loc[0, "optional"]))
        self.assertEqual(result.loc[1, "optional"], "present")

    def test_logger_order_summary_has_one_row_per_event(self) -> None:
        repeated = pd.DataFrame(
            [
                {
                    "strip": "S3",
                    "event_id": "event",
                    "logger_position": logger,
                    "any_bottom_before_top_or_middle": True,
                    "any_alt_bottom_before_top_or_middle": False,
                    "arrival_6in_logger_order_class": "expected",
                    "arrival_12in_logger_order_class": "expected",
                    "arrival_18in_logger_order_class": (
                        "bottom_before_top_or_middle"
                    ),
                    "alt_arrival_6in_logger_order_class": "expected",
                    "alt_arrival_12in_logger_order_class": "expected",
                    "alt_arrival_18in_logger_order_class": "expected",
                }
                for logger in ("T", "M", "B")
            ]
        )

        result = logger_order_flag_summary(repeated)

        self.assertEqual(len(result), 1)
        self.assertNotIn("logger_position", result.columns)


if __name__ == "__main__":
    unittest.main()
