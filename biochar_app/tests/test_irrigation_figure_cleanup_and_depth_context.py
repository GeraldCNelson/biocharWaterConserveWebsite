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
from biochar_app.scripts.management.irrigation_analysis.holding_capacity import (
    build_event_storage_by_event,
    build_first_pass_water_balance_table,
)
from biochar_app.scripts.management.irrigation_analysis.plotting import (
    save_failed_event_pair_qc_plots,
)
from biochar_app.scripts.management.irrigation_analysis.utils import (
    add_flow_rate_comparison_fields,
)


class FlowRateComparisonTests(unittest.TestCase):
    def test_large_boundary_difference_is_flagged(self) -> None:
        frame = pd.DataFrame([{
            "start_flow_gpm": 400.0,
            "end_flow_gpm": 390.0,
            "avg_flow_gpm_group": 200.0,
        }])

        row = add_flow_rate_comparison_fields(frame).iloc[0]

        self.assertTrue(row["flow_rate_review_required"])
        self.assertEqual(
            row["flow_rate_comparison_status"],
            "review_large_difference",
        )
        self.assertEqual(row["max_boundary_flow_difference_gpm"], 200.0)

    def test_normal_boundary_variation_is_not_flagged(self) -> None:
        frame = pd.DataFrame([{
            "start_flow_gpm": 250.0,
            "end_flow_gpm": 220.0,
            "avg_flow_gpm_group": 235.0,
        }])

        row = add_flow_rate_comparison_fields(frame).iloc[0]

        self.assertFalse(row["flow_rate_review_required"])
        self.assertEqual(
            row["flow_rate_comparison_status"],
            "within_review_threshold",
        )


class UnretainedWaterTests(unittest.TestCase):
    def test_complete_profile_reports_short_unretained_fields(self) -> None:
        zones = pd.DataFrame(
            {
                "event_id": ["event"] * 3,
                "strip": ["S1"] * 3,
                "logger_position": ["T", "M", "B"],
                "estimated_zone_storage_gal_0_18in": [100.0, 200.0, 300.0],
                "gallons_strip": [1000.0] * 3,
                "event_duration_hours": [2.0] * 3,
                "avg_flow_gph_strip": [500.0] * 3,
            }
        )

        row = build_event_storage_by_event(zones).iloc[0]

        self.assertEqual(row["unretained_gal_strip"], 400.0)
        self.assertEqual(row["unretained_fraction"], 0.4)
        self.assertNotIn("potential_surface_runoff_gal", row.index)
        self.assertNotIn("water_not_stored_0_18in_gal_strip", row.index)

    def test_incomplete_profile_does_not_inflate_unretained_water(self) -> None:
        zones = pd.DataFrame(
            {
                "event_id": ["event"] * 2,
                "strip": ["S1"] * 2,
                "logger_position": ["T", "M"],
                "estimated_zone_storage_gal_0_18in": [100.0, 200.0],
                "gallons_strip": [1000.0] * 2,
            }
        )

        row = build_event_storage_by_event(zones).iloc[0]

        self.assertFalse(row["complete_three_zone_coverage"])
        self.assertTrue(pd.isna(row["unretained_gal_strip"]))

    def test_unretained_water_does_not_require_bottom_arrival(self) -> None:
        zones = pd.DataFrame(
            {
                "year": [2026] * 3,
                "event_id": ["event"] * 3,
                "strip": ["S1"] * 3,
                "event_duration_hours": [2.0] * 3,
                "avg_flow_gph_strip": [500.0] * 3,
                "logger_position": ["T", "M", "B"],
                "estimated_zone_storage_gal_0_18in": [100.0, 200.0, 300.0],
                "gallons_strip": [1000.0] * 3,
            }
        )
        trusted = pd.DataFrame(
            [{
                "year": 2026,
                "event_id": "event",
                "strip": "S1",
                "trustworthy_event": True,
                "trustworthy_reason": "ok",
            }]
        )

        row = build_first_pass_water_balance_table(
            trusted, zones, pd.DataFrame()
        ).iloc[0]

        self.assertFalse(row["bottom_6in_response_observed"])
        self.assertTrue(row["holding_capacity_eligible"])
        self.assertTrue(row["unretained_eligible"])
        self.assertEqual(row["unretained_gal_strip"], 400.0)


class IrrigationFigureCleanupTests(unittest.TestCase):
    def test_failed_event_plot_compares_both_strips_and_three_loggers(self) -> None:
        index = pd.date_range("2026-04-21 08:00", periods=16, freq="15min")
        data = {}
        for strip in ("S1", "S2"):
            for position in ("T", "M", "B"):
                for depth in (1, 2, 3):
                    data[f"VWC_{depth}_raw_{strip}_{position}"] = range(16)
        frame = pd.DataFrame(data, index=index)
        failures = pd.DataFrame([{
            "event_id": "2026-04-21_S1_S2_test",
            "irrigation_start": "2026-04-21 09:00",
            "irrigation_end": "2026-04-21 10:00",
            "strip": "S1",
            "depth_inches": 6,
            "trustworthy_event": False,
            "trustworthy_reason": "unexplained_pre_start_response",
        }])

        with tempfile.TemporaryDirectory() as temporary_directory:
            log = save_failed_event_pair_qc_plots(
                frame, failures, temporary_directory, hours_before=1, hours_after=1
            )
            self.assertEqual(len(log), 1)
            self.assertEqual(log.iloc[0]["strip_pair"], "S1/S2")
            self.assertTrue(Path(log.iloc[0]["output_file"]).exists())

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

    def test_logger_position_order_classes_are_canonical(self) -> None:
        primary_minutes = {
            6: {"T": 10.0, "M": 20.0, "B": 30.0},
            12: {"T": 20.0, "M": 10.0, "B": 30.0},
            18: {"T": 10.0, "M": 30.0, "B": 20.0},
        }
        rows = []
        for depth, positions in primary_minutes.items():
            for position, arrival in positions.items():
                rows.append(
                    {
                        "year": 2026,
                        "event_id": "event",
                        "strip": "S3",
                        "logger_position": position,
                        "depth_inches": depth,
                        "baseline_vwc": 20.0,
                        "arrival_minutes_after_irrigation_start": arrival,
                        "alt_arrival_minutes_after_irrigation_start": (
                            None
                            if depth == 18 and position == "B"
                            else {"T": 10.0, "M": 20.0, "B": 30.0}[position]
                        ),
                        "alt_arrival_before_irrigation_start": False,
                    }
                )

        result = build_arrival_order_diagnostics(pd.DataFrame(rows)).iloc[0]

        self.assertEqual(result["arrival_6in_logger_order_class"], "expected")
        self.assertEqual(
            result["arrival_12in_logger_order_class"],
            "middle_before_top",
        )
        self.assertEqual(
            result["arrival_18in_logger_order_class"],
            "bottom_before_top_or_middle",
        )
        self.assertEqual(
            result["alt_arrival_18in_logger_order_class"],
            "missing_loggers",
        )


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
