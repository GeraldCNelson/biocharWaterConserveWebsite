"""Regression tests for piecewise Campbell logger clock corrections.

Run from the repository root with:

    python -m unittest \
      biochar_app/tests/test_etl_logger_clock_corrections.py -v
"""

from __future__ import annotations

import unittest

import pandas as pd

from biochar_app.scripts.etl import (
    apply_logger_clock_corrections,
    apply_logger_seasonal_civil_time,
    build_logger_clock_corrections_audit,
)


class LoggerClockCorrectionTests(unittest.TestCase):
    def test_s3m_offsets_are_absolute_states_not_cumulative(self) -> None:
        raw = pd.Series(
            pd.to_datetime(
                [
                    "2023-09-04 10:29:59",
                    "2023-09-04 10:30:00",
                    "2024-07-07 06:29:59",
                    "2024-07-07 06:30:00",
                    "2025-01-16 23:45:00",
                ]
            )
        )

        corrected = apply_logger_clock_corrections(raw, "S3M")

        expected = pd.Series(
            pd.to_datetime(
                [
                    "2023-09-04 17:59:59",
                    "2023-09-04 17:00:00",
                    "2024-07-07 12:59:59",
                    "2024-07-07 12:00:00",
                    "2025-01-17 04:15:00",
                ]
            )
        )
        pd.testing.assert_series_equal(corrected, expected)

    def test_s3m_2026_clock_reset_ends_the_manual_offset(self) -> None:
        raw = pd.Series(
            pd.to_datetime(
                [
                    "2026-02-19 14:59:59",
                    "2026-02-19 15:00:00",
                    "2026-07-24 09:45:00",
                ]
            )
        )

        corrected = apply_logger_clock_corrections(raw, "S3M")

        expected = pd.Series(
            pd.to_datetime(
                [
                    "2026-02-19 19:29:59",
                    "2026-02-19 15:00:00",
                    "2026-07-24 09:45:00",
                ]
            )
        )
        pd.testing.assert_series_equal(corrected, expected)

    def test_s3t_forward_change_ends_february_stitching_offset(self) -> None:
        raw = pd.Series(
            pd.to_datetime(
                [
                    "2024-02-23 11:29:59",
                    "2024-02-23 11:30:00",
                    "2024-03-21 14:45:00",
                    "2024-03-21 16:00:00",
                    "2024-04-19 16:15:00",
                ]
            )
        )

        corrected = apply_logger_clock_corrections(raw, "S3T")

        expected = pd.Series(
            pd.to_datetime(
                [
                    "2024-02-23 10:29:59",
                    "2024-02-23 11:30:00",
                    "2024-03-21 14:45:00",
                    "2024-03-21 15:00:00",
                    "2024-04-19 15:15:00",
                ]
            )
        )
        pd.testing.assert_series_equal(corrected, expected)

    def test_latest_s4b_state_replaces_prior_states(self) -> None:
        raw = pd.Series(pd.to_datetime(["2026-07-24 09:45:00"]))

        corrected = apply_logger_clock_corrections(raw, "S4B")

        expected = pd.Series(pd.to_datetime(["2026-07-24 09:45:00"]))
        pd.testing.assert_series_equal(corrected, expected)

    def test_s2t_2026_mst_sync_ends_the_manual_offset(self) -> None:
        raw = pd.Series(
            pd.to_datetime(
                [
                    "2026-02-18 08:44:59",
                    "2026-02-18 08:45:00",
                    "2026-04-21 14:15:00",
                ]
            )
        )

        corrected_mst = apply_logger_clock_corrections(raw, "S2T")

        expected_mst = pd.Series(
            pd.to_datetime(
                [
                    "2026-02-18 08:44:59",
                    "2026-02-18 08:45:00",
                    "2026-04-21 14:15:00",
                ]
            )
        )
        pd.testing.assert_series_equal(corrected_mst, expected_mst)

        april_civil = apply_logger_seasonal_civil_time(corrected_mst.iloc[[2]])
        self.assertEqual(
            april_civil.iloc[0].strftime("%Y-%m-%d %H:%M %z"),
            "2026-04-21 15:15 -0600",
        )

    def test_february_2026_resets_end_absolute_pre_reset_states(self) -> None:
        cases = {
            "S1B": ("2026-02-23 08:44:59", "2026-02-23 08:45:00", 0, 0),
            "S1M": ("2026-02-23 08:44:59", "2026-02-23 08:45:00", -60, 0),
            "S1T": ("2026-02-23 08:44:59", "2026-02-23 08:45:00", -60, 0),
            "S2B": ("2026-02-23 08:44:59", "2026-02-23 08:45:00", -60, 0),
            "S2M": ("2026-02-23 08:44:59", "2026-02-23 08:45:00", -60, 0),
            "S3B": ("2026-02-23 08:44:59", "2026-02-23 08:45:00", -60, 0),
            "S3M": ("2026-02-19 14:59:59", "2026-02-19 15:00:00", 270, 0),
            "S3T": ("2026-02-23 08:44:59", "2026-02-23 08:45:00", -60, 0),
            "S4B": ("2026-02-23 08:59:59", "2026-02-23 09:00:00", -60, 0),
            "S4M": ("2026-02-23 08:59:59", "2026-02-23 09:00:00", -60, 0),
            "S4T": ("2026-02-23 08:59:59", "2026-02-23 09:00:00", -60, 0),
        }

        for logger, (before, after, before_offset, after_offset) in cases.items():
            with self.subTest(logger=logger):
                raw = pd.Series(pd.to_datetime([before, after]))
                corrected = apply_logger_clock_corrections(raw, logger)
                offsets = (corrected - raw).dt.total_seconds().div(60).astype(int)
                self.assertEqual(offsets.tolist(), [before_offset, after_offset])

    def test_summer_standard_time_is_converted_to_denver_daylight_time(self) -> None:
        corrected_mst = pd.Series(pd.to_datetime(["2026-07-24 09:45:00"]))

        civil = apply_logger_seasonal_civil_time(corrected_mst)

        self.assertEqual(civil.iloc[0].strftime("%Y-%m-%d %H:%M %z"), "2026-07-24 10:45 -0600")

    def test_audit_marks_screenshot_anchored_states_as_absolute(self) -> None:
        audit = build_logger_clock_corrections_audit().set_index(
            ["logger", "correction_start_raw"]
        )

        propagated = audit.loc[("S3T", "2024-03-21 16:00:00")]
        self.assertEqual(
            propagated["evidence_scope"],
            "continuity_and_absolute_time",
        )
        self.assertTrue(bool(propagated["absolute_time_verified"]))

        anchored = audit.loc[("S3M", "2026-02-19 15:00:00")]
        self.assertEqual(
            anchored["evidence_scope"],
            "continuity_and_absolute_time",
        )
        self.assertTrue(bool(anchored["absolute_time_verified"]))

        self.assertTrue(audit["absolute_time_verified"].all())
        self.assertFalse(audit["reason"].eq("No metadata recorded").any())


if __name__ == "__main__":
    unittest.main()
