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
                    "2023-09-04 10:29:59",
                    "2023-09-04 09:30:00",
                    "2024-07-07 05:29:59",
                    "2024-07-07 04:30:00",
                    "2025-01-16 20:45:00",
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
                    "2026-02-19 11:59:59",
                    "2026-02-19 15:00:00",
                    "2026-07-24 09:45:00",
                ]
            )
        )
        pd.testing.assert_series_equal(corrected, expected)

    def test_latest_s4b_state_replaces_prior_states(self) -> None:
        raw = pd.Series(pd.to_datetime(["2026-07-24 09:45:00"]))

        corrected = apply_logger_clock_corrections(raw, "S4B")

        expected = pd.Series(pd.to_datetime(["2026-07-24 08:45:00"]))
        pd.testing.assert_series_equal(corrected, expected)

    def test_summer_standard_time_is_converted_to_denver_daylight_time(self) -> None:
        corrected_mst = pd.Series(pd.to_datetime(["2026-07-24 09:45:00"]))

        civil = apply_logger_seasonal_civil_time(corrected_mst)

        self.assertEqual(civil.iloc[0].strftime("%Y-%m-%d %H:%M %z"), "2026-07-24 10:45 -0600")


if __name__ == "__main__":
    unittest.main()
