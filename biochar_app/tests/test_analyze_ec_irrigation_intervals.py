"""Focused tests for the inter-irrigation EC analysis."""

from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from biochar_app.scripts.research.analyze_ec_irrigation_intervals import (
    _fixed_effect_exponent,
    build_irrigation_intervals,
    temperature_correct_ec25,
)


class ECIntervalAnalysisTests(unittest.TestCase):
    def test_temperature_correction_converts_stored_fahrenheit(self) -> None:
        corrected = temperature_correct_ec25(
            pd.Series([1.0, 1.0]),
            pd.Series([77.0, 59.0]),
        )

        self.assertAlmostEqual(corrected[0], 1.0)
        self.assertAlmostEqual(corrected[1], 1.25)

    def test_intervals_exclude_both_irrigation_dates(self) -> None:
        events = pd.DataFrame(
            {
                "year": [2025, 2025, 2025, 2025],
                "strip_group": ["S1_S2"] * 4,
                "event_id": ["a", "a", "b", "b"],
                "start_timestamp": [
                    "2025-06-01 08:00",
                    "2025-06-01 08:00",
                    "2025-06-10 09:00",
                    "2025-06-10 09:00",
                ],
                "end_timestamp": [
                    "2025-06-01 16:00",
                    "2025-06-01 16:00",
                    "2025-06-10 17:00",
                    "2025-06-10 17:00",
                ],
            }
        )

        intervals = build_irrigation_intervals(events)

        self.assertEqual(len(intervals), 1)
        self.assertEqual(intervals.iloc[0]["first_full_date"], pd.Timestamp("2025-06-02"))
        self.assertEqual(intervals.iloc[0]["last_full_date"], pd.Timestamp("2025-06-09"))
        self.assertEqual(intervals.iloc[0]["full_days"], 8)

    def test_fixed_effect_exponent_removes_interval_intercepts(self) -> None:
        rows = []
        for interval_id, scale in (("a", 2.0), ("b", 5.0)):
            for strip in ("S1", "S2"):
                for theta in (0.18, 0.22, 0.27, 0.32, 0.36):
                    rows.append(
                        {
                            "interval_id": interval_id,
                            "strip": strip,
                            "theta": theta,
                            "vwc_pct": theta * 100,
                            "ec25": scale * theta**1.5,
                        }
                    )
        frame = pd.DataFrame(rows)

        estimate = _fixed_effect_exponent(frame)

        self.assertTrue(np.isfinite(estimate))
        self.assertAlmostEqual(estimate, 1.5, places=10)


if __name__ == "__main__":
    unittest.main()
