#!/usr/bin/env python3
"""
Tests for build_irrigation_from_master.py.

Purpose
-------
Verify deterministic IDs, strip allocation, concurrent shared-meter handling,
and exclusion of events without defensible water volumes.

Run from the repository root
----------------------------

    python -m unittest \
        biochar_app/tests/test_build_irrigation_from_master.py -v

Introduced
----------
2026-07-25

Maintainer
----------
Biochar Water Conservation project (Gerald Nelson).
"""

from __future__ import annotations

import unittest

import pandas as pd

from biochar_app.scripts.management.build_irrigation_from_master import (
    build_candidate_from_events,
    stable_event_id,
)


def event(
    *,
    strip_group: str,
    reported_gallons: float | None,
    source_row: int,
) -> dict[str, object]:
    return {
        "year": 2026,
        "workbook_date": pd.Timestamp("2026-07-24"),
        "strip_group": strip_group,
        "location": "west" if strip_group == "S1_S2" else "east",
        "workbook_start_timestamp": pd.Timestamp("2026-07-24 08:25:00"),
        "workbook_end_timestamp": pd.Timestamp("2026-07-24 15:43:00"),
        "workbook_start_counter": 237972.0,
        "workbook_end_counter": 239337.0,
        "workbook_reported_gallons": reported_gallons,
        "workbook_reported_acre_ft": (
            reported_gallons / 325_851.0
            if reported_gallons is not None
            else None
        ),
        "workbook_reported_gpm": (
            reported_gallons / (7.3 * 60.0)
            if reported_gallons is not None
            else None
        ),
        "workbook_start_flow_gpm": 275.0,
        "workbook_end_flow_gpm": 350.0,
        "workbook_notes": "",
        "source_sheet": "2026 IRRIGATION",
        "source_row": source_row,
        "workbook_event_duration_hours": 7.3,
        "workbook_totalizer_derived_gallons": 136500.0,
    }


class BuildIrrigationFromMasterTests(unittest.TestCase):
    """Test canonical candidate construction without touching project files."""

    def test_shared_meter_event_expands_to_four_strip_rows(self) -> None:
        events = pd.DataFrame(
            [
                event(
                    strip_group="S1_S2",
                    reported_gallons=68250.0,
                    source_row=13,
                ),
                event(
                    strip_group="S3_S4",
                    reported_gallons=68250.0,
                    source_row=14,
                ),
            ]
        )

        candidate, invalid = build_candidate_from_events(events)

        self.assertTrue(invalid.empty)
        self.assertEqual(len(candidate), 4)
        self.assertEqual(
            set(candidate["strip"]),
            {"S1", "S2", "S3", "S4"},
        )
        self.assertTrue(candidate["gallons_group"].eq(68250.0).all())
        self.assertTrue(candidate["gallons_strip"].eq(34125.0).all())
        self.assertTrue(candidate["concurrent_group_count"].eq(2).all())
        self.assertTrue(
            candidate["calculated_total_meter_gallons"].eq(136500.0).all()
        )
        self.assertTrue(
            candidate[
                "calculated_group_gallons_from_totalizer"
            ].eq(68250.0).all()
        )

    def test_missing_volume_event_is_excluded_for_review(self) -> None:
        events = pd.DataFrame(
            [
                event(
                    strip_group="S1_S2",
                    reported_gallons=68250.0,
                    source_row=13,
                ),
                event(
                    strip_group="S3_S4",
                    reported_gallons=None,
                    source_row=14,
                ),
            ]
        )

        candidate, invalid = build_candidate_from_events(events)

        self.assertEqual(len(candidate), 2)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid.iloc[0]["strip_group"], "S3_S4")

    def test_event_id_is_deterministic(self) -> None:
        first = stable_event_id(
            "2026-07-24",
            "2026-07-24 08:25:00",
            "S1_S2",
        )
        second = stable_event_id(
            "2026-07-24",
            "2026-07-24 08:25:00",
            "S1_S2",
        )

        self.assertEqual(first, second)
        self.assertRegex(
            first,
            r"^2026-07-24_S1_S2_[0-9a-f]{8}$",
        )


if __name__ == "__main__":
    unittest.main()
