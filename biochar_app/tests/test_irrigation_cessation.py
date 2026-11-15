"""Focused tests for the July 4 irrigation-cessation screening analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd

from biochar_app.scripts.management.irrigation_analysis.analyze_irrigation_cessation import (
    build_daily_profile,
    build_pair_summary,
    build_strip_profile_water,
)


def test_profile_water_requires_all_nine_sensors() -> None:
    timestamps = pd.date_range("2026-07-04", periods=2, freq="15min")
    source = pd.DataFrame({"timestamp": timestamps})
    for position in "TMB":
        for depth in (1, 2, 3):
            source[f"VWC_{depth}_raw_S1_{position}"] = 20.0

    result = build_strip_profile_water(source, strip="S1")
    assert np.isclose(result.loc[0, "strip_profile_water_in"], 3.6)

    source.loc[1, "VWC_3_raw_S1_B"] = np.nan
    result = build_strip_profile_water(source, strip="S1")
    assert pd.isna(result.loc[1, "strip_profile_water_in"])


def test_daily_profile_requires_72_complete_observations() -> None:
    timestamps = pd.date_range("2026-07-04", periods=96, freq="15min")
    profile = pd.DataFrame({
        "timestamp": timestamps,
        "strip": "S1",
        "strip_profile_water_in": 4.2,
    })
    profile.loc[:24, "strip_profile_water_in"] = np.nan

    result = build_daily_profile(profile)
    assert result.loc[0, "complete_profile_observations"] == 71
    assert not result.loc[0, "daily_eligible"]
    assert pd.isna(result.loc[0, "daily_median_profile_water_in"])


def test_pair_summary_keeps_storage_and_duration_questions_separate() -> None:
    rows = []
    values = {
        "S1": (5.0, 10.0),
        "S2": (4.5, 8.0),
        "S3": (4.0, 6.0),
        "S4": (4.2, 9.0),
    }
    for strip, (water, days) in values.items():
        rows.append({
            "year": 2026,
            "strip": strip,
            "july4_profile_available": True,
            "july4_profile_water_in": water,
            "full_use_days_at_0p23_in_per_day": days,
        })

    result = build_pair_summary(pd.DataFrame(rows)).set_index("pair")
    assert np.isclose(
        result.loc["S1_S2", "biochar_minus_control_july4_water_in"], 0.5
    )
    assert np.isclose(
        result.loc[
            "S1_S2", "difference_equivalent_days_at_0p23_in_per_day"
        ],
        0.5 / 0.23,
    )
    assert np.isclose(
        result.loc[
            "S1_S2", "biochar_minus_control_days_to_own_reference_at_0p23"
        ],
        2.0,
    )
    assert np.isclose(
        result.loc[
            "S3_S4", "biochar_minus_control_days_to_own_reference_at_0p23"
        ],
        -3.0,
    )
