from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from biochar_app.config.field_management_metadata import ZONE_AREAS_SQFT_BY_STRIP
from biochar_app.scripts.management.irrigation_analysis.analyze_shallow_layer_retention import (
    GALLONS_PER_ACRE_INCH,
    build_daily_pair_differences,
    build_matched_events,
    build_shallow_strip_water,
    summarize_interval,
    summarize_daily_irrigation_seasons,
    summarize_pairs,
)


def test_shallow_water_requires_and_area_weights_all_three_positions() -> None:
    timestamps = pd.date_range("2026-06-01", periods=2, freq="15min")
    logger = pd.DataFrame({
        "timestamp": timestamps,
        "VWC_1_raw_S1_T": [10.0, 10.0],
        "VWC_1_raw_S1_M": [20.0, np.nan],
        "VWC_1_raw_S1_B": [30.0, 30.0],
    })

    result = build_shallow_strip_water(logger, strip="S1")
    areas = ZONE_AREAS_SQFT_BY_STRIP["S1"]
    expected_vwc = sum(
        areas[position] * value
        for position, value in {"T": 10.0, "M": 20.0, "B": 30.0}.items()
    ) / sum(areas.values())

    assert result.loc[0, "strip_shallow_vwc_pct"] == pytest.approx(expected_vwc)
    assert result.loc[0, "strip_shallow_water_in"] == pytest.approx(
        expected_vwc / 100.0 * 6.0
    )
    assert bool(result.loc[0, "complete_spatial_coverage"])
    assert pd.isna(result.loc[1, "strip_shallow_water_in"])
    assert not bool(result.loc[1, "complete_spatial_coverage"])


def _event_row(
    *,
    strip: str,
    group: str,
    reference: float,
    rain: float,
    year: int = 2026,
) -> dict[str, object]:
    return {
        "year": year,
        "strip_group": group,
        "strip": strip,
        "event_id": f"{year}-06-01_{group}",
        "irrigation_start": pd.Timestamp(f"{year}-06-01 08:00"),
        "irrigation_end": pd.Timestamp(f"{year}-06-01 14:00"),
        "next_irrigation_start": pd.Timestamp(f"{year}-06-15 08:00"),
        "interval_days": 13.75,
        "reference_water_in": reference,
        "water_24h_in": reference,
        "water_72h_in": reference - 0.05,
        "water_7d_in": reference - 0.10,
        "water_half_interval_in": reference - 0.10,
        "water_pre_next_in": reference - 0.20,
        "mean_interval_water_in": reference - 0.10,
        "precip_0_reference_in": rain,
        "precip_0_24h_in": rain,
        "precip_0_72h_in": rain,
        "precip_0_7d_in": rain,
        "precip_0_half_interval_in": rain,
        "precip_0_next_irrigation_in": rain,
    }


def test_matched_summary_reports_inches_and_gallons_per_acre() -> None:
    events = pd.DataFrame([
        _event_row(strip="S1", group="S1_S2", reference=2.10, rain=0.0),
        _event_row(strip="S2", group="S1_S2", reference=2.00, rain=0.0),
        _event_row(strip="S3", group="S3_S4", reference=1.90, rain=0.2),
        _event_row(strip="S4", group="S3_S4", reference=2.00, rain=0.2),
    ])

    matched = build_matched_events(events)
    monthly = matched.loc[matched["pair"].eq("S1_S2")].iloc[0]
    assert monthly["difference__reference_water_in"] == pytest.approx(0.10)
    assert monthly[
        "gallons_per_acre_difference__reference_water_in"
    ] == pytest.approx(0.10 * GALLONS_PER_ACRE_INCH)

    summary = summarize_pairs(matched, bootstrap_replicates=50)
    monthly_reference = summary.loc[
        summary["pair"].eq("S1_S2")
        & summary["scope"].eq("all_matched")
        & summary["outcome"].eq("reference_water_in")
    ].iloc[0]
    assert monthly_reference["biochar_minus_control_mean_in"] == pytest.approx(0.10)
    assert monthly_reference[
        "biochar_minus_control_mean_gal_per_acre"
    ] == pytest.approx(0.10 * GALLONS_PER_ACRE_INCH)

    rain_free_biweekly = summary.loc[
        summary["pair"].eq("S3_S4")
        & summary["scope"].eq("rain_free_since_irrigation")
    ]
    assert rain_free_biweekly.empty


def test_daily_season_requires_75_percent_simultaneous_coverage() -> None:
    complete = pd.date_range("2026-04-01", periods=96, freq="15min")
    partial = pd.date_range("2026-04-02", periods=48, freq="15min")
    timestamps = complete.append(partial)
    water_by_strip = {
        "S1": 2.1,
        "S2": 2.0,
        "S3": 2.2,
        "S4": 2.0,
    }
    shallow = {
        strip: pd.DataFrame({
            "timestamp": timestamps,
            "strip_shallow_water_in": value,
        })
        for strip, value in water_by_strip.items()
    }

    daily = build_daily_pair_differences(
        shallow,
        year=2026,
        source_end=pd.Timestamp("2026-04-03 23:45"),
    )
    monthly = daily.loc[daily["pair"].eq("S1_S2")].set_index("date")

    assert bool(monthly.loc[pd.Timestamp("2026-04-01"), "daily_eligible"])
    assert not bool(monthly.loc[pd.Timestamp("2026-04-02"), "daily_eligible"])
    assert monthly.loc[
        pd.Timestamp("2026-04-01"),
        "biochar_minus_control_daily_mean_in",
    ] == pytest.approx(0.1)

    summary = summarize_daily_irrigation_seasons(daily)
    monthly_summary = summary.loc[summary["pair"].eq("S1_S2")].iloc[0]
    assert monthly_summary["eligible_daily_pairs"] == 1
    assert monthly_summary["mean_daily_difference_in"] == pytest.approx(0.1)
    assert monthly_summary["cumulative_water_inch_days"] == pytest.approx(0.1)


def test_interval_mean_requires_75_percent_complete_coverage() -> None:
    timestamps = pd.date_range("2026-06-01 14:00", periods=40, freq="15min")
    interval = pd.DataFrame({
        "timestamp": timestamps,
        "strip_shallow_water_in": 2.0,
        "year": 2026,
        "strip_group": "S1_S2",
        "strip": "S1",
        "event_id": "2026-06-01_S1_S2",
        "irrigation_start": pd.Timestamp("2026-06-01 08:00"),
        "irrigation_end": pd.Timestamp("2026-06-01 14:00"),
        "next_irrigation_start": pd.Timestamp("2026-06-02 00:00"),
        "irrigation_interval_hours": 10.0,
        "hours_since_irrigation_end": np.arange(40) / 4.0,
    })
    weather = pd.DataFrame(columns=["timestamp", "precip_in"])

    interval.loc[interval.index >= 29, "strip_shallow_water_in"] = np.nan
    sparse = summarize_interval(interval, weather)
    assert sparse is not None
    assert sparse["interval_coverage_fraction"] == pytest.approx(29 / 40)
    assert not sparse["mean_interval_eligible"]
    assert pd.isna(sparse["mean_interval_water_in"])

    interval.loc[29, "strip_shallow_water_in"] = 2.0
    sufficient = summarize_interval(interval, weather)
    assert sufficient is not None
    assert sufficient["interval_coverage_fraction"] == pytest.approx(0.75)
    assert sufficient["mean_interval_eligible"]
    assert sufficient["mean_interval_water_in"] == pytest.approx(2.0)
