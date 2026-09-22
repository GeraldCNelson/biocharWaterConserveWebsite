"""Tests for independent precipitation retention screening."""

from __future__ import annotations

import pandas as pd
import pytest

from biochar_app.config.field_management_metadata import (
    ZONE_AREAS_SQFT_BY_STRIP,
    ZONE_GALLONS_PER_INCH_BY_STRIP,
)

from biochar_app.scripts.management.irrigation_analysis.precipitation_retention import (
    PrecipitationRetentionConfig,
    analyze_precipitation_sensor_responses,
    bootstrap_matched_profile_differences,
    bootstrap_unmatched_profile_differences,
    build_combined_empirical_capacity,
    build_irrigation_capacity_observations,
    build_irrigation_available_storage_comparison,
    build_irrigation_operational_diagnostics,
    build_matched_complete_profiles,
    build_paired_treatment_summary,
    build_profile_retention,
    build_profile_best_results,
    build_zone_upper_retained_water,
    compare_matched_and_unmatched_bootstraps,
    flag_irrigation_overlap,
    identify_precipitation_events,
    summarize_empirical_maxima,
)


def test_identifies_events_and_allows_one_dry_day() -> None:
    timestamps = pd.date_range("2025-06-01", "2025-06-05", freq="15min")
    weather = pd.DataFrame({"timestamp": timestamps, "precip_in": 0.0})
    weather.loc[weather["timestamp"].eq(pd.Timestamp("2025-06-01 06:00")), "precip_in"] = 0.10
    weather.loc[weather["timestamp"].eq(pd.Timestamp("2025-06-03 08:00")), "precip_in"] = 0.20

    events = identify_precipitation_events(weather)

    assert len(events) == 1
    assert events.iloc[0]["precip_total_in"] == pytest.approx(0.30)


def test_irrigation_overlap_uses_full_analysis_window() -> None:
    events = pd.DataFrame({
        "event_id": ["rain"], "year": [2025],
        "event_start": [pd.Timestamp("2025-06-02 00:00")],
        "event_end": [pd.Timestamp("2025-06-02 06:00")],
        "precip_total_in": [0.5],
    })
    irrigation = pd.DataFrame({
        "start_timestamp": ["2025-06-04 00:00"],
        "end_timestamp": ["2025-06-04 01:00"],
    })

    flagged = flag_irrigation_overlap(events, irrigation)

    assert bool(flagged.iloc[0]["irrigation_overlap"])
    assert not bool(flagged.iloc[0]["independent_event"])


def _logger_frame(*, minimum_temperature: float = 40.0) -> pd.DataFrame:
    timestamps = pd.date_range("2025-05-31", "2025-06-06", freq="15min")
    data: dict[str, object] = {"timestamp": timestamps}
    for strip, offset in (("S1", 2.0), ("S2", 0.0)):
        for depth in (1, 2, 3):
            values = pd.Series(20.0 + offset, index=timestamps)
            values.loc[timestamps >= pd.Timestamp("2025-06-02 00:00")] += 5.0
            data[f"VWC_{depth}_raw_{strip}_T"] = values.to_numpy()
            temperatures = pd.Series(40.0, index=timestamps)
            temperatures.loc[pd.Timestamp("2025-06-02 03:00")] = minimum_temperature
            data[f"T_{depth}_raw_{strip}_T"] = temperatures.to_numpy()
    return pd.DataFrame(data)


def _event_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "event_id": ["rain"], "year": [2025],
        "event_start": [pd.Timestamp("2025-06-02 00:00")],
        "event_end": [pd.Timestamp("2025-06-02 06:00")],
        "precip_total_in": [0.5], "independent_event": [True],
    })


def test_uses_matching_logger_temperature_and_builds_profiles_and_pairs() -> None:
    results = analyze_precipitation_sensor_responses(_logger_frame(), _event_frame())

    assert results["temperature_source"].eq("co_located_cs650_logger").all()
    assert results["retention_eligible"].all()
    assert results["empirical_max_eligible"].all()
    profiles = build_profile_retention(results)
    assert len(profiles) == 2
    assert profiles.loc[
        profiles["strip"].eq("S1"), "retained_72h_profile_water_in"
    ].iloc[0] == pytest.approx(4.86)
    paired = build_paired_treatment_summary(results)
    assert len(paired) == 3
    assert paired["difference_retained_72h_vwc"].eq(2.0).all()


def test_freeze_flag_is_sensor_level_and_blocks_eligibility() -> None:
    results = analyze_precipitation_sensor_responses(
        _logger_frame(minimum_temperature=31.5), _event_frame(),
        config=PrecipitationRetentionConfig(freeze_threshold_f=32.5),
    )

    assert results["freeze_affected"].all()
    assert not results["empirical_max_eligible"].any()
    assert results["eligibility_reason"].str.contains("local_temperature_near_freezing").all()


def test_remote_gauge_event_without_field_corroboration_is_excluded() -> None:
    event = _event_frame().assign(
        event_id="precip_20260729_1615_20260729_1615"
    )
    results = analyze_precipitation_sensor_responses(_logger_frame(), event)

    assert results["material_response"].all()
    assert results["field_event_excluded"].all()
    assert not results["retention_eligible"].any()
    assert not results["empirical_max_eligible"].any()
    assert results["eligibility_reason"].str.contains(
        "remote_weather_station_precipitation_not_corroborated_at_field"
    ).all()


def test_cold_nonfreezing_events_are_corroborating_not_empirical_maxima() -> None:
    results = analyze_precipitation_sensor_responses(
        _logger_frame(minimum_temperature=35.0), _event_frame(),
    )

    assert results["retention_eligible"].all()
    assert not results["empirical_max_eligible"].any()
    assert results["local_temperature_class"].eq("cold_nonfreezing").all()


def test_empirical_maxima_are_rounded_and_classified() -> None:
    rows = []
    for index, value in enumerate((20.0, 21.0, 22.0, 40.0), start=1):
        rows.append({
            "event_id": f"event-{index}", "year": 2025,
            "strip": "S1", "treatment": "biochar", "pair": "S1_S2",
            "logger_position": "T", "depth_index": 1, "depth_inches": 6,
            "sensor_col": "VWC_1_raw_S1_T", "retained_48h_vwc": value,
            "retained_72h_vwc": value - 0.1234, "empirical_max_eligible": True,
        })

    summary = summarize_empirical_maxima(pd.DataFrame(rows)).iloc[0]

    assert summary["retained_vwc_p90"] == pytest.approx(34.6)
    assert summary["retained_vwc_median"] == pytest.approx(21.5)
    assert summary["adoption_classification"] == "review_max_p90_gap"
    assert summary["recommended_upper_retained_vwc"] == summary["retained_vwc_p90"]


def test_combines_trustworthy_irrigation_and_precipitation_capacity() -> None:
    precipitation = analyze_precipitation_sensor_responses(
        _logger_frame(), _event_frame()
    )
    irrigation_events = pd.DataFrame({
        "year": [2025, 2025], "strip": ["S1", "S1"],
        "event_id": ["irrigation-good", "irrigation-below-baseline"],
        "sensor_col": ["VWC_1_raw_S1_T", "VWC_1_raw_S1_T"],
        "logger_position": ["T", "T"], "depth_index": [1, 1],
        "depth_inches": [6, 6], "baseline_vwc": [20.0, 30.0],
        "plateau_vwc": [32.0, 29.0], "plateau_method": ["flat_segment", "fallback_window"],
        "irrigation_start": ["2025-07-01 08:00", "2025-07-15 08:00"],
        "irrigation_end": ["2025-07-01 12:00", "2025-07-15 12:00"],
        "time_to_plateau_hours": [12.0, 12.0],
    })
    irrigation_qc = pd.DataFrame({
        "year": [2025, 2025], "strip": ["S1", "S1"],
        "event_id": ["irrigation-good", "irrigation-below-baseline"],
        "sensor_col": ["VWC_1_raw_S1_T", "VWC_1_raw_S1_T"],
        "trustworthy_event": [True, True],
    })

    irrigation = build_irrigation_capacity_observations(
        irrigation_events, irrigation_qc
    )
    observations, combined = build_combined_empirical_capacity(
        precipitation, irrigation
    )
    row = combined.loc[combined["sensor_col"].eq("VWC_1_raw_S1_T")].iloc[0]

    assert len(irrigation) == 1
    assert set(observations["capacity_source"]) == {"precipitation", "irrigation"}
    assert row["irrigation_source_event_count"] == 1
    assert row["precipitation_source_event_count"] == 1
    assert row["highest_observed_source"] == "irrigation"
    assert row["recommended_upper_retained_vwc"] == pytest.approx(31.5)


def test_converts_three_depth_p90_profile_to_zone_water_volume() -> None:
    rows = []
    for depth_index, depth_inches, vwc in (
        (1, 6, 40.0), (2, 12, 30.0), (3, 18, 20.0)
    ):
        rows.append({
            "strip": "S1", "treatment": "biochar", "pair": "S1_S2",
            "logger_position": "T", "depth_index": depth_index,
            "depth_inches": depth_inches,
            "recommended_upper_retained_vwc": vwc,
            "eligible_event_count": 5,
            "adoption_classification": "supported",
            "source_agreement_classification": "within_2_vwc",
        })

    result = build_zone_upper_retained_water(pd.DataFrame(rows)).iloc[0]

    assert result["profile_lower_bound_in"] == pytest.approx(3.0)
    assert result["profile_upper_bound_in"] == pytest.approx(21.0)
    assert result["represented_profile_thickness_in"] == pytest.approx(18.0)
    assert result["profile_equivalent_water_in"] == pytest.approx(5.4)
    assert result["zone_area_sqft"] == pytest.approx(
        ZONE_AREAS_SQFT_BY_STRIP["S1"]["T"], abs=0.01
    )
    assert result["estimated_upper_retained_water_gal"] == pytest.approx(
        5.4 * ZONE_GALLONS_PER_INCH_BY_STRIP["S1"]["T"], abs=1
    )
    assert result["estimate_status"] == "supported"


def test_compares_irrigation_with_available_nine_sensor_storage() -> None:
    event_rows = []
    qc_rows = []
    capacity_rows = []
    for position in "TMB":
        for depth in (1, 2, 3):
            sensor = f"VWC_{depth}_raw_S1_{position}"
            event_rows.append({
                "year": 2025, "event_id": "event-1", "strip": "S1",
                "strip_group": "S1_S2", "logger_position": position,
                "depth_index": depth, "sensor_col": sensor,
                "baseline_vwc": 20.0, "gallons_strip": 100_000.0,
                "irrigation_start": "2025-06-01 08:00",
                "irrigation_end": "2025-06-01 14:00",
            })
            qc_rows.append({
                "year": 2025, "event_id": "event-1", "strip": "S1",
                "sensor_col": sensor, "trustworthy_event": True,
            })
            capacity_rows.append({
                "strip": "S1", "logger_position": position,
                "depth_index": depth, "recommended_upper_retained_vwc": 40.0,
            })

    result = build_irrigation_available_storage_comparison(
        pd.DataFrame(event_rows), pd.DataFrame(qc_rows), pd.DataFrame(capacity_rows)
    ).iloc[0]
    expected_available = sum(
        3.6 * ZONE_GALLONS_PER_INCH_BY_STRIP["S1"][position]
        for position in "TMB"
    )
    assert result["comparison_eligible"]
    assert result["available_storage_gal_strip"] == pytest.approx(
        expected_available, abs=0.02
    )
    assert result["irrigation_exceeds_available_storage"]
    assert result["estimated_water_not_retained_gal_strip"] == pytest.approx(
        100_000 - expected_available, abs=0.02
    )


def test_builds_retrospective_candidate_inspection_alert() -> None:
    storage = pd.DataFrame([{
        "year": 2025, "event_id": "event-1", "strip": "S1",
        "irrigation_start": "2025-06-01 08:00",
        "irrigation_end": "2025-06-01 18:00",
        "comparison_eligible": True,
        "available_storage_gal_strip": 50_000.0,
        "applied_irrigation_gal_strip": 100_000.0,
    }])
    arrivals = pd.DataFrame([
        {
            "year": 2025, "event_id": "event-1", "strip": "S1",
            "logger_position": position, "depth_index": depth,
            "arrival_time": pd.Timestamp("2025-06-01 08:00")
            + pd.Timedelta(hours=delay),
            "arrival_minutes_after_irrigation_start": delay * 60,
        }
        for position, base_delay in (("T", 1.0), ("M", 2.0), ("B", 3.0))
        for depth, delay in ((1, base_delay), (2, base_delay + 0.5), (3, base_delay + 1.0))
    ])
    metadata = pd.DataFrame([{
        "year": 2025, "event_id": "event-1", "strip": "S1",
        "event_duration_hours": 10.0, "avg_flow_gph_strip": 10_000.0,
        "avg_flow_gpm_strip": 166.67, "avg_flow_gpm_group": 333.33,
        "start_flow_gpm": 350.0, "end_flow_gpm": 300.0,
    }])

    result = build_irrigation_operational_diagnostics(
        storage, arrivals, metadata
    ).iloc[0]

    assert result["modeled_hours_to_80pct_available_storage"] == pytest.approx(4.0)
    assert result["bottom_earliest_arrival_delay_hr"] == pytest.approx(3.0)
    assert result["bottom_all_depths_arrival_delay_hr"] == pytest.approx(4.0)
    assert result["candidate_inspection_alert_time"] == pd.Timestamp("2025-06-01 12:00")
    assert result["runtime_after_candidate_alert_hr"] == pytest.approx(6.0)
    assert result["applied_after_candidate_alert_gal"] == pytest.approx(60_000.0)
    assert result[
        "max_avg_flow_gph_to_fit_available_storage_at_actual_duration"
    ] == pytest.approx(5_000.0)
    assert result[
        "modeled_avg_flow_reduction_fraction_needed_at_actual_duration"
    ] == pytest.approx(0.5)


def test_bootstraps_only_matched_complete_three_depth_profiles() -> None:
    rows = []
    for event_id, biochar_vwc, control_vwc in (
        ("event-1", 40.0, 30.0),
        ("event-2", 35.0, 30.0),
    ):
        for strip, treatment, vwc in (
            ("S1", "biochar", biochar_vwc),
            ("S2", "control", control_vwc),
        ):
            for depth_index, depth_inches in ((1, 6), (2, 12), (3, 18)):
                rows.append({
                    "capacity_source": "irrigation", "event_id": event_id,
                    "year": 2025, "pair": "S1_S2", "strip": strip,
                    "treatment": treatment, "logger_position": "T",
                    "depth_index": depth_index, "depth_inches": depth_inches,
                    "capacity_vwc": vwc,
                })
    # An incomplete third event must not enter the matched analysis.
    rows.append({
        "capacity_source": "irrigation", "event_id": "event-incomplete",
        "year": 2025, "pair": "S1_S2", "strip": "S1",
        "treatment": "biochar", "logger_position": "T", "depth_index": 1,
        "depth_inches": 6, "capacity_vwc": 50.0,
    })
    profiles, matched = build_matched_complete_profiles(pd.DataFrame(rows))
    bootstrap = bootstrap_matched_profile_differences(
        matched, iterations=1_000, random_seed=7
    )
    combined = bootstrap.loc[bootstrap["capacity_source"].eq("combined")].iloc[0]

    assert len(profiles) == 4
    assert len(matched) == 2
    assert matched["biochar_minus_control_profile_water_in"].tolist() == [1.8, 0.9]
    assert combined["matched_event_count"] == 2
    assert combined["bootstrap_direction_classification"] == "insufficient_events"

    unmatched = bootstrap_unmatched_profile_differences(
        profiles, iterations=1_000, random_seed=7
    )
    comparison = compare_matched_and_unmatched_bootstraps(bootstrap, unmatched)
    unmatched_combined = unmatched.loc[
        unmatched["capacity_source"].eq("combined")
    ].iloc[0]
    assert unmatched_combined["biochar_complete_profile_count"] == 2
    assert unmatched_combined["control_complete_profile_count"] == 2
    assert unmatched_combined["mean_difference_in"] == pytest.approx(1.35)
    assert comparison.iloc[0]["direction_comparison"] == "agreement"
    best = build_profile_best_results(comparison)
    assert best.iloc[0]["statistical_repeatability"] == "insufficient"
    assert best.iloc[0]["causal_confidence"] == "limited_single_strip_per_treatment"
