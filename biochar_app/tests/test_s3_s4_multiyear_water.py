from __future__ import annotations

import pandas as pd

from biochar_app.scripts.research.analyze_s3_s4_multiyear_water import (
    build_2026_event_audit,
    build_paired_water_balance,
    group_deep_wetting_episodes,
    identify_unexplained_deep_rises,
    summarize_photo_source,
    summarize_deep_dominance,
)
from biochar_app.scripts.management.irrigation_analysis.analyze_s3m_timing import (
    build_lateral_diagnostics,
)


def _daily_rows(strip: str, position: str, values: list[tuple[float, float, float]]) -> pd.DataFrame:
    dates = pd.date_range("2026-06-10", periods=len(values), freq="D")
    return pd.DataFrame(
        {
            "year": 2026,
            "date": dates,
            "strip": strip,
            "position": position,
            "vwc_6": [value[0] for value in values],
            "vwc_12": [value[1] for value in values],
            "vwc_18": [value[2] for value in values],
            "deep_minus_shallow_max": [
                value[2] - max(value[0], value[1]) for value in values
            ],
            "precip_in": 0.0,
            "irrigation_day": False,
        }
    )


def test_deep_dominance_summary_excludes_western_comparison_strips() -> None:
    values = [(20.0, 21.0, 25.0)] * 14
    daily = pd.concat(
        [_daily_rows("S1", "T", values), _daily_rows("S3", "T", values)],
        ignore_index=True,
    )

    result = summarize_deep_dominance(daily)

    assert result["strip"].tolist() == ["S3"]
    assert bool(result.iloc[0]["persistent_at_first_available_data"])
    assert result.iloc[0]["onset_interpretation"] == "present_at_or_before_first_available_date"


def test_unexplained_rise_screen_tracks_western_comparison_and_exclusions() -> None:
    baseline = [(20.0, 20.0, 20.0), (20.1, 20.2, 22.0)]
    daily = pd.concat(
        [
            _daily_rows("S1", "B", baseline),
            _daily_rows("S3", "B", baseline),
            _daily_rows("S4", "B", baseline),
        ],
        ignore_index=True,
    )
    # Rain excludes S4 while leaving the otherwise identical S3 and S1 candidates.
    daily.loc[daily["strip"].eq("S4") & daily["date"].eq(pd.Timestamp("2026-06-11")), "precip_in"] = 0.1

    result = identify_unexplained_deep_rises(daily)

    assert result[["strip", "position"]].values.tolist() == [["S3", "B"]]
    assert bool(result.iloc[0]["west_comparable_deep_rise"])
    assert result.iloc[0]["west_candidate_strips"] == "S1"


def test_episode_grouping_identifies_s4_first_s3_later() -> None:
    rises = pd.DataFrame(
        {
            "year": [2025, 2025, 2025],
            "date": pd.to_datetime(["2025-05-26", "2025-05-27", "2025-06-10"]),
            "strip": ["S4", "S3", "S4"],
            "position": ["M", "B", "B"],
            "deep_daily_change": [3.0, 2.0, 1.5],
            "deep_change_beyond_shallow": [2.5, 1.5, 1.0],
            "west_comparable_deep_rise": [False, False, True],
        }
    )

    result = group_deep_wetting_episodes(rises)

    assert len(result) == 2
    first = result.iloc[0]
    assert bool(first["starts_in_s4"])
    assert bool(first["reaches_s3_later"])
    assert first["days_s4_to_s3"] == 1
    assert not bool(first["west_comparable_deep_rise"])
    assert bool(result.iloc[1]["west_comparable_deep_rise"])


def test_lateral_diagnostic_rejects_field_wide_pre_start_response() -> None:
    rows = []
    for strip_group, strips in (("S1_S2", ("S1", "S2")), ("S3_S4", ("S3", "S4"))):
        for strip in strips:
            rows.append(
                {
                    "year": 2026,
                    "event_id": f"event-{strip_group}",
                    "strip_group": strip_group,
                    "irrigation_start": pd.Timestamp("2026-04-21 14:54"),
                    "strip": strip,
                    "logger_position": "T" if strip in ("S1", "S3") else "B",
                    "flag_pre_start_response": True,
                    "flag_unexplained_pre_start_response": True,
                    "likely_precip_driven_pre_start_response": False,
                }
            )

    result = build_lateral_diagnostics(pd.DataFrame(rows))

    assert len(result) == 1
    assert bool(result.iloc[0]["field_wide_pre_start_drift"])
    assert bool(result.iloc[0]["west_comparable_unexplained_pre_start"])
    assert not bool(result.iloc[0]["possible_lateral_entry"])


def test_photo_availability_is_separate_from_exact_time_agreement() -> None:
    audit = build_2026_event_audit()

    assert int(audit["start_photo_available"].sum()) == 9
    assert int(audit["absolute_start_anchor"].eq("photo_verified").sum()) == 7
    june_27 = audit.loc[audit["event_id"].str.startswith("2026-06-27")].iloc[0]
    august_14 = audit.loc[audit["event_id"].str.startswith("2026-08-14")].iloc[0]
    assert bool(june_27["start_photo_available"])
    assert june_27["absolute_start_anchor"] == "photo_workbook_disagreement"
    assert bool(august_14["start_photo_available"])
    assert august_14["start_photo_filename"] == "IMG_8789.jpg"


def test_equal_applied_water_makes_residual_difference_negative_storage_difference() -> None:
    paired, _summary = build_paired_water_balance()
    equal = paired.loc[
        paired["equal_strip_allocation_assumed"]
        & paired["biochar_minus_control_storage_gal"].notna()
    ]

    assert not equal.empty
    assert equal["signed_residual_difference_identity_error_gal"].abs().max() < 0.01


def test_photo_source_uses_canonical_inventory_and_retains_unmatched_latest_photos() -> None:
    audit = build_2026_event_audit()
    summary = summarize_photo_source(audit)

    assert summary["reviewed_2026_photos"] == 24
    assert summary["latest_unmatched_2026_photo_count"] == 3
    assert summary["latest_unmatched_2026_filenames"] == [
        "IMG_9123.HEIC", "IMG_9124.HEIC", "IMG_9139.HEIC",
    ]
    assert summary["all_2026_referenced_photos_match_review"]
