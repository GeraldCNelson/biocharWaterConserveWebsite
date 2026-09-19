from __future__ import annotations

import pandas as pd

from biochar_app.scripts.gseason_utils import (
    compute_period_summary_rows,
    rebase_periods_to_anchor_year,
)


def test_three_named_custom_periods_are_summarized_independently() -> None:
    timestamps = pd.date_range("2026-01-01", "2026-06-30", freq="D")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "VWC_1_raw_S1_T": timestamps.month.astype(float),
            "VWC_1_ratio_S1_S2_T": timestamps.month.astype(float) / 2,
        }
    )
    periods = [
        {"code": "CUSTOM_1", "label": "Dormant", "start": "2026-01-01", "end": "2026-02-28"},
        {"code": "CUSTOM_2", "label": "Green-up", "start": "2026-03-01", "end": "2026-04-30"},
        {"code": "CUSTOM_3", "label": "Peak growth", "start": "2026-05-01", "end": "2026-06-30"},
    ]

    rows = compute_period_summary_rows(
        frame,
        year=2026,
        periods=periods,
        variable="VWC",
        strip="S1",
        depth="1",
    )

    assert {row["period_code"] for row in rows} == {
        "CUSTOM_1",
        "CUSTOM_2",
        "CUSTOM_3",
    }
    assert {row["period_label"] for row in rows} == {
        "Dormant",
        "Green-up",
        "Peak growth",
    }
    raw_rows = [row for row in rows if row.get("raw_mean") is not None]
    assert [row["raw_mean"] for row in raw_rows] == [1.4746, 3.4918, 5.4918]
    assert all(row["logger_location"] == "T" for row in raw_rows)


def test_period_summary_reports_valid_observations_and_coverage() -> None:
    timestamps = pd.date_range("2025-01-01", "2025-01-01 23:45", freq="15min")
    values = pd.Series(range(len(timestamps)), dtype=float)
    values.iloc[10] = float("nan")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "VWC_1_raw_S1_T": values,
            "VWC_1_ratio_S1_S2_T": values,
        }
    )

    rows = compute_period_summary_rows(
        frame,
        year=2025,
        periods=[
            {
                "code": "CUSTOM_1",
                "label": "One day",
                "start": "2025-01-01",
                "end": "2025-01-01",
            }
        ],
        variable="VWC",
        strip="S1",
        depth="1",
    )

    raw_row = next(row for row in rows if row.get("raw_mean") is not None)
    ratio_row = next(row for row in rows if row.get("ratio_mean") is not None)

    assert raw_row["raw_n"] == 95
    assert raw_row["raw_expected_n"] == 96
    assert raw_row["raw_coverage_pct"] == 99.0
    assert ratio_row["ratio_n"] == 95
    assert ratio_row["ratio_expected_n"] == 96
    assert ratio_row["ratio_coverage_pct"] == 99.0


def test_custom_periods_are_rebased_for_cross_year_comparisons() -> None:
    periods = [
        {
            "code": "WINTER",
            "label": "Winter",
            "start": "2025-11-01",
            "end": "2026-03-31",
        },
        {
            "code": "GROWING",
            "label": "Growing Season",
            "start": "2026-04-01",
            "end": "2026-10-31",
        },
    ]

    rebased = rebase_periods_to_anchor_year(
        periods,
        source_year=2026,
        target_year=2024,
    )

    assert rebased == [
        {
            "code": "WINTER",
            "label": "Winter",
            "start": "2023-11-01",
            "end": "2024-03-31",
        },
        {
            "code": "GROWING",
            "label": "Growing Season",
            "start": "2024-04-01",
            "end": "2024-10-31",
        },
    ]
