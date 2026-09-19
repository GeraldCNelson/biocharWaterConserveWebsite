from __future__ import annotations

import pandas as pd

from biochar_app.scripts.gseason_utils import compute_period_summary_rows


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
