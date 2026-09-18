"""Regression tests for efficient rendering of dense time-series traces."""

from __future__ import annotations

import pandas as pd

from biochar_app.scripts.plot_builder import (
    WEBGL_POINT_THRESHOLD,
    make_ratio_figure,
    make_raw_figure,
)


def _raw_frame(row_count: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-08-01", periods=row_count, freq="15min"),
            "VWC_1_raw_S1_T": 20.0,
            "VWC_2_raw_S1_T": 25.0,
            "VWC_3_raw_S1_T": 30.0,
            "precip_in": 0.0,
        }
    )


def _figure(row_count: int) -> dict:
    return make_raw_figure(
        df=_raw_frame(row_count),
        variable="VWC",
        strip="S1",
        logger_location="T",
        depth="1",
        unit_system="us",
        year=2026,
        granularity="15min",
        start="2026-08-01",
        end="2026-09-30",
        trace_option="depth",
    )


def test_large_time_series_use_webgl_without_dropping_points() -> None:
    row_count = WEBGL_POINT_THRESHOLD + 1
    figure = _figure(row_count)

    line_traces = figure["data"][:3]
    assert {trace["type"] for trace in line_traces} == {"scattergl"}
    assert all(len(trace["x"]) == row_count for trace in line_traces)


def test_small_time_series_keep_svg_scatter_traces() -> None:
    figure = _figure(10)

    assert {trace["type"] for trace in figure["data"][:3]} == {"scatter"}


def test_large_ratio_series_use_webgl_without_dropping_points() -> None:
    row_count = WEBGL_POINT_THRESHOLD + 1
    timestamps = pd.date_range("2026-08-01", periods=row_count, freq="15min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "VWC_1_ratio_S1_S2_T": 1.5,
            "VWC_1_ratio_S3_S4_T": 1.2,
        }
    )

    figure = make_ratio_figure(
        df=frame,
        variable="VWC",
        strip="S1",
        logger_location="T",
        unit_system="us",
        granularity="15min",
        year=2026,
        start="2026-08-01",
        end="2026-09-30",
        depth="1",
    )

    assert {trace["type"] for trace in figure["data"]} == {"scattergl"}
    assert all(len(trace["x"]) == row_count for trace in figure["data"])
