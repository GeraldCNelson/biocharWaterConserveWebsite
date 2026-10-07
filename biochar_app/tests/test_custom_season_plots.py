import asyncio

import pandas as pd
import pytest

from biochar_app.scripts import data_loading, gseason_utils, routes_utils, routes


@pytest.fixture
def seasonal_sources(tmp_path, monkeypatch):
    root = tmp_path / "parquet"
    logger = root / "summary" / "15min"
    weather = root / "summary" / "weather" / "daily"
    logger.mkdir(parents=True)
    weather.mkdir(parents=True)
    monkeypatch.setattr(data_loading, "PARQUET_SUMMARY_DIR", root / "summary")
    monkeypatch.setattr(routes_utils, "PARQUET_DIR", root)
    monkeypatch.setattr(gseason_utils, "PARQUET_DIR", root)
    monkeypatch.setattr(routes_utils, "gseason_source_paths", lambda y: list(logger.glob(f"{y}_*.parquet")))
    routes_utils._PLOT_SEASON_CACHE.clear()
    for year, day, value in [(2024, "12-31", 10.0), (2025, "01-01", 30.0)]:
        pd.DataFrame({"timestamp": pd.to_datetime([f"{year}-{day}"]),
                      "VWC_1_raw_S1_T": [value], "VWC_1_raw_S2_T": [10.0],
                      "unrelated_huge_column": [999.0]}).to_parquet(logger / f"{year}_15min.parquet", index=False)
        pd.DataFrame({"timestamp": pd.to_datetime([f"{year}-{day}"]), "precip_in": [1.0]}).to_parquet(weather / f"{year}_daily.parquet", index=False)
    yield root
    routes_utils._PLOT_SEASON_CACHE.clear()


def test_plot_custom_window_includes_prior_year_and_shares_aggregation(seasonal_sources, monkeypatch):
    periods = [{"code": "WINTER", "label": "Winter", "start": "2024-12-31", "end": "2025-01-01"}]
    result = routes_utils.load_gseason_df(2025, periods, variable="VWC")
    assert result.iloc[0]["VWC_1_raw_S1_T"] == 20
    assert result.iloc[0]["precip_in"] == 2
    assert result.iloc[0]["period_incomplete"]
    assert result.iloc[0]["period_expected_n"] == 192
    def forbidden(*args, **kwargs):
        raise AssertionError("Raw/ratio pair must share cached aggregation")
    monkeypatch.setattr(routes_utils, "load_seasonal_logger_slice", forbidden)
    pd.testing.assert_frame_equal(result, routes_utils.load_gseason_df(2025, periods, variable="VWC", use_ratios=True))


def test_empty_first_winter_is_not_reported_complete(seasonal_sources):
    result = routes_utils.load_gseason_df(2023, [{"code": "WINTER", "label": "Winter", "start": "2022-11-01", "end": "2023-03-31"}], variable="VWC")
    assert result.iloc[0]["period_observed_n"] == 0
    assert result.iloc[0]["period_incomplete"]


def test_plot_labels_identify_partial_periods(seasonal_sources):
    from biochar_app.scripts.plot_builder import make_raw_gseason_figure
    periods = [{"code": "WINTER", "label": "Winter", "start": "2024-12-31", "end": "2025-01-01"}]
    result = routes_utils.load_gseason_df(2025, periods, variable="VWC")
    figure = make_raw_gseason_figure(df=result, periods=periods, variable="VWC", strip="S1", logger_location="T", depth=1, unit_system="us", year=2025, trace_option="depth")
    assert "incomplete data" in figure["data"][0]["x"][0]


def test_month_day_defaults_are_expanded_for_requested_year(seasonal_sources):
    result = routes_utils.load_gseason_df(2025, [{"code": "WINTER", "label": "Winter", "start": "11-01", "end": "03-31"}], variable="VWC")
    assert result.iloc[0]["start"] == pd.Timestamp("2024-11-01")
    assert result.iloc[0]["VWC_1_raw_S1_T"] == 20


def test_plot_year_rebases_applied_dates(seasonal_sources, monkeypatch):
    seen = []
    def figure(**kwargs):
        seen.append(kwargs["periods"])
        return {"data": [], "layout": {}}
    monkeypatch.setattr(routes, "make_raw_gseason_figure", figure)
    monkeypatch.setattr(routes, "make_ratio_gseason_figure", figure)
    request = routes.PlotRequest(year=2025, variable="VWC", strip="S1", depth="1", loggerLocation="T", granularity="gseason", traceOption="depth", unitSystem="us", startDate="", endDate="", periodsAnchorYear=2026,
        periods=[{"code": "WINTER", "label": "Winter", "start": "2025-12-31", "end": "2026-01-01"}])
    asyncio.run(routes.api_plot_raw(request))
    asyncio.run(routes.api_plot_ratio(request))
    assert len(seen) == 2
    assert all(p[0]["start"] == "2024-12-31" and p[0]["end"] == "2025-01-01" for p in seen)
