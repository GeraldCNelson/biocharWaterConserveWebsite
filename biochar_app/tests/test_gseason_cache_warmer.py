from __future__ import annotations

import pandas as pd

from biochar_app.scripts import gseason_cache_warmer


def test_warmer_materializes_each_requested_filter_and_unit(monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-04-01", periods=4, freq="15min"),
            "VWC_1_raw_S2_T": [20.0, 21.0, 22.0, 23.0],
            "VWC_1_ratio_S1_S2_T": [1.0, 1.1, 1.2, 1.3],
            "VWC_1_ratio_S3_S4_T": [0.8, 0.9, 1.0, 1.1],
        }
    )
    saved: list[dict] = []
    cleared: list[bool] = []
    monkeypatch.setattr(
        gseason_cache_warmer,
        "load_materialized_gseason_summary",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(gseason_cache_warmer, "clear_logger_data_cache", lambda: cleared.append(True))
    monkeypatch.setattr(gseason_cache_warmer, "load_logger_data", lambda year, granularity: frame)
    monkeypatch.setattr(
        gseason_cache_warmer,
        "save_materialized_gseason_summary",
        lambda **kwargs: saved.append(kwargs),
    )

    result = gseason_cache_warmer.warm_standard_gseason_cache(
        2026,
        variables=["VWC"],
        strips=["S2"],
        depths=["1"],
        unit_systems=["us", "metric"],
    )

    assert cleared == [True]
    assert result["status"] == "warmed"
    assert result["computed_configurations"] == 1
    assert result["reused_configurations"] == 0
    assert result["cache_entries"] == 2
    assert {item["unit_system"] for item in saved} == {"us", "metric"}
    assert all(item["year"] == 2026 for item in saved)
    assert all(item["rows"] for item in saved)


def test_warmer_reuses_valid_entries_without_loading_source(monkeypatch) -> None:
    monkeypatch.setattr(
        gseason_cache_warmer,
        "load_materialized_gseason_summary",
        lambda **kwargs: [{"period": "Growing Season"}],
    )
    monkeypatch.setattr(
        gseason_cache_warmer,
        "load_logger_data",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("source loaded")),
    )

    result = gseason_cache_warmer.warm_standard_gseason_cache(
        2025,
        variables=["VWC"],
        strips=["S3"],
        depths=["1"],
        unit_systems=["us", "metric"],
    )

    assert result["computed_configurations"] == 0
    assert result["reused_configurations"] == 1
    assert result["cache_entries"] == 0
    assert result["reused_cache_entries"] == 2


def test_all_years_aggregates_computed_and_reused_entries(monkeypatch) -> None:
    monkeypatch.setattr(
        gseason_cache_warmer,
        "warm_standard_gseason_cache",
        lambda year: {
            "year": year,
            "computed_configurations": 1 if year == 2026 else 0,
            "reused_configurations": 0 if year == 2026 else 1,
            "cache_entries": 2 if year == 2026 else 0,
            "reused_cache_entries": 0 if year == 2026 else 2,
            "summary_rows": 3,
            "duration_seconds": 0.1,
        },
    )

    result = gseason_cache_warmer.warm_standard_gseason_caches([2025, 2026])

    assert result["years"] == [2025, 2026]
    assert result["computed_configurations"] == 1
    assert result["reused_configurations"] == 1
    assert result["cache_entries"] == 2
    assert result["reused_cache_entries"] == 2


def test_main_runs_requested_year_and_prints_result(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        gseason_cache_warmer,
        "warm_standard_gseason_cache",
        lambda year: {"status": "warmed", "year": year},
    )

    assert gseason_cache_warmer.main(["--year", "2026"]) == 0
    assert '"status": "warmed"' in capsys.readouterr().out
