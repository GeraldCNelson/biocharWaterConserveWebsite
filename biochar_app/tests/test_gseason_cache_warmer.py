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
    assert result["cache_entries"] == 2
    assert {item["unit_system"] for item in saved} == {"us", "metric"}
    assert all(item["year"] == 2026 for item in saved)
    assert all(item["rows"] for item in saved)


def test_main_runs_requested_year_and_prints_result(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        gseason_cache_warmer,
        "warm_standard_gseason_cache",
        lambda year: {"status": "warmed", "year": year},
    )

    assert gseason_cache_warmer.main(["--year", "2026"]) == 0
    assert '"status": "warmed"' in capsys.readouterr().out
