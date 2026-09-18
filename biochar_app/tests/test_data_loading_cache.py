"""Tests for the shared dashboard data cache."""

from __future__ import annotations

import pandas as pd

from biochar_app.scripts import data_loading


def test_logger_loader_reuses_year_granularity_frame(monkeypatch) -> None:
    calls: list[tuple[int, str | None]] = []
    expected = pd.DataFrame(
        {"timestamp": [pd.Timestamp("2026-01-01")], "value": [1.0]}
    )

    def fake_loader(year: int, granularity: str | None = None) -> pd.DataFrame:
        calls.append((year, granularity))
        return expected

    data_loading.clear_logger_data_cache()
    monkeypatch.setattr(data_loading, "_load_logger_data_uncached", fake_loader)

    first = data_loading.load_logger_data(2026, "15MIN")
    second = data_loading.load_logger_data(2026, "15min")

    assert first is expected
    assert second is expected
    assert calls == [(2026, "15min")]
    data_loading.clear_logger_data_cache()


def test_logger_cache_can_be_cleared(monkeypatch) -> None:
    calls = 0

    def fake_loader(year: int, granularity: str | None = None) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return pd.DataFrame({"timestamp": [pd.Timestamp("2026-01-01")]})

    data_loading.clear_logger_data_cache()
    monkeypatch.setattr(data_loading, "_load_logger_data_uncached", fake_loader)

    data_loading.load_logger_data(2026, "daily")
    data_loading.clear_logger_data_cache()
    data_loading.load_logger_data(2026, "daily")

    assert calls == 2
    data_loading.clear_logger_data_cache()
