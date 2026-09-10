"""Tests for the daily PakBus acceptance diagnostics."""

from __future__ import annotations

import pandas as pd

from biochar_app.pakbus.core.daily_download import (
    diagnose_download,
    merge_download_frames,
    missing_stations,
)


def _station_rows(station: str, logger_id: int, *, periods: int = 96, battery: float = 12.4) -> pd.DataFrame:
    timestamps = pd.date_range("2026-09-08T07:45:00Z", periods=periods, freq="15min")
    return pd.DataFrame(
        {
            "station": station,
            "logger_id": logger_id,
            "Datetime": timestamps,
            "RecNbr": range(periods),
            "BattV_Min": battery,
        }
    )


def test_healthy_download_is_accepted() -> None:
    frame = _station_rows("S1T", 2)
    findings, summary = diagnose_download(
        frame,
        expected_stations=["S1T"],
        reference_time=pd.Timestamp("2026-09-09T08:00:00Z"),
    )
    assert findings == []
    assert summary["S1T"]["rows"] == 96
    assert summary["S1T"]["maximum_gap_minutes"] == 15.0


def test_missing_station_gap_and_low_battery_are_critical() -> None:
    frame = _station_rows("S1T", 2, periods=95, battery=9.5).drop(index=[40, 41])
    findings, _summary = diagnose_download(
        frame,
        expected_stations=["S1T", "S1M"],
        reference_time=pd.Timestamp("2026-09-09T08:00:00Z"),
    )
    codes = {(item.station, item.code, item.severity) for item in findings}
    assert ("S1M", "station_missing", "critical") in codes
    assert ("S1T", "timestamp_gap", "critical") in codes
    assert ("S1T", "battery_critical", "critical") in codes


def test_moderately_low_battery_is_warning() -> None:
    frame = _station_rows("S1T", 2, battery=11.2)
    findings, _summary = diagnose_download(
        frame,
        expected_stations=["S1T"],
        reference_time=pd.Timestamp("2026-09-09T08:00:00Z"),
    )
    assert [(item.code, item.severity) for item in findings] == [("battery_warning", "warning")]


def test_recovery_rows_fill_missing_station_without_duplicates() -> None:
    initial = _station_rows("S1T", 2)
    recovery = pd.concat(
        [_station_rows("S1T", 2).tail(1), _station_rows("S1M", 3)],
        ignore_index=True,
    )

    assert missing_stations(initial, ["S1T", "S1M"]) == ["S1M"]
    merged = merge_download_frames(initial, recovery)

    assert len(merged) == 192
    assert missing_stations(merged, ["S1T", "S1M"]) == []
