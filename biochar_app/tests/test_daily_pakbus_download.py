"""Tests for the daily PakBus acceptance diagnostics."""

from __future__ import annotations

import pandas as pd

from biochar_app.pakbus.core.daily_download import (
    _build_report_email_body,
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


def test_freshness_uses_each_station_download_completion_time() -> None:
    frame = _station_rows("S1T", 2)
    findings, summary = diagnose_download(
        frame,
        expected_stations=["S1T"],
        # A later station can make the complete batch finish much later.
        reference_time=pd.Timestamp("2026-09-11T10:00:00Z"),
        station_reference_times={
            "S1T": pd.Timestamp("2026-09-09T08:00:00Z")
        },
    )

    assert findings == []
    assert summary["S1T"]["latest_age_minutes"] == 30.0


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


def test_summary_records_exact_missing_time_range() -> None:
    frame = _station_rows("S1T", 2).drop(index=[40, 41])
    _findings, summary = diagnose_download(
        frame,
        expected_stations=["S1T"],
        reference_time=pd.Timestamp("2026-09-09T08:00:00Z"),
    )

    assert summary["S1T"]["missing_time_ranges"] == [
        {
            "after": "2026-09-08T17:30:00+00:00",
            "before": "2026-09-08T18:15:00+00:00",
            "missing_intervals": 2,
        }
    ]


def test_report_email_summarizes_recovery_gaps_battery_and_next_steps() -> None:
    report = {
        "status": "rejected",
        "started_at": "2026-09-11T00:15:17-06:00",
        "completed_at": "2026-09-11T00:51:27-06:00",
        "diagnostic_report": "/tmp/report.json",
        "recovery": {
            "requested_stations": ["S2T", "S3B", "S4M"],
            "still_missing": ["S3B"],
            "station_timings": [
                {
                    "station": "S3B",
                    "started_at": "2026-09-11T00:42:20-06:00",
                    "completed_at": "2026-09-11T00:50:00-06:00",
                    "duration_seconds": 460.0,
                    "exit_code": 1,
                    "rows": 0,
                }
            ],
        },
        "station_timings": [
            {
                "station": "S2T",
                "started_at": "2026-09-11T00:18:43-06:00",
                "completed_at": "2026-09-11T00:22:26-06:00",
                "duration_seconds": 223.0,
                "exit_code": 1,
                "rows": 0,
            }
        ],
        "stations": {
            "S2T": {"missing_time_ranges": []},
            "S4M": {
                "missing_time_ranges": [
                    {
                        "after": "2026-09-10T06:00:00+00:00",
                        "before": "2026-09-10T06:45:00+00:00",
                        "missing_intervals": 2,
                    }
                ]
            },
        },
        "findings": [
            {"severity": "critical", "code": "station_missing", "message": "No records returned", "station": "S3B"},
            {"severity": "warning", "code": "battery_warning", "message": "Minimum battery voltage is 11.20 V", "station": "S4M"},
        ],
    }

    body = _build_report_email_body(report)

    assert "Healthy stations (2): S2T, S4M" in body
    assert "Failed initial attempts (3): S2T, S3B, S4M" in body
    assert "Recovered stations (2): S2T, S4M" in body
    assert "Unresolved stations (1): S3B" in body
    assert "S2T (initial, failed): 2026-09-11T00:18:43-06:00 to 2026-09-11T00:22:26-06:00" in body
    assert "S3B (recovery, failed): 2026-09-11T00:42:20-06:00 to 2026-09-11T00:50:00-06:00" in body
    assert "S4M: after 2026-09-10T06:00:00+00:00" in body
    assert "battery_warning [S4M]" in body
    assert "check logger/radio communications at S3B" in body
    assert "incomplete download was rejected" in body


def test_report_marks_missing_station_gap_coverage_unavailable() -> None:
    report = {
        "status": "rejected",
        "stations": {"S1T": {"missing_time_ranges": []}},
        "findings": [
            {
                "severity": "critical",
                "code": "station_missing",
                "message": "No records returned",
                "station": "S4T",
            }
        ],
        "recovery": {"requested_stations": ["S4T"], "still_missing": ["S4T"]},
    }

    body = _build_report_email_body(report)

    assert "none detected in responding stations" in body
    assert "not assessable for S4T because no records were returned" in body
