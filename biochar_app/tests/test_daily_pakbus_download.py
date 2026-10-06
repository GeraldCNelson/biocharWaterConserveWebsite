"""Tests for the daily PakBus acceptance diagnostics."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest
import json

from biochar_app.pakbus.core.daily_download import (
    _build_report_email_body,
    _build_success_email_body,
    _publish_to_production,
    Finding,
    apply_archive_continuity,
    build_communication_reliability,
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


@pytest.mark.parametrize("recover", [True, False])
def test_empty_initial_pass_reaches_recovery_and_never_publishes_incomplete_data(
    monkeypatch, tmp_path, recover
):
    from biochar_app.pakbus.core import daily_download as daily

    calls, waits, reports, archives = [], [], [], []

    def run(output, **kwargs):
        calls.append(kwargs)
        recovered = recover and len(calls) == 2
        if recovered:
            frames = []
            for number, station in enumerate(daily.EXPECTED_STATIONS, 2):
                frame = _station_rows(station, number)
                frame["Datetime"] = pd.date_range(
                    end=pd.Timestamp.now(tz="UTC").floor("15min"), periods=96, freq="15min")
                frames.append(frame)
            pd.concat(frames).to_csv(output, index=False)
        timings = [{"station": station, "exit_code": 0 if recovered else 1,
                    "rows": 96 if recovered else 0,
                    "completed_at": pd.Timestamp.now(tz="UTC").isoformat()}
                   for station in daily.EXPECTED_STATIONS]
        kwargs["timing_output"].write_text(json.dumps(timings))
        return SimpleNamespace(returncode=0 if recovered else 1)

    monkeypatch.setattr(daily, "quick_port_check_ipv6", lambda *args: (True, "ok"))
    monkeypatch.setattr(daily, "_run_client", run)
    monkeypatch.setattr(daily.time, "sleep", waits.append)
    monkeypatch.setattr(daily, "_finish_report", lambda report, *args: reports.append(report.copy()))
    monkeypatch.setattr(daily, "promote_accepted_frame",
                        lambda frame, **kwargs: archives.append(len(frame)) or {"status": "test"})
    monkeypatch.setattr(daily, "_publish_operational_update",
                        lambda *args, **kwargs: pytest.fail("unexpected publication"))
    result = daily.main(["--run-root", str(tmp_path / "runs"), "--lock", str(tmp_path / "lock"),
                         "--archive-root", str(tmp_path / "archive"), "--skip-publication",
                         "--recovery-wait", "5", "--recovery-passes", "2"])
    assert calls[0]["attempts"] == 3
    assert calls[1]["stations"] == list(daily.EXPECTED_STATIONS)
    assert calls[1]["attempts"] == 3
    assert waits == ([5] if recover else [5, 5])
    assert result == (0 if recover else 1)
    assert reports[-1]["status"] == ("accepted" if recover else "rejected")
    assert reports[-1]["recovery"]["still_missing"] == ([] if recover else list(daily.EXPECTED_STATIONS))
    assert archives == ([12 * 96] if recover else [])
    assert reports[-1]["completed_at"]


def test_daily_client_command_enables_hybrid_policy(monkeypatch, tmp_path):
    from biochar_app.pakbus.core import daily_download as daily

    commands = []
    monkeypatch.setattr(daily.subprocess, "run",
                        lambda command, **kwargs: commands.append(command) or SimpleNamespace(returncode=0))
    daily._run_client(tmp_path / "data.csv", hours=26, attempts=3,
                      station_pause=15, timezone="America/Denver")
    assert "--defer-no-data" in commands[0]
    assert commands[0][commands[0].index("--attempts") + 1] == "3"


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


def test_routine_report_email_uses_compact_summary() -> None:
    report = {
        "status": "accepted",
        "started_at": "2026-09-22T00:15:50-06:00",
        "completed_at": "2026-09-22T00:37:49-06:00",
        "diagnostic_report": "/tmp/diagnostic_report.json",
        "recovery": {"requested_stations": [], "still_missing": [], "station_timings": []},
        "station_timings": [
            {
                "station": "S1T",
                "duration_seconds": 95.1,
                "exit_code": 0,
                "rows": 96,
            }
        ],
        "stations": {"S1T": {"missing_time_ranges": []}},
        "findings": [],
        "archive": {
            "status": "promoted",
            "rows_received": 96,
            "rows_added": 96,
        },
        "archive_continuity": {
            "status": "continuous",
            "lookback_days": 7,
            "gap_count": 0,
            "gaps": [],
        },
        "publication": {
            "status": "published",
            "logger_latest_timestamp": "2026-09-22T00:00:00",
            "weather_latest_timestamp": "2026-09-22T00:15:00",
            "website_service": "active",
        },
        "communication_reliability": {
            "S3B": {
                "recurrent_problem": True,
                "initial_failures_last_7": 1,
                "initial_failures_last_30": 6,
                "unresolved_failures_last_30": 2,
            }
        },
    }

    body = _build_report_email_body(report)

    assert "Stations: 1 healthy; 0 initial failures; 0 unresolved" in body
    assert "Historical communication warnings:" in body
    assert "2 unresolved failures in the last 30 runs" in body
    assert "Action required: none" in body
    assert "Station download timing:" not in body
    assert "Battery warnings:" not in body


def test_report_email_includes_archive_gap_recovery_command() -> None:
    command = (
        "tools/pakbus-download station S3B --record-start 100244 "
        "--record-end 100338 --attempts 5 --response-timeout 30 --log-level INFO"
    )
    report = {
        "status": "rejected",
        "started_at": "2026-09-27T00:16:39-06:00",
        "completed_at": "2026-09-27T00:53:45-06:00",
        "stations": {},
        "findings": [
            {
                "severity": "critical",
                "code": "archive_timestamp_gap",
                "station": "S3B",
                "message": "Archive gap",
            }
        ],
        "archive": {"status": "promoted", "rows_received": 96, "rows_added": 96},
        "archive_continuity": {
            "status": "gaps_found",
            "lookback_days": 7,
            "gap_count": 1,
            "gaps": [
                {
                    "station": "S3B",
                    "after": "2026-09-26T06:15:00+00:00",
                    "before": "2026-09-27T06:15:00+00:00",
                    "missing_intervals": 95,
                    "record_start": 100244,
                    "record_end": 100338,
                    "recovery_command": command,
                }
            ],
        },
    }

    body = _build_report_email_body(report)

    assert "Archive continuity:" in body
    assert "records 100244-100338" in body
    assert command in body
    assert "before publication" in body


def test_archive_gap_rejects_run_before_publication() -> None:
    report = {"status": "accepted", "findings": []}
    findings: list[Finding] = []
    continuity = {
        "status": "gaps_found",
        "lookback_days": 7,
        "gap_count": 1,
        "gaps": [
            {
                "station": "S3B",
                "after": "2026-09-26T06:15:00+00:00",
                "before": "2026-09-27T06:15:00+00:00",
                "missing_intervals": 95,
                "record_start": 100244,
                "record_end": 100338,
            }
        ],
    }

    apply_archive_continuity(report, findings, continuity)

    assert report["status"] == "rejected"
    assert report["archive_continuity"] == continuity
    assert report["findings"] == [
        {
            "severity": "critical",
            "code": "archive_timestamp_gap",
            "message": (
                "Archive gap after 2026-09-26T06:15:00+00:00 through before "
                "2026-09-27T06:15:00+00:00 (95 missing 15-minute interval(s); "
                "recover records 100244-100338)"
            ),
            "station": "S3B",
        }
    ]


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


def test_communication_history_flags_repeated_initial_failures(tmp_path) -> None:
    reports = [
        {
            "started_at": f"2026-09-{day:02d}T00:15:00-06:00",
            "recovery": {
                "requested_stations": ["S1M"] if day in {16, 17} else [],
                "still_missing": [],
            },
        }
        for day in range(12, 18)
    ]
    for index, report in enumerate(reports):
        path = tmp_path / str(index) / "diagnostic_report.json"
        path.parent.mkdir()
        path.write_text(__import__("json").dumps(report), encoding="utf-8")

    current = {
        "started_at": "2026-09-18T00:15:00-06:00",
        "recovery": {"requested_stations": [], "still_missing": []},
    }
    summary = build_communication_reliability(
        tmp_path, current, stations=["S1M", "S1B"]
    )

    assert summary["S1M"]["initial_failures_last_7"] == 2
    assert summary["S1M"]["recurrent_problem"] is True
    assert summary["S1M"]["consecutive_initial_failures"] == 0
    assert summary["S1B"]["recurrent_problem"] is False


def test_success_email_is_one_line_and_mentions_recovery() -> None:
    body = _build_success_email_body(
        {
            "publication": {
                "logger_latest_timestamp": "2026-09-18T00:00:00",
                "production": {"status": "published"},
            },
            "recovery": {
                "requested_stations": ["S1B", "S1M"],
                "still_missing": [],
            },
            "communication_reliability": {},
        }
    )

    assert body.count("\n") == 0
    assert "completed successfully through 2026-09-18T00:00:00" in body
    assert "Production website publication and health checks passed" in body
    assert "S1B, S1M" in body


def test_publish_to_production_runs_guarded_deploy_and_records_log(
    tmp_path, monkeypatch
) -> None:
    deploy_script = tmp_path / "deploy.sh"
    deploy_script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    observed = {}

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout="Deployment finished.\n", stderr="")

    monkeypatch.setattr(
        "biochar_app.pakbus.core.daily_download.subprocess.run",
        fake_run,
    )

    result = _publish_to_production(
        2026,
        tmp_path,
        deploy_script=deploy_script,
    )

    assert result["status"] == "published"
    assert observed["command"] == [
        str(deploy_script),
        "--year", "2026",
        "--no-git-check",
    ]
    assert "timeout" not in observed["kwargs"]
    assert (tmp_path / "production_publication.log").read_text(encoding="utf-8") == (
        "Deployment finished.\n"
    )


def test_report_email_includes_failed_production_publication() -> None:
    report = {
        "status": "accepted_with_warnings",
        "stations": {},
        "findings": [
            {
                "severity": "warning",
                "code": "production_publication_failed",
                "message": "Production was not updated",
            }
        ],
        "archive": {"status": "promoted", "rows_received": 96, "rows_added": 96},
        "publication": {
            "status": "published",
            "website_service": "active",
            "production": {"status": "failed", "detail": "SSH failed"},
        },
    }

    body = _build_report_email_body(report)

    assert "production website: failed (SSH failed)" in body
    assert "Review the production publication log" in body
