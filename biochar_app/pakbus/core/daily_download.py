#!/usr/bin/env python3
"""Run and validate the daily all-station PakBus download.

The job deliberately separates connection preflight, raw acquisition, and
data acceptance.  A partial or unhealthy download remains available for
troubleshooting, but the command exits non-zero so downstream ETL must not
publish it.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import smtplib
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import pandas as pd

from biochar_app.config.pakbus import DAILY_SETTINGS, DOWNLOAD_SETTINGS, ID_BY_STATION, PAKBUS
from biochar_app.pakbus.core.client import quick_port_check_ipv6
from biochar_app.pakbus.core.archive import DEFAULT_ARCHIVE_ROOT, promote_accepted_frame
from biochar_app.scripts.gseason_cache_warmer import warm_standard_gseason_cache


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUN_ROOT = REPO_ROOT / "biochar_app" / "data-raw" / "pakbus_daily"
DEFAULT_LOCK = REPO_ROOT / "biochar_app" / "data-processed" / "pakbus_daily.lock"
DEFAULT_ALERT_CONFIG = REPO_ROOT / "biochar_app" / "config" / "pipeline_alerts.json"
DEFAULT_TIMEZONE = str(DOWNLOAD_SETTINGS["timezone"])
CSV_FLOAT_FORMAT = f"%.{int(DOWNLOAD_SETTINGS['csv_decimal_places'])}f"
EXPECTED_STATIONS = tuple(name for name in ID_BY_STATION if name != "CR800")
PARQUET_SUMMARY_ROOT = REPO_ROOT / "biochar_app/data-processed/parquet/summary"
LOGGER_PARQUET = PARQUET_SUMMARY_ROOT / "15min"
WEATHER_PARQUET = PARQUET_SUMMARY_ROOT / "weather/15min"


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    message: str
    station: str | None = None


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _acquire_lock(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(fd)
        raise RuntimeError(f"daily PakBus job is already running: {path}") from exc
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode())
    return fd


def _release_lock(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


def _station_names(items: Iterable[str]) -> str:
    names = list(items)
    return ", ".join(names) if names else "none"


def _build_report_email_body(report: dict) -> str:
    """Build an operational summary from a completed diagnostic report."""
    findings = report.get("findings", [])
    critical_stations = {
        item.get("station")
        for item in findings
        if item.get("severity") == "critical" and item.get("station")
    }
    station_summaries = report.get("stations", {})
    healthy = sorted(set(station_summaries).difference(critical_stations))
    recovery = report.get("recovery", {})
    attempted = recovery.get("requested_stations", [])
    still_missing = recovery.get("still_missing", [])
    recovered = sorted(set(attempted).difference(still_missing))
    battery_findings = [
        item for item in findings if item.get("code") in {"battery_warning", "battery_critical", "battery_missing"}
    ]

    all_timings = [
        ("initial", item) for item in report.get("station_timings", [])
    ] + [
        ("recovery", item) for item in recovery.get("station_timings", [])
    ]
    gaps_found = any(
        summary.get("missing_time_ranges", [])
        for summary in station_summaries.values()
    )
    critical = [item for item in findings if item.get("severity") == "critical"]
    recurrent = {
        station: summary
        for station, summary in report.get("communication_reliability", {}).items()
        if summary.get("recurrent_problem")
    }
    archive = report.get("archive", {})
    publication = report.get("publication", {})
    unusually_slow = any(
        float(item.get("duration_seconds") or 0) >= 120
        for _, item in all_timings
    )
    routine_run = (
        report.get("status") == "accepted"
        and not attempted
        and not still_missing
        and not gaps_found
        and not battery_findings
        and not critical
        and not unusually_slow
        and archive.get("status") == "promoted"
        and publication.get("status") == "published"
        and publication.get("website_service") == "active"
    )

    if routine_run:
        lines = [
            f"Daily logger run status: {report.get('status')}",
            f"Started: {report.get('started_at')}",
            f"Completed: {report.get('completed_at', 'not completed')}",
            f"Stations: {len(healthy)} healthy; 0 initial failures; 0 unresolved",
            (
                f"Archive: {archive.get('rows_received', 0)} validated rows promoted "
                f"({archive.get('rows_added', 0)} new)"
            ),
            (
                f"Published: logger through {publication.get('logger_latest_timestamp')}; "
                f"weather through {publication.get('weather_latest_timestamp')}"
            ),
            f"Website service: {publication.get('website_service')}",
        ]
        seasonal_cache = publication.get("seasonal_cache", {})
        if seasonal_cache:
            lines.append(
                "Seasonal summaries: "
                f"{seasonal_cache.get('status')} "
                f"({seasonal_cache.get('cache_entries', 0)} cache entries in "
                f"{seasonal_cache.get('duration_seconds', 0)} seconds)"
            )
        if recurrent:
            lines.extend(["", "Historical communication warnings:"])
            for station, summary in sorted(recurrent.items()):
                lines.append(
                    f"- {station}: {summary.get('initial_failures_last_7', 0)} initial failures "
                    f"in the last 7 runs; {summary.get('initial_failures_last_30', 0)} in the "
                    f"last 30; {summary.get('unresolved_failures_last_30', 0)} unresolved "
                    "failures in the last 30 runs"
                )
        else:
            lines.extend(["", "Historical communication warnings: none"])
        lines.extend([
            "",
            "Action required: none",
            f"Full diagnostic report: {report.get('diagnostic_report')}",
        ])
        return "\n".join(lines)

    lines = [
        f"Daily logger run status: {report.get('status')}",
        f"Started: {report.get('started_at')}",
        f"Completed: {report.get('completed_at', 'not completed')}",
        f"Diagnostic report: {report.get('diagnostic_report')}",
        "",
        f"Healthy stations ({len(healthy)}): {_station_names(healthy)}",
        f"Failed initial attempts ({len(attempted)}): {_station_names(attempted)}",
        f"Recovered stations ({len(recovered)}): {_station_names(recovered)}",
        f"Unresolved stations ({len(still_missing)}): {_station_names(still_missing)}",
        "",
        "Station download timing:",
    ]
    if all_timings:
        for pass_name, item in all_timings:
            if pass_name == "recovery" and item.get("recovery_pass"):
                pass_name = f"recovery pass {item['recovery_pass']}"
            result = "success" if item.get("exit_code") == 0 and item.get("rows", 0) else "failed"
            lines.append(
                f"- {item.get('station')} ({pass_name}, {result}): "
                f"{item.get('started_at')} to {item.get('completed_at')} "
                f"({item.get('duration_seconds'):.1f} seconds, {item.get('rows', 0)} rows)"
            )
    else:
        lines.append("- unavailable")
    lines.extend([
        "",
        "Missing time ranges:",
    ])
    gaps_found = False
    for station, summary in sorted(station_summaries.items()):
        for gap in summary.get("missing_time_ranges", []):
            gaps_found = True
            lines.append(
                f"- {station}: after {gap['after']} through before {gap['before']} "
                f"({gap['missing_intervals']} missing 15-minute interval(s))"
            )
    if not gaps_found:
        unresolved = sorted(set(still_missing) or critical_stations)
        lines.append("- none detected in responding stations")
        if unresolved:
            lines.append(
                f"- not assessable for {_station_names(unresolved)} because no records were returned"
            )

    lines.extend(["", "Battery warnings:"])
    if battery_findings:
        for item in battery_findings:
            station = f" [{item['station']}]" if item.get("station") else ""
            lines.append(f"- {item.get('code')}{station}: {item.get('message')}")
    else:
        lines.append("- none")

    lines.extend(["", "Accepted-data archive:"])
    if archive.get("status") == "promoted":
        lines.append(
            f"- promoted {archive.get('rows_received', 0)} validated rows "
            f"({archive.get('rows_added', 0)} new)"
        )
        for path in archive.get("files", []):
            lines.append(f"- {path}")
    elif archive:
        lines.append(f"- {archive.get('status')}: {archive.get('detail', 'no details available')}")
    else:
        lines.append("- not promoted")

    lines.extend(["", "Website publication:"])
    if publication.get("status") == "published":
        lines.append(
            f"- logger through {publication.get('logger_latest_timestamp')}"
        )
        lines.append(
            f"- weather through {publication.get('weather_latest_timestamp')}"
        )
        lines.append(
            f"- website service: {publication.get('website_service')}"
        )
        seasonal_cache = publication.get("seasonal_cache", {})
        if seasonal_cache:
            cache_detail = seasonal_cache.get("detail")
            lines.append(
                f"- seasonal summaries: {seasonal_cache.get('status')}"
                + (f" ({cache_detail})" if cache_detail else "")
            )
    elif publication:
        lines.append(
            f"- {publication.get('status')}: {publication.get('detail', 'no details available')}"
        )
    else:
        lines.append("- not attempted")

    lines.extend(["", "Critical findings:"])
    if critical:
        for item in critical:
            station = f" [{item['station']}]" if item.get("station") else ""
            lines.append(f"- {item.get('code')}{station}: {item.get('message')}")
    else:
        lines.append("- none")

    lines.extend(["", "Recurring communication warnings:"])
    if recurrent:
        for station, summary in sorted(recurrent.items()):
            lines.append(
                f"- {station}: {summary.get('initial_failures_last_7', 0)} initial failures "
                f"in the last 7 runs; {summary.get('initial_failures_last_30', 0)} in the "
                f"last 30; {summary.get('unresolved_failures_last_30', 0)} unresolved "
                "failures in the last 30 runs"
            )
    else:
        lines.append("- none")

    next_steps: list[str] = []
    if still_missing:
        next_steps.append(
            "Compare the station timing above, check logger/radio communications at "
            f"{_station_names(still_missing)}, then run a station-only PakBus download."
        )
    if recovered:
        next_steps.append(f"Monitor intermittent communications at {_station_names(recovered)} on the next run.")
    if gaps_found:
        next_steps.append("Run a targeted backfill for the listed time ranges before publishing the data.")
    if battery_findings:
        battery_stations = sorted({item.get("station") for item in battery_findings if item.get("station")})
        next_steps.append(f"Inspect the power system at {_station_names(battery_stations)}.")
    if report.get("status") == "failed_preflight":
        next_steps.append("Check the gateway/network endpoint before testing individual stations.")
    if report.get("status") == "failed_download":
        next_steps.append("Review the downloader service log and retry after correcting the reported process error.")
    if not next_steps:
        next_steps.append("No action is required.")
    lines.extend(["", "Next steps:"] + [f"- {step}" for step in next_steps])
    if report.get("status") == "rejected":
        lines.extend(["", "This incomplete download was rejected and was not approved for downstream publishing."])
    return "\n".join(lines)


def _initial_failure_stations(report: dict) -> list[str]:
    return sorted(set(report.get("recovery", {}).get("requested_stations", [])))


def build_communication_reliability(
    run_root: Path,
    current_report: dict,
    *,
    stations: Iterable[str] = EXPECTED_STATIONS,
) -> dict[str, dict]:
    """Summarize initial and unresolved failures from the most recent 30 runs."""
    reports: list[dict] = []
    for path in run_root.rglob("diagnostic_report.json"):
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if item.get("started_at") != current_report.get("started_at"):
            reports.append(item)
    reports.append(current_report)
    reports.sort(key=lambda item: str(item.get("started_at", "")))
    reports = reports[-30:]

    result: dict[str, dict] = {}
    for station in stations:
        initial_flags = [station in _initial_failure_stations(item) for item in reports]
        unresolved_flags = [
            station in set(item.get("recovery", {}).get("still_missing", []))
            for item in reports
        ]
        consecutive = 0
        for failed in reversed(initial_flags):
            if not failed:
                break
            consecutive += 1
        last_failure = next(
            (
                item.get("started_at")
                for item, failed in zip(reversed(reports), reversed(initial_flags))
                if failed
            ),
            None,
        )
        failures_7 = sum(initial_flags[-7:])
        failures_30 = sum(initial_flags)
        recurrent = bool(
            failures_7 >= 2
            or failures_30 >= 3
            or consecutive >= 2
            or any(unresolved_flags)
        )
        result[station] = {
            "runs_observed": len(reports),
            "initial_failures_last_7": failures_7,
            "initial_failures_last_30": failures_30,
            "initial_success_percent": round(
                100.0 * (len(reports) - failures_30) / len(reports), 1
            ) if reports else None,
            "recovered_failures_last_30": sum(
                failed and not unresolved
                for failed, unresolved in zip(initial_flags, unresolved_flags)
            ),
            "unresolved_failures_last_30": sum(unresolved_flags),
            "consecutive_initial_failures": consecutive,
            "most_recent_initial_failure": last_failure,
            "recurrent_problem": recurrent,
        }
    return result


def _build_success_email_body(report: dict) -> str:
    publication = report.get("publication", {})
    latest = publication.get("logger_latest_timestamp", "unknown")
    recovered = sorted(
        set(_initial_failure_stations(report)).difference(
            report.get("recovery", {}).get("still_missing", [])
        )
    )
    message = f"Nightly logger and weather update completed successfully through {latest}."
    if recovered:
        message += f" Recovered after initial communication failures: {_station_names(recovered)}."
    recurrent = sorted(
        station
        for station, summary in report.get("communication_reliability", {}).items()
        if summary.get("recurrent_problem")
    )
    if recurrent:
        message += f" Recurrent communication warning: {_station_names(recurrent)}."
    return message


def _publish_operational_update(year: int, run_dir: Path, *, restart: bool) -> dict:
    """Run ETL, verify current outputs, and restart the website service."""
    log_path = run_dir / "operational_update.log"
    command = [
        sys.executable, "-m", "biochar_app.scripts.etl",
        "--year", str(year), "--operational-update",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    log_path.write_text(
        (result.stdout or "") + ("\n" if result.stdout and result.stderr else "") + (result.stderr or ""),
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"operational ETL exited with status {result.returncode}; see {log_path}")

    logger_path = LOGGER_PARQUET / f"{year}_15min.parquet"
    weather_path = WEATHER_PARQUET / f"{year}_15min.parquet"
    logger_zip = REPO_ROOT / f"biochar_app/data-processed/downloads/loggers/Biochar_Loggers_15min_{year}_USunits.zip"
    weather_zip = REPO_ROOT / f"biochar_app/data-processed/downloads/weather/Biochar_Weather_15min_{year}_USunits.zip"
    for path in (logger_path, weather_path, logger_zip, weather_zip):
        if not path.exists() or path.stat().st_size == 0:
            raise RuntimeError(f"publication output missing or empty: {path}")

    logger_frame = pd.read_parquet(logger_path, columns=["timestamp"])
    weather_frame = pd.read_parquet(weather_path, columns=["timestamp"])
    logger_latest = pd.to_datetime(logger_frame["timestamp"], errors="coerce").max()
    weather_latest = pd.to_datetime(weather_frame["timestamp"], errors="coerce").max()
    if pd.isna(logger_latest) or pd.isna(weather_latest):
        raise RuntimeError("publication output has no valid latest timestamp")

    try:
        seasonal_cache = warm_standard_gseason_cache(year)
    except Exception as exc:
        seasonal_cache = {
            "status": "failed",
            "year": int(year),
            "detail": f"{type(exc).__name__}: {exc}",
        }

    restart_status = "skipped"
    if restart:
        service = os.getenv("BIOCHAR_WEBSITE_SERVICE", "biochar")
        restart_result = subprocess.run(
            ["sudo", "-n", "systemctl", "restart", service],
            capture_output=True, text=True, check=False,
        )
        if restart_result.returncode != 0:
            detail = (restart_result.stderr or restart_result.stdout).strip()
            raise RuntimeError(f"website restart failed: {detail}")
        active_result = subprocess.run(
            ["systemctl", "is-active", service],
            capture_output=True, text=True, check=False,
        )
        if active_result.returncode != 0 or active_result.stdout.strip() != "active":
            raise RuntimeError(f"website service is not active: {active_result.stdout.strip()}")
        restart_status = "active"

    return {
        "status": "published",
        "operational_log": str(log_path),
        "logger_latest_timestamp": logger_latest.isoformat(),
        "weather_latest_timestamp": weather_latest.isoformat(),
        "logger_rows": int(len(logger_frame)),
        "weather_rows": int(len(weather_frame)),
        "website_service": restart_status,
        "seasonal_cache": seasonal_cache,
    }


def _send_report_email(report: dict, config_path: Path) -> str:
    """Send one nightly operational summary when SMTP is configured."""
    username = os.getenv("BIOCHAR_SMTP_USERNAME")
    password = os.getenv("BIOCHAR_SMTP_PASSWORD")
    if not username or not password:
        return "not_configured"
    if not config_path.exists():
        return "config_missing"

    config = json.loads(config_path.read_text(encoding="utf-8"))
    recipients = [
        str(item).strip()
        for item in config.get("recipients", [])
        if str(item).strip()
    ]
    sender = str(config.get("from_email", "")).strip()
    if not recipients or not sender:
        return "config_incomplete"

    message = EmailMessage()
    success = report.get("status") == "accepted" and report.get("publication", {}).get("status") == "published"
    recurrent = any(
        item.get("recurrent_problem")
        for item in report.get("communication_reliability", {}).values()
    )
    message["Subject"] = (
        "Biochar nightly update successful"
        if success and not recurrent
        else f"Biochar logger update {report.get('status', 'failed')}"
    )
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(
        _build_success_email_body(report)
        if success and not recurrent
        else _build_report_email_body(report)
    )

    host = os.getenv("BIOCHAR_SMTP_HOST", "email-smtp.us-east-2.amazonaws.com")
    port = int(os.getenv("BIOCHAR_SMTP_PORT", "587"))
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(message)
    return "sent"


def _finish_report(report: dict, report_path: Path, config_path: Path) -> None:
    report["diagnostic_report"] = str(report_path)
    _write_json(report_path, report)
    try:
        report["report_email"] = _send_report_email(report, config_path)
    except Exception as exc:
        report["report_email"] = f"failed: {type(exc).__name__}: {exc}"
    _write_json(report_path, report)


def diagnose_download(
    frame: pd.DataFrame,
    *,
    expected_stations: Iterable[str] = EXPECTED_STATIONS,
    reference_time: pd.Timestamp | None = None,
    station_reference_times: dict[str, pd.Timestamp] | None = None,
    minimum_rows: int = 90,
    maximum_gap_minutes: int = 30,
    maximum_age_minutes: int = 90,
    battery_warning: float = 11.5,
    battery_critical: float = 10.5,
) -> tuple[list[Finding], dict[str, dict]]:
    """Evaluate completeness, continuity, recency, and battery condition."""
    findings: list[Finding] = []
    station_summary: dict[str, dict] = {}
    required = {"station", "logger_id", "Datetime", "RecNbr", "BattV_Min"}
    missing_columns = sorted(required.difference(frame.columns))
    if missing_columns:
        findings.append(
            Finding(
                "critical",
                "missing_columns",
                f"Missing columns: {', '.join(missing_columns)}",
            )
        )
        return findings, station_summary

    data = frame.copy()
    data["station"] = data["station"].astype(str).str.upper()
    data["Datetime"] = pd.to_datetime(data["Datetime"], errors="coerce", utc=True)
    data["BattV_Min"] = pd.to_numeric(data["BattV_Min"], errors="coerce")
    invalid_timestamps = int(data["Datetime"].isna().sum())
    if invalid_timestamps:
        findings.append(
            Finding(
                "critical",
                "invalid_timestamps",
                f"{invalid_timestamps} rows have invalid timestamps",
            )
        )
    data = data.dropna(subset=["Datetime"])

    now = reference_time or pd.Timestamp.now(tz="UTC")
    now = pd.Timestamp(now)
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")

    for station in expected_stations:
        subset = data.loc[data["station"] == station].sort_values("Datetime")
        if subset.empty:
            findings.append(Finding("critical", "station_missing", "No records returned", station))
            continue

        duplicate_count = int(subset.duplicated(["Datetime"]).sum())
        deltas = subset["Datetime"].drop_duplicates().diff().dropna()
        max_gap = float(deltas.dt.total_seconds().max() / 60) if not deltas.empty else None
        unique_times = subset["Datetime"].drop_duplicates().sort_values()
        missing_time_ranges = []
        for previous, current in zip(unique_times.iloc[:-1], unique_times.iloc[1:]):
            delta_minutes = (current - previous).total_seconds() / 60
            if delta_minutes > 15:
                missing_time_ranges.append(
                    {
                        "after": previous.isoformat(),
                        "before": current.isoformat(),
                        "missing_intervals": max(1, round(delta_minutes / 15) - 1),
                    }
                )
        latest = subset["Datetime"].max()
        station_now = (station_reference_times or {}).get(station, now)
        station_now = pd.Timestamp(station_now)
        if station_now.tzinfo is None:
            station_now = station_now.tz_localize("UTC")
        else:
            station_now = station_now.tz_convert("UTC")
        age_minutes = float((station_now - latest).total_seconds() / 60)
        battery_min = (
            float(subset["BattV_Min"].min())
            if subset["BattV_Min"].notna().any()
            else None
        )
        summary = {
            "logger_id": int(subset["logger_id"].iloc[0]),
            "rows": int(len(subset)),
            "first_timestamp": subset["Datetime"].min().isoformat(),
            "latest_timestamp": latest.isoformat(),
            "latest_age_minutes": round(age_minutes, 1),
            "maximum_gap_minutes": round(max_gap, 1) if max_gap is not None else None,
            "missing_time_ranges": missing_time_ranges,
            "duplicate_timestamps": duplicate_count,
            "minimum_battery_volts": round(battery_min, 3) if battery_min is not None else None,
        }
        station_summary[station] = summary

        if len(subset) < minimum_rows:
            findings.append(
                Finding(
                    "critical",
                    "too_few_rows",
                    f"Returned {len(subset)} rows; expected at least {minimum_rows}",
                    station,
                )
            )
        if duplicate_count:
            findings.append(Finding("warning", "duplicate_timestamps", f"Found {duplicate_count} duplicate timestamps", station))
        if max_gap is not None and max_gap > maximum_gap_minutes:
            findings.append(Finding("critical", "timestamp_gap", f"Maximum interval is {max_gap:.1f} minutes", station))
        if age_minutes > maximum_age_minutes:
            findings.append(Finding("critical", "stale_latest_record", f"Latest record is {age_minutes:.1f} minutes old", station))
        if battery_min is None:
            findings.append(Finding("warning", "battery_missing", "No usable BattV_Min values", station))
        elif battery_min < battery_critical:
            findings.append(Finding("critical", "battery_critical", f"Minimum battery voltage is {battery_min:.2f} V", station))
        elif battery_min < battery_warning:
            findings.append(Finding("warning", "battery_warning", f"Minimum battery voltage is {battery_min:.2f} V", station))

    unexpected = sorted(set(data["station"]).difference(expected_stations))
    if unexpected:
        findings.append(Finding("warning", "unexpected_stations", f"Unexpected stations: {', '.join(unexpected)}"))
    return findings, station_summary


def missing_stations(
    frame: pd.DataFrame, expected_stations: Iterable[str] = EXPECTED_STATIONS
) -> list[str]:
    """Return configured stations absent from a downloaded frame."""
    if "station" not in frame.columns:
        return list(expected_stations)
    returned = set(frame["station"].dropna().astype(str).str.upper())
    return [station for station in expected_stations if station not in returned]


def merge_download_frames(initial: pd.DataFrame, recovery: pd.DataFrame) -> pd.DataFrame:
    """Merge recovery rows without duplicating previously downloaded records."""
    combined = pd.concat([initial, recovery], ignore_index=True)
    keys = [key for key in ("station", "logger_id", "Datetime", "RecNbr") if key in combined]
    if keys:
        combined = combined.drop_duplicates(subset=keys, keep="last")
    sort_keys = [key for key in ("station", "Datetime", "RecNbr") if key in combined]
    if sort_keys:
        combined = combined.sort_values(sort_keys)
    return combined.reset_index(drop=True)


def _run_client(
    output: Path,
    *,
    hours: int,
    attempts: int,
    station_pause: float,
    timezone: str,
    stations: Iterable[str] | None = None,
    timing_output: Path | None = None,
    response_timeout: float = PAKBUS.response_timeout_seconds,
) -> subprocess.CompletedProcess:
    command = [
        sys.executable,
        "-m",
        "biochar_app.pakbus.core.client",
        "--hours",
        str(hours),
        "--attempts",
        str(attempts),
        "--station-pause",
        str(station_pause),
        "--response-timeout",
        str(response_timeout),
        "--timezone",
        timezone,
        "--log-level",
        "INFO",
    ]
    selected = list(stations or [])
    if selected:
        command.extend(["--stations", *selected])
    if timing_output is not None:
        command.extend(["--timing-output", str(timing_output)])
    command.extend(["--output", str(output)])
    return subprocess.run(command, check=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose, download, and validate all PakBus logger data")
    parser.add_argument("--hours", type=int, default=int(DAILY_SETTINGS["hours"]))
    parser.add_argument("--attempts", type=int, default=int(DAILY_SETTINGS["attempts"]))
    parser.add_argument("--station-pause", type=float, default=float(DAILY_SETTINGS["station_pause_seconds"]))
    parser.add_argument("--response-timeout", type=float, default=PAKBUS.response_timeout_seconds)
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--alert-config", type=Path, default=DEFAULT_ALERT_CONFIG)
    parser.add_argument("--minimum-rows", type=int, default=int(DAILY_SETTINGS["minimum_rows"]))
    parser.add_argument("--maximum-gap-minutes", type=int, default=int(DAILY_SETTINGS["maximum_gap_minutes"]))
    parser.add_argument("--maximum-age-minutes", type=int, default=int(DAILY_SETTINGS["maximum_age_minutes"]))
    parser.add_argument("--battery-warning", type=float, default=float(DAILY_SETTINGS["battery_warning_volts"]))
    parser.add_argument("--battery-critical", type=float, default=float(DAILY_SETTINGS["battery_critical_volts"]))
    parser.add_argument("--recovery-passes", type=int, default=int(DAILY_SETTINGS["recovery_passes"]))
    parser.add_argument("--recovery-attempts", type=int, default=int(DAILY_SETTINGS["recovery_attempts"]))
    parser.add_argument("--recovery-pause", type=float, default=float(DAILY_SETTINGS["recovery_pause_seconds"]))
    parser.add_argument("--recovery-wait", type=float, default=float(DAILY_SETTINGS["recovery_wait_seconds"]))
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument(
        "--skip-publication",
        action="store_true",
        help="Accept and archive the download without running operational ETL.",
    )
    parser.add_argument(
        "--skip-restart",
        action="store_true",
        help="Run and verify operational ETL without restarting the website.",
    )
    args = parser.parse_args(argv)

    try:
        lock_fd = _acquire_lock(args.lock)
    except RuntimeError as exc:
        print(f"CRITICAL: {exc}", file=sys.stderr)
        return 2

    started = datetime.now(ZoneInfo(args.timezone))
    run_dir = args.run_root / started.strftime("%Y/%m/%d") / started.strftime("%Y%m%dT%H%M%S%z")
    run_dir.mkdir(parents=True, exist_ok=False)
    raw_csv = run_dir / "logger_data.csv"
    report_path = run_dir / "diagnostic_report.json"
    report: dict = {
        "started_at": started.isoformat(),
        "host": PAKBUS.host,
        "port": PAKBUS.port,
        "hours_requested": args.hours,
        "raw_csv": str(raw_csv),
        "status": "running",
        "findings": [],
        "stations": {},
    }

    try:
        reachable, reason = quick_port_check_ipv6(PAKBUS.host, PAKBUS.port)
        report["preflight"] = {"reachable": reachable, "detail": reason}
        if not reachable:
            report["status"] = "failed_preflight"
            report["findings"] = [asdict(Finding("critical", "endpoint_unreachable", reason))]
            _finish_report(report, report_path, args.alert_config)
            print(f"CRITICAL: PakBus endpoint unavailable: {reason}", file=sys.stderr)
            return 1

        print(f"Preflight passed: [{PAKBUS.host}]:{PAKBUS.port}")
        result = _run_client(
            raw_csv,
            hours=args.hours,
            attempts=args.attempts,
            station_pause=args.station_pause,
            timezone=args.timezone,
            timing_output=run_dir / "station_timings.json",
            response_timeout=args.response_timeout,
        )
        timing_path = run_dir / "station_timings.json"
        if timing_path.exists():
            report["station_timings"] = json.loads(timing_path.read_text(encoding="utf-8"))
        report["download_exit_code"] = result.returncode
        if result.returncode != 0 or not raw_csv.exists():
            finding = Finding("critical", "download_failed", f"Downloader exited with status {result.returncode}")
            report["status"] = "failed_download"
            report["findings"] = [asdict(finding)]
            _finish_report(report, report_path, args.alert_config)
            print(f"CRITICAL: {finding.message}; see {report_path}", file=sys.stderr)
            return 1

        frame = pd.read_csv(raw_csv)
        initially_missing = missing_stations(frame)
        if initially_missing:
            report["recovery"] = {
                "requested_stations": initially_missing,
                "passes": [],
                "station_timings": [],
            }
            remaining = initially_missing
            for pass_number in range(1, args.recovery_passes + 1):
                print(
                    f"Recovery pass {pass_number}/{args.recovery_passes} will retry "
                    f"{', '.join(remaining)} after {args.recovery_wait:.0f} seconds."
                )
                if args.recovery_wait > 0:
                    time.sleep(args.recovery_wait)
                recovery_csv = run_dir / f"recovery_{pass_number}_logger_data.csv"
                recovery_timing_path = run_dir / f"recovery_{pass_number}_station_timings.json"
                recovery_result = _run_client(
                    recovery_csv,
                    hours=args.hours,
                    attempts=args.recovery_attempts,
                    station_pause=args.recovery_pause,
                    timezone=args.timezone,
                    stations=remaining,
                    timing_output=recovery_timing_path,
                    response_timeout=args.response_timeout,
                )
                pass_report = {
                    "pass": pass_number,
                    "requested_stations": remaining,
                    "exit_code": recovery_result.returncode,
                    "raw_csv": str(recovery_csv),
                    "record_count": 0,
                    "station_timings": [],
                }
                if recovery_timing_path.exists():
                    pass_report["station_timings"] = json.loads(
                        recovery_timing_path.read_text(encoding="utf-8")
                    )
                    for timing in pass_report["station_timings"]:
                        timing["recovery_pass"] = pass_number
                    report["recovery"]["station_timings"].extend(
                        pass_report["station_timings"]
                    )
                if recovery_csv.exists():
                    recovery_frame = pd.read_csv(recovery_csv)
                    pass_report["record_count"] = int(len(recovery_frame))
                    frame = merge_download_frames(frame, recovery_frame)
                    frame.to_csv(raw_csv, index=False, float_format=CSV_FLOAT_FORMAT)
                remaining = missing_stations(frame)
                pass_report["still_missing"] = remaining
                report["recovery"]["passes"].append(pass_report)
                if not remaining:
                    break
            report["recovery"]["still_missing"] = remaining

        successful_timings = [
            item
            for item in (
                report.get("station_timings", [])
                + report.get("recovery", {}).get("station_timings", [])
            )
            if item.get("exit_code") == 0 and item.get("rows", 0) > 0
        ]
        station_reference_times = {
            str(item["station"]): pd.Timestamp(item["completed_at"])
            for item in successful_timings
        }
        findings, station_summary = diagnose_download(
            frame,
            reference_time=pd.Timestamp.now(tz="UTC"),
            station_reference_times=station_reference_times,
            minimum_rows=args.minimum_rows,
            maximum_gap_minutes=args.maximum_gap_minutes,
            maximum_age_minutes=args.maximum_age_minutes,
            battery_warning=args.battery_warning,
            battery_critical=args.battery_critical,
        )
        report["record_count"] = int(len(frame))
        report["stations"] = station_summary
        report["findings"] = [asdict(item) for item in findings]
        report["completed_at"] = datetime.now(ZoneInfo(args.timezone)).isoformat()
        report["status"] = "rejected" if any(item.severity == "critical" for item in findings) else "accepted_with_warnings" if findings else "accepted"
        if report["status"] in {"accepted", "accepted_with_warnings"}:
            try:
                report["archive"] = promote_accepted_frame(
                    frame,
                    archive_root=args.archive_root,
                )
            except Exception as exc:
                archive_finding = Finding(
                    "critical",
                    "archive_promotion_failed",
                    f"Accepted data could not be archived: {type(exc).__name__}: {exc}",
                )
                findings.append(archive_finding)
                report["findings"] = [asdict(item) for item in findings]
                report["status"] = "rejected"
                report["archive"] = {"status": "failed", "detail": str(exc)}
        if (
            report["status"] in {"accepted", "accepted_with_warnings"}
            and not args.skip_publication
        ):
            try:
                report["publication"] = _publish_operational_update(
                    started.year,
                    run_dir,
                    restart=not args.skip_restart,
                )
                seasonal_cache = report["publication"].get("seasonal_cache", {})
                if seasonal_cache.get("status") != "warmed":
                    findings.append(Finding(
                        "warning",
                        "seasonal_cache_warm_failed",
                        "Website data were published, but standard seasonal summaries "
                        f"were not precomputed: {seasonal_cache.get('detail', 'unknown error')}",
                    ))
                    report["findings"] = [asdict(item) for item in findings]
                    report["status"] = "accepted_with_warnings"
            except Exception as exc:
                publication_finding = Finding(
                    "critical",
                    "publication_failed",
                    f"Accepted data were archived but not published: {type(exc).__name__}: {exc}",
                )
                findings.append(publication_finding)
                report["findings"] = [asdict(item) for item in findings]
                report["status"] = "publication_failed"
                report["publication"] = {"status": "failed", "detail": str(exc)}
        elif args.skip_publication:
            report["publication"] = {"status": "skipped"}

        report["communication_reliability"] = build_communication_reliability(
            args.run_root,
            report,
        )
        _finish_report(report, report_path, args.alert_config)

        print(f"Raw download: {raw_csv}")
        print(f"Diagnostic report: {report_path}")
        print(f"Result: {report['status']}")
        for item in findings:
            label = f" ({item.station})" if item.station else ""
            print(f"{item.severity.upper()}{label}: {item.message}")
        return 1 if report["status"] in {"rejected", "publication_failed"} else 0
    finally:
        _release_lock(lock_fd)


if __name__ == "__main__":
    raise SystemExit(main())
