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

from biochar_app.config.pakbus import ID_BY_STATION, PAKBUS
from biochar_app.pakbus.core.client import quick_port_check_ipv6


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUN_ROOT = REPO_ROOT / "biochar_app" / "data-raw" / "pakbus_daily"
DEFAULT_LOCK = REPO_ROOT / "biochar_app" / "data-processed" / "pakbus_daily.lock"
DEFAULT_ALERT_CONFIG = REPO_ROOT / "biochar_app" / "config" / "pipeline_alerts.json"
DEFAULT_TIMEZONE = "America/Denver"
EXPECTED_STATIONS = tuple(name for name in ID_BY_STATION if name != "CR800")


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


def _send_failure_email(report: dict, config_path: Path) -> str:
    """Send one failure summary when SMTP credentials are configured."""
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

    critical = [item for item in report.get("findings", []) if item.get("severity") == "critical"]
    lines = [
        f"Daily logger run status: {report.get('status')}",
        f"Started: {report.get('started_at')}",
        f"Diagnostic report: {report.get('diagnostic_report')}",
        "",
        "Critical findings:",
    ]
    for item in critical:
        station = f" [{item['station']}]" if item.get("station") else ""
        lines.append(f"- {item.get('code')}{station}: {item.get('message')}")

    message = EmailMessage()
    message["Subject"] = f"Biochar logger download {report.get('status', 'failed')}"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content("\n".join(lines))

    host = os.getenv("BIOCHAR_SMTP_HOST", "email-smtp.us-east-2.amazonaws.com")
    port = int(os.getenv("BIOCHAR_SMTP_PORT", "587"))
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(message)
    return "sent"


def _finish_failed_report(report: dict, report_path: Path, config_path: Path) -> None:
    report["diagnostic_report"] = str(report_path)
    _write_json(report_path, report)
    try:
        report["failure_email"] = _send_failure_email(report, config_path)
    except Exception as exc:
        report["failure_email"] = f"failed: {type(exc).__name__}: {exc}"
    _write_json(report_path, report)


def diagnose_download(
    frame: pd.DataFrame,
    *,
    expected_stations: Iterable[str] = EXPECTED_STATIONS,
    reference_time: pd.Timestamp | None = None,
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
        latest = subset["Datetime"].max()
        age_minutes = float((now - latest).total_seconds() / 60)
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
        "--timezone",
        timezone,
        "--log-level",
        "INFO",
    ]
    selected = list(stations or [])
    if selected:
        command.extend(["--stations", *selected])
    command.extend(["--output", str(output)])
    return subprocess.run(command, check=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose, download, and validate all PakBus logger data")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--station-pause", type=float, default=15.0)
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--alert-config", type=Path, default=DEFAULT_ALERT_CONFIG)
    parser.add_argument("--minimum-rows", type=int, default=90)
    parser.add_argument("--maximum-gap-minutes", type=int, default=30)
    parser.add_argument("--maximum-age-minutes", type=int, default=90)
    parser.add_argument("--battery-warning", type=float, default=11.5)
    parser.add_argument("--battery-critical", type=float, default=10.5)
    parser.add_argument("--recovery-attempts", type=int, default=8)
    parser.add_argument("--recovery-pause", type=float, default=30.0)
    parser.add_argument("--recovery-wait", type=float, default=120.0)
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
            _finish_failed_report(report, report_path, args.alert_config)
            print(f"CRITICAL: PakBus endpoint unavailable: {reason}", file=sys.stderr)
            return 1

        print(f"Preflight passed: [{PAKBUS.host}]:{PAKBUS.port}")
        result = _run_client(
            raw_csv,
            hours=args.hours,
            attempts=args.attempts,
            station_pause=args.station_pause,
            timezone=args.timezone,
        )
        report["download_exit_code"] = result.returncode
        if result.returncode != 0 or not raw_csv.exists():
            finding = Finding("critical", "download_failed", f"Downloader exited with status {result.returncode}")
            report["status"] = "failed_download"
            report["findings"] = [asdict(finding)]
            _finish_failed_report(report, report_path, args.alert_config)
            print(f"CRITICAL: {finding.message}; see {report_path}", file=sys.stderr)
            return 1

        frame = pd.read_csv(raw_csv)
        initially_missing = missing_stations(frame)
        if initially_missing:
            print(
                "Initial pass missed "
                f"{', '.join(initially_missing)}; waiting {args.recovery_wait:.0f} "
                "seconds before the recovery pass."
            )
            if args.recovery_wait > 0:
                time.sleep(args.recovery_wait)
            recovery_csv = run_dir / "recovery_logger_data.csv"
            recovery_result = _run_client(
                recovery_csv,
                hours=args.hours,
                attempts=args.recovery_attempts,
                station_pause=args.recovery_pause,
                timezone=args.timezone,
                stations=initially_missing,
            )
            report["recovery"] = {
                "requested_stations": initially_missing,
                "exit_code": recovery_result.returncode,
                "raw_csv": str(recovery_csv),
            }
            if recovery_csv.exists():
                recovery_frame = pd.read_csv(recovery_csv)
                report["recovery"]["record_count"] = int(len(recovery_frame))
                frame = merge_download_frames(frame, recovery_frame)
                frame.to_csv(raw_csv, index=False)
            report["recovery"]["still_missing"] = missing_stations(frame)

        findings, station_summary = diagnose_download(
            frame,
            reference_time=pd.Timestamp.now(tz="UTC"),
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
        if report["status"] == "rejected":
            _finish_failed_report(report, report_path, args.alert_config)
        else:
            report["diagnostic_report"] = str(report_path)
            _write_json(report_path, report)

        print(f"Raw download: {raw_csv}")
        print(f"Diagnostic report: {report_path}")
        print(f"Result: {report['status']}")
        for item in findings:
            label = f" ({item.station})" if item.station else ""
            print(f"{item.severity.upper()}{label}: {item.message}")
        return 1 if report["status"] == "rejected" else 0
    finally:
        _release_lock(lock_fd)


if __name__ == "__main__":
    raise SystemExit(main())
