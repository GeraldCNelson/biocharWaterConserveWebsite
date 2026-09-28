#!/usr/bin/env python3
"""Promote accepted PakBus runs and compare them with PC400 TOA5 files."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from datetime import datetime, timezone
import json
import os
import tempfile
from pathlib import Path

import pandas as pd

from biochar_app.config.pakbus import (
    ARCHIVE_SETTINGS,
    DAILY_SETTINGS,
    DOWNLOAD_SETTINGS,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ARCHIVE_ROOT = Path(
    os.getenv(
        "BIOCHAR_PAKBUS_ARCHIVE_ROOT",
        REPO_ROOT / str(ARCHIVE_SETTINGS["root"]),
    )
)
ARCHIVE_START_YEAR = int(ARCHIVE_SETTINGS["start_year"])
CSV_FLOAT_FORMAT = f"%.{int(DOWNLOAD_SETTINGS['csv_decimal_places'])}f"
VALUE_COLUMNS = (
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
)
REQUIRED_COLUMNS = {"station", "logger_id", "Datetime", "RecNbr", *VALUE_COLUMNS}
ARCHIVE_KEY = ["station", "logger_id", "Datetime", "RecNbr"]
DEFAULT_MANUAL_ROOT = REPO_ROOT / "biochar_app/data-raw/pakbus_manual"


def _atomic_write_csv(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        frame.to_csv(temporary, index=False, float_format=CSV_FLOAT_FORMAT)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def normalize_download_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate and canonicalize downloader rows for durable storage."""
    missing = sorted(REQUIRED_COLUMNS.difference(frame.columns))
    if missing:
        raise ValueError(f"PakBus data is missing columns: {', '.join(missing)}")
    normalized = frame.copy()
    normalized["station"] = normalized["station"].astype(str).str.upper()
    normalized["logger_id"] = pd.to_numeric(
        normalized["logger_id"], errors="raise"
    ).astype("int64")
    normalized["RecNbr"] = pd.to_numeric(
        normalized["RecNbr"], errors="raise"
    ).astype("int64")
    timestamps = pd.to_datetime(normalized["Datetime"], errors="coerce", utc=True)
    if timestamps.isna().any():
        raise ValueError("PakBus data contains invalid Datetime values")
    # ISO UTC is stable across machines. Conversion back to the logger's fixed
    # MST clock happens only when comparing with PC400 or entering the ETL.
    normalized["Datetime"] = timestamps.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    for column in VALUE_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized[[*ARCHIVE_KEY, *VALUE_COLUMNS]]


def promote_accepted_frame(
    frame: pd.DataFrame,
    *,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    start_year: int = ARCHIVE_START_YEAR,
) -> dict:
    """Atomically merge a complete accepted run into annual raw archives."""
    normalized = normalize_download_frame(frame)
    years = pd.to_datetime(normalized["Datetime"], utc=True).dt.year
    normalized = normalized.loc[years >= start_year].copy()
    if normalized.empty:
        raise ValueError(f"Accepted run contains no records from {start_year} onward")

    outputs: list[str] = []
    added = 0
    for year in sorted(pd.to_datetime(normalized["Datetime"], utc=True).dt.year.unique()):
        incoming = normalized.loc[
            pd.to_datetime(normalized["Datetime"], utc=True).dt.year == year
        ]
        destination = archive_root / str(year) / "logger_data.csv"
        existing = pd.read_csv(destination) if destination.exists() else incoming.iloc[0:0]
        before_keys = set(map(tuple, existing[ARCHIVE_KEY].astype(str).to_numpy())) if not existing.empty else set()
        combined = normalize_download_frame(pd.concat([existing, incoming], ignore_index=True))
        combined = (
            combined.drop_duplicates(subset=ARCHIVE_KEY, keep="last")
            .sort_values(["Datetime", "station", "RecNbr"])
            .reset_index(drop=True)
        )
        after_keys = set(map(tuple, combined[ARCHIVE_KEY].astype(str).to_numpy()))
        added += len(after_keys.difference(before_keys))
        _atomic_write_csv(combined, destination)
        outputs.append(str(destination))
    return {"status": "promoted", "rows_received": len(normalized), "rows_added": added, "files": outputs}


def promote_run_directory(run_dir: Path, archive_root: Path = DEFAULT_ARCHIVE_ROOT) -> dict:
    """Promote an existing run only when its report says it was accepted."""
    report_path = run_dir / "diagnostic_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("status") not in {"accepted", "accepted_with_warnings"}:
        raise ValueError(f"Run status is {report.get('status')!r}; rejected runs cannot be promoted")
    raw_path = Path(report.get("raw_csv") or run_dir / "logger_data.csv")
    return promote_accepted_frame(pd.read_csv(raw_path), archive_root=archive_root)


def repair_rejected_run(
    run_dir: Path,
    recovery_csvs: list[Path],
    *,
    output_dir: Path | None = None,
) -> dict:
    """Create a separately validated run from a rejected run plus recovery rows.

    The original run and diagnostic report remain unchanged. The returned repair
    directory can be passed to ``promote`` only if its new report is accepted.
    """
    from biochar_app.pakbus.core.daily_download import (
        diagnose_download,
        merge_download_frames,
    )

    source_report_path = run_dir / "diagnostic_report.json"
    source_report = json.loads(source_report_path.read_text(encoding="utf-8"))
    if source_report.get("status") != "rejected":
        raise ValueError(
            "Only a rejected daily run can be repaired; source status is "
            f"{source_report.get('status')!r}"
        )
    source_csv = Path(source_report.get("raw_csv") or run_dir / "logger_data.csv")
    if not source_csv.is_absolute():
        source_csv = run_dir / source_csv
    if not source_csv.exists():
        raise FileNotFoundError(f"Rejected run data not found: {source_csv}")
    if not recovery_csvs:
        raise ValueError("At least one recovery CSV is required")

    frame = pd.read_csv(source_csv)
    for recovery_csv in recovery_csvs:
        if not recovery_csv.exists():
            raise FileNotFoundError(f"Recovery data not found: {recovery_csv}")
        frame = merge_download_frames(frame, pd.read_csv(recovery_csv))

    timestamps = pd.to_datetime(frame["Datetime"], errors="coerce", utc=True)
    if timestamps.notna().any():
        reference_time = timestamps.max() + pd.Timedelta(minutes=15)
    else:
        reference_time = pd.Timestamp.now(tz="UTC")
    findings, station_summary = diagnose_download(
        frame,
        reference_time=reference_time,
        minimum_rows=int(DAILY_SETTINGS["minimum_rows"]),
        maximum_gap_minutes=int(DAILY_SETTINGS["maximum_gap_minutes"]),
        maximum_age_minutes=int(DAILY_SETTINGS["maximum_age_minutes"]),
        battery_warning=float(DAILY_SETTINGS["battery_warning_volts"]),
        battery_critical=float(DAILY_SETTINGS["battery_critical_volts"]),
    )
    has_critical = any(item.severity == "critical" for item in findings)
    status = (
        "rejected"
        if has_critical
        else "accepted_with_warnings"
        if findings
        else "accepted"
    )

    destination = output_dir or run_dir / "repair"
    destination.mkdir(parents=True, exist_ok=True)
    repaired_csv = destination / "logger_data.csv"
    normalized = normalize_download_frame(frame)
    _atomic_write_csv(
        normalized.sort_values(["Datetime", "station", "RecNbr"]),
        repaired_csv,
    )
    repair_report = {
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_run": str(run_dir),
        "source_report": str(source_report_path),
        "recovery_csvs": [str(path) for path in recovery_csvs],
        "raw_csv": str(repaired_csv),
        "record_count": int(len(normalized)),
        "stations": station_summary,
        "findings": [asdict(item) for item in findings],
    }
    report_path = destination / "diagnostic_report.json"
    report_path.write_text(
        json.dumps(repair_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    repair_report["diagnostic_report"] = str(report_path)
    return repair_report


def validate_archive_recovery(
    recovery_csvs: list[Path],
    *,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    output_dir: Path | None = None,
) -> dict:
    """Validate targeted gap rows against their immediate archive neighbors."""
    if not recovery_csvs:
        raise ValueError("At least one recovery CSV is required")

    recovered_frames = []
    for recovery_csv in recovery_csvs:
        if not recovery_csv.exists():
            raise FileNotFoundError(f"Recovery data not found: {recovery_csv}")
        recovered_frames.append(normalize_download_frame(pd.read_csv(recovery_csv)))
    recovered = normalize_download_frame(
        pd.concat(recovered_frames, ignore_index=True)
    )
    duplicate_rows = int(recovered.duplicated(ARCHIVE_KEY, keep=False).sum())
    recovered = recovered.drop_duplicates(ARCHIVE_KEY, keep="last").copy()
    recovered["_timestamp"] = pd.to_datetime(
        recovered["Datetime"], utc=True
    )

    findings: list[dict] = []
    if duplicate_rows:
        findings.append({
            "severity": "warning",
            "code": "duplicate_recovery_rows",
            "message": f"Removed {duplicate_rows} duplicate recovery rows",
        })
    station_reports: dict[str, dict] = {}
    for station, incoming_station in recovered.groupby("station", sort=True):
        years = sorted(incoming_station["_timestamp"].dt.year.unique())
        existing_parts = []
        for year in years:
            archive_path = archive_root / str(year) / "logger_data.csv"
            if not archive_path.exists():
                findings.append({
                    "severity": "critical",
                    "code": "archive_missing",
                    "station": station,
                    "message": f"Archive does not exist: {archive_path}",
                })
                continue
            existing_parts.append(normalize_download_frame(pd.read_csv(archive_path)))
        if not existing_parts:
            continue
        existing = normalize_download_frame(
            pd.concat(existing_parts, ignore_index=True)
        )
        existing = existing.loc[existing["station"] == station].copy()
        existing["_timestamp"] = pd.to_datetime(existing["Datetime"], utc=True)
        existing_keys = set(map(tuple, existing[ARCHIVE_KEY].to_numpy()))
        incoming_keys = set(map(tuple, incoming_station[ARCHIVE_KEY].to_numpy()))
        already_present = len(existing_keys.intersection(incoming_keys))
        if already_present:
            findings.append({
                "severity": "critical",
                "code": "recovery_already_present",
                "station": station,
                "message": (
                    f"{already_present} recovery rows already exist in the archive"
                ),
            })

        combined = pd.concat([existing, incoming_station], ignore_index=True)
        combined = (
            combined.drop_duplicates(ARCHIVE_KEY, keep="last")
            .sort_values(["RecNbr", "_timestamp"])
            .reset_index(drop=True)
        )
        row_reports = []
        for _row_index, row in incoming_station.sort_values("RecNbr").iterrows():
            record_number = int(row["RecNbr"])
            timestamp = row["_timestamp"]
            position = combined.index[
                (combined["RecNbr"] == record_number)
                & (combined["_timestamp"] == timestamp)
            ]
            if len(position) != 1:
                findings.append({
                    "severity": "critical",
                    "code": "recovery_key_ambiguous",
                    "station": station,
                    "message": f"Could not uniquely locate recovered record {record_number}",
                })
                continue
            index = int(position[0])
            previous = combined.iloc[index - 1] if index > 0 else None
            following = combined.iloc[index + 1] if index + 1 < len(combined) else None
            previous_ok = (
                previous is not None
                and int(previous["RecNbr"]) == record_number - 1
                and timestamp - previous["_timestamp"] == pd.Timedelta(minutes=15)
            )
            following_ok = (
                following is not None
                and int(following["RecNbr"]) == record_number + 1
                and following["_timestamp"] - timestamp == pd.Timedelta(minutes=15)
            )
            if not previous_ok or not following_ok:
                findings.append({
                    "severity": "critical",
                    "code": "recovery_not_contiguous",
                    "station": station,
                    "message": (
                        f"Recovered record {record_number} does not have contiguous "
                        "15-minute record-number neighbors"
                    ),
                })
            row_reports.append({
                "record": record_number,
                "timestamp": timestamp.isoformat(),
                "previous_neighbor_valid": previous_ok,
                "following_neighbor_valid": following_ok,
            })
        station_reports[station] = {
            "rows": int(len(incoming_station)),
            "records": sorted(int(value) for value in incoming_station["RecNbr"]),
            "already_present": already_present,
            "row_checks": row_reports,
        }

    has_critical = any(item["severity"] == "critical" for item in findings)
    status = (
        "rejected"
        if has_critical
        else "accepted_with_warnings"
        if findings
        else "accepted"
    )
    if output_dir is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S+0000")
        output_dir = DEFAULT_MANUAL_ROOT / f"{stamp}_archive_recovery"
    output_dir.mkdir(parents=True, exist_ok=False)
    recovery_output = output_dir / "logger_data.csv"
    export = recovered.drop(columns="_timestamp")
    _atomic_write_csv(
        export.sort_values(["Datetime", "station", "RecNbr"]), recovery_output
    )
    report = {
        "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "archive_root": str(archive_root),
        "recovery_csvs": [str(path) for path in recovery_csvs],
        "raw_csv": str(recovery_output),
        "record_count": int(len(export)),
        "stations": station_reports,
        "findings": findings,
    }
    report_path = output_dir / "diagnostic_report.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    report["diagnostic_report"] = str(report_path)
    return report


def _read_toa5(path: Path) -> pd.DataFrame:
    with path.open("r", newline="") as source:
        reader = csv.reader(source)
        next(reader, None)
        columns = next(reader, None)
        next(reader, None)
        next(reader, None)
    if not columns:
        raise ValueError(f"{path}: missing TOA5 header")
    return pd.read_csv(
        path,
        skiprows=4,
        header=None,
        names=[c.strip() for c in columns],
        low_memory=False,
    )


def compare_with_pc400(
    pakbus_csv: Path,
    dat_dir: Path,
    *,
    tolerance: float = 0.0001,
) -> dict:
    """Compare overlapping PakBus and PC400 records using raw logger time."""
    pakbus = normalize_download_frame(pd.read_csv(pakbus_csv))
    pakbus["raw_timestamp"] = (
        pd.to_datetime(pakbus["Datetime"], utc=True)
        .dt.tz_convert("Etc/GMT+7")
        .dt.tz_localize(None)
    )
    report: dict = {"pakbus_csv": str(pakbus_csv), "dat_dir": str(dat_dir), "stations": {}}
    totals = {
        "overlap": 0,
        "value_mismatches": 0,
        "missing_within_pc400_range": 0,
        "beyond_pc400_range": 0,
    }

    for station, incoming in pakbus.groupby("station", sort=True):
        dat_path = dat_dir / f"{station}_Table1.dat"
        if not dat_path.exists():
            report["stations"][station] = {"error": f"missing {dat_path.name}"}
            totals["missing_within_pc400_range"] += len(incoming)
            continue
        pc400 = _read_toa5(dat_path)
        pc400["raw_timestamp"] = pd.to_datetime(pc400["TIMESTAMP"], errors="coerce")
        pc400["RecNbr"] = pd.to_numeric(pc400["RECORD"], errors="coerce").astype("Int64")
        for column in VALUE_COLUMNS:
            pc400[column] = pd.to_numeric(pc400[column], errors="coerce")
        merged = incoming.merge(
            pc400[["raw_timestamp", "RecNbr", *VALUE_COLUMNS]],
            on=["raw_timestamp", "RecNbr"], how="left", suffixes=("_pakbus", "_pc400"), indicator=True,
        )
        overlap = merged["_merge"].eq("both")
        pc400_start = pc400["raw_timestamp"].min()
        pc400_stop = pc400["raw_timestamp"].max()
        within_pc400_range = merged["raw_timestamp"].between(pc400_start, pc400_stop)
        mismatches: dict[str, int] = {}
        maximum_difference: dict[str, float] = {}
        for column in VALUE_COLUMNS:
            left = merged.loc[overlap, f"{column}_pakbus"]
            right = merged.loc[overlap, f"{column}_pc400"]
            differences = (left - right).abs()
            bad = differences.gt(tolerance) | left.isna().ne(right.isna())
            mismatches[column] = int(bad.sum())
            maximum_difference[column] = round(float(differences.max()), 8) if differences.notna().any() else 0.0
        station_mismatches = sum(mismatches.values())
        missing_count = int((~overlap & within_pc400_range).sum())
        beyond_count = int((~overlap & ~within_pc400_range).sum())
        report["stations"][station] = {
            "pakbus_rows": len(incoming), "overlap_rows": int(overlap.sum()),
            "missing_within_pc400_range": missing_count,
            "beyond_pc400_range": beyond_count,
            "value_mismatches": station_mismatches,
            "mismatches_by_column": mismatches, "maximum_absolute_difference": maximum_difference,
        }
        totals["overlap"] += int(overlap.sum())
        totals["missing_within_pc400_range"] += missing_count
        totals["beyond_pc400_range"] += beyond_count
        totals["value_mismatches"] += station_mismatches
    report["totals"] = totals
    report["equivalent"] = (
        totals["overlap"] > 0
        and totals["missing_within_pc400_range"] == 0
        and totals["value_mismatches"] == 0
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    promote = subparsers.add_parser("promote", help="promote an accepted daily run")
    promote.add_argument("run_dir", type=Path)
    promote.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    repair = subparsers.add_parser(
        "repair",
        help="validate a rejected run completed with one or more recovery CSVs",
    )
    repair.add_argument("run_dir", type=Path)
    repair.add_argument("recovery_csv", type=Path, nargs="+")
    repair.add_argument("--output-dir", type=Path)
    validate_recovery = subparsers.add_parser(
        "validate-recovery",
        help="validate targeted gap rows against the current archive",
    )
    validate_recovery.add_argument("recovery_csv", type=Path, nargs="+")
    validate_recovery.add_argument(
        "--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT
    )
    validate_recovery.add_argument("--output-dir", type=Path)
    compare = subparsers.add_parser("compare", help="compare PakBus CSV with PC400 .dat files")
    compare.add_argument("pakbus_csv", type=Path)
    compare.add_argument("--dat-dir", type=Path, default=REPO_ROOT / "biochar_app/data-raw/datfiles_2026")
    compare.add_argument("--tolerance", type=float, default=0.0001)
    compare.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "promote":
        result = promote_run_directory(args.run_dir, args.archive_root)
    elif args.command == "repair":
        result = repair_rejected_run(
            args.run_dir,
            args.recovery_csv,
            output_dir=args.output_dir,
        )
    elif args.command == "validate-recovery":
        result = validate_archive_recovery(
            args.recovery_csv,
            archive_root=args.archive_root,
            output_dir=args.output_dir,
        )
    else:
        result = compare_with_pc400(args.pakbus_csv, args.dat_dir, tolerance=args.tolerance)
    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered)
    if getattr(args, "output", None):
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0 if result.get("equivalent", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
