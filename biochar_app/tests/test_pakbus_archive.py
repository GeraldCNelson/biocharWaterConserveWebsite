"""Tests for accepted-run archiving and PC400 equivalence checks."""

from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import pytest

from biochar_app.pakbus.core.archive import (
    VALUE_COLUMNS,
    compare_with_pc400,
    promote_accepted_frame,
    repair_rejected_run,
    promote_run_directory,
    validate_archive_recovery,
)


def _download_rows() -> pd.DataFrame:
    rows = []
    for station, logger_id in (("S3B", 10), ("S4T", 11)):
        for offset, record in ((0, 100), (15, 101)):
            row = {
                "station": station,
                "logger_id": logger_id,
                "Datetime": (pd.Timestamp("2026-09-12T09:00:00-07:00") + pd.Timedelta(minutes=offset)).isoformat(),
                "RecNbr": record,
            }
            row.update({column: float(index) + 0.123456 for index, column in enumerate(VALUE_COLUMNS)})
            rows.append(row)
    return pd.DataFrame(rows)


def _write_toa5(path: Path, rows: pd.DataFrame) -> None:
    columns = ["TIMESTAMP", "RECORD", *VALUE_COLUMNS]
    lines = [
        '"TOA5","station","CR200X"',
        ",".join(f'"{column}"' for column in columns),
        ",".join('""' for _ in columns),
        ",".join('""' for _ in columns),
    ]
    for row in rows.to_dict(orient="records"):
        raw_time = pd.Timestamp(row["Datetime"]).tz_convert("Etc/GMT+7").strftime("%Y-%m-%d %H:%M:%S")
        values = [raw_time, row["RecNbr"], *(row[column] for column in VALUE_COLUMNS)]
        lines.append(",".join(f'"{value}"' for value in values))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_promote_merges_and_deduplicates_accepted_rows(tmp_path: Path) -> None:
    rows = _download_rows()
    first = promote_accepted_frame(rows, archive_root=tmp_path)
    second = promote_accepted_frame(rows, archive_root=tmp_path)

    archived = pd.read_csv(tmp_path / "2026/logger_data.csv")
    assert len(archived) == 4
    assert first["rows_added"] == 4
    assert second["rows_added"] == 0


def test_rejected_run_cannot_be_promoted(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "diagnostic_report.json").write_text(
        '{"status": "rejected", "raw_csv": "logger_data.csv"}', encoding="utf-8"
    )
    with pytest.raises(ValueError, match="rejected runs cannot be promoted"):
        promote_run_directory(run_dir, tmp_path / "archive")


def test_repair_rejected_run_creates_separate_accepted_artifact(
    tmp_path: Path,
) -> None:
    stations = [
        "S1T", "S1M", "S1B", "S2T", "S2M", "S2B",
        "S3T", "S3M", "S3B", "S4T", "S4M", "S4B",
    ]

    def station_rows(station: str, periods: int) -> pd.DataFrame:
        station_index = stations.index(station)
        timestamps = pd.date_range(
            "2026-09-26T07:00:00Z", periods=periods, freq="15min"
        )
        rows = pd.DataFrame(
            {
                "station": station,
                "logger_id": station_index + 2,
                "Datetime": timestamps,
                "RecNbr": range(100000 + station_index * 100, 100000 + station_index * 100 + periods),
            }
        )
        for column in VALUE_COLUMNS:
            rows[column] = 12.5 if column == "BattV_Min" else 1.0
        return rows

    run_dir = tmp_path / "rejected"
    run_dir.mkdir()
    source = pd.concat(
        [station_rows(station, 96) for station in stations if station != "S3B"],
        ignore_index=True,
    )
    source.to_csv(run_dir / "logger_data.csv", index=False)
    source_report = {"status": "rejected", "raw_csv": "logger_data.csv"}
    (run_dir / "diagnostic_report.json").write_text(
        json.dumps(source_report), encoding="utf-8"
    )
    recovery_csv = tmp_path / "s3b.csv"
    station_rows("S3B", 95).to_csv(recovery_csv, index=False)

    result = repair_rejected_run(run_dir, [recovery_csv])

    assert result["status"] == "accepted"
    assert result["record_count"] == 11 * 96 + 95
    assert result["stations"]["S3B"]["rows"] == 95
    repair_dir = run_dir / "repair"
    assert (repair_dir / "logger_data.csv").exists()
    assert json.loads(
        (repair_dir / "diagnostic_report.json").read_text(encoding="utf-8")
    )["status"] == "accepted"
    assert json.loads(
        (run_dir / "diagnostic_report.json").read_text(encoding="utf-8")
    ) == source_report


def test_validate_archive_recovery_checks_immediate_neighbors(
    tmp_path: Path,
) -> None:
    def row(record: int, minute: int) -> dict:
        values = {
            "station": "S3B",
            "logger_id": 10,
            "Datetime": (
                pd.Timestamp("2026-09-25T06:00:00Z")
                + pd.Timedelta(minutes=minute)
            ).isoformat(),
            "RecNbr": record,
        }
        values.update({column: 12.5 for column in VALUE_COLUMNS})
        return values

    archive_root = tmp_path / "archive"
    promote_accepted_frame(
        pd.DataFrame([row(100, 0), row(103, 45)]),
        archive_root=archive_root,
    )
    first_recovery = tmp_path / "first.csv"
    second_recovery = tmp_path / "second.csv"
    pd.DataFrame([row(101, 15)]).to_csv(first_recovery, index=False)
    pd.DataFrame([row(102, 30)]).to_csv(second_recovery, index=False)

    output_dir = tmp_path / "validated"
    result = validate_archive_recovery(
        [first_recovery, second_recovery],
        archive_root=archive_root,
        output_dir=output_dir,
    )

    assert result["status"] == "accepted"
    assert result["record_count"] == 2
    assert result["findings"] == []
    assert result["stations"]["S3B"]["records"] == [101, 102]
    assert all(
        check["previous_neighbor_valid"]
        and check["following_neighbor_valid"]
        for check in result["stations"]["S3B"]["row_checks"]
    )
    assert (output_dir / "logger_data.csv").exists()
    assert json.loads(
        (output_dir / "diagnostic_report.json").read_text(encoding="utf-8")
    )["status"] == "accepted"


def test_compare_matches_pc400_with_rounding_tolerance(tmp_path: Path) -> None:
    rows = _download_rows()
    pakbus_path = tmp_path / "logger_data.csv"
    rows.round({column: 4 for column in VALUE_COLUMNS}).to_csv(pakbus_path, index=False)
    dat_dir = tmp_path / "datfiles_2026"
    dat_dir.mkdir()
    for station, station_rows in rows.groupby("station"):
        _write_toa5(dat_dir / f"{station}_Table1.dat", station_rows)

    report = compare_with_pc400(pakbus_path, dat_dir)

    assert report["equivalent"] is True
    assert report["totals"] == {
        "overlap": 4,
        "value_mismatches": 0,
        "missing_within_pc400_range": 0,
        "beyond_pc400_range": 0,
    }


def test_compare_detects_value_mismatch(tmp_path: Path) -> None:
    rows = _download_rows().loc[lambda frame: frame["station"] == "S3B"].copy()
    pakbus_path = tmp_path / "logger_data.csv"
    rows.to_csv(pakbus_path, index=False)
    dat_dir = tmp_path / "datfiles_2026"
    dat_dir.mkdir()
    altered = rows.copy()
    altered.loc[altered.index[0], "BattV_Min"] += 1
    _write_toa5(dat_dir / "S3B_Table1.dat", altered)

    report = compare_with_pc400(pakbus_path, dat_dir)

    assert report["equivalent"] is False
    assert report["totals"]["value_mismatches"] == 1
