"""Tests for accepted-run archiving and PC400 equivalence checks."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from biochar_app.pakbus.core.archive import (
    VALUE_COLUMNS,
    compare_with_pc400,
    promote_accepted_frame,
    promote_run_directory,
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
