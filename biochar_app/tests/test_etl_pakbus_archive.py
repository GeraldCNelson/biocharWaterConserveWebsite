"""Tests for combining accepted PakBus archives with PC400 logger files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from biochar_app.scripts.etl import (
    _read_pakbus_archive,
    read_logger_data,
    require_frozen_year_permission,
    write_parquet_atomic,
)


def test_pakbus_archive_timestamp_returns_to_fixed_mst(tmp_path: Path) -> None:
    archive = tmp_path / "2026"
    archive.mkdir()
    pd.DataFrame(
        [
            {
                "station": "S3B",
                "logger_id": 10,
                "Datetime": "2026-09-12T21:15:00Z",
                "RecNbr": 123,
                "BattV_Min": 13.2,
            },
            {
                "station": "S4T",
                "logger_id": 11,
                "Datetime": "2026-09-12T21:15:00Z",
                "RecNbr": 456,
                "BattV_Min": 13.3,
            },
        ]
    ).to_csv(archive / "logger_data.csv", index=False)

    result = _read_pakbus_archive("S3B", 2026, tmp_path)

    assert result is not None
    assert len(result) == 1
    assert result.iloc[0]["timestamp"] == pd.Timestamp("2026-09-12 14:15:00")
    assert result.iloc[0]["RECORD"] == 123


def test_pc400_value_wins_over_overlapping_pakbus_value(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    dat_dir = raw_root / "datfiles_2026"
    dat_dir.mkdir(parents=True)
    columns = [
        "TIMESTAMP", "RECORD", "BattV_Min",
        "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
        "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
        "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
    ]
    header = [
        '"TOA5","S3B","CR200X"',
        ",".join(f'"{column}"' for column in columns),
        ",".join('""' for _ in columns),
        ",".join('""' for _ in columns),
        '"2026-09-12 14:15:00",123,13.234567,0.1,0.2,20,0.1,0.2,20,0.1,0.2,20',
    ]
    (dat_dir / "S3B_Table1.dat").write_text("\n".join(header) + "\n", encoding="utf-8")

    archive = tmp_path / "archive" / "2026"
    archive.mkdir(parents=True)
    pd.DataFrame(
        [{
            "station": "S3B", "logger_id": 10,
            "Datetime": "2026-09-12T21:15:00Z", "RecNbr": 123,
            "BattV_Min": 13.2346,
            "VWC_1_Avg": 0.1, "EC_1_Avg": 0.2, "T_1_Avg": 20,
            "VWC_2_Avg": 0.1, "EC_2_Avg": 0.2, "T_2_Avg": 20,
            "VWC_3_Avg": 0.1, "EC_3_Avg": 0.2, "T_3_Avg": 20,
        }]
    ).to_csv(archive / "logger_data.csv", index=False)

    result = read_logger_data(
        "S3B", 2026, data_raw_dir=raw_root, archive_root=tmp_path / "archive"
    )

    assert result is not None
    assert len(result) == 1
    assert result.iloc[0]["BattV_Min_S3_B"] == 13.234567


def test_frozen_year_requires_explicit_permission() -> None:
    try:
        require_frozen_year_permission([2025, 2026], allow=False)
    except ValueError as exc:
        assert "2025" in str(exc)
        assert "--rebuild-frozen-years" in str(exc)
    else:
        raise AssertionError("Frozen year was accepted without permission")

    require_frozen_year_permission([2025, 2026], allow=True)
    require_frozen_year_permission([2026], allow=False)


def test_atomic_parquet_write_replaces_complete_file(tmp_path: Path) -> None:
    destination = tmp_path / "logger.parquet"
    write_parquet_atomic(pd.DataFrame({"value": [1, 2]}), destination)

    assert pd.read_parquet(destination)["value"].tolist() == [1, 2]
    assert not list(tmp_path.glob(".logger.parquet.*.parquet"))


def test_atomic_parquet_failure_preserves_existing_file(
    tmp_path: Path, monkeypatch,
) -> None:
    destination = tmp_path / "logger.parquet"
    destination.write_bytes(b"existing-good-file")

    def fail_write(*args, **kwargs) -> None:
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_write)

    try:
        write_parquet_atomic(pd.DataFrame({"value": [3]}), destination)
    except RuntimeError as exc:
        assert "simulated" in str(exc)
    else:
        raise AssertionError("Expected the simulated write failure")

    assert destination.read_bytes() == b"existing-good-file"
    assert not list(tmp_path.glob(".logger.parquet.*.parquet"))
