"""Tests for loading reviewed meter photographs for workbook comparison."""

from __future__ import annotations

import pandas as pd

from biochar_app.scripts.management.compare_meter_photos_to_irrigation import (
    load_photo_readings,
)


def test_loads_only_usable_canonical_inventory_rows(tmp_path) -> None:
    path = tmp_path / "photo_inventory_unique.csv"
    pd.DataFrame(
        [
            {
                "filename": "start.heic",
                "sha256": "a" * 64,
                "effective_datetime": "2025-04-20 08:13:03-06:00",
                "meter_reading": "201676",
                "review_status": "readable",
                "include": "TRUE",
                "notes": "",
            },
            {
                "filename": "uncertain.heic",
                "sha256": "b" * 64,
                "effective_datetime": "2025-04-20 08:15:20",
                "meter_reading": "201684",
                "review_status": "uncertain",
                "include": "TRUE",
                "notes": "needs review",
            },
            {
                "filename": "short-reading.heic",
                "sha256": "c" * 64,
                "effective_datetime": "2025-04-20 08:45:21",
                "meter_reading": "12345",
                "review_status": "readable",
                "include": "TRUE",
                "notes": "",
            },
            {
                "filename": "excluded.heic",
                "sha256": "d" * 64,
                "effective_datetime": "2025-04-20 09:00:00",
                "meter_reading": "201700",
                "review_status": "readable",
                "include": "FALSE",
                "notes": "",
            },
        ]
    ).to_csv(path, index=False)

    result = load_photo_readings(path)

    assert result["photo_id"].tolist() == ["a" * 64]
    assert result["photo_filename"].tolist() == ["start.heic"]
    assert result["photo_counter_value"].tolist() == [201676]
    assert result["photo_qc_status"].tolist() == ["clean"]
    assert result["photo_datetime"].iloc[0] == pd.Timestamp(
        "2025-04-20 08:13:03"
    )


def test_legacy_clean_photo_schema_remains_supported(tmp_path) -> None:
    path = tmp_path / "meter_photo_readings_clean.csv"
    pd.DataFrame(
        [
            {
                "photo_id": "legacy-photo",
                "photo_datetime": "2025-04-20 08:13:03",
                "meter_counter_value": 201676,
                "qc_status": "clean",
                "preferred_filename": "legacy.heic",
                "notes": "",
            }
        ]
    ).to_csv(path, index=False)

    result = load_photo_readings(path)

    assert result["photo_id"].tolist() == ["legacy-photo"]
    assert result["photo_counter_value"].tolist() == [201676]
    assert result["photo_filename"].tolist() == ["legacy.heic"]
