from __future__ import annotations

from pathlib import Path

from biochar_app.scripts.gseason_materialized_cache import (
    load_materialized_gseason_summary,
    save_materialized_gseason_summary,
)


def cache_arguments(tmp_path: Path, source: Path) -> dict:
    return {
        "year": 2025,
        "variable": "VWC",
        "strip": "S1",
        "depth": "1",
        "unit_system": "us",
        "periods": [
            {
                "code": "Q2_Growing",
                "label": "Growing Season",
                "start": "2025-04-01",
                "end": "2025-10-31",
            }
        ],
        "cache_dir": tmp_path / "cache",
        "source_paths": [source],
    }


def test_materialized_summary_is_reused_until_source_changes(tmp_path: Path) -> None:
    source = tmp_path / "2025_15min.parquet"
    source.write_bytes(b"first source version")
    arguments = cache_arguments(tmp_path, source)
    rows = [{"period_code": "Q2_Growing", "raw_mean": 24.5}]

    save_materialized_gseason_summary(**arguments, rows=rows)

    assert load_materialized_gseason_summary(**arguments) == rows

    source.write_bytes(b"a changed and longer source version")

    assert load_materialized_gseason_summary(**arguments) is None


def test_period_configuration_has_its_own_cache_entry(tmp_path: Path) -> None:
    source = tmp_path / "2025_15min.parquet"
    source.write_bytes(b"source")
    arguments = cache_arguments(tmp_path, source)
    save_materialized_gseason_summary(**arguments, rows=[])

    changed = dict(arguments)
    changed["periods"] = [
        {
            "code": "CUSTOM_1",
            "label": "Harvest",
            "start": "2025-08-01",
            "end": "2025-09-15",
        }
    ]

    assert load_materialized_gseason_summary(**changed) is None
