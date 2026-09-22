from pathlib import Path

import pandas as pd
import pytest

from biochar_app.scripts.etl import collect_dataset_metadata_from_processed_outputs


def _write_processed_year(root: Path, year: int, *, vwc: float, air: float) -> None:
    logger_path = root / str(year) / f"{year}_raw_logger.parquet"
    logger_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime([f"{year}-01-01 00:00"]),
            "VWC_1_raw_S1_T": [vwc],
            "T_1_raw_S1_T": [40.0 + vwc],
            "EC_1_raw_S1_T": [0.1 + vwc / 100.0],
        }
    ).to_parquet(logger_path, index=False)

    weather_path = root / "summary" / "weather" / "15min" / f"{year}_15min.parquet"
    weather_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [f"{year}-01-01 00:00", f"{year}-01-01 00:15"]
            ),
            "temp_air_degF": [air, air + 1.0],
            "precip_in": [0.1, 0.2],
        }
    ).to_parquet(weather_path, index=False)


def test_global_metadata_uses_every_processed_year(tmp_path: Path) -> None:
    _write_processed_year(tmp_path, 2023, vwc=5.0, air=10.0)
    _write_processed_year(tmp_path, 2026, vwc=55.0, air=100.0)

    metadata = collect_dataset_metadata_from_processed_outputs(
        [2023, 2026],
        parquet_dir=tmp_path,
    )

    assert metadata["vwc_percent"] == {"min": 5.0, "max": 55.0}
    assert metadata["air_temperature_f"] == {"min": 10.0, "max": 101.0}
    assert metadata["daily_precipitation_in"] == {
        "min": pytest.approx(0.3),
        "max": pytest.approx(0.3),
    }


def test_global_metadata_refuses_incomplete_year_set(tmp_path: Path) -> None:
    _write_processed_year(tmp_path, 2026, vwc=55.0, air=100.0)

    with pytest.raises(FileNotFoundError, match="2023_raw_logger.parquet"):
        collect_dataset_metadata_from_processed_outputs(
            [2023, 2026],
            parquet_dir=tmp_path,
        )
