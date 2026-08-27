"""Focused tests for CS650 sensing-volume water diagnostics."""

from __future__ import annotations

import pandas as pd
import pytest

from biochar_app.config import CS650_SENSING_VOLUME_CM3, UNIT_CONVERSIONS
from biochar_app.scripts.etl import add_cs650_sensing_volume_water


VWC_COLUMN = "VWC_1_raw_S1_T"
WATER_L_COLUMN = "CS650_water_L_S1_T_1"
WATER_GAL_COLUMN = "CS650_water_gal_S1_T_1"


def test_calculates_water_from_vwc_and_documented_sensing_volume() -> None:
    frame = pd.DataFrame({VWC_COLUMN: [0.0, 25.0, 100.0]})

    result = add_cs650_sensing_volume_water(frame)

    sensing_volume_l = CS650_SENSING_VOLUME_CM3 / 1000.0
    sensing_volume_gal = UNIT_CONVERSIONS["metric_to_us"]["irrigation"](
        sensing_volume_l
    )
    assert result[WATER_L_COLUMN].tolist() == pytest.approx(
        [0.0, sensing_volume_l * 0.25, sensing_volume_l]
    )
    assert result[WATER_GAL_COLUMN].tolist() == pytest.approx(
        [0.0, sensing_volume_gal * 0.25, sensing_volume_gal]
    )


def test_skips_sensor_combinations_without_a_vwc_column() -> None:
    frame = pd.DataFrame({VWC_COLUMN: [30.0], "unrelated": [1]})

    result = add_cs650_sensing_volume_water(frame)

    generated_columns = [
        column for column in result if column.startswith("CS650_water_")
    ]
    assert generated_columns == [WATER_L_COLUMN, WATER_GAL_COLUMN]


def test_coerces_non_numeric_vwc_values_to_missing() -> None:
    frame = pd.DataFrame({VWC_COLUMN: ["25", "not-a-number", None]})

    result = add_cs650_sensing_volume_water(frame)

    assert result.loc[0, WATER_L_COLUMN] == pytest.approx(1.95)
    assert pd.isna(result.loc[1, WATER_L_COLUMN])
    assert pd.isna(result.loc[2, WATER_L_COLUMN])


def test_copy_option_controls_whether_input_is_mutated() -> None:
    original = pd.DataFrame({VWC_COLUMN: [50.0]})

    copied = add_cs650_sensing_volume_water(original)

    assert WATER_L_COLUMN not in original
    assert copied is not original

    in_place = add_cs650_sensing_volume_water(original, copy=False)

    assert in_place is original
    assert original.loc[0, WATER_L_COLUMN] == pytest.approx(3.9)
