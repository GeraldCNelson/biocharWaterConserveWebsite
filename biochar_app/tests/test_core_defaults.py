"""Tests for current dashboard date defaults."""

from __future__ import annotations

import datetime

from biochar_app.config.core import (
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    DEFAULT_YEAR,
    VARIABLE_NAME_ABBREV,
    VARIABLE_NAME_MAPPING,
)
from biochar_app.config.units import label_name_mapping


def test_default_year_dates_are_bounded_to_2026() -> None:
    start = datetime.date.fromisoformat(DEFAULT_START_DATE)
    end = datetime.date.fromisoformat(DEFAULT_END_DATE)

    assert DEFAULT_YEAR == 2026
    assert start == datetime.date(2026, 1, 1)
    assert start <= end <= datetime.date(2026, 12, 31)


def test_logger_ec_labels_identify_bulk_measurement() -> None:
    assert VARIABLE_NAME_MAPPING["EC"] == "Bulk Electrical Conductivity (EC)"
    assert VARIABLE_NAME_ABBREV["EC"] == "Bulk EC"
    assert label_name_mapping["EC"]["us"] == "Bulk Electrical Conductivity (dS/m)"
    assert label_name_mapping["EC"]["metric"] == "Bulk Electrical Conductivity (dS/m)"
