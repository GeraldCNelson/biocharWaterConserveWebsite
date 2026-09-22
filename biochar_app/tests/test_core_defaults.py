"""Tests for current dashboard date defaults."""

from __future__ import annotations

import datetime

from biochar_app.config.core import (
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    DEFAULT_YEAR,
)


def test_default_year_dates_are_bounded_to_2026() -> None:
    start = datetime.date.fromisoformat(DEFAULT_START_DATE)
    end = datetime.date.fromisoformat(DEFAULT_END_DATE)

    assert DEFAULT_YEAR == 2026
    assert start == datetime.date(2026, 1, 1)
    assert start <= end <= datetime.date(2026, 12, 31)
