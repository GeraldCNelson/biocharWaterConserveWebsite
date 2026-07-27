"""Regression tests for precipitation-bar widths in Plotly figures."""

from __future__ import annotations

import unittest

import pandas as pd
import plotly.graph_objects as go

from biochar_app.config.core import bar_width_map
from biochar_app.scripts.plot_builder import add_precipitation_bars


class PrecipitationBarWidthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.data = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2025-05-17 00:00", "2025-05-17 00:15"]
                ),
                "precip_in": [0.01, 0.02],
            }
        )

    def precipitation_width(self, granularity: str) -> int:
        figure = go.Figure()
        add_precipitation_bars(
            figure,
            self.data,
            unit_system="us",
            granularity=granularity,
        )
        self.assertEqual(len(figure.data), 1)
        return int(figure.data[0].width)

    def test_15min_bar_uses_visible_30_minute_display_width(self) -> None:
        self.assertEqual(
            self.precipitation_width("15min"),
            30 * 60 * 1000,
        )

    def test_15minute_alias_uses_15min_width(self) -> None:
        self.assertEqual(
            self.precipitation_width("15-minute"),
            bar_width_map["15min"],
        )

    def test_hourly_bar_uses_one_hour_width(self) -> None:
        self.assertEqual(
            self.precipitation_width("hourly"),
            60 * 60 * 1000,
        )

    def test_daily_bar_uses_half_day_width(self) -> None:
        self.assertEqual(
            self.precipitation_width("daily"),
            int(0.5 * 24 * 60 * 60 * 1000),
        )
