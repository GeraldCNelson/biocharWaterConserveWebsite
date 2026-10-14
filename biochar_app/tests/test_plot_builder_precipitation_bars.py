"""Regression tests for precipitation-bar widths in Plotly figures."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd
import plotly.graph_objects as go

from biochar_app.config.core import bar_width_map
from biochar_app.config.core import PLOT_COLORS
from biochar_app.scripts.plot_builder import (
    add_fertilization_shapes,
    add_irrigation_shapes,
    add_precipitation_bars,
    make_ratio_figure,
    make_raw_figure,
)
from biochar_app.scripts.plot_components import (
    common_yaxis2_config,
    load_fertilization_events,
)


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

    def test_right_axis_identifies_precipitation_and_units(self) -> None:
        figure = go.Figure()
        add_precipitation_bars(
            figure,
            self.data,
            unit_system="us",
            granularity="daily",
        )

        self.assertEqual(figure.data[0].name, "Precip (in)")
        self.assertEqual(figure.layout.yaxis2.title.text, "Precipitation (in)")
        self.assertEqual(
            common_yaxis2_config("metric")["title"]["text"],
            "Precipitation (mm)",
        )

    def test_15min_bar_uses_visible_three_hour_display_width(self) -> None:
        self.assertEqual(
            self.precipitation_width("15min"),
            3 * 60 * 60 * 1000,
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

    def test_zero_precipitation_rows_are_omitted_from_trace(self) -> None:
        data = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2025-05-17 00:00", "2025-05-17 00:15", "2025-05-17 00:30"]
                ),
                "precip_in": [0.0, 0.02, 0.0],
            }
        )
        figure = go.Figure()

        add_precipitation_bars(
            figure,
            data,
            unit_system="us",
            granularity="15min",
        )

        self.assertEqual(list(figure.data[0].x), [data.loc[1, "timestamp"]])
        self.assertEqual(list(figure.data[0].y), [0.02])


class ECHydrologyOverlayTests(unittest.TestCase):
    @staticmethod
    def irrigation_events(strip: str, _year: int) -> pd.DataFrame:
        event_dates = {
            "S1": "2025-06-02 14:30",
            "S3": "2025-06-03 09:15",
        }
        event_date = event_dates.get(strip, "2025-06-02")
        return pd.DataFrame(
            {
                "start": [pd.Timestamp(event_date)],
                "gallons_strip": [12_000.0],
            }
        )

    @patch(
        "biochar_app.scripts.plot_builder.load_irrigation_events",
        side_effect=irrigation_events,
    )
    def test_raw_ec_includes_precipitation_and_irrigation(
        self,
        _mock_events,
    ) -> None:
        data = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-06-01", "2025-06-02"]),
                "EC_1_raw_S1_T": [0.31, 0.47],
                "precip_in": [0.0, 0.12],
            }
        )

        figure = make_raw_figure(
            df=data,
            variable="EC",
            strip="S1",
            logger_location="T",
            depth="1",
            unit_system="us",
            year=2025,
            granularity="daily",
            start="2025-06-01",
            end="2025-06-02",
            trace_option="depth",
        )

        trace_names = [trace["name"] for trace in figure["data"]]
        self.assertIn("Precip (in)", trace_names)
        self.assertIn("Irrigation", trace_names)
        self.assertEqual(len(figure["layout"]["shapes"]), 1)
        self.assertEqual(
            pd.Timestamp(figure["layout"]["shapes"][0]["x0"]),
            pd.Timestamp("2025-06-02 00:00"),
        )
        self.assertEqual(figure["layout"]["annotations"][0]["text"], "12 <br>k gal")
        self.assertEqual(figure["layout"]["yaxis2"]["side"], "right")
        self.assertEqual(
            figure["layout"]["yaxis2"]["title"]["text"],
            "Precipitation (in)",
        )
        self.assertEqual(
            figure["layout"]["yaxis"]["title"]["text"],
            "Bulk Electrical Conductivity (dS/m)",
        )
        self.assertEqual(figure["layout"]["legend"]["orientation"], "h")
        self.assertEqual(figure["layout"]["legend"]["x"], 0.0)
        self.assertGreater(figure["layout"]["legend"]["y"], 1.1)
        self.assertGreaterEqual(figure["layout"]["margin"]["t"], 100)

    @patch(
        "biochar_app.scripts.plot_builder.load_irrigation_events",
        side_effect=irrigation_events,
    )
    def test_ec_ratio_distinguishes_the_two_irrigation_groups(
        self,
        _mock_events,
    ) -> None:
        data = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-06-01", "2025-06-02"]),
                "EC_1_ratio_S1_S2_T": [1.5, 1.7],
                "EC_1_ratio_S3_S4_T": [1.2, 1.3],
                "precip_in": [0.0, 0.12],
            }
        )

        figure = make_ratio_figure(
            df=data,
            variable="EC",
            strip="S1",
            logger_location="T",
            unit_system="us",
            granularity="daily",
            year=2025,
            start="2025-06-01",
            end="2025-06-03",
            depth="1",
        )

        trace_names = [trace["name"] for trace in figure["data"]]
        self.assertIn("Precip (in)", trace_names)
        self.assertIn("Irrigation S1/S2", trace_names)
        self.assertIn("Irrigation S3/S4", trace_names)
        self.assertEqual(len(figure["layout"]["shapes"]), 3)
        self.assertNotIn("annotations", figure["layout"])

        irrigation_colors = {
            shape["line"]["color"]
            for shape in figure["layout"]["shapes"]
            if shape["line"].get("dash") == "dot"
        }
        self.assertEqual(
            irrigation_colors,
            {PLOT_COLORS["ratio_S1_S2"], PLOT_COLORS["ratio_S3_S4"]},
        )
        self.assertEqual(
            figure["layout"]["yaxis"]["title"]["text"],
            "Bulk EC ratio (dimensionless)",
        )
        self.assertEqual(
            figure["layout"]["legend"]["title"]["text"],
            "<b>Bulk EC ratio</b>",
        )
        self.assertEqual(figure["layout"]["legend"]["orientation"], "h")
        self.assertEqual(figure["layout"]["legend"]["x"], 0.0)
        self.assertGreater(figure["layout"]["legend"]["y"], 1.1)

    @patch(
        "biochar_app.scripts.plot_builder.load_irrigation_events",
        return_value=pd.DataFrame(
            {
                "start": [pd.Timestamp("2025-06-02 14:30")],
                "gallons_strip": [12_000.0],
            }
        ),
    )
    def test_finer_resolution_irrigation_marker_keeps_recorded_time(
        self,
        _mock_events,
    ) -> None:
        figure = go.Figure()
        add_irrigation_shapes(
            figure,
            strip="S1",
            year=2025,
            unit_system="us",
            granularity="15min",
        )

        self.assertEqual(
            pd.Timestamp(figure.layout.shapes[0].x0),
            pd.Timestamp("2025-06-02 14:30"),
        )

    @patch(
        "biochar_app.scripts.plot_builder.load_fertilization_events",
        return_value=pd.DataFrame(
            {
                "date": [pd.Timestamp("2025-04-12")],
                "scope": ["field"],
            }
        ),
    )
    def test_fertilization_marker_is_range_filtered_and_distinct(
        self,
        _mock_fertilizer,
    ) -> None:
        figure = go.Figure()

        add_fertilization_shapes(
            figure,
            year=2025,
            start="2025-04-01",
            end="2025-04-30",
        )

        self.assertEqual([trace.name for trace in figure.data], ["Fertilization"])
        self.assertEqual(len(figure.layout.shapes), 1)
        self.assertEqual(figure.layout.shapes[0].line.dash, "dash")
        self.assertEqual(
            figure.layout.shapes[0].line.color,
            PLOT_COLORS["fertilizer"],
        )
        self.assertEqual(figure.layout.annotations[0].text, "Fert.")

        outside = go.Figure()
        add_fertilization_shapes(
            outside,
            year=2025,
            start="2025-05-01",
            end="2025-10-31",
        )
        self.assertEqual(len(outside.data), 0)
        self.assertEqual(len(outside.layout.shapes), 0)

    def test_loader_excludes_recommendation_rows(self) -> None:
        events = load_fertilization_events(2025)

        self.assertEqual(len(events), 1)
        self.assertEqual(events.loc[0, "date"], pd.Timestamp("2025-04-12"))
        self.assertEqual(events.loc[0, "scope"], "field")
        self.assertEqual(events.loc[0, "source_cell"], "D5")

    def test_standard_raw_and_ratio_ec_figures_omit_fertilization_marker(self) -> None:
        fertilizer_event = pd.DataFrame(
            {
                "date": [pd.Timestamp("2025-04-12")],
                "scope": ["field"],
            }
        )
        no_irrigation = pd.DataFrame(columns=["start", "gallons_strip"])
        data = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2025-04-11", "2025-04-13"]),
                "EC_1_raw_S1_T": [0.31, 0.47],
                "EC_1_ratio_S1_S2_T": [1.5, 1.7],
                "precip_in": [0.0, 0.12],
            }
        )

        with patch(
            "biochar_app.scripts.plot_builder.load_irrigation_events",
            return_value=no_irrigation,
        ), patch(
            "biochar_app.scripts.plot_builder.load_fertilization_events",
            return_value=fertilizer_event,
        ):
            raw = make_raw_figure(
                df=data,
                variable="EC",
                strip="S1",
                logger_location="T",
                depth="1",
                unit_system="us",
                year=2025,
                granularity="daily",
                start="2025-04-11",
                end="2025-04-13",
                trace_option="depth",
            )
            ratio = make_ratio_figure(
                df=data,
                variable="EC",
                strip="S1",
                logger_location="T",
                unit_system="us",
                granularity="daily",
                year=2025,
                start="2025-04-11",
                end="2025-04-13",
                depth="1",
            )

        self.assertNotIn("Fertilization", [trace["name"] for trace in raw["data"]])
        self.assertNotIn("Fertilization", [trace["name"] for trace in ratio["data"]])
        self.assertFalse(
            any(
                shape["line"].get("dash") == "dash"
                for shape in raw["layout"].get("shapes", [])
            )
        )
        self.assertFalse(
            any(
                shape["line"].get("dash") == "dash"
                for shape in ratio["layout"].get("shapes", [])
            )
        )
