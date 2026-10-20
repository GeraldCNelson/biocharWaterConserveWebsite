from __future__ import annotations

import pandas as pd
import pytest

from biochar_app.scripts.lab.biomass_field_tables import (
    get_biomass_field_table_payload,
)


def _set_by_key(payload: dict, key: str) -> dict:
    return next(item for item in payload["sets"] if item["key"] == key)


def test_payload_adds_annual_strip_totals_and_preserves_samples(tmp_path) -> None:
    biomass_csv = tmp_path / "biomass.csv"
    fertilizer_csv = tmp_path / "missing-fertilizer.csv"
    pd.DataFrame(
        {
            "location": ["S1T", "S1M", "S1B", "S2T"],
            "2023-06-12": [10, 20, 30, 5],
            "2023-07-25": [1, 2, 3, 7],
            "2024-05-31": [4, 5, 6, 9],
        }
    ).to_csv(biomass_csv, index=False)

    payload = get_biomass_field_table_payload(
        biomass_csv,
        min_year=2023,
        fertilizer_csv_path=fertilizer_csv,
    )

    assert payload["title"] == "Biomass Samples and Fertilizer Recommendations"

    totals = _set_by_key(payload, "biomass_field_annual_totals")
    assert totals["variables"][0]["decimal_places"] == 1
    values = totals["data"]["annual_dry_biomass_total_g"]
    assert totals["periods"] == ["2023", "2024"]
    assert values["S1"] == {"2023": 66.0, "2024": 15.0}
    assert values["S2"] == {"2023": 12.0, "2024": 9.0}
    assert values["S3"] == {"2023": None, "2024": None}
    assert "not whole-strip hay yields" in totals["note"]

    samples = _set_by_key(payload, "biomass_field_samples")
    assert samples["variables"][0]["decimal_places"] == 1
    assert samples["data"]["dry_biomass_g"]["S1T"]["2023-06-12"] == 10.0


def test_payload_adds_labeled_fertilizer_context(tmp_path) -> None:
    biomass_csv = tmp_path / "biomass.csv"
    fertilizer_csv = tmp_path / "fertilizer.csv"
    pd.DataFrame(
        {
            "location": ["S1T", "S1M", "S1B"],
            "2023-06-12": [10, 20, 30],
        }
    ).to_csv(biomass_csv, index=False)
    pd.DataFrame(
        [
            {
                "year": 2023,
                "date": None,
                "record_type": "recommendation",
                "strip": "S1",
                "estimated_n_lb_per_acre": 12.5,
                "estimated_p_lb_per_acre": 4.0,
                "estimated_k_lb_per_acre": 0.0,
            },
            {
                "year": 2023,
                "date": None,
                "record_type": "recommendation",
                "strip": "S1",
                "estimated_n_lb_per_acre": 7.5,
                "estimated_p_lb_per_acre": 0.0,
                "estimated_k_lb_per_acre": 3.0,
            },
            {
                "year": 2023,
                "date": "2023-03-17",
                "record_type": "fertilization_activity",
                "strip": None,
                "estimated_n_lb_per_acre": None,
                "estimated_p_lb_per_acre": None,
                "estimated_k_lb_per_acre": None,
            },
        ]
    ).to_csv(fertilizer_csv, index=False)

    payload = get_biomass_field_table_payload(
        biomass_csv,
        min_year=2023,
        fertilizer_csv_path=fertilizer_csv,
    )

    recommendations = _set_by_key(payload, "fertilizer_recommendations")
    assert recommendations["variables"][0]["decimal_places"] == 1
    values = recommendations["data"]["recommended_elemental_nutrient_lb_per_acre"]
    assert values["S1_N"]["2023"] == pytest.approx(20.0)
    assert values["S1_P"]["2023"] == pytest.approx(4.0)
    assert values["S1_K"]["2023"] == pytest.approx(3.0)
    assert "not confirmed fertilizer applications" in recommendations["note"]

    activities = _set_by_key(payload, "fertilizer_activity_dates")
    activity_values = activities["data"]["recorded_fertilization_activity_date"]
    assert activity_values["field"]["2023"] == "2023-03-17"
    assert "field-level" in activities["note"]
