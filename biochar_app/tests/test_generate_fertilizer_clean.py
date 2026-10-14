from __future__ import annotations

import pytest

from biochar_app.config.core import STRIP_AREA_ACRES
from biochar_app.config.paths import BIOCHAR_MASTER_WORKBOOK
from biochar_app.scripts.management.generate_fertilizer_clean import (
    build_and_install_fertilizer,
    build_fertilizer_clean,
    canonical_product_name,
)


@pytest.fixture(scope="module")
def fertilizer_data():
    return build_fertilizer_clean(BIOCHAR_MASTER_WORKBOOK)


def _rate(frame, year: int, strip: str, product: str) -> float:
    match = frame.loc[
        (frame["year"] == year)
        & (frame["record_type"] == "recommendation")
        & (frame["strip"] == strip)
        & (frame["product"] == product),
        "recommended_product_lb_per_acre",
    ]
    assert len(match) == 1
    return float(match.iloc[0])


def test_builds_recommendations_and_separate_field_activity_events(
    fertilizer_data,
) -> None:
    frame = fertilizer_data
    recommendations = frame.loc[frame["record_type"] == "recommendation"]
    activities = frame.loc[frame["record_type"] == "fertilization_activity"]

    assert set(frame["year"]) == {2023, 2024, 2025, 2026}
    assert len(recommendations) == 52
    assert recommendations["date"].isna().all()
    assert set(recommendations["scope"]) == {"strip"}
    assert len(activities) == 4
    assert set(activities["scope"]) == {"field"}
    assert activities.set_index("year")["date"].to_dict() == {
        2023: "2023-03-17",
        2024: "2024-04-09",
        2025: "2025-04-12",
        2026: "2026-06-27",
    }
    assert activities.set_index("year")["source_cell"].to_dict() == {
        2023: "B5",
        2024: "C5",
        2025: "D5",
        2026: "E5",
    }
    assert activities["strip"].isna().all()
    assert activities["product"].isna().all()
    assert activities["recommended_product_lb_per_acre"].isna().all()
    assert not any("applied" in column for column in frame.columns)


def test_recalculates_product_rates_instead_of_using_bad_sheet_formulas(
    fertilizer_data,
) -> None:
    frame = fertilizer_data

    # 2023 shared N/P recommendations apply to each strip; they are not split
    # in half as the previous ETL did.
    assert _rate(frame, 2023, "S1", "11-52-0") == pytest.approx(115.385, abs=0.001)
    assert _rate(frame, 2023, "S2", "11-52-0") == pytest.approx(115.385, abs=0.001)

    # Urea is the remaining N requirement after accounting for N supplied by
    # 11-52-0, rather than the worksheet's dimensionally incorrect formula.
    assert _rate(frame, 2025, "S1", "UREA") == pytest.approx(42.851, abs=0.001)

    # These cells are 0.32 and 0.274 in the workbook because they divide by a
    # neighboring result. The correct 32%-Mn product rates are 21.875 and 18.75.
    assert _rate(frame, 2025, "S2", "MANGANESE SULFATE (MNSO4)") == pytest.approx(
        21.875, abs=0.001
    )
    assert _rate(frame, 2026, "S4", "MANGANESE SULFATE (MNSO4)") == pytest.approx(
        18.750, abs=0.001
    )


def test_product_totals_use_strip_area(fertilizer_data) -> None:
    frame = fertilizer_data
    row = frame.loc[
        (frame["year"] == 2026)
        & (frame["strip"] == "S1")
        & (frame["product"] == "SULFATE OF POTASH")
    ].iloc[0]

    expected_total = row["recommended_product_lb_per_acre"] * STRIP_AREA_ACRES["S1"]
    assert row["recommended_product_total_lb"] == pytest.approx(expected_total, abs=0.001)


@pytest.mark.parametrize(
    ("source_name", "canonical"),
    [
        ("46-0-0 UREA (N)", "UREA"),
        ("11-52-1 (PHOSPHORUS)", "11-52-0"),
        ("POTASSIUM SULFATE (K2SO4)", "SULFATE OF POTASH"),
    ],
)
def test_normalizes_product_aliases(source_name: str, canonical: str) -> None:
    assert canonical_product_name(source_name) == canonical


def test_build_and_install_fertilizer_writes_atomic_output(tmp_path) -> None:
    output_csv = tmp_path / "fertilizer_clean.csv"

    audit = build_and_install_fertilizer(
        input_workbook=BIOCHAR_MASTER_WORKBOOK,
        output_csv=output_csv,
    )

    assert output_csv.exists()
    assert audit["rows"] == 56
    assert audit["recommendation_rows"] == 52
    assert audit["activity_rows"] == 4
    assert audit["latest_activity_date"] == "2026-06-27"
