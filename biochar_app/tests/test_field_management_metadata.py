import pytest

from biochar_app.config.field_management_metadata import (
    FIELD_WIDTH_NORTH_FT,
    FIELD_WIDTH_SOUTH_FT,
    NOMINAL_STRIP_WIDTH_FT,
    PHOTO_GPS_EQUAL_QUARTER_WIDTH_FT,
    ZONE_AREAS_SQFT_BY_STRIP,
    ZONE_AREA_SOURCE_BY_STRIP,
    ZONE_LENGTHS_FT_BY_STRIP,
)


def test_photo_gps_width_is_retained_as_diagnostic_only() -> None:
    expected = (FIELD_WIDTH_NORTH_FT + FIELD_WIDTH_SOUTH_FT) / 2 / 4
    assert PHOTO_GPS_EQUAL_QUARTER_WIDTH_FT == pytest.approx(expected)
    assert PHOTO_GPS_EQUAL_QUARTER_WIDTH_FT == pytest.approx(42.3875)
    assert NOMINAL_STRIP_WIDTH_FT == pytest.approx(47.0)


def test_capacity_zone_areas_use_nominal_strip_width() -> None:
    for strip in ("S1", "S2", "S3", "S4"):
        for position in ("T", "M", "B"):
            assert ZONE_AREAS_SQFT_BY_STRIP[strip][position] == pytest.approx(
                NOMINAL_STRIP_WIDTH_FT * ZONE_LENGTHS_FT_BY_STRIP[strip][position]
            )
            assert ZONE_AREA_SOURCE_BY_STRIP[strip][position] == (
                "nominal_47ft_strip_by_centerline_zone_length"
            )
