"""Tests for projected logger influence-zone geometry."""

from __future__ import annotations

import geopandas as gpd
from PIL import Image

from biochar_app.geospatial.build_fruita_field_layout import (
    INPUT_GEOJSON,
    LOGGER_FEATURES,
    WORKING_CRS,
    build_field_boundary,
    build_logger_influence_zones,
    build_strip_centerlines,
    build_strip_polygons,
    get_named_points,
    load_control_points,
    render_field_layout,
)


def test_logger_influence_zones_partition_field_boundary() -> None:
    control_points = load_control_points(INPUT_GEOJSON).to_crs(WORKING_CRS)
    boundary = build_field_boundary(get_named_points(control_points))

    zones = build_logger_influence_zones(control_points, boundary)

    assert set(zones["feature_id"]) == set(LOGGER_FEATURES)
    assert (zones["area_sqft"] > 0).all()
    assert abs(zones.geometry.area.sum() - boundary.area) < 1e-6
    assert zones.geometry.is_valid.all()


def test_render_field_layout_writes_png_and_webp(tmp_path) -> None:
    control_points = load_control_points(INPUT_GEOJSON).to_crs(WORKING_CRS)
    points = get_named_points(control_points)
    boundary = build_field_boundary(points)
    field_boundary = gpd.GeoDataFrame(
        [{"geometry": boundary}], crs=WORKING_CRS
    )
    strip_polygons = gpd.GeoDataFrame(
        build_strip_polygons(points), crs=WORKING_CRS
    )
    strip_centerlines = gpd.GeoDataFrame(
        build_strip_centerlines(points), crs=WORKING_CRS
    )
    png_path = tmp_path / "field_layout.png"
    webp_path = tmp_path / "field_layout.webp"

    render_field_layout(
        control_points,
        field_boundary,
        strip_polygons,
        strip_centerlines,
        png_path=png_path,
        webp_path=webp_path,
    )

    assert png_path.is_file()
    assert webp_path.is_file()
    with Image.open(png_path) as png, Image.open(webp_path) as webp:
        assert png.format == "PNG"
        assert webp.format == "WEBP"
        assert png.size == webp.size
        assert png.width > 1000
        assert png.height > png.width
