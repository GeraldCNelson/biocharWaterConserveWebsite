"""
Build core Fruita biochar field geometry from geotagged field photo locations.

Inputs:
    biochar_app/data-processed/field-mapping/field_photo_locations.geojson

Outputs:
    biochar_app/geospatial/field_layout/Fruita_Biochar_Field_Layout.gpkg

Requires:
    geopandas
    shapely
    pyproj
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_GEOJSON = (
    PROJECT_ROOT
    / "geospatial"
    / "field_layout"
    / "field_photo_locations.geojson"
)

OUTPUT_DIR = PROJECT_ROOT / "geospatial" / "field_layout"
OUTPUT_GPKG = OUTPUT_DIR / "Fruita_Biochar_Field_Layout.gpkg"

SOURCE_CRS = "EPSG:4326"
WORKING_CRS = "EPSG:3742"  # NAD83(HARN) / UTM zone 12N, meters


REQUIRED_FEATURES = {
    "field_nw",
    "field_ne",
    "field_se",
    "field_sw",
}

STRIP_METADATA = {
    "S1": {"treatment": "Biochar", "irrigation": "Monthly", "biochar": True,  "strip_order": 1},
    "S2": {"treatment": "Control", "irrigation": "Monthly", "biochar": False, "strip_order": 2},
    "S3": {"treatment": "Biochar", "irrigation": "Biweekly", "biochar": True,  "strip_order": 3},
    "S4": {"treatment": "Control", "irrigation": "Biweekly", "biochar": False, "strip_order": 4},
}


def load_control_points(path: Path) -> gpd.GeoDataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input GeoJSON: {path}")

    gdf = gpd.read_file(path)

    if "feature_id" not in gdf.columns:
        raise ValueError(
            "GeoJSON must contain a 'feature_id' column. "
            "Add feature_id values in QGIS first."
        )

    if gdf.crs is None:
        gdf = gdf.set_crs(SOURCE_CRS)

    return gdf


def get_named_points(gdf: gpd.GeoDataFrame) -> dict[str, object]:
    named = {}

    for feature_id in REQUIRED_FEATURES:
        matches = gdf[gdf["feature_id"] == feature_id]

        if matches.empty:
            raise ValueError(f"Missing required feature_id: {feature_id}")

        if len(matches) > 1:
            raise ValueError(f"Duplicate feature_id found: {feature_id}")

        named[feature_id] = matches.iloc[0].geometry

    return named


def build_field_boundary(points: dict[str, object]) -> Polygon:
    return Polygon(
        [
            points["field_nw"],
            points["field_ne"],
            points["field_se"],
            points["field_sw"],
            points["field_nw"],
        ]
    )


def build_field_edges(points: dict[str, object]) -> list[dict[str, object]]:
    return [
        {
            "edge_id": "north_edge",
            "description": "Start of furrows / irrigation head end",
            "geometry": LineString([points["field_nw"], points["field_ne"]]),
        },
        {
            "edge_id": "east_edge",
            "description": "East field boundary",
            "geometry": LineString([points["field_ne"], points["field_se"]]),
        },
        {
            "edge_id": "south_edge",
            "description": "End of furrows",
            "geometry": LineString([points["field_se"], points["field_sw"]]),
        },
        {
            "edge_id": "west_edge",
            "description": "West field boundary",
            "geometry": LineString([points["field_sw"], points["field_nw"]]),
        },
    ]

def interpolate_point(a: Point, b: Point, fraction: float) -> Point:
    """Return point a + fraction * (b - a)."""
    return Point(
        a.x + (b.x - a.x) * fraction,
        a.y + (b.y - a.y) * fraction,
    )


def build_strip_polygons(points: dict[str, Point]) -> list[dict[str, object]]:
    """
    Build S1-S4 polygons by dividing the north and south field edges
    into four equal-width strips.

    Assumes:
        west-to-east order: S1, S2, S3, S4
        north edge: field_nw -> field_ne
        south edge: field_sw -> field_se
    """
    north_points = [
        interpolate_point(points["field_nw"], points["field_ne"], i / 4.0)
        for i in range(5)
    ]
    south_points = [
        interpolate_point(points["field_sw"], points["field_se"], i / 4.0)
        for i in range(5)
    ]

    strips = []
    for idx, strip in enumerate(["S1", "S2", "S3", "S4"]):
        polygon = Polygon(
            [
                north_points[idx],
                north_points[idx + 1],
                south_points[idx + 1],
                south_points[idx],
                north_points[idx],
            ]
        )

        strips.append(
            {
                "strip": strip,
                **STRIP_METADATA[strip],
                "geometry": polygon,
            }
        )

    return strips


def build_strip_centerlines(points: dict[str, Point]) -> list[dict[str, object]]:
    """
    Build strip centerlines from midpoint of each strip on the north edge
    to midpoint of each strip on the south edge.
    """
    center_fractions = [0.125, 0.375, 0.625, 0.875]

    centerlines = []
    for strip, fraction in zip(["S1", "S2", "S3", "S4"], center_fractions):
        north_center = interpolate_point(
            points["field_nw"],
            points["field_ne"],
            fraction,
        )
        south_center = interpolate_point(
            points["field_sw"],
            points["field_se"],
            fraction,
        )

        centerlines.append(
            {
                "strip": strip,
                **STRIP_METADATA[strip],
                "geometry": LineString([north_center, south_center]),
            }
        )

    return centerlines

def write_layers(control_points: gpd.GeoDataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Work in projected coordinates for area/length.
    control_points_m = control_points.to_crs(WORKING_CRS)
    points = get_named_points(control_points_m)

    boundary = build_field_boundary(points)

    field_boundary = gpd.GeoDataFrame(
        [
            {
                "field_id": "Fruita2026",
                "site": "CSU Fruita",
                "description": "Biochar irrigation experiment field boundary",
                "area_m2": boundary.area,
                "area_sqft": boundary.area * 10.76391041671,
                "geometry": boundary,
            }
        ],
        crs=WORKING_CRS,
    )

    field_edges = gpd.GeoDataFrame(
        build_field_edges(points),
        crs=WORKING_CRS,
    )
    field_edges["length_m"] = field_edges.geometry.length
    field_edges["length_ft"] = field_edges["length_m"] * 3.280839895

    strip_polygons = gpd.GeoDataFrame(
        build_strip_polygons(points),
        crs=WORKING_CRS,
    )
    strip_polygons["area_m2"] = strip_polygons.geometry.area
    strip_polygons["area_sqft"] = strip_polygons["area_m2"] * 10.76391041671

    strip_centerlines = gpd.GeoDataFrame(
        build_strip_centerlines(points),
        crs=WORKING_CRS,
    )
    strip_centerlines["length_m"] = strip_centerlines.geometry.length
    strip_centerlines["length_ft"] = strip_centerlines["length_m"] * 3.280839895
    # Save the original control points too, but projected to the working CRS.
    control_points_out = control_points_m.copy()

    # Remove existing output if present so layers are cleanly rebuilt.
    if OUTPUT_GPKG.exists():
        OUTPUT_GPKG.unlink()

    field_boundary.to_file(OUTPUT_GPKG, layer="field_boundary", driver="GPKG")
    field_edges.to_file(OUTPUT_GPKG, layer="field_edges", driver="GPKG")
    control_points_out.to_file(OUTPUT_GPKG, layer="control_points", driver="GPKG")
    strip_polygons.to_file(OUTPUT_GPKG, layer="strip_polygons", driver="GPKG")
    strip_centerlines.to_file(OUTPUT_GPKG, layer="strip_centerlines", driver="GPKG")

    print(f"Wrote: {OUTPUT_GPKG}")
    print()
    print("Field boundary:")
    print(f"  area_m2   = {field_boundary.iloc[0]['area_m2']:.1f}")
    print(f"  area_sqft = {field_boundary.iloc[0]['area_sqft']:.1f}")
    print()
    print("Field edges:")
    print(field_edges[["edge_id", "length_ft"]].to_string(index=False))
    print()
    print("Strip polygons:")
    print(strip_polygons[["strip", "area_sqft"]].to_string(index=False))

    print()
    print("Strip centerlines:")
    print(strip_centerlines[["strip", "length_ft"]].to_string(index=False))


def main() -> None:
    control_points = load_control_points(INPUT_GEOJSON)
    write_layers(control_points)


if __name__ == "__main__":
    main()