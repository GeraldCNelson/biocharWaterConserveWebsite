"""
Build core Fruita biochar field geometry from geotagged field photo locations.

Inputs:
    biochar_app/data-processed/field-mapping/field_photo_locations.geojson

Outputs:
    biochar_app/geospatial/field_layout/Fruita_Biochar_Field_Layout.gpkg
    biochar_app/geospatial/field_layout/logger_influence_zone_areas.csv
    biochar_app/geospatial/field_layout/fruita_field_layout.png
    biochar_app/static/images/experiment_design/field_layout.webp

Requires:
    geopandas
    shapely
    pyproj
"""

from __future__ import annotations

import os
from pathlib import Path
import tempfile

import geopandas as gpd

_MATPLOTLIB_CONFIG_DIR = Path(tempfile.gettempdir()) / "biochar-matplotlib"
_MATPLOTLIB_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MATPLOTLIB_CONFIG_DIR))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from PIL import Image
from shapely import voronoi_polygons
from shapely.geometry import LineString, Point, Polygon
from shapely.geometry import MultiPoint


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_GEOJSON = (
    PROJECT_ROOT
    / "geospatial"
    / "field_layout"
    / "field_photo_locations.geojson"
)

OUTPUT_DIR = PROJECT_ROOT / "geospatial" / "field_layout"
OUTPUT_GPKG = OUTPUT_DIR / "Fruita_Biochar_Field_Layout.gpkg"
OUTPUT_ZONE_AREAS_CSV = OUTPUT_DIR / "logger_influence_zone_areas.csv"
OUTPUT_LAYOUT_PNG = OUTPUT_DIR / "fruita_field_layout.png"
OUTPUT_LAYOUT_WEBP = (
    PROJECT_ROOT / "static" / "images" / "experiment_design" / "field_layout.webp"
)

SOURCE_CRS = "EPSG:4326"
WORKING_CRS = "EPSG:3742"  # NAD83(HARN) / UTM zone 12N, meters


REQUIRED_FEATURES = {
    "field_nw",
    "field_ne",
    "field_se",
    "field_sw",
}
LOGGER_FEATURES = tuple(
    f"S{strip_number}{position}"
    for strip_number in range(1, 5)
    for position in ("T", "M", "B")
)

STRIP_METADATA = {
    "S1": {"treatment": "Biochar", "irrigation": "Monthly", "biochar": True,  "strip_order": 1},
    "S2": {"treatment": "Control", "irrigation": "Monthly", "biochar": False, "strip_order": 2},
    "S3": {"treatment": "Biochar", "irrigation": "Biweekly", "biochar": True,  "strip_order": 3},
    "S4": {"treatment": "Control", "irrigation": "Biweekly", "biochar": False, "strip_order": 4},
}
STRIP_COLORS = {
    "S1": "#0b84c6",
    "S2": "#e9a400",
    "S3": "#00a67d",
    "S4": "#d96600",
}
STRIP_HEADINGS = {
    "S1": "Half Water\nBiochar\n(BC50)",
    "S2": "Half Water\nControl\n(CON50)",
    "S3": "Full Water\nBiochar\n(BC100)",
    "S4": "Full Water\nControl\n(CON100)",
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


def build_logger_influence_zones(
    control_points: gpd.GeoDataFrame,
    field_boundary: Polygon,
) -> gpd.GeoDataFrame:
    """Build one nearest-logger (Voronoi) zone per field logger.

    Cells are calculated in the projected working CRS and clipped to the
    photographed field boundary. The result uses the measured logger and
    corner coordinates rather than assuming equal strip widths or rectangular
    logger zones.
    """
    logger_rows = []
    logger_points = []
    for feature_id in LOGGER_FEATURES:
        matches = control_points.loc[control_points["feature_id"].eq(feature_id)]
        if len(matches) != 1:
            raise ValueError(
                f"Expected exactly one logger feature {feature_id!r}; found {len(matches)}"
            )
        logger_rows.append(feature_id)
        logger_points.append(matches.iloc[0].geometry)

    cells = voronoi_polygons(
        MultiPoint(logger_points),
        extend_to=field_boundary,
        ordered=True,
    )
    records = []
    for feature_id, cell in zip(logger_rows, cells.geoms, strict=True):
        geometry = cell.intersection(field_boundary)
        records.append({
            "feature_id": feature_id,
            "strip": feature_id[:2],
            "logger_position": feature_id[-1],
            "geometry_method": "projected_voronoi_clipped_to_field_boundary",
            "source_crs": SOURCE_CRS,
            "working_crs": WORKING_CRS,
            "geometry": geometry,
        })

    zones = gpd.GeoDataFrame(records, crs=WORKING_CRS)
    zones["area_m2"] = zones.geometry.area
    zones["area_sqft"] = zones["area_m2"] * 10.76391041671
    zones["gallons_per_water_inch"] = zones["area_sqft"] * 0.623

    if len(zones) != len(LOGGER_FEATURES) or zones.geometry.is_empty.any():
        raise ValueError("Logger influence-zone construction was incomplete.")
    relative_area_error = abs(zones.geometry.area.sum() - field_boundary.area) / field_boundary.area
    if relative_area_error > 1e-8:
        raise ValueError(
            "Logger influence zones do not partition the field boundary; "
            f"relative area error={relative_area_error:.3g}"
        )
    return zones


def render_field_layout(
    control_points: gpd.GeoDataFrame,
    field_boundary: gpd.GeoDataFrame,
    strip_polygons: gpd.GeoDataFrame,
    strip_centerlines: gpd.GeoDataFrame,
    *,
    png_path: Path = OUTPUT_LAYOUT_PNG,
    webp_path: Path = OUTPUT_LAYOUT_WEBP,
) -> None:
    """Render the public field-layout diagram from projected control points."""
    logger_points = control_points.loc[
        control_points["feature_id"].isin(LOGGER_FEATURES)
    ].copy()
    corners = control_points.loc[
        control_points["feature_id"].isin(REQUIRED_FEATURES)
    ].copy()
    infrastructure = control_points.loc[
        control_points["description"].fillna("").str.contains(
            "flow meter", case=False
        )
    ].copy()

    fig, ax = plt.subplots(figsize=(7.5, 12))
    field_boundary.boundary.plot(ax=ax, color="black", linewidth=1.4, zorder=1)
    strip_polygons.boundary.plot(ax=ax, color="0.55", linewidth=1.0, zorder=1)

    # Draw the dashed guides through the measured logger locations. The
    # geometric strip centerlines can be noticeably offset from the installed
    # logger transects, especially in S2, and are misleading on a location map.
    boundary_geometry = field_boundary.iloc[0].geometry

    def extend_to_margin(origin: Point, adjacent: Point) -> Point:
        """Continue a logger segment from origin to the field boundary."""
        dx = origin.x - adjacent.x
        dy = origin.y - adjacent.y
        distance = (dx * dx + dy * dy) ** 0.5
        if distance == 0:
            return origin
        far_point = Point(
            origin.x + 1000.0 * dx / distance,
            origin.y + 1000.0 * dy / distance,
        )
        hits = LineString([origin, far_point]).intersection(
            boundary_geometry.boundary
        )
        if hits.is_empty:
            return origin
        candidates = list(hits.geoms) if hasattr(hits, "geoms") else [hits]
        points = [geometry for geometry in candidates if isinstance(geometry, Point)]
        return max(points, key=origin.distance) if points else origin

    logger_transects = []
    for strip in ("S1", "S2", "S3", "S4"):
        installed = logger_points.loc[
            logger_points["feature_id"].isin(
                [f"{strip}T", f"{strip}M", f"{strip}B"]
            )
        ].set_index("feature_id")
        top = installed.loc[f"{strip}T"].geometry
        middle = installed.loc[f"{strip}M"].geometry
        bottom = installed.loc[f"{strip}B"].geometry
        logger_transects.append(
            LineString(
                [
                    extend_to_margin(top, middle),
                    top,
                    middle,
                    bottom,
                    extend_to_margin(bottom, middle),
                ]
            )
        )
    gpd.GeoSeries(logger_transects, crs=control_points.crs).plot(
        ax=ax, color="0.25", linewidth=1.0, linestyle="--", zorder=1
    )

    bounds = field_boundary.total_bounds
    x_min, y_min, x_max, y_max = bounds
    field_height = y_max - y_min
    field_width = x_max - x_min

    for strip in ("S1", "S2", "S3", "S4"):
        subset = logger_points.loc[
            logger_points["feature_id"].astype(str).str.startswith(strip)
        ].copy()
        color = STRIP_COLORS[strip]
        ax.scatter(
            subset.geometry.x,
            subset.geometry.y,
            s=105,
            color=color,
            edgecolor="white",
            linewidth=0.7,
            zorder=4,
        )
        centerline = strip_centerlines.loc[strip_centerlines["strip"].eq(strip)]
        center_x = centerline.iloc[0].geometry.centroid.x
        ax.text(
            center_x,
            y_max - 0.055 * field_height,
            STRIP_HEADINGS[strip],
            ha="center",
            va="top",
            fontsize=10,
            fontweight="bold",
            color=color,
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 1.5},
        )
        for row in subset.itertuples():
            ax.annotate(
                row.feature_id,
                (row.geometry.x, row.geometry.y),
                xytext=(0, 11),
                textcoords="offset points",
                ha="center",
                fontsize=8,
                fontweight="bold",
                zorder=5,
                bbox={"facecolor": "white", "edgecolor": "none", "pad": 0.8},
            )

        line = centerline.iloc[0].geometry
        bottom_logger = subset.loc[subset["feature_id"].eq(f"{strip}B")].iloc[0]
        south_endpoint = Point(line.coords[-1])
        bottom_to_end_ft = bottom_logger.geometry.distance(south_endpoint) * 3.280839895
        ax.text(
            center_x - 0.045 * field_width,
            y_min + 0.085 * field_height,
            f"{bottom_to_end_ft:.0f} ft",
            ha="center",
            va="center",
            fontsize=8,
            color="0.25",
        )

    ax.scatter(
        corners.geometry.x,
        corners.geometry.y,
        marker="D",
        s=65,
        color="black",
        zorder=6,
    )
    corner_offsets = {
        "field_nw": (7, -7, "left", "top"),
        "field_ne": (-7, -7, "right", "top"),
        "field_sw": (7, 7, "left", "bottom"),
        "field_se": (-7, 7, "right", "bottom"),
    }
    for row in corners.itertuples():
        dx, dy, align, vertical_align = corner_offsets[row.feature_id]
        label = str(row.description or row.feature_id).replace("\\n", "\n")
        ax.annotate(
            label,
            (row.geometry.x, row.geometry.y),
            xytext=(dx, dy),
            textcoords="offset points",
            ha=align,
            va=vertical_align,
            fontsize=8,
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 1.0},
        )

    if not infrastructure.empty:
        ax.scatter(
            infrastructure.geometry.x,
            infrastructure.geometry.y,
            marker="s",
            s=55,
            color="black",
            zorder=6,
        )
        for row in infrastructure.itertuples():
            ax.annotate(
                str(row.description).replace("\\n", "\n"),
                (row.geometry.x, row.geometry.y),
                xytext=(8, 8),
                textcoords="offset points",
                ha="left",
                va="bottom",
                fontsize=8,
            )

    named_corners = {
        row.feature_id: row.geometry
        for row in corners.itertuples()
    }
    south_width_ft = (
        named_corners["field_sw"].distance(named_corners["field_se"])
        * 3.280839895
    )
    east_length_ft = (
        named_corners["field_ne"].distance(named_corners["field_se"])
        * 3.280839895
    )
    ax.text(
        (x_min + x_max) / 2,
        y_max - 0.025 * field_height,
        "Start of furrows",
        ha="center",
        va="top",
        fontsize=10,
        style="italic",
        color="0.25",
        bbox={"facecolor": "white", "edgecolor": "none", "pad": 1.5},
    )
    ax.text(
        (x_min + x_max) / 2,
        y_min + 0.035 * field_height,
        f"End of furrows\n{south_width_ft:.0f} ft",
        ha="center",
        va="bottom",
        fontsize=10,
        style="italic",
        color="0.25",
        bbox={"facecolor": "white", "edgecolor": "none", "pad": 1.5},
    )
    ax.text(
        x_max + 0.015 * field_width,
        (y_min + y_max) / 2,
        f"{east_length_ft:.0f} ft",
        ha="left",
        va="center",
        fontsize=8,
        color="0.25",
    )
    ax.annotate(
        "",
        xy=(x_max - 0.025 * field_width, y_min + 0.12 * field_height),
        xytext=(x_max - 0.025 * field_width, y_min + 0.04 * field_height),
        arrowprops={"arrowstyle": "-|>", "lw": 1.5},
    )
    ax.text(
        x_max - 0.025 * field_width,
        y_min + 0.025 * field_height,
        "N",
        ha="center",
        va="top",
        fontsize=10,
        fontweight="bold",
    )

    ax.set_title("Field Locations", fontsize=18, fontweight="bold", pad=12)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xlim(x_min - 0.06 * field_width, x_max + 0.06 * field_width)
    ax.set_ylim(y_min - 0.04 * field_height, y_max + 0.04 * field_height)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlabel("")
    ax.set_ylabel("")
    fig.tight_layout()

    png_path.parent.mkdir(parents=True, exist_ok=True)
    webp_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(png_path, dpi=200, facecolor="white")
    plt.close(fig)
    with Image.open(png_path) as image:
        image.save(webp_path, "WEBP", quality=90, method=6)
    print(f"Wrote: {png_path}")
    print(f"Wrote: {webp_path}")

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
    logger_influence_zones = build_logger_influence_zones(
        control_points_m, boundary
    )
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
    logger_influence_zones.to_file(
        OUTPUT_GPKG, layer="logger_influence_zones", driver="GPKG"
    )
    logger_influence_zones.drop(columns="geometry").to_csv(
        OUTPUT_ZONE_AREAS_CSV, index=False
    )
    render_field_layout(
        control_points_out,
        field_boundary,
        strip_polygons,
        strip_centerlines,
    )

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
    print()
    print("Logger influence zones:")
    print(
        logger_influence_zones[
            ["feature_id", "strip", "logger_position", "area_sqft"]
        ].to_string(index=False)
    )
    print(f"Wrote: {OUTPUT_ZONE_AREAS_CSV}")


def main() -> None:
    control_points = load_control_points(INPUT_GEOJSON)
    write_layers(control_points)


if __name__ == "__main__":
    main()
