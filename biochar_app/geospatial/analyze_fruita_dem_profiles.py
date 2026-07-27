"""
Analyze cross-field elevation profiles across logger rows.

Samples the 2016 LiDAR-derived 2 ft DEM along east-west transects passing
through the top, middle, and bottom logger rows.

Inputs:
    biochar_app/geospatial/field_layout/Fruita_Biochar_Field_Layout.gpkg
    biochar_app/geospatial/lidar/clipped/Fruita_Field_DEM_2016_2ft_clip_m.tif

Outputs:
    biochar_app/geospatial/lidar/analysis/fruita_2016_lidar_logger_row_elevation_profiles.csv
    biochar_app/geospatial/lidar/analysis/fruita_2016_lidar_logger_row_elevation_summary.csv
    biochar_app/geospatial/lidar/analysis/fruita_2016_lidar_logger_row_transects.gpkg
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import rasterio
from shapely.geometry import LineString, Point


PROJECT_ROOT = Path(__file__).resolve().parents[1]

FIELD_LAYOUT_GPKG = (
    PROJECT_ROOT / "geospatial" / "field_layout" / "Fruita_Biochar_Field_Layout.gpkg"
)

DEM_PATH = (
    PROJECT_ROOT
    / "geospatial"
    / "lidar"
    / "clipped"
    / "Fruita_Field_DEM_2016_2ft_clip_m.tif"
)

OUTPUT_DIR = PROJECT_ROOT / "geospatial" / "lidar" / "analysis"

PROFILE_CSV = OUTPUT_DIR / "fruita_2016_lidar_logger_row_elevation_profiles.csv"
SUMMARY_CSV = OUTPUT_DIR / "fruita_2016_lidar_logger_row_elevation_summary.csv"
TRANSECTS_GPKG = OUTPUT_DIR / "fruita_2016_lidar_logger_row_transects.gpkg"

LIDAR_YEAR = 2016
DEM_RESOLUTION_FT = 2.0
DEM_VERTICAL_UNITS = "US survey feet"
WORKING_CRS = "EPSG:3742"

LOGGER_ROWS = {
    "top": ["S1T", "S2T", "S3T", "S4T"],
    "middle": ["S1M", "S2M", "S3M", "S4M"],
    "bottom": ["S1B", "S2B", "S3B", "S4B"],
}


def load_layers() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    control_points = gpd.read_file(FIELD_LAYOUT_GPKG, layer="control_points")
    field_boundary = gpd.read_file(FIELD_LAYOUT_GPKG, layer="field_boundary")

    if control_points.crs is None:
        raise ValueError("control_points layer has no CRS")

    if field_boundary.crs is None:
        raise ValueError("field_boundary layer has no CRS")

    control_points = control_points.to_crs(WORKING_CRS)
    field_boundary = field_boundary.to_crs(WORKING_CRS)

    return control_points, field_boundary


def get_row_y(control_points: gpd.GeoDataFrame, feature_ids: list[str]) -> float:
    matches = control_points[control_points["feature_id"].isin(feature_ids)].copy()

    missing = sorted(set(feature_ids) - set(matches["feature_id"]))
    if missing:
        raise ValueError(f"Missing logger control points: {missing}")

    return float(matches.geometry.y.mean())


def build_horizontal_transect(boundary_geom, y: float) -> LineString:
    minx, miny, maxx, maxy = boundary_geom.bounds
    padding = 10.0

    long_line = LineString(
        [
            (minx - padding, y),
            (maxx + padding, y),
        ]
    )

    intersection = boundary_geom.intersection(long_line)

    if intersection.is_empty:
        raise ValueError(f"Transect at y={y} does not intersect field boundary")

    if intersection.geom_type == "LineString":
        return intersection

    if intersection.geom_type == "MultiLineString":
        return max(intersection.geoms, key=lambda g: g.length)

    raise ValueError(f"Unexpected transect intersection type: {intersection.geom_type}")


def sample_line(line: LineString, raster_path: Path, sample_spacing_m: float) -> list[dict]:
    records = []

    with rasterio.open(raster_path) as src:
        nodata = src.nodata

        n_steps = int(line.length // sample_spacing_m)

        for i in range(n_steps + 1):
            distance_m = min(i * sample_spacing_m, line.length)
            point = line.interpolate(distance_m)

            value = next(src.sample([(point.x, point.y)]))[0]

            if nodata is not None and value == nodata:
                elevation_ft = None
            else:
                elevation_ft = float(value)

            records.append(
                {
                    "distance_m": distance_m,
                    "distance_ft": distance_m * 3.280839895,
                    "x": point.x,
                    "y": point.y,
                    "elevation_ft": elevation_ft,
                    "geometry": Point(point.x, point.y),
                }
            )

    return records


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    control_points, field_boundary = load_layers()
    boundary_geom = field_boundary.geometry.iloc[0]

    sample_spacing_m = DEM_RESOLUTION_FT * 0.3048006096012192

    all_profile_records = []
    transect_records = []

    for row_name, feature_ids in LOGGER_ROWS.items():
        y = get_row_y(control_points, feature_ids)
        transect = build_horizontal_transect(boundary_geom, y)

        transect_records.append(
            {
                "transect_id": f"{row_name}_logger_row",
                "logger_row": row_name,
                "lidar_year": LIDAR_YEAR,
                "dem_resolution_ft": DEM_RESOLUTION_FT,
                "vertical_units": DEM_VERTICAL_UNITS,
                "length_m": transect.length,
                "length_ft": transect.length * 3.280839895,
                "geometry": transect,
            }
        )

        sampled = sample_line(transect, DEM_PATH, sample_spacing_m)

        for rec in sampled:
            rec.update(
                {
                    "transect_id": f"{row_name}_logger_row",
                    "logger_row": row_name,
                    "lidar_year": LIDAR_YEAR,
                    "dem_resolution_ft": DEM_RESOLUTION_FT,
                    "vertical_units": DEM_VERTICAL_UNITS,
                }
            )
            all_profile_records.append(rec)

    profiles_gdf = gpd.GeoDataFrame(
        all_profile_records,
        geometry="geometry",
        crs=WORKING_CRS,
    )

    transects_gdf = gpd.GeoDataFrame(
        transect_records,
        geometry="geometry",
        crs=WORKING_CRS,
    )

    profile_df = pd.DataFrame(profiles_gdf.drop(columns="geometry"))

    summary_df = (
        profile_df.dropna(subset=["elevation_ft"])
        .groupby(["transect_id", "logger_row", "lidar_year", "dem_resolution_ft"])
        .agg(
            n_samples=("elevation_ft", "count"),
            transect_length_ft=("distance_ft", "max"),
            min_elevation_ft=("elevation_ft", "min"),
            max_elevation_ft=("elevation_ft", "max"),
            mean_elevation_ft=("elevation_ft", "mean"),
            elevation_range_ft=("elevation_ft", lambda s: s.max() - s.min()),
        )
        .reset_index()
    )

    summary_df["elevation_range_in"] = summary_df["elevation_range_ft"] * 12.0

    profile_df.to_csv(PROFILE_CSV, index=False)
    summary_df.to_csv(SUMMARY_CSV, index=False)

    if TRANSECTS_GPKG.exists():
        TRANSECTS_GPKG.unlink()

    transects_gdf.to_file(TRANSECTS_GPKG, layer="logger_row_transects", driver="GPKG")
    profiles_gdf.to_file(TRANSECTS_GPKG, layer="profile_sample_points", driver="GPKG")

    print(f"Wrote: {PROFILE_CSV}")
    print(f"Wrote: {SUMMARY_CSV}")
    print(f"Wrote: {TRANSECTS_GPKG}")
    print()
    print("Logger-row elevation summary:")
    print(
        summary_df[
            [
                "logger_row",
                "n_samples",
                "transect_length_ft",
                "min_elevation_ft",
                "max_elevation_ft",
                "elevation_range_ft",
                "elevation_range_in",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()