from biochar_app.config.experiment_config import (
    DATALOGGER_NAMES,
    LOGGER_LOCATIONS,
    LOGGER_LOCATION_MAPPING,
)

from biochar_app.config.geospatial.lidar import (
    FIELD_LAYOUT_GPKG,
    get_lidar_product,
)

import matplotlib.pyplot as plt
from pathlib import Path
from datetime import date
import geopandas as gpd
import pandas as pd
import rasterio
from shapely.geometry import LineString, Point

PRODUCT_KEY = "mesa_2016_2ft_min"
PRODUCT = get_lidar_product(PRODUCT_KEY)

LIDAR_YEAR = PRODUCT["lidar_year"]
DEM_RESOLUTION_FT = PRODUCT["resolution_ft"]
DEM_VERTICAL_UNITS = PRODUCT["vertical_units"]
WORKING_CRS = PRODUCT["crs"]
DEM_PATH = PRODUCT["dem_path"]

OUTPUT_DIR = PRODUCT["analysis_dir"]
FIGURE_DIR = PRODUCT["figure_dir"]

PROFILE_CSV = OUTPUT_DIR / "logger_row_elevation_profiles.csv"
SUMMARY_CSV = OUTPUT_DIR / "logger_row_elevation_summary.csv"
TRANSECTS_GPKG = OUTPUT_DIR / "logger_row_transects.gpkg"
REPORT_MD = OUTPUT_DIR / "topography_report.md"

RELATIVE_PROFILE_FIG = FIGURE_DIR / "logger_row_profiles_relative.png"
ABSOLUTE_PROFILE_FIG = FIGURE_DIR / "logger_row_profiles_absolute.png"

def build_logger_rows() -> dict[str, list[str]]:
    """
    Return datalogger IDs grouped by logger row.
    Returns
    -------
    {
        "top":    ["S1T", "S2T", "S3T", "S4T"],
        "middle": ["S1M", "S2M", "S3M", "S4M"],
        "bottom": ["S1B", "S2B", "S3B", "S4B"],
    }
    """
    logger_rows = {}

    for location in LOGGER_LOCATIONS:
        key = LOGGER_LOCATION_MAPPING[location].lower()
        logger_rows[key] = [
            name for name in DATALOGGER_NAMES if name.endswith(location)
        ]

    return logger_rows

def load_layers() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    control_points = gpd.read_file(FIELD_LAYOUT_GPKG, layer="control_points").to_crs(WORKING_CRS)
    field_boundary = gpd.read_file(FIELD_LAYOUT_GPKG, layer="field_boundary").to_crs(WORKING_CRS)
    return control_points, field_boundary


def get_row_y(control_points: gpd.GeoDataFrame, feature_ids: list[str]) -> float:
    matches = control_points[control_points["feature_id"].isin(feature_ids)]
    missing = sorted(set(feature_ids) - set(matches["feature_id"]))
    if missing:
        raise ValueError(f"Missing logger control points: {missing}")
    return float(matches.geometry.y.mean())


def build_horizontal_transect(boundary_geom, y: float) -> LineString:
    minx, _, maxx, _ = boundary_geom.bounds
    line = LineString([(minx - 10, y), (maxx + 10, y)])
    intersection = boundary_geom.intersection(line)

    if intersection.geom_type == "LineString":
        return intersection
    if intersection.geom_type == "MultiLineString":
        return max(intersection.geoms, key=lambda g: g.length)

    raise ValueError(f"Unexpected transect geometry: {intersection.geom_type}")


def sample_line(line: LineString, raster_path: Path, spacing_m: float) -> list[dict]:
    records = []

    with rasterio.open(raster_path) as src:
        nodata = src.nodata
        n_steps = int(line.length // spacing_m)

        for i in range(n_steps + 1):
            distance_m = min(i * spacing_m, line.length)
            point = line.interpolate(distance_m)
            value = next(src.sample([(point.x, point.y)]))[0]

            records.append(
                {
                    "distance_m": distance_m,
                    "distance_ft": distance_m * 3.280839895,
                    "x": point.x,
                    "y": point.y,
                    "elevation_ft": None if value == nodata else float(value),
                    "geometry": Point(point.x, point.y),
                }
            )

    return records


def build_profiles() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    control_points, field_boundary = load_layers()
    boundary_geom = field_boundary.geometry.iloc[0]
    spacing_m = DEM_RESOLUTION_FT * 0.3048006096012192

    profile_records = []
    transect_records = []

    logger_rows = build_logger_rows()

    for row_name, feature_ids in logger_rows.items():
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

        for rec in sample_line(transect, DEM_PATH, spacing_m):
            rec.update(
                {
                    "transect_id": f"{row_name}_logger_row",
                    "logger_row": row_name,
                    "lidar_year": LIDAR_YEAR,
                    "dem_resolution_ft": DEM_RESOLUTION_FT,
                    "vertical_units": DEM_VERTICAL_UNITS,
                }
            )
            profile_records.append(rec)

    profiles_gdf = gpd.GeoDataFrame(profile_records, geometry="geometry", crs=WORKING_CRS)
    transects_gdf = gpd.GeoDataFrame(transect_records, geometry="geometry", crs=WORKING_CRS)

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
            std_elevation_ft=("elevation_ft", "std"),
            elevation_range_ft=("elevation_ft", lambda s: s.max() - s.min()),
        )
        .reset_index()
    )
    summary_df["elevation_range_in"] = summary_df["elevation_range_ft"] * 12

    return profiles_gdf, transects_gdf, summary_df


def write_outputs(
    profiles_gdf: gpd.GeoDataFrame,
    transects_gdf: gpd.GeoDataFrame,
    summary_df: pd.DataFrame,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(profiles_gdf.drop(columns="geometry")).to_csv(PROFILE_CSV, index=False)
    summary_df.to_csv(SUMMARY_CSV, index=False)

    if TRANSECTS_GPKG.exists():
        TRANSECTS_GPKG.unlink()

    transects_gdf.to_file(TRANSECTS_GPKG, layer="logger_row_transects", driver="GPKG")
    profiles_gdf.to_file(TRANSECTS_GPKG, layer="profile_sample_points", driver="GPKG")


def get_logger_distance_positions(
    transects_gdf: gpd.GeoDataFrame,
) -> dict[str, float]:
    control_points, _ = load_layers()
    logger_rows = build_logger_rows()

    positions: dict[str, list[float]] = {}

    for row_name, feature_ids in logger_rows.items():
        transect = transects_gdf.loc[
            transects_gdf["logger_row"] == row_name, "geometry"
        ].iloc[0]

        for feature_id in feature_ids:
            point = control_points.loc[
                control_points["feature_id"] == feature_id, "geometry"
            ].iloc[0]

            strip = feature_id[:2]
            positions.setdefault(strip, []).append(
                transect.project(point) * 3.280839895
            )

    return {
        strip: sum(values) / len(values)
        for strip, values in sorted(positions.items())
    }


def add_logger_position_lines(
    ax,
    transects_gdf: gpd.GeoDataFrame,
) -> None:
    logger_positions = get_logger_distance_positions(transects_gdf)

    for strip, distance_ft in logger_positions.items():
        ax.axvline(
            distance_ft,
            linestyle=(0, (2, 3)),
            linewidth=0.9,
            alpha=0.8,
        )
        ax.text(
            distance_ft,
            0.985,
            strip,
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="top",
            fontsize=10,
            fontweight="bold",
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.7, pad=1.5),
        )


def make_relative_profile_plot(
    profiles_gdf: gpd.GeoDataFrame,
    transects_gdf: gpd.GeoDataFrame,
) -> None:
    df = pd.DataFrame(profiles_gdf.drop(columns="geometry")).dropna(
        subset=["elevation_ft"]
    )

    fig, ax = plt.subplots(figsize=(10.5, 6))

    for row_name in ["top", "middle", "bottom"]:
        row = df[df["logger_row"] == row_name].copy()
        reference_ft = row.iloc[0]["elevation_ft"]
        row["relative_elevation_in"] = (
            row["elevation_ft"] - reference_ft
        ) * 12.0

        row_range_in = (
            row["relative_elevation_in"].max()
            - row["relative_elevation_in"].min()
        )

        ax.plot(
            row["distance_ft"],
            row["relative_elevation_in"],
            linewidth=1.8,
            label=f"{row_name.title()} row; range = {row_range_in:.1f} in",
        )

    add_logger_position_lines(ax, transects_gdf)

    ax.axhline(0, linestyle="--", linewidth=1.0, alpha=0.8)

    ax.set_title(
        "Cross-field Elevation Change Along Logger Rows\n"
        "Each row starts at 0 inches at the west edge",
        pad=18,
    )
    ax.set_xlabel("Distance across field from west edge (ft)")
    ax.set_ylabel("Elevation change from west edge (inches)")

    ax.minorticks_on()
    ax.grid(True, which="major", alpha=0.25, linewidth=0.7)
    ax.grid(True, which="minor", alpha=0.12, linewidth=0.5)

    ax.legend(title="Logger row", loc="best", fontsize=8)

    fig.tight_layout(rect=[0.03, 0, 1, 0.94])
    fig.savefig(RELATIVE_PROFILE_FIG, dpi=200)
    plt.close(fig)


def make_absolute_profile_plot(
    profiles_gdf: gpd.GeoDataFrame,
    transects_gdf: gpd.GeoDataFrame,
) -> None:
    df = pd.DataFrame(profiles_gdf.drop(columns="geometry")).dropna(
        subset=["elevation_ft"]
    )

    fig, ax = plt.subplots(figsize=(10.5, 6))

    for row_name in ["top", "middle", "bottom"]:
        row = df[df["logger_row"] == row_name]
        ax.plot(
            row["distance_ft"],
            row["elevation_ft"],
            linewidth=1.8,
            label=f"{row_name.title()} logger row",
        )

    add_logger_position_lines(ax, transects_gdf)

    ax.set_title(
        "Absolute Logger-Row Elevation Profiles from 2016 LiDAR",
        pad=18,
    )
    ax.set_xlabel("Distance across field from west edge (ft)")
    ax.set_ylabel("Elevation (ft)")

    ax.minorticks_on()
    ax.grid(True, which="major", alpha=0.25, linewidth=0.7)
    ax.grid(True, which="minor", alpha=0.12, linewidth=0.5)

    ax.legend(title="Logger row", loc="best", fontsize=8)

    fig.tight_layout(rect=[0.03, 0, 1, 0.94])
    fig.savefig(ABSOLUTE_PROFILE_FIG, dpi=200)
    plt.close(fig)

def write_report(summary_df: pd.DataFrame) -> None:
    total_min = summary_df["min_elevation_ft"].min()
    total_max = summary_df["max_elevation_ft"].max()
    total_range = total_max - total_min

    lines = [
        "# Fruita Field Topography Analysis",
        "",
        f"Generated: {date.today().isoformat()}",
        "",
        "## Data source",
        "",
        f"- LiDAR year: {LIDAR_YEAR}",
        f"- DEM: `{DEM_PATH.name}`",
        f"- Horizontal resolution: {DEM_RESOLUTION_FT} ft",
        f"- Vertical units: {DEM_VERTICAL_UNITS}",
        "- Ground source: 2016 LiDAR-derived ground DEM",
        "",
        "## Field elevation summary",
        "",
        f"- Minimum logger-row elevation sampled: {total_min:.2f} ft",
        f"- Maximum logger-row elevation sampled: {total_max:.2f} ft",
        f"- Logger-row sampled relief: {total_range:.2f} ft ({total_range * 12:.1f} in)",
        "",
        "## Logger-row transects",
        "",
        "| Logger row | Samples | Length (ft) | Min (ft) | Max (ft) | Range (ft) | Range (in) | Std dev (ft) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for _, row in summary_df.sort_values("logger_row").iterrows():
        lines.append(
            f"| {row['logger_row']} | "
            f"{int(row['n_samples'])} | "
            f"{row['transect_length_ft']:.1f} | "
            f"{row['min_elevation_ft']:.2f} | "
            f"{row['max_elevation_ft']:.2f} | "
            f"{row['elevation_range_ft']:.2f} | "
            f"{row['elevation_range_in']:.1f} | "
            f"{row['std_elevation_ft']:.2f} |"
        )

    lines += [
        "",
        "## Interpretation notes",
        "",
        "- Cross-field elevation variation is modest at the logger rows.",
        "- The bottom row has the largest cross-field range, roughly one foot.",
        "- The full field relief is larger than the logger-row cross-sections, suggesting important north-south elevation change or localized high/low areas.",
        "- A north-south linear feature is visible in the hillshade and should be investigated as possible trenching, ditching, or field infrastructure.",
        "",
        "## Vertical accuracy note",
        "",
        "The DEM stores elevation values as floating-point feet, but this should not be interpreted as sub-inch vertical accuracy. "
        "LiDAR vertical accuracy is controlled by the source survey and processing workflow. Small pixel-to-pixel differences should be interpreted cautiously.",
        "",
        f"Relative profile figure: `{RELATIVE_PROFILE_FIG.relative_to(OUTPUT_DIR)}`",
        f"Absolute profile figure: `{ABSOLUTE_PROFILE_FIG.relative_to(OUTPUT_DIR)}`",
        "",
    ]

    REPORT_MD.write_text("\n".join(lines))


def main() -> None:
    profiles_gdf, transects_gdf, summary_df = build_profiles()
    write_outputs(profiles_gdf, transects_gdf, summary_df)
    make_relative_profile_plot(profiles_gdf, transects_gdf)
    make_absolute_profile_plot(profiles_gdf, transects_gdf)
    write_report(summary_df)

    print(f"Wrote: {RELATIVE_PROFILE_FIG}")
    print(f"Wrote: {ABSOLUTE_PROFILE_FIG}")
    print(f"Wrote: {SUMMARY_CSV}")
    print(f"Wrote: {TRANSECTS_GPKG}")
    print(f"Wrote: {REPORT_MD}")
    print()
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()