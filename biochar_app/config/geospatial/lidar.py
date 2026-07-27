from __future__ import annotations

from biochar_app.config.paths import (
    FIELD_LAYOUT_DIR,
    LIDAR_ANALYSIS_DIR,
    LIDAR_CLIPPED_DIR,
    LIDAR_DIR,
    LIDAR_PIPELINES_DIR,
)

from biochar_app.config.geospatial.crs import CRS

FIELD_LAYOUT_GPKG = FIELD_LAYOUT_DIR / "Fruita_Biochar_Field_Layout.gpkg"

GROUND_CLASSIFICATION = 2
SOURCE_TILE = "290235"

SOURCE_LAS = (
    LIDAR_DIR
    / "MesaCounty_2016_LiDAR"
    / f"{SOURCE_TILE}.las"
)

DEM_PRODUCTS = {
    "mesa_2016_2ft_min": {
        "label": "Mesa County 2016 LiDAR 2-ft minimum DEM",
        "lidar_year": 2016,
        "dem_path": LIDAR_CLIPPED_DIR / "Fruita_Field_DEM_2016_2ft_clip_m.tif",
        "resolution_ft": 2.0,
        "vertical_units": "US survey feet",
        "source_las": SOURCE_LAS,
        "source_tile": SOURCE_TILE,
        "ground_classification": GROUND_CLASSIFICATION,
        "dem_output_type": "min",
        "crs": CRS["NAD83_HARN_UTM12N"],
        "source_description": (
            "Mesa County 2016 LiDAR tile 290235; ground-classified "
            "points rasterized with PDAL writers.gdal output_type='min'."
        ),
    },
    "mesa_2016_0p25ft_sparse": {
        "label": "Mesa County 2016 0.25-ft sparse DEM test",
        "lidar_year": 2016,
        "dem_path": LIDAR_CLIPPED_DIR / "Fruita_Field_DEM_2016_0p25ft_clip_m.tif",
        "resolution_ft": 0.25,
        "vertical_units": "US survey feet",
        "source_las": None,
        "source_tile": SOURCE_TILE,
        "ground_classification": None,
        "crs": CRS["NAD83_HARN_UTM12N"],
        "dem_output_type": "unknown",
        "source_description": (
            "High-resolution 0.25-ft raster test. Sparse valid pixels; "
            "not recommended as the primary analysis DEM until provenance "
            "and processing are confirmed."
        ),
    },
}

DEFAULT_LIDAR_PRODUCT_KEY = "mesa_2016_2ft_min"


def get_lidar_product(product_key: str | None = None) -> dict:
    key = product_key or DEFAULT_LIDAR_PRODUCT_KEY

    if key not in DEM_PRODUCTS:
        valid = ", ".join(sorted(DEM_PRODUCTS))
        raise KeyError(f"Unknown LiDAR product: {key}. Valid products: {valid}")

    product = dict(DEM_PRODUCTS[key])
    product["product_key"] = key
    product["analysis_dir"] = LIDAR_ANALYSIS_DIR / key
    product["figure_dir"] = product["analysis_dir"] / "figures"

    return product