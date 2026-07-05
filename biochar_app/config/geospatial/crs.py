"""
Coordinate Reference Systems (CRSs) used by the Biochar Water Conservation
project.

This module stores the Coordinate Reference Systems approved for use within
a project. Only EPSG identifiers are stored here; the complete definitions
are maintained by the official EPSG Geodetic Parameter Dataset and accessed
through PROJ (used by GDAL, Rasterio, GeoPandas, PyProj, and QGIS).

New CRSs should be added only when required by a new geospatial dataset or
analysis workflow. Reuse an existing CRS whenever practical.

Project-specific metadata documents why each CRS is used, while the EPSG
identifier provides the authoritative technical definition.

Notes
-----
Horizontal coordinate units are defined by the CRS.

Vertical units are NOT part of the CRS and are documented separately with
each DEM product in lidar.py because different elevation datasets may use
different vertical units.
"""

# ---------------------------------------------------------------------------
# Official EPSG Coordinate Reference Systems
# ---------------------------------------------------------------------------

NAD83_HARN_UTM12N_CRS = "EPSG:3742"
WGS84_CRS = "EPSG:4326"

# Future examples
# NAD83_UTM12N_CRS = "EPSG:26912"
# WEB_MERCATOR_CRS = "EPSG:3857"


# ---------------------------------------------------------------------------
# Coordinate Reference Systems used by this project
# ---------------------------------------------------------------------------

CRS = {
    # -----------------------------------------------------------------------
    # Fruita Biochar Experiment
    #
    # Official designation:
    #     NAD83(HARN) / UTM Zone 12N
    #
    # Primary projected coordinate system for nearly all geospatial analyses.
    #
    # Used for:
    #   • Field layout GeoPackage
    #   • LiDAR point clouds
    #   • Digital Elevation Models (DEMs)
    #   • Hillshade generation
    #   • Contour generation
    #   • Logger-row transects
    #   • Distance and area calculations
    #   • Spatial overlays
    #
    # Horizontal units:
    #   meters
    #
    # Vertical units:
    #   Defined by the individual DEM product (see lidar.py)
    #
    "NAD83_HARN_UTM12N": NAD83_HARN_UTM12N_CRS,

    # -----------------------------------------------------------------------
    # World Geodetic System 1984
    #
    # Geographic latitude / longitude coordinates.
    #
    # Used for:
    #   • GPS coordinates
    #   • Google Earth
    #   • Web mapping services
    #   • Exchange with external mapping applications
    #
    # Horizontal units:
    #   decimal degrees
    #
    "WGS84": WGS84_CRS,
}