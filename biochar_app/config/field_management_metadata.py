"""
Field geometry assumptions used for irrigation water-holding estimates.

Geometry is based on 2026-05-31 geotagged field photos. Values are approximate
field estimates, not survey-grade measurements.
"""

from biochar_app.config.experiment_config import STRIPS

# Original estimates from M. Lobato.
# STRIP_WIDTH_FT = 46.0
# STRIP_LENGTH_FT = 280.0


# ---------------------------------------------------------------------
# Field geometry
# ---------------------------------------------------------------------

# GPS/photo-derived width estimates.
FIELD_WIDTH_NORTH_FT = 172.1
FIELD_WIDTH_SOUTH_FT = 167.0

# Average strip width used for storage calculations.
STRIP_WIDTH_FT = 42.4

# GPS/photo-derived strip-centerline lengths.
STRIP_LENGTHS_FT = {
    "S1": 346.6,
    "S2": 344.1,
    "S3": 340.9,
    "S4": 337.8,
}

# Backward-compatible average strip length.
STRIP_LENGTH_FT = sum(STRIP_LENGTHS_FT.values()) / len(STRIP_LENGTHS_FT)


# ---------------------------------------------------------------------
# Logger distances along irrigation-flow direction
# ---------------------------------------------------------------------

# Distances are measured along each strip/logger centerline:
# irrigation start -> top logger -> middle logger -> bottom logger -> field end.
LOGGER_ZONE_SEGMENTS_FT = {
    "S1": {
        "start_to_top": 51.7,
        "top_to_middle": 115.3,
        "middle_to_bottom": 115.3,
        "bottom_to_end": 64.3,
    },
    "S2": {
        "start_to_top": 50.9,
        "top_to_middle": 120.3,
        "middle_to_bottom": 114.3,
        "bottom_to_end": 58.6,
    },
    "S3": {
        "start_to_top": 52.2,
        "top_to_middle": 118.3,
        "middle_to_bottom": 119.3,
        "bottom_to_end": 51.1,
    },
    "S4": {
        "start_to_top": 54.5,
        "top_to_middle": 117.3,
        "middle_to_bottom": 117.3,
        "bottom_to_end": 48.7,
    },
}

LOGGER_POSITIONS_PER_STRIP = 3

# Backward-compatible average logger spacings.
LOGGER_DISTANCE_TOP_TO_MIDDLE_FT = sum(
    v["top_to_middle"] for v in LOGGER_ZONE_SEGMENTS_FT.values()
) / len(LOGGER_ZONE_SEGMENTS_FT)

LOGGER_DISTANCE_MIDDLE_TO_BOTTOM_FT = sum(
    v["middle_to_bottom"] for v in LOGGER_ZONE_SEGMENTS_FT.values()
) / len(LOGGER_ZONE_SEGMENTS_FT)

LOGGER_DISTANCE_TOP_TO_BOTTOM_FT = (
    LOGGER_DISTANCE_TOP_TO_MIDDLE_FT + LOGGER_DISTANCE_MIDDLE_TO_BOTTOM_FT
)


# ---------------------------------------------------------------------
# Water-volume conversion
# ---------------------------------------------------------------------

INCHES_WATER_TO_GALLONS_PER_SQFT = 0.623


# ---------------------------------------------------------------------
# Legacy equal-third profile geometry
# ---------------------------------------------------------------------

STRIP_AREA_SQFT = STRIP_WIDTH_FT * STRIP_LENGTH_FT
PROFILE_AREA_SQFT = STRIP_AREA_SQFT / LOGGER_POSITIONS_PER_STRIP
PROFILE_GALLONS_PER_INCH = PROFILE_AREA_SQFT * INCHES_WATER_TO_GALLONS_PER_SQFT


# ---------------------------------------------------------------------
# Influence-zone geometry for T/M/B loggers
# ---------------------------------------------------------------------


def build_zone_lengths_ft(strip: str) -> dict[str, float]:
    seg = LOGGER_ZONE_SEGMENTS_FT[strip]

    return {
        "T": seg["start_to_top"] + seg["top_to_middle"] / 2.0,
        "M": seg["top_to_middle"] / 2.0 + seg["middle_to_bottom"] / 2.0,
        "B": seg["middle_to_bottom"] / 2.0 + seg["bottom_to_end"],
    }


ZONE_LENGTHS_FT_BY_STRIP = {
    strip: build_zone_lengths_ft(strip)
    for strip in STRIPS
}

ZONE_AREAS_SQFT_BY_STRIP = {
    strip: {
        zone: STRIP_WIDTH_FT * zone_length_ft
        for zone, zone_length_ft in zone_lengths.items()
    }
    for strip, zone_lengths in ZONE_LENGTHS_FT_BY_STRIP.items()
}

ZONE_GALLONS_PER_INCH_BY_STRIP = {
    strip: {
        zone: zone_area_sqft * INCHES_WATER_TO_GALLONS_PER_SQFT
        for zone, zone_area_sqft in zone_areas.items()
    }
    for strip, zone_areas in ZONE_AREAS_SQFT_BY_STRIP.items()
}


# ---------------------------------------------------------------------
# Combined strip geometry dictionary
# ---------------------------------------------------------------------

STRIP_GEOMETRY = {
    strip: {
        "strip_width_ft": STRIP_WIDTH_FT,
        "strip_length_ft": STRIP_LENGTHS_FT.get(strip, STRIP_LENGTH_FT),
        "strip_area_sqft": STRIP_WIDTH_FT * STRIP_LENGTHS_FT.get(strip, STRIP_LENGTH_FT),
        "logger_positions_per_strip": LOGGER_POSITIONS_PER_STRIP,

        # Legacy equal-third profile values.
        "profile_area_sqft": (
            STRIP_WIDTH_FT * STRIP_LENGTHS_FT.get(strip, STRIP_LENGTH_FT)
        ) / LOGGER_POSITIONS_PER_STRIP,
        "profile_gallons_per_inch": (
            (
                STRIP_WIDTH_FT * STRIP_LENGTHS_FT.get(strip, STRIP_LENGTH_FT)
            ) / LOGGER_POSITIONS_PER_STRIP
        ) * INCHES_WATER_TO_GALLONS_PER_SQFT,

        # New measured influence-zone values.
        "logger_zone_segments_ft": LOGGER_ZONE_SEGMENTS_FT[strip],
        "zone_lengths_ft": ZONE_LENGTHS_FT_BY_STRIP[strip],
        "zone_areas_sqft": ZONE_AREAS_SQFT_BY_STRIP[strip],
        "zone_gallons_per_inch": ZONE_GALLONS_PER_INCH_BY_STRIP[strip],
    }
    for strip in STRIPS
}