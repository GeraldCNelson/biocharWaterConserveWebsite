"""
Field geometry assumptions used for irrigation water-holding estimates.
Temporary first-pass values; revise when measured field dimensions are finalized.
"""

from biochar_app.config.core import STRIPS

STRIP_WIDTH_FT = 46.0
STRIP_LENGTH_FT = 280.0
LOGGER_POSITIONS_PER_STRIP = 3

INCHES_WATER_TO_GALLONS_PER_SQFT = 0.623

STRIP_AREA_SQFT = STRIP_WIDTH_FT * STRIP_LENGTH_FT
PROFILE_AREA_SQFT = STRIP_AREA_SQFT / LOGGER_POSITIONS_PER_STRIP
PROFILE_GALLONS_PER_INCH = PROFILE_AREA_SQFT * INCHES_WATER_TO_GALLONS_PER_SQFT

STRIP_GEOMETRY = {
    strip: {
        "strip_width_ft": STRIP_WIDTH_FT,
        "strip_length_ft": STRIP_LENGTH_FT,
        "strip_area_sqft": STRIP_AREA_SQFT,
        "logger_positions_per_strip": LOGGER_POSITIONS_PER_STRIP,
        "profile_area_sqft": PROFILE_AREA_SQFT,
        "profile_gallons_per_inch": PROFILE_GALLONS_PER_INCH,
    }
    for strip in STRIPS
}