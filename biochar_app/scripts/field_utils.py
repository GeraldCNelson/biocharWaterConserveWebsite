from __future__ import annotations

from biochar_app.config.core import (
    FIELD_GEOMETRY,
    STRIP_AREA_ACRES,
    GALLONS_PER_ACRE_INCH,
)


def get_strip_width_ft(strip: str) -> float:
    return float(FIELD_GEOMETRY[strip]["width_ft"])


def get_strip_length_ft(strip: str) -> float:
    return float(FIELD_GEOMETRY[strip]["length_ft"])


def get_strip_area_acres(strip: str) -> float:
    return float(STRIP_AREA_ACRES[strip])


def total_to_lb_ac(total_lb: float, strip: str) -> float:
    return float(total_lb) / get_strip_area_acres(strip)


def lb_ac_to_total(lb_ac: float, strip: str) -> float:
    return float(lb_ac) * get_strip_area_acres(strip)


def gallons_to_inches_applied(gallons: float, strip: str) -> float:
    gallons_per_acre = float(gallons) / get_strip_area_acres(strip)
    return gallons_per_acre / GALLONS_PER_ACRE_INCH


def gpm_per_foot(gpm: float, strip: str) -> float:
    return float(gpm) / get_strip_width_ft(strip)