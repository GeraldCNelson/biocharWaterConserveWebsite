#!/usr/bin/env python3
"""
plot_helpers.py — Shared Plotting Helpers & Unit-Aware Utilities.

Irrigation terminology:
    total_meter_gallons = water measured at the meter, upstream of split valve
    gallons_group       = water delivered/assigned to a strip pair/group
    gallons_strip       = water assigned to one individual strip

Plot overlays should use gallons_strip.
"""

from __future__ import annotations

import logging
import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, cast

import pandas as pd
from fastapi import HTTPException

from biochar_app.config.core import (
    strip_name_mapping,
    variable_name_mapping,
    SENSOR_DEPTH_LABELS,
    logger_location_mapping,
)
from biochar_app.config.units import UNIT_CONVERSIONS, label_name_mapping
from biochar_app.scripts.type_utils import UnitSystem
from biochar_app.scripts.data_loading import load_irrigation_data

logger = logging.getLogger(__name__)

IRRIGATION_WORKBOOK_PATH = Path("biochar_app/data-raw/biochar-data-master.xlsx")
GALLONS_PER_ACRE_FT = 325_851.0

_IRRIGATION_SHEETS_CACHE: dict[int, pd.DataFrame] = {}
_IRRIGATION_XLS: Optional[pd.ExcelFile] = None
_IRRIGATION_CACHE: dict[tuple[int, str], pd.DataFrame] = {}

COLUMN_CATEGORY_RULES = {
    "temp": [
        lambda col: col.startswith("T_"),
        lambda col: ("temp" in col) and col.endswith("degF"),
    ],
    "precip": [
        lambda col: "precip_in" in col,
    ],
    "irrigation": [
        lambda col: "gallons_strip" in col,
        lambda col: "gallons_group" in col,
        lambda col: "total_meter_gallons" in col,
        lambda col: "irrigation" in col and col.endswith("_gal"),
    ],
}


def bad_request(msg: str) -> None:
    raise HTTPException(status_code=400, detail=msg)


def sanitize_json(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, bool, int, float)):
        return obj

    if isinstance(obj, pd.Timestamp):
        return None if pd.isna(obj) else obj.isoformat()

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    try:
        item = getattr(obj, "item", None)
        if callable(item):
            return item()
    except (TypeError, ValueError):
        pass

    try:
        tolist = getattr(obj, "tolist", None)
        if callable(tolist):
            return tolist()
    except (TypeError, ValueError):
        pass

    if isinstance(obj, dict):
        return {sanitize_json(k): sanitize_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [sanitize_json(v) for v in obj]

    return str(obj)


def compute_global_min_max(df: pd.DataFrame, cols: list[str]) -> Tuple[float, float]:
    if not cols:
        raise ValueError("No data columns to compute min/max")

    the_min = df[cols].min().min()
    the_max = df[cols].max().max()

    if pd.isna(the_min) or pd.isna(the_max):
        raise ValueError("No valid numeric data to compute min/max")

    min_val = float(the_min)
    max_val = float(the_max)

    if min_val == max_val:
        if min_val == 0.0:
            return 0.0, 1.0
        pad = abs(min_val) * 0.1
        return min_val - pad, max_val + pad

    return min_val, max_val


def common_xaxis_config(_granularity: str, start: str, end: str) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "title": {"text": "Date", "font": {"size": 12}},
        "type": "date",
        "showline": True,
        "linecolor": "black",
        "linewidth": 1,
        "rangeslider": {"visible": False},
        "tickfont": {"size": 10},
    }

    try:
        start_ts = pd.to_datetime(start, errors="raise")
        end_ts = pd.to_datetime(end, errors="raise")
    except (TypeError, ValueError):
        return cfg

    cfg["range"] = [start_ts, end_ts]

    total_hours = (end_ts - start_ts).total_seconds() / 3600.0
    total_days = total_hours / 24.0

    if total_hours <= 30:
        cfg.update({
            "tickmode": "linear",
            "dtick": 2 * 60 * 60 * 1000,
            "tickformat": "%m/%d<br>%H:%M",
            "tickangle": 0,
        })
        return cfg

    if total_hours <= 72:
        cfg.update({
            "tickmode": "linear",
            "dtick": 6 * 60 * 60 * 1000,
            "tickformat": "%m/%d<br>%H:%M",
            "tickangle": 0,
        })
        return cfg

    if total_days <= 31:
        cfg.update({
            "tickmode": "linear",
            "dtick": 24 * 60 * 60 * 1000,
            "tickformat": "%m/%d",
            "tickangle": 0,
        })
        return cfg

    is_full_year = (
        start_ts.month == 1
        and start_ts.day == 1
        and end_ts.month == 12
        and end_ts.day == 31
        and start_ts.year == end_ts.year
    )

    if is_full_year:
        year = int(start_ts.year)
        month_starts = pd.date_range(f"{year}-01-01", f"{year}-12-01", freq="MS")

        cfg.update({
            "tickmode": "array",
            "tickvals": [dt.to_pydatetime() for dt in month_starts],
            "ticktext": [
                dt.strftime("%b\n%Y") if dt.month in (1, 12) else dt.strftime("%b")
                for dt in month_starts
            ],
            "tickangle": 0,
        })

    return cfg


def common_yaxis_config(
    kind: str,
    variable: str,
    unit_system: UnitSystem,
    global_min: Optional[float],
    global_max: Optional[float],
) -> Dict[str, Any]:
    if global_min is None:
        raise HTTPException(400, "Cannot build y-axis: global_min is None")
    if global_max is None:
        raise HTTPException(400, "Cannot build y-axis: global_max is None")

    gmin = float(global_min)
    gmax = float(global_max)

    if not math.isfinite(gmin) or not math.isfinite(gmax):
        raise HTTPException(
            400,
            f"Invalid y-axis data range: global_min={gmin}, global_max={gmax}",
        )

    label_entry = label_name_mapping.get(variable, variable)
    if isinstance(label_entry, dict):
        full_label = cast(dict[str, str], label_entry).get(unit_system, variable)
    else:
        full_label = str(label_entry)

    m = re.match(r"(.+?)\s*\((.+)\)", str(full_label))
    if m:
        title_base, unit = m.groups()
    else:
        title_base, unit = str(full_label), ""

    if kind == "raw":
        y_min = gmin * 0.95
        y_max = gmax * 1.05
    else:
        pad = abs(gmax) * 0.05
        y_min = gmin - pad
        y_max = gmax + pad
        if y_max <= y_min:
            y_min -= 1.0
            y_max += 1.0

    title_text = (
        f"{variable} (dimensionless)"
        if kind == "ratio"
        else f"{title_base}{f' ({unit})' if unit else ''}"
    )

    axis_cfg: Dict[str, Any] = {
        "title": {"text": title_text, "font": {"size": 12}},
        "tickformat": None,
        "showgrid": True,
        "showline": True,
        "linecolor": "black",
        "linewidth": 1,
        "zeroline": (kind == "ratio"),
        "tickfont": {"size": 11},
    }

    if math.isfinite(y_min) and math.isfinite(y_max):
        axis_cfg["range"] = [y_min, y_max]

    return axis_cfg


def common_yaxis2_config(unit_system: UnitSystem = "us") -> Dict[str, Any]:
    unit_label = "mm" if unit_system == "metric" else "in"

    return {
        "title": {
            "text": f"Precip ({unit_label})",
            "font": {"size": 12},
            "standoff": 20,
        },
        "overlaying": "y",
        "side": "right",
        "showgrid": False,
        "showline": True,
        "linecolor": "black",
        "linewidth": 1,
        "tickfont": {"size": 11},
        "rangemode": "tozero",
        "constrain": "range",
        "zeroline": False,
        "tickformat": ",.0f" if unit_system == "metric" else ".2~f",
        "nticks": 5,
        "automargin": False,
    }


def common_legend_config(title: str) -> dict:
    return dict(
        title=dict(text=f"<b>{title}</b>", side="top"),
        bgcolor="rgba(255, 255, 255, 0.50)",
        bordercolor="rgba(0, 0, 0, 0.15)",
        borderwidth=1,
        orientation="v",
        x=1.02,
        xanchor="left",
        y=1.0,
        yanchor="top",
        font=dict(size=10),
        itemsizing="constant",
        tracegroupgap=0,
        itemwidth=30,
    )


def get_unit_aware_label(variable: str, unit_system: UnitSystem) -> str:
    label = label_name_mapping.get(variable, variable)
    if isinstance(label, dict):
        return str(cast(dict[str, str], label).get(unit_system, variable))

    label_s = str(label)
    if unit_system == "metric":
        return (
            label_s
            .replace("(°F)", "(°C)")
            .replace("(inches)", "(mm)")
            .replace("(in)", "(mm)")
        )
    return label_s


def convert_units(df: pd.DataFrame, unit_system: UnitSystem) -> pd.DataFrame:
    if unit_system != "metric":
        return df

    df_conv = df.copy()

    to_c = UNIT_CONVERSIONS["us_to_metric"]["temp"]
    t_cols = [c for c in df_conv.columns if c.startswith("T_") and "_raw_" in c]
    for col in t_cols:
        df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").apply(to_c)

    to_liters = UNIT_CONVERSIONS["us_to_metric"]["irrigation"]
    for col in ["gallons_strip", "gallons_group", "total_meter_gallons"]:
        if col in df_conv.columns:
            df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").apply(to_liters)

    return df_conv


def parse_sensor_column(col: str, unit_system: UnitSystem) -> dict[str, str]:
    parts = col.split("_")
    if len(parts) < 5:
        raise ValueError(f"Unrecognized sensor column format: {col!r}")

    var_code = parts[0]
    depth_code = parts[1]

    if parts[2] == "ratio":
        if len(parts) < 6:
            raise ValueError(f"Unrecognized ratio sensor column format: {col!r}")
        strip1_code, strip2_code, loc_code = parts[3], parts[4], parts[5]
        human_strip = f"{strip_name_mapping[strip1_code]} / {strip_name_mapping[strip2_code]}"
    elif parts[2] == "raw":
        strip_code, loc_code = parts[3], parts[4]
        human_strip = strip_name_mapping[strip_code]
    else:
        raise ValueError(f"Unrecognized sensor column format: {col!r}")

    human_var = variable_name_mapping.get(var_code, var_code)
    human_depth = SENSOR_DEPTH_LABELS.get(depth_code, {}).get(unit_system, depth_code)
    human_loc = logger_location_mapping.get(loc_code, loc_code)

    return {
        "variable": str(human_var),
        "depth": str(human_depth),
        "strip": str(human_strip),
        "logger_location": str(human_loc),
    }


def _get_irrigation_workbook() -> Optional[pd.ExcelFile]:
    """
    Legacy workbook helper retained for diagnostics only.

    Active plot overlays should use load_irrigation_events(), which reads the
    cleaned canonical irrigation data.
    """
    global _IRRIGATION_XLS

    if _IRRIGATION_XLS is not None:
        return _IRRIGATION_XLS

    if not IRRIGATION_WORKBOOK_PATH.exists():
        logger.warning(
            "Irrigation workbook %s not found.",
            IRRIGATION_WORKBOOK_PATH,
        )
        return None

    _IRRIGATION_XLS = pd.ExcelFile(IRRIGATION_WORKBOOK_PATH)
    logger.info(
        "Loaded irrigation workbook %s with sheets: %s",
        IRRIGATION_WORKBOOK_PATH,
        ", ".join(map(str, _IRRIGATION_XLS.sheet_names)),
    )
    return _IRRIGATION_XLS


def _clean_irrigation_column(name: str) -> str:
    return (
        str(name)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace("#", "num")
        .replace("/", "_")
    )


def _find_irrigation_sheet_name(xls: pd.ExcelFile, year: int) -> Optional[str]:
    target = str(year)
    for sheet in xls.sheet_names:
        s_clean = str(sheet).strip()
        if target in s_clean and "IRRIGATION" in s_clean.upper():
            return str(sheet)
    return None


def _load_irrigation_sheet(year: int) -> pd.DataFrame:
    """
    Legacy workbook loader retained for diagnostics only.

    This returns raw workbook-style fields, not the canonical strip-level
    irrigation schema.
    """
    if year in _IRRIGATION_SHEETS_CACHE:
        return _IRRIGATION_SHEETS_CACHE[year]

    if not IRRIGATION_WORKBOOK_PATH.exists():
        logger.warning("Irrigation workbook %s not found.", IRRIGATION_WORKBOOK_PATH)
        df_empty = pd.DataFrame()
        _IRRIGATION_SHEETS_CACHE[year] = df_empty
        return df_empty

    xls = pd.ExcelFile(IRRIGATION_WORKBOOK_PATH, engine="openpyxl")
    sheet_name = _find_irrigation_sheet_name(xls, year)
    if sheet_name is None:
        logger.warning("No irrigation sheet found for year %s in %s", year, IRRIGATION_WORKBOOK_PATH)
        df_empty = pd.DataFrame()
        _IRRIGATION_SHEETS_CACHE[year] = df_empty
        return df_empty

    logger.info("Loaded irrigation sheet %r for year %s from %s", sheet_name, year, IRRIGATION_WORKBOOK_PATH)

    df = xls.parse(sheet_name=sheet_name)
    df.columns = [_clean_irrigation_column(c) for c in df.columns]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].copy()

    if "gal_used_x_100" in df.columns and "acre_ft_used" in df.columns:
        group_volume = pd.to_numeric(df["gal_used_x_100"], errors="coerce")
        acre_ft = pd.to_numeric(df["acre_ft_used"], errors="coerce")
        mask = group_volume.notna() & acre_ft.notna() & (acre_ft > 0)
        if bool(mask.any()):
            expected = acre_ft[mask] * GALLONS_PER_ACRE_FT
            rel_diff = (group_volume[mask] - expected).abs() / expected
            max_diff = float(rel_diff.max())
            if max_diff > 0.05:
                logger.warning(
                    "Irrigation workbook volume vs acre-ft mismatch in %s (year %s): "
                    "max relative difference ~ %.1f%%",
                    sheet_name,
                    year,
                    max_diff * 100,
                )

    _IRRIGATION_SHEETS_CACHE[year] = df
    return df


def _safe_datestr(val: Any) -> str:
    if val is None or pd.isna(val):
        return ""

    if isinstance(val, pd.Timestamp):
        return "" if pd.isna(val) else val.date().isoformat()

    try:
        ts = pd.to_datetime(val, errors="coerce")
        if isinstance(ts, pd.Timestamp) and not pd.isna(ts):
            return ts.date().isoformat()
    except (TypeError, ValueError):
        pass

    return str(val)


def _reconcile_irrigation_volume_with_acre_ft(
    df: pd.DataFrame,
    year: int,
    group: str,
    volume_col: str = "gallons_group",
    acreft_col: str = "acre_ft",
    rel_tol: float = 0.15,
) -> None:
    """
    Diagnostic helper for checking a group-level gallons column against acre-ft.

    This should only be used on group-level data, never on gallons_strip.
    """
    if volume_col == "gallons_strip":
        raise ValueError(
            "_reconcile_irrigation_volume_with_acre_ft expects group-level volume, "
            "not gallons_strip."
        )

    if volume_col not in df.columns or acreft_col not in df.columns:
        logger.debug(
            "reconcile_irrigation_volume_with_acre_ft: %s %d missing %r or %r; skipping.",
            group,
            year,
            volume_col,
            acreft_col,
        )
        return

    halved_rows: list[str] = []
    mismatch_rows: list[str] = []

    group_volume = pd.to_numeric(df[volume_col], errors="coerce")
    acre_ft = pd.to_numeric(df[acreft_col], errors="coerce")

    for pos, (group_gal, af) in enumerate(zip(group_volume.to_list(), acre_ft.to_list())):
        if not (isinstance(group_gal, (int, float)) and isinstance(af, (int, float))):
            continue
        if not (math.isfinite(float(group_gal)) and math.isfinite(float(af))) or float(af) <= 0:
            continue

        group_gal_f = float(group_gal)
        af_f = float(af)
        expected = af_f * GALLONS_PER_ACRE_FT
        if expected <= 0:
            continue

        ratio = group_gal_f / expected
        err = abs(group_gal_f - expected) / expected

        if 1.9 <= ratio <= 2.1:
            corrected = group_gal_f / 2.0
            df.at[df.index[pos], volume_col] = corrected
            date_val = df["date"].iloc[pos] if "date" in df.columns else None
            datestr = _safe_datestr(date_val)
            halved_rows.append(
                f"{datestr} ({volume_col}={group_gal_f:.1f} → {corrected:.1f}, expected≈{expected:.1f})"
            )
            continue

        if err > rel_tol:
            date_val = df["date"].iloc[pos] if "date" in df.columns else None
            datestr = _safe_datestr(date_val)
            mismatch_rows.append(
                f"{datestr} ({volume_col}={group_gal_f:.1f}, af={af_f:.6f}, expected≈{expected:.1f}, err={err * 100:.1f}%)"
            )

    if halved_rows:
        logger.info(
            "🔁 Irrigation %s %d: detected ~2× volume/acre-ft cases; auto-halved %s on %d row(s):\n  %s",
            group,
            year,
            volume_col,
            len(halved_rows),
            "\n  ".join(halved_rows),
        )

    if mismatch_rows:
        logger.warning(
            "⚠️ Irrigation %s %s: volume/acre-ft mismatches beyond %.0f%%:\n%s",
            group,
            year,
            rel_tol * 100,
            "\n".join(mismatch_rows),
        )


def load_irrigation_events(strip: str, year: int) -> pd.DataFrame:
    """
    Return strip-level irrigation events for one strip and year.

    Output columns:
        start
        end
        gallons_strip
        gallons_group
        total_meter_gallons
        event_duration_hours
    """
    cache_key = (year, strip)

    if cache_key in _IRRIGATION_CACHE:
        return _IRRIGATION_CACHE[cache_key].copy()

    output_cols = [
        "start",
        "end",
        "gallons_strip",
        "gallons_group",
        "total_meter_gallons",
        "event_duration_hours",
    ]
    empty = pd.DataFrame(columns=output_cols)

    try:
        df = load_irrigation_data()
    except Exception as exc:
        logger.warning("Failed to load irrigation data: %s", exc)
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    if df.empty:
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    required_cols = {
        "strip",
        "year",
        "start_timestamp",
        "end_timestamp",
        "gallons_strip",
    }
    missing = sorted(required_cols - set(df.columns))
    if missing:
        logger.warning("Irrigation data missing required overlay columns: %s", missing)
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    select_cols = ["start_timestamp", "end_timestamp", "gallons_strip"]

    for optional_col in [
        "gallons_group",
        "total_meter_gallons",
        "event_duration_hours",
        "duration_hours",
    ]:
        if optional_col in df.columns:
            select_cols.append(optional_col)

    year_series = pd.to_numeric(df["year"], errors="coerce")
    events = df.loc[
        (df["strip"] == strip) & (year_series == int(year)),
        select_cols,
    ].copy()

    if events.empty:
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    events.rename(
        columns={
            "start_timestamp": "start",
            "end_timestamp": "end",
        },
        inplace=True,
    )

    events["start"] = pd.to_datetime(events["start"], errors="coerce")
    events["end"] = pd.to_datetime(events["end"], errors="coerce")
    events["gallons_strip"] = pd.to_numeric(events["gallons_strip"], errors="coerce")

    if "gallons_group" in events.columns:
        events["gallons_group"] = pd.to_numeric(events["gallons_group"], errors="coerce")
    else:
        events["gallons_group"] = pd.NA

    if "total_meter_gallons" in events.columns:
        events["total_meter_gallons"] = pd.to_numeric(events["total_meter_gallons"], errors="coerce")
    else:
        events["total_meter_gallons"] = pd.NA

    if "event_duration_hours" not in events.columns:
        if "duration_hours" in events.columns:
            events["event_duration_hours"] = events["duration_hours"]
        else:
            events["event_duration_hours"] = (
                events["end"] - events["start"]
            ).dt.total_seconds() / 3600.0

    events["event_duration_hours"] = pd.to_numeric(
        events["event_duration_hours"],
        errors="coerce",
    )

    events = events.loc[
        events["start"].notna()
        & events["end"].notna()
        & events["gallons_strip"].notna()
        & (events["gallons_strip"] > 0)
    ].copy()

    overnight = (
        (events["end"] < events["start"])
        & events["end"].notna()
        & events["start"].notna()
    )
    if bool(overnight.any()):
        events.loc[overnight, "end"] = events.loc[overnight, "end"] + pd.Timedelta(days=1)

    events = events[output_cols].copy()
    events.sort_values("start", inplace=True)
    events.reset_index(drop=True, inplace=True)

    _IRRIGATION_CACHE[cache_key] = events
    return events.copy()