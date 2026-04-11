#!/usr/bin/env python3
"""
================================================================================
plot_helpers.py — Shared Plotting Helpers & Unit-Aware Utilities
================================================================================

This module contains low-level helpers used by plot_utils.py and the API routes
to build Plotly figures in a consistent, unit-aware way.

It is the central place for:
    • x-axis configuration (date vs. category; growing-season handling)
    • y-axis configuration for raw vs. ratio plots
    • secondary y-axis config for precipitation overlays
    • legend layout and styling
    • global min/max range computation for dynamic y-axis limits
    • sensor-column parsing (depth, strip, logger location, etc.)
    • unit-aware label generation and numeric unit conversions
    • irrigation event loading for overlays and seasonal summaries

------------------------------------------------------------------------------
DATA & UNIT ASSUMPTIONS
------------------------------------------------------------------------------
• All logger and weather Parquet data are stored in **US customary units**:
      – Soil & air temperature: °F
      – Precipitation: inches
      – Irrigation volume: gallons
      – SWC volumes: gallons (with L pre-computed in ETL where needed)

• convert_units(df, unit_system) is meant for **display-time conversions only**.
  It inspects column names and applies UNIT_CONVERSIONS from config.py.

• get_unit_aware_label(variable, unit_system) uses label_name_mapping from
  config.py and performs lightweight unit-string substitution when necessary.

------------------------------------------------------------------------------
MAINTENANCE NOTES
------------------------------------------------------------------------------
• Any new variables or units should be reflected in:
      – label_name_mapping
      – UNIT_CONVERSIONS
      – convert_units() column-name logic
------------------------------------------------------------------------------
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

from biochar_app.config.units import (
    UNIT_CONVERSIONS,
    label_name_mapping,
)

from biochar_app.scripts.type_utils import NAN, UnitSystem
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Strong typing for unit system (fixes "Expected Literal['us','metric'] got str")
# ---------------------------------------------------------------------------

# Path to the master irrigation workbook
IRRIGATION_WORKBOOK_PATH = Path("biochar_app/data-raw/biochar-data-master.xlsx")
GALLONS_PER_ACRE_FT = 325_851.0  # gallons per acre-foot

# cache: year -> cleaned irrigation DataFrame
_IRRIGATION_SHEETS_CACHE: dict[int, pd.DataFrame] = {}
# Cached ExcelFile (loaded once at import / first use)
_IRRIGATION_XLS: Optional[pd.ExcelFile] = None
# Optional cache for (year, group) → DataFrame of events
_IRRIGATION_CACHE: dict[tuple[int, str], pd.DataFrame] = {}

# Each logical unit category in UNIT_CONVERSIONS["us_to_metric"]
# is mapped to a list of rules that decide whether a DataFrame column
# belongs to that category.
COLUMN_CATEGORY_RULES = {
    "temp": [
        # Logger temperatures: T_1_raw_S1_T, T_2_raw...
        lambda col: col.startswith("T_"),
        # Weather temp columns: temp_air_degF, soil_temp_6in_degF
        lambda col: ("temp" in col) and col.endswith("degF"),
    ],
    "precip": [
        # e.g. precip_in_15min, precip_in_daily
        lambda col: "precip_in" in col,
    ],
    "irrigation": [
        # e.g. irrigation_volume_gal, volume_gal_2024
        lambda col: "volume_gal" in col,
    ],
    # SWC is not applied to columns; handled inside SWC calc only
}


def bad_request(msg: str) -> None:
    raise HTTPException(status_code=400, detail=msg)


# ---------------------------------------------------------------------------
# JSON sanitization
# ---------------------------------------------------------------------------
def sanitize_json(obj: Any) -> Any:
    """
    Recursively walk `obj` and convert into JSON-safe primitives.

    Handles:
      - pd.Timestamp (incl. NaT) → ISO string or None
      - datetime/date            → ISO string
      - numpy/pandas scalars     → native Python scalar via .item() if present
      - arrays                   → list via .tolist() if present
      - dict/list/tuple          → recursive
      - fallback                 → str(obj)
    """
    if obj is None or isinstance(obj, (str, bool, int, float)):
        return obj

    if isinstance(obj, pd.Timestamp):
        return None if pd.isna(obj) else obj.isoformat()

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    # Try scalar-like
    try:
        item = getattr(obj, "item", None)
        if callable(item):
            return item()
    except (TypeError, ValueError):
        pass

    # Try array-like
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


# ---------------------------------------------------------------------------
# Global min / max helpers
# ---------------------------------------------------------------------------
def compute_global_min_max(df: pd.DataFrame, cols: list[str]) -> Tuple[float, float]:
    """
    Given a DataFrame `df` and a list of numeric column names `cols`,
    return (global_min, global_max).

    - If cols is empty, ValueError.
    - If all values are NaN, ValueError.
    - If min == max, expand by 10% (or by 1 if the value is zero).
    """
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


# ---------------------------------------------------------------------------
# Axis configs
# ---------------------------------------------------------------------------
def common_xaxis_config(_granularity: str, start: str, end: str) -> Dict[str, Any]:
    """
    Shared x-axis config.

    If the requested window is exactly a single calendar year
    (YYYY-01-01 .. YYYY-12-31), use month ticks:
      Jan\\nYYYY, Feb, Mar, ..., Nov, Dec\\nYYYY
    Otherwise, let Plotly pick ticks automatically.
    """
    cfg: Dict[str, Any] = {
        "title": {"text": "Date", "font": {"size": 12}},
        "type": "date",
        "showline": True,
        "linecolor": "black",
        "linewidth": 1,
        "rangeslider": {"visible": False},
        "tickfont": {"size": 11},
    }

    try:
        start_ts = pd.to_datetime(start, errors="raise")
        end_ts = pd.to_datetime(end, errors="raise")
    except (TypeError, ValueError):
        return cfg

    is_full_year = (
        start_ts.month == 1
        and start_ts.day == 1
        and end_ts.month == 12
        and end_ts.day == 31
        and start_ts.year == end_ts.year
    )
    if not is_full_year:
        return cfg

    year = int(start_ts.year)
    month_starts = pd.date_range(f"{year}-01-01", f"{year}-12-01", freq="MS")

    # JSON-safe: plain python datetimes
    tickvals = [dt.to_pydatetime() for dt in month_starts]

    ticktext: list[str] = []
    for dt in month_starts:
        ticktext.append(dt.strftime("%b\n%Y") if dt.month in (1, 12) else dt.strftime("%b"))

    cfg.update({"tickmode": "array", "tickvals": tickvals, "ticktext": ticktext, "tickangle": 0})
    return cfg


def common_yaxis_config(
    kind: str,
    variable: str,
    unit_system: UnitSystem,
    global_min: Optional[float],
    global_max: Optional[float],
) -> Dict[str, Any]:
    """
    Build a yaxis config dict for Plotly.
    Raises a clear HTTP 400 if the data range is invalid.
    """
    if global_min is None:
        raise HTTPException(400, "Cannot build y-axis: global_min is None")
    if global_max is None:
        raise HTTPException(400, "Cannot build y-axis: global_max is None")

    gmin = float(global_min)
    gmax = float(global_max)

    if not math.isfinite(gmin) or not math.isfinite(gmax):
        raise HTTPException(400, f"Invalid y-axis data range: global_min={gmin}, global_max={gmax}")

    full_label = (
        label_name_mapping.get(variable, {}).get(unit_system)
        or variable.replace("_", " ").title()
    )

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

    if kind == "ratio":
        title_text = f"{variable} (dimensionless)"
    else:
        title_text = f"{title_base}{f' ({unit})' if unit else ''}"

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
    """
    Secondary y-axis config for precipitation overlays.
    """
    unit_label = "mm" if unit_system == "metric" else "in"

    return {
        "title": {
            "text": f"Precip ({unit_label})",
            "font": {"size": 12},
            # Keep some separation, but do not force the title too far inward.
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
        # Cleaner tick formatting:
        #   metric -> integers when possible
        #   us     -> concise decimals
        "tickformat": ",.0f" if unit_system == "metric" else ".2~f",
        "nticks": 5,
        # Do not let Plotly auto-pull the axis title inward in narrow layouts.
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


# ---------------------------------------------------------------------------
# Labels & unit-aware text
# ---------------------------------------------------------------------------
def get_unit_aware_label(variable: str, unit_system: UnitSystem) -> str:
    """
    Given a variable key and unit_system ("us" or "metric"), return a label.
    """
    label = label_name_mapping.get(variable, variable)
    if isinstance(label, dict):
        return str(label.get(unit_system, variable))

    label_s = str(label)
    if unit_system == "metric":
        return (
            label_s
            .replace("(°F)", "(°C)")
            .replace("(inches)", "(mm)")
            .replace("(in)", "(mm)")
        )
    return label_s


# ---------------------------------------------------------------------------
# Unit conversions for display
# ---------------------------------------------------------------------------
def convert_units(df: pd.DataFrame, unit_system: UnitSystem) -> pd.DataFrame:
    """
    Return a *copy* of df with values converted for display.

    Currently converts soil temperature (T_*_raw_*) when unit_system == "metric".
    """
    if unit_system != "metric":
        return df

    df_conv = df.copy()
    to_c = UNIT_CONVERSIONS["us_to_metric"]["temp"]

    t_cols = [c for c in df_conv.columns if c.startswith("T_") and "_raw_" in c]
    for col in t_cols:
        df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").apply(to_c)

    return df_conv


# ---------------------------------------------------------------------------
# Sensor column parsing
# ---------------------------------------------------------------------------
def parse_sensor_column(col: str, unit_system: UnitSystem) -> dict[str, str]:
    """
    Parse standardized sensor column names into human labels.

    Examples
    --------
    "VWC_1_raw_S1_T" →
        var_code   = "VWC"
        depth_code = "1"
        strip_code = "S1"
        loc_code   = "T"

    "VWC_1_ratio_S1_S2_T" →
        var_code   = "VWC"
        depth_code = "1"
        ratio of strip1/strip2, at loc_code "T"
    """
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


# ---------------------------------------------------------------------------
# Irrigation workbook & overlays
# ---------------------------------------------------------------------------
def _get_irrigation_workbook() -> Optional[pd.ExcelFile]:
    """
    Lazily load and cache the irrigation Excel workbook.
    Returns None if the file does not exist.
    """
    global _IRRIGATION_XLS

    if _IRRIGATION_XLS is not None:
        return _IRRIGATION_XLS

    if not IRRIGATION_WORKBOOK_PATH.exists():
        logger.warning(
            "Irrigation workbook %s not found; skipping irrigation overlays.",
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
    """
    Normalize Excel column names from the irrigation workbook
    into safe, lower_snake_case tokens.
    """
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
    """
    Given an ExcelFile, find the sheet that corresponds to the requested year,
    e.g. '2023 IRRIGATION ' or '2024 IRRIGATION'.
    """
    target = str(year)
    for sheet in xls.sheet_names:
        s_clean = str(sheet).strip()
        if target in s_clean and "IRRIGATION" in s_clean.upper():
            return str(sheet)
    return None


def _load_irrigation_sheet(year: int) -> pd.DataFrame:
    """
    Load and clean the irrigation sheet for a given year, with caching.
    Also performs a gallon <-> acre-ft consistency check (legacy diagnostic).
    """
    if year in _IRRIGATION_SHEETS_CACHE:
        return _IRRIGATION_SHEETS_CACHE[year]

    if not IRRIGATION_WORKBOOK_PATH.exists():
        logger.warning(
            "Irrigation workbook %s not found; skipping overlays.",
            IRRIGATION_WORKBOOK_PATH,
        )
        df_empty = pd.DataFrame()
        _IRRIGATION_SHEETS_CACHE[year] = df_empty
        return df_empty

    xls = pd.ExcelFile(IRRIGATION_WORKBOOK_PATH, engine="openpyxl")
    sheet_name = _find_irrigation_sheet_name(xls, year)
    if sheet_name is None:
        logger.warning(
            "No irrigation sheet found for year %s in %s",
            year,
            IRRIGATION_WORKBOOK_PATH,
        )
        df_empty = pd.DataFrame()
        _IRRIGATION_SHEETS_CACHE[year] = df_empty
        return df_empty

    logger.info(
        "Loaded irrigation sheet %r for year %s from %s",
        sheet_name,
        year,
        IRRIGATION_WORKBOOK_PATH,
    )

    df = xls.parse(sheet_name=sheet_name)
    df.columns = [_clean_irrigation_column(c) for c in df.columns]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].copy()

    # Legacy diagnostic check
    if "gal_used_x_100" in df.columns and "acre_ft_used" in df.columns:
        vol = pd.to_numeric(df["gal_used_x_100"], errors="coerce")
        af = pd.to_numeric(df["acre_ft_used"], errors="coerce")
        mask = vol.notna() & af.notna() & (af > 0)
        if bool(mask.any()):
            expected = af[mask] * GALLONS_PER_ACRE_FT
            rel_diff = (vol[mask] - expected).abs() / expected
            max_diff = float(rel_diff.max())
            if max_diff > 0.05:
                logger.warning(
                    "Irrigation gallon vs acre-ft mismatch in %s (year %s): "
                    "max relative difference ~ %.1f%%",
                    sheet_name,
                    year,
                    max_diff * 100,
                )

    _IRRIGATION_SHEETS_CACHE[year] = df
    return df


def _safe_datestr(val: Any) -> str:
    """
    Return a stable date string without ever calling strftime on NaT/NaN.
    """
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


def _reconcile_irrigation_gallons(
    df: pd.DataFrame,
    year: int,
    group: str,
    gallons_col: str = "gallons",
    acreft_col: str = "acre_ft",
    rel_tol: float = 0.15,
) -> None:
    """
    Ensure gallons and acre-feet are consistent, with a special case for
    the “all four strips at once” event.
    """
    if gallons_col not in df.columns or acreft_col not in df.columns:
        logger.debug(
            "reconcile_irrigation_gallons: %s %d missing %r or %r; skipping.",
            group,
            year,
            gallons_col,
            acreft_col,
        )
        return

    halved_rows: list[str] = []
    mismatch_rows: list[str] = []

    gallons = pd.to_numeric(df[gallons_col], errors="coerce")
    acre_ft = pd.to_numeric(df[acreft_col], errors="coerce")

    for pos, (gal, af) in enumerate(zip(gallons.to_list(), acre_ft.to_list())):
        if not (isinstance(gal, (int, float)) and isinstance(af, (int, float))):
            continue
        if not (math.isfinite(float(gal)) and math.isfinite(float(af))) or float(af) <= 0:
            continue

        gal_f = float(gal)
        af_f = float(af)
        expected = af_f * GALLONS_PER_ACRE_FT
        if expected <= 0:
            continue

        ratio = gal_f / expected
        err = abs(gal_f - expected) / expected

        # Special case: approx 2× gallons vs acre-ft (4 strips on one meter)
        if 1.9 <= ratio <= 2.1:
            corrected = gal_f / 2.0
            df.at[df.index[pos], gallons_col] = corrected
            date_val = df["date"].iloc[pos] if "date" in df.columns else None
            datestr = _safe_datestr(date_val)
            halved_rows.append(
                f"{datestr} (gal={gal_f:.1f} → {corrected:.1f}, expected≈{expected:.1f})"
            )
            continue

        if err > rel_tol:
            date_val = df["date"].iloc[pos] if "date" in df.columns else None
            datestr = _safe_datestr(date_val)
            mismatch_rows.append(
                f"{datestr} (gal={gal_f:.1f}, af={af_f:.6f}, expected≈{expected:.1f}, err={err*100:.1f}%)"
            )

    if halved_rows:
        logger.info(
            "🔁 Irrigation %s %d: detected ~2× gallon/acre-ft cases; auto-halved gallons on %d row(s):\n  %s",
            group,
            year,
            len(halved_rows),
            "\n  ".join(halved_rows),
        )

    if mismatch_rows:
        logger.warning(
            "⚠️ Irrigation %s %s: gallon/acre-ft mismatches beyond %.0f%%:\n%s",
            group,
            year,
            rel_tol * 100,
            "\n".join(mismatch_rows),
        )


def load_irrigation_events(strip: str, year: int) -> pd.DataFrame:
    """
    Load irrigation events for a given strip + year from the cleaned
    management CSV, not directly from the workbook.

    Returns DataFrame with columns:
        start (Timestamp), end (Timestamp), volume_gal (float)

    Notes
    -----
    - Irrigation is applied by strip pair:
        S1/S2 -> west
        S3/S4 -> east
    - The cleaned CSV stores this as `strip_group` with values:
        S1_S2, S3_S4
    """
    from biochar_app.config.paths import IRRIGATION_CSV

    group = "S1_S2" if strip in ("S1", "S2") else "S3_S4"
    cache_key = (year, group)

    if cache_key in _IRRIGATION_CACHE:
        return _IRRIGATION_CACHE[cache_key].copy()

    empty = pd.DataFrame(columns=["start", "end", "volume_gal"])

    if not IRRIGATION_CSV.exists():
        logger.warning("Irrigation CSV not found: %s", IRRIGATION_CSV)
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    try:
        df = pd.read_csv(IRRIGATION_CSV)
    except Exception as e:
        logger.warning("Failed to read irrigation CSV %s: %s", IRRIGATION_CSV, e)
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    if df.empty:
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    required_cols = {"year", "strip_group", "start_timestamp", "end_timestamp", "gallons"}
    missing = sorted(required_cols - set(df.columns))
    if missing:
        logger.warning(
            "Irrigation CSV %s missing required columns: %s",
            IRRIGATION_CSV,
            missing,
        )
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    # Filter to requested year + strip pair
    df = df.copy()
    # derive year from timestamp (more reliable than CSV column)
    start_ts = pd.to_datetime(df["start_timestamp"], errors="coerce")
    df = df.assign(_year=start_ts.dt.year)

    df = df[
        (df["_year"] == year)
        & (df["strip_group"].astype(str).str.strip() == group)
        ].copy()

    if df.empty:
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    start = pd.to_datetime(df["start_timestamp"], errors="coerce")
    end = pd.to_datetime(df["end_timestamp"], errors="coerce")
    gallons = pd.to_numeric(df["gallons"], errors="coerce")

    events = pd.DataFrame(
        {
            "start": start,
            "end": end,
            "volume_gal": gallons,
        },
        index=df.index,
    )

    # Safety for overnight events, though extraction script should already handle this.
    overnight = (events["end"] < events["start"]) & events["end"].notna() & events["start"].notna()
    if bool(overnight.any()):
        events.loc[overnight, "end"] = events.loc[overnight, "end"] + pd.Timedelta(days=1)

    events = events[
        events["start"].notna()
        & events["end"].notna()
        & events["volume_gal"].notna()
        & (events["volume_gal"] > 0)
    ].copy()

    events.sort_values("start", inplace=True)
    events.reset_index(drop=True, inplace=True)

    _IRRIGATION_CACHE[cache_key] = events
    return events.copy()