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
  It inspects column names and applies UNIT_CONVERSIONS from config.py:

      unit_system == "metric":
          – Fahrenheit → Celsius
          – Inches → millimeters
          – Gallons → liters
          – SWC depth/volume conversions where appropriate

  The underlying Parquet files remain unchanged.

• get_unit_aware_label(variable, unit_system) uses label_name_mapping from
  config.py and performs lightweight unit-string substitution when necessary
  (e.g., “(°F)” → “(°C)”, “(inches)” → “(mm)”).

------------------------------------------------------------------------------
KEY FUNCTIONS
------------------------------------------------------------------------------
• common_xaxis_config(...)
      – Builds consistent x-axis config for all time-based plots.

• common_yaxis_config(kind, variable, unit_system, global_min, global_max)
      – Computes y-axis styling + range for raw vs ratio plots.

• common_yaxis2_config(unit_system)
      – Standardized y2 config for precipitation overlays.

• common_legend_config(title)
      – Legend styling (position, background, font) reused across views.

• compute_global_min_max(df, columns)
      – Robust min/max for auto-scaling primary y-axis.

• parse_sensor_column(col, unit_system)
      – Parses standardized column names like
        “VWC_1_raw_S1_T” → depth, strip, logger location, etc.

• convert_units(df, unit_system)
      – Applies unit-system conversions for plotting and downloads.

• load_irrigation_events(strip, year)
      – Returns a DataFrame of irrigation events used by overlays in plots.

------------------------------------------------------------------------------
MAINTENANCE NOTES
------------------------------------------------------------------------------
• Any new variables or units should be reflected in:
      – label_name_mapping
      – UNIT_CONVERSIONS
      – convert_units() column-name logic

• plot_helpers.py should remain free of any routing or I/O logic; it is strictly
  a utility layer shared by multiple plotting entry points.
------------------------------------------------------------------------------
"""

import os
import math
import re
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Any, Optional, Tuple, Dict, Sequence

import numpy as np
import pandas as pd

from fastapi import HTTPException

from biochar_app.scripts.config import (
    strip_name_mapping,
    variable_name_mapping,
    sensor_depth_mapping,
    UNIT_CONVERSIONS,
    label_name_mapping,
    human_label,
    logger_location_mapping,
    DEFAULT_GSEASON_PERIODS,
)

logger = logging.getLogger(__name__)

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
    Recursively walk through `obj` and convert everything into plain Python primitives
    that Flask/FastAPI JSON can handle:
      - pd.Timestamp → ISO string
      - datetime/date → ISO string
      - NumPy scalar  → native Python scalar
      - NumPy array   → Python list
      - dict          → sanitize each key/value
      - list/tuple    → sanitize each element
      - any other type→ str(obj)
    """
    # Already-safe primitives
    if obj is None or isinstance(obj, (str, bool, int, float)):
        return obj

    # Pandas Timestamp
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    # native datetime or date
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    # NumPy scalar
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()

    # NumPy array → Python list
    if isinstance(obj, np.ndarray):
        return obj.tolist()

    # dict → sanitize each key-value
    if isinstance(obj, dict):
        new_dict: dict[str, Any] = {}
        for k, v in obj.items():
            key_sanitized = sanitize_json(k)
            val_sanitized = sanitize_json(v)
            new_dict[key_sanitized] = val_sanitized
        return new_dict

    # list or tuple → sanitize each element
    if isinstance(obj, (list, tuple)):
        return [sanitize_json(v) for v in obj]

    # Fallback: string repr
    return str(obj)


# ---------------------------------------------------------------------------
# Global min / max helpers
# ---------------------------------------------------------------------------

def compute_global_min_max(
    df: pd.DataFrame, cols: list[str]
) -> Tuple[float, float]:
    """
    Given a DataFrame `df` and a list of numeric column names `cols`,
    return (global_min, global_max).

    - If cols is empty, ValueError.
    - If all values are NaN, ValueError.
    - If min == max, expand by 10% (or by 1 if the value is zero).
    """
    if not cols:
        raise ValueError("No data columns to compute min/max")

    # compute raw min/max
    the_min = df[cols].min().min()
    the_max = df[cols].max().max()

    # guard NaN
    if pd.isna(the_min) or pd.isna(the_max):
        raise ValueError("No valid numeric data to compute min/max")

    min_val = float(the_min)
    max_val = float(the_max)

    # if flat line, add a little padding
    if min_val == max_val:
        if min_val == 0:
            min_val, max_val = 0, 1
        else:
            pad = abs(min_val) * 0.1
            min_val -= pad
            max_val += pad

    return min_val, max_val


# ---------------------------------------------------------------------------
# Axis configs
# ---------------------------------------------------------------------------

def common_xaxis_config(granularity: str, start: str, end: str) -> Dict[str, Any]:
    """
    Shared x-axis config.

    If the requested window is exactly a single calendar year
    (YYYY-01-01 .. YYYY-12-31), use month ticks:

      Jan\\nYYYY, Feb, Mar, ..., Nov, Dec\\nYYYY

    Otherwise, let Plotly pick ticks automatically.
    """
    cfg: Dict[str, Any] = {
        "title": {"text": "Date", "font": {"size": 12}},  # ✅ Plotly-safe replacement
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
    except Exception:
        # fall back to simple date axis
        return cfg

    is_full_year = (
        start_ts.month == 1 and start_ts.day == 1 and
        end_ts.month == 12 and end_ts.day == 31 and
        start_ts.year == end_ts.year
    )

    if not is_full_year:
        return cfg

    year = start_ts.year

    # Month starts: Jan 1, Feb 1, ..., Dec 1
    month_starts = pd.date_range(f"{year}-01-01", f"{year}-12-01", freq="MS")

    # ✅ Convert to plain python datetimes for safety when JSON-encoding later
    tickvals = [dt.to_pydatetime() for dt in month_starts]

    ticktext = []
    for dt in month_starts:
        if dt.month in (1, 12):
            ticktext.append(dt.strftime("%b\n%Y"))  # Jan\n2025 , Dec\n2025
        else:
            ticktext.append(dt.strftime("%b"))

    cfg.update(
        {
            "tickmode": "array",
            "tickvals": tickvals,
            "ticktext": ticktext,
            "tickangle": 0,
        }
    )
    return cfg


def common_yaxis_config(
        kind: str,
        variable: str,
        unit_system: str,
        global_min: Optional[float],
        global_max: Optional[float],
) -> Dict[str, Any]:
    """
    Build a yaxis config dict for Plotly.
    Raises a clear HTTP 400 if the data range is invalid.
    """
    # 1) Make sure we actually have numbers

    if global_min is None:
        raise HTTPException(400, "Cannot build y-axis: global_min is None")
    if global_max is None:
        raise HTTPException(400, "Cannot build y-axis: global_max is None")

    # 2) Reject any NaN or infinite values
    if not np.isfinite(global_min) or not np.isfinite(global_max):
        raise HTTPException(
            400,
            f"Invalid y-axis data range: "
            f"global_min={global_min}, global_max={global_max}"
        )

    # 3) Look up human label …
    full_label = (
        label_name_mapping.get(variable, {}).get(unit_system)
        or variable.replace("_", " ").title()
    )

    # 4) Split into title text + suffix
    m = re.match(r"(.+?)\s*\((.+)\)", full_label)
    if m:
        title_base, unit = m.groups()
    else:
        title_base, unit = full_label, ""

    # 5) Compute padded min/max
    if kind == "raw":
        y_min = float(global_min) * 0.95
        y_max = float(global_max) * 1.05
    else:
        pad = float(global_max) * 0.05
        y_min = float(global_min) - pad
        y_max = float(global_max) + pad
        if y_max <= y_min:
            y_min -= 1
            y_max += 1

    # 6) Build axis dict
    if kind == "ratio":
        # Ratios are unitless; keep the y-axis short and unambiguous.
        # Use the variable code directly (e.g., "VWC") since the full name
        # is already shown in the plot title/UI.
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

    # 7) Only include a fixed range if both ends are still finite
    if np.isfinite(y_min) and np.isfinite(y_max):
        axis_cfg["range"] = [y_min, y_max]

    return axis_cfg


def common_yaxis2_config(unit_system: str = "us") -> Dict[str, Any]:
    """
    Secondary y-axis config for precipitation overlays.
    """
    unit_label = "mm" if unit_system == "metric" else "inches"

    return {
        # Title (with explicit font size)
        "title": {"text": f"Precipitation ({unit_label})", "font": {"size": 12}},

        # Axis placement
        "overlaying": "y",
        "side": "right",

        # Styling
        "showgrid": False,
        "showline": True,
        "linecolor": "black",
        "linewidth": 1,

        # Tick label font size
        "tickfont": {"size": 11},

        # Keep precip non-negative and grounded at zero
        "rangemode": "tozero",
        "constrain": "range",
        "zeroline": False,

        # Units formatting
        "tickformat": ".2f",
        "nticks": 5,
    }


def common_legend_config(title: str) -> dict:
    """
    Standardized legend settings for all plots.
    Makes the legend compact with minimal vertical spacing.
    """
    return dict(
        title=dict(text=title),

        # Background / border
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor="rgba(0, 0, 0, 0.25)",
        borderwidth=1,

        # Position: vertical legend on the right
        orientation="v",
        x=1.02,
        xanchor="left",
        y=1.0,
        yanchor="top",

        # Compact look
        font=dict(size=11),
        itemsizing="constant",  # same box height for all entries
        tracegroupgap=0,        # no extra gap between legend groups
        itemwidth=30,           # narrower entry width → less wrapping
        # no valign here; older stubs complain and it’s not essential
    )

# ---------------------------------------------------------------------------
# Labels & unit-aware text
# ---------------------------------------------------------------------------

def get_unit_aware_label(variable: str, unit_system: str) -> str:
    """
    Given a “variable key” (e.g. "T" or "VWC" or "precip_in" or "irrigation"),
    and a unit_system ("us" or "metric"), return the correct axis-label text
    from label_name_mapping (falling back if needed).
    """
    label = label_name_mapping.get(variable, variable)
    if isinstance(label, dict):
        return label.get(unit_system, variable)

    # If the value is just a string (not a dict), then do a best-guess replacement:
    if unit_system == "metric":
        return (
            label
            .replace("(°F)", "(°C)")
            .replace("(inches)", "(mm)")
            .replace("(in)", "(mm)")
        )
    return label


# ---------------------------------------------------------------------------
# Unit conversions for display
# ---------------------------------------------------------------------------

def convert_units(df: pd.DataFrame, unit_system: str) -> pd.DataFrame:
    """
    Return a *copy* of df with values converted for display.

    Assumptions:
      - Logger soil temps T_*_raw_* are stored in °F.
      - VWC columns are already in %, EC in dS/m, etc.
      - Weather data already contains both temp_air_degF and temp_air_degC,
        so we do **not** touch those here.

    Currently this only converts soil temperature when unit_system == "metric".
    It is safe to call for any variable.
    """
    if unit_system != "metric":
        # US is the storage format; nothing to do.
        return df

    df_conv = df.copy()
    to_c = UNIT_CONVERSIONS["us_to_metric"]["temp"]

    # Soil temperature columns look like: T_1_raw_S1_T, T_2_raw_S3_M, etc.
    t_cols = [c for c in df_conv.columns if c.startswith("T_") and "_raw_" in c]

    for col in t_cols:
        df_conv[col] = pd.to_numeric(df_conv[col], errors="coerce").apply(to_c)

    return df_conv


# ---------------------------------------------------------------------------
# Sensor column parsing
# ---------------------------------------------------------------------------

def parse_sensor_column(col: str, unit_system: str) -> dict[str, str]:
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
    var_code   = parts[0]
    depth_code = parts[1]

    if parts[2] == "ratio":
        strip1_code, strip2_code, loc_code = parts[3], parts[4], parts[5]
        human_strip = (
            f"{strip_name_mapping[strip1_code]}"
            f" / {strip_name_mapping[strip2_code]}"
        )
    elif parts[2] == "raw":
        strip_code, loc_code = parts[3], parts[4]
        human_strip = strip_name_mapping[strip_code]
    else:
        raise ValueError(f"Unrecognized sensor column format: {col!r}")

    human_var   = variable_name_mapping[var_code]
    human_depth = sensor_depth_mapping[depth_code][unit_system]
    human_loc   = logger_location_mapping[loc_code]

    return {
        "variable":        human_var,
        "depth":           human_depth,
        "strip":           human_strip,
        "logger_location": human_loc,
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
        ", ".join(_IRRIGATION_XLS.sheet_names),
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
            return sheet
    return None


def _load_irrigation_sheet(year: int) -> pd.DataFrame:
    """
    Load and clean the irrigation sheet for a given year, with caching.
    Also performs a gallon <-> acre-ft consistency check (legacy diagnostic).
    """
    # cached?
    if year in _IRRIGATION_SHEETS_CACHE:
        return _IRRIGATION_SHEETS_CACHE[year]

    if not os.path.exists(IRRIGATION_WORKBOOK_PATH):
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

    # Coerce dates and drop non-data rows (totals, notes, etc.)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].copy()

    # Legacy diagnostic check (using original gal_used_x_100 / acre_ft_used)
    if "gal_used_x_100" in df.columns and "acre_ft_used" in df.columns:
        vol = pd.to_numeric(df["gal_used_x_100"], errors="coerce")
        af = pd.to_numeric(df["acre_ft_used"], errors="coerce")
        mask = vol.notna() & af.notna() & (af > 0)

        if mask.any():
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

    Behavior
    --------
    - For each row with finite gallons & acre_ft:
        expected = acre_ft * GALLONS_PER_ACRE_FT
        ratio    = actual / expected

      * If 1.9 <= ratio <= 2.1:
          -> Treat as “all four strips recorded on one meter”
             and auto-halves gallons in place.
          -> Log an INFO message.

      * Else if relative error > rel_tol (default 15%):
          -> Keep values as-is but log a WARNING (true mismatch).

      * Else:
          -> Values are close enough; do nothing.
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

    for idx, (gal, af) in enumerate(zip(gallons, acre_ft)):
        if not (math.isfinite(gal) and math.isfinite(af)) or af <= 0:
            continue

        expected = af * GALLONS_PER_ACRE_FT
        if expected <= 0:
            continue

        ratio = gal / expected
        err = abs(gal - expected) / expected

        # Special case: approx 2× gallons vs acre-ft (4 strips on one meter)
        if 1.9 <= ratio <= 2.1:
            corrected = gal / 2.0
            df.at[df.index[idx], gallons_col] = corrected
            date_val = df.get("date", pd.Series(index=df.index, dtype="datetime64[ns]")).iloc[idx]
            if isinstance(date_val, pd.Timestamp):
                datestr = date_val.date().isoformat()
            else:
                datestr = str(date_val)
            halved_rows.append(
                f"{datestr} (gal={gal:.1f} → {corrected:.1f}, "
                f"expected≈{expected:.1f})"
            )
            continue

        # Otherwise, log real mismatches beyond tolerance
        if err > rel_tol:
            date_val = df.get("date", pd.Series(index=df.index, dtype="datetime64[ns]")).iloc[idx]
            if isinstance(date_val, pd.Timestamp):
                datestr = date_val.date().isoformat()
            else:
                datestr = str(date_val)
            mismatch_rows.append(
                f"{datestr} (gal={gal:.1f}, af={af:.6f}, "
                f"expected≈{expected:.1f}, err={err*100:.1f}%)"
            )

    if halved_rows:
        logger.info(
            "🔁 Irrigation %s %d: detected ~2× gallon/acre-ft cases; "
            "auto-halved gallons on %d row(s):\n  %s",
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
    Load irrigation events for a given strip + year from the master workbook.

    Returns DataFrame with:
        start (Timestamp), end (Timestamp), volume_gal (float)
    """

    # Map strip → group
    group = "west" if strip in ("S1", "S2") else "east"

    # Cache check
    cache_key = (year, group)
    if cache_key in _IRRIGATION_CACHE:
        return _IRRIGATION_CACHE[cache_key].copy()

    # Load workbook
    xls = _get_irrigation_workbook()
    if xls is None:
        empty = pd.DataFrame(columns=["start", "end", "volume_gal"])
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    # Identify correct worksheet
    target_prefix = f"{year}"
    sheet_name: Optional[str] = None
    for s in xls.sheet_names:
        s_clean = str(s).strip()
        if target_prefix in s_clean and "IRRIGATION" in s_clean.upper():
            sheet_name = s
            break

    if sheet_name is None:
        logger.warning(
            "No irrigation sheet found for year %s. Sheets available: %s",
            year,
            xls.sheet_names,
        )
        empty = pd.DataFrame(columns=["start", "end", "volume_gal"])
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    df_raw = xls.parse(sheet_name=sheet_name)
    if df_raw.empty:
        empty = pd.DataFrame(columns=["start", "end", "volume_gal"])
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    # ------------------------------
    # Normalize column names
    # ------------------------------
    rename_map: dict[str, str] = {}
    for col in df_raw.columns:
        c = str(col).strip().upper()

        if c.startswith("DATE"):
            rename_map[col] = "date"
        elif "STRIP" in c:
            rename_map[col] = "strip_id"
        elif c.startswith("LOCATION"):
            rename_map[col] = "location"
        elif "TIME ON" in c or ("START" in c and "METER" not in c):
            rename_map[col] = "time_on"
        elif "TIME OFF" in c or ("END" in c and "METER" not in c):
            rename_map[col] = "time_off"
        elif "GAL. USED" in c:
            rename_map[col] = "gallons"
        elif "ACRE FT" in c:
            rename_map[col] = "acre_ft"

    df = df_raw.rename(columns=rename_map)

    # Drop duplicate columns if any were renamed to same thing
    if df.columns.duplicated().any():
        dup_cols = df.columns[df.columns.duplicated()].tolist()
        logger.info(
            "Duplicate irrigation columns after rename: %s; keeping first occurrence(s). Should only happen in 2025.",
            dup_cols,
        )
        df = df.loc[:, ~df.columns.duplicated()]

    # Required columns
    if "date" not in df or ("gallons" not in df and "acre_ft" not in df):
        logger.warning(
            "Irrigation sheet %s missing essential columns. Found: %s",
            sheet_name,
            list(df.columns),
        )
        empty = pd.DataFrame(columns=["start", "end", "volume_gal"])
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    # ------------------------------
    # Determine west/east group
    # ------------------------------
    def infer_group(row: pd.Series) -> str:
        loc = str(row.get("location", "")).strip().lower()
        if loc in {"west", "w"}:
            return "west"
        if loc in {"east", "e"}:
            return "east"

        strip_id = str(row.get("strip_id", "")).strip()
        if strip_id.startswith(("1", "2")):
            return "west"
        if strip_id.startswith(("3", "4")):
            return "east"

        return "unknown"

    df["group"] = df.apply(infer_group, axis=1)
    df = df[df["group"] == group].copy()

    if df.empty:
        empty = pd.DataFrame(columns=["start", "end", "volume_gal"])
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    # ------------------------------
    # Build datetime start/end
    # ------------------------------

    # DATE column → normalized YYYY-MM-DD
    date_str = (
        pd.to_datetime(df["date"], errors="coerce")
        .dt.strftime("%Y-%m-%d")
    )

    # REQUIRED: time_on / time_off must exist
    if "time_on" not in df or "time_off" not in df:
        logger.warning(
            "Year %s irrigation sheet lacks TIME ON / TIME OFF columns. Columns available: %s",
            year,
            list(df.columns),
        )
        empty = pd.DataFrame(columns=["start", "end", "volume_gal"])
        _IRRIGATION_CACHE[cache_key] = empty
        return empty.copy()

    # Handle possible duplicate-column issue where df["time_on"] → DataFrame
    time_on_raw = df["time_on"]
    if isinstance(time_on_raw, pd.DataFrame):
        logger.warning(
            "Multiple 'time_on' columns detected for year %s; using first one. Subcolumns: %s",
            year,
            list(time_on_raw.columns),
        )
        time_on_raw = time_on_raw.iloc[:, 0]

    time_off_raw = df["time_off"]
    if isinstance(time_off_raw, pd.DataFrame):
        logger.warning(
            "Multiple 'time_off' columns detected for year %s; using first one. Subcolumns: %s",
            year,
            list(time_off_raw.columns),
        )
        time_off_raw = time_off_raw.iloc[:, 0]

    time_on = time_on_raw.astype(str).str.strip()
    time_off = time_off_raw.astype(str).str.strip()

    # First try seconds, then fallback HH:MM
    start = pd.to_datetime(
        date_str + " " + time_on,
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    start = start.fillna(
        pd.to_datetime(
            date_str + " " + time_on,
            format="%Y-%m-%d %H:%M",
            errors="coerce",
        )
    )

    end = pd.to_datetime(
        date_str + " " + time_off,
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    end = end.fillna(
        pd.to_datetime(
            date_str + " " + time_off,
            format="%Y-%m-%d %H:%M",
            errors="coerce",
        )
    )

    # ------------------------------
    # Gallons & acre-ft
    # ------------------------------
    df["gallons"] = pd.to_numeric(df.get("gallons", np.nan), errors="coerce")
    df["acre_ft"] = pd.to_numeric(df.get("acre_ft", np.nan), errors="coerce")

    # Reconcile gallons vs acre-ft, including the special “4 strips at once” case
    _reconcile_irrigation_gallons(df, year=year, group=group)

    # Back-fill gallons if missing but acre-ft is present
    missing_gal = df["gallons"].isna() & df["acre_ft"].notna()
    if missing_gal.any():
        df.loc[missing_gal, "gallons"] = (
            df.loc[missing_gal, "acre_ft"] * GALLONS_PER_ACRE_FT
        )

    gallons = df["gallons"]

    # ------------------------------
    # Build final table
    # ------------------------------
    events = pd.DataFrame(
        {"start": start, "end": end, "volume_gal": gallons}
    )

    # Overnight events fix
    overnight = (events["end"] < events["start"]) & events["end"].notna()
    if overnight.any():
        events.loc[overnight, "end"] += pd.Timedelta(days=1)

    # Remove unusable rows
    events = events[
        events["start"].notna()
        & events["end"].notna()
        & events["volume_gal"].notna()
        & (events["volume_gal"] > 0)
    ].copy()

    events.sort_values("start", inplace=True)
    events.reset_index(drop=True, inplace=True)

    # Cache final result
    _IRRIGATION_CACHE[cache_key] = events

    return events.copy()