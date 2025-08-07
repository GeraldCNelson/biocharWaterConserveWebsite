
import os
import numpy as np
import pandas as pd
from datetime import datetime, date
from typing import Any, Optional, Tuple, Dict,Sequence
import re
from flask import abort
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


def sanitize_json(obj: Any) -> Any:
    """
    Recursively walk through `obj` and convert everything into plain Python primitives
    that Flask’s `jsonify` can handle:
      - pd.Timestamp → ISO string
      - datetime/date   → ISO string
      - NumPy scalar    → native Python scalar
      - NumPy array     → Python list
      - dict            → sanitize each key/value
      - list/tuple      → sanitize each element
      - any other type  → str(obj)
    """
    # Already‐safe primitives
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

    # dict → sanitize each key‐value
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


def common_xaxis_config(
    granularity: str,
    start: Optional[str] = None,
    end:   Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the common x-axis config for time series.
    If granularity == "gseason", we leave it as a free date axis.
    Otherwise, if start/end are provided, we set a fixed range.
    """
    cfg: Dict[str, Any] = {
        "title":    "Date",
        "showgrid": True,
        "type":     "date",
        "tickformat": "%b %Y",
        "tickmode":   "linear",
        "dtick":      "M1",
        "showline":   True,
        "linecolor":  "black",
        "linewidth":  1,
    }
    # only set a numeric range if it's not gseason
    if granularity.lower() != "gseason" and start and end:
        # Plotly wants an array of two strings here
        cfg["range"] = [start, end]
    return cfg


def common_yaxis_config(
        kind: str,
        variable: str,
        unit_system: str,
        global_min: Optional[float],
        global_max: Optional[float],
) -> Dict[str, Any]:
    """
    Build a yaxis (or yaxis2) config dict for Plotly.
    Raises a clear HTTP 400 if the data range is invalid.
    """
    # 1) Make sure we actually have numbers

    # 1) Missing entirely?
    if global_min is None:
        raise HTTPException(400, f"Cannot build y-axis: global_min is None")
    if global_max is None:
        raise HTTPException(400, f"Cannot build y-axis: global_max is None")

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
    axis_cfg: Dict[str, Any] = {
        "title": {"text": f"{title_base}{f' ({unit})' if unit else ''}"
                          + (" Ratio" if kind == "ratio" else "")},
        "tickformat": None,
        "showgrid": True,
        "showline": True,
        "linecolor": "black",
        "linewidth": 1,
        "zeroline": (kind == "ratio"),
    }

    # 7) Only include a fixed range if both ends are still finite
    if np.isfinite(y_min) and np.isfinite(y_max):
        axis_cfg["range"] = [y_min, y_max]

    return axis_cfg

def common_yaxis2_config(unit_system: str = "us") -> dict[str, Any]:
    """
    Build a yaxis2 configuration dict for Plotly’s secondary axis
    (precipitation), overlayed on the right.
    """
    # choose label based on units
    unit_label = "mm" if unit_system == "metric" else "inches"

    return {
        # axis title
        "title": {"text": f"Precipitation ({unit_label})"},
        # overlay on the primary y
        "overlaying": "y",
        # show on the right side
        "side": "right",
        # don’t draw grid lines for this axis
        "showgrid": False,
        "showline": True,
        "linecolor": "black",
        "linewidth": 1,
        # always start at zero
        "rangemode": "tozero",
        "tickformat": ".0f",
    }


def common_legend_config(title: str = "Legend") -> dict[str, Any]:
    """
    Always put the legend in the top‐right.
    """
    return {
        "title":      {"text": title, "side": "top"},
        "orientation": "v",
        "yanchor":    "top",
        "y":          1.02,
        "xanchor":    "right",
        "x":          1,
    }


def get_unit_aware_label(variable: str, unit_system: str) -> str:
    """
    Given a “variable key” (e.g. "T" or "VWC" or "precip_in" or "irrigation"),
    and a unit_system ("us" or "metric"), return the correct axis‐label text
    from label_name_mapping (falling back if needed).
    """
    label = label_name_mapping.get(variable, variable)
    if isinstance(label, dict):
        return label.get(unit_system, variable)

    # If the value is just a string (not a dict), then do a best‐guess replacement:
    if unit_system == "metric":
        return (
            label
            .replace("(°F)", "(°C)")
            .replace("(inches)", "(cm)")
            .replace("(in)", "(cm)")
            .replace("(in)", "(mm)")
        )
    return label

def convert_units_for_download(df: pd.DataFrame, unit_system: str) -> pd.DataFrame:
    """
    Scan through the entire DataFrame’s columns (e.g., if they contain “T_…_degF”
    or “precip_in” or “gallons” in their name) and convert each column
    into metric (if unit_system=="metric"). Used for the “Download Data” endpoint.
    """
    df_converted = df.copy()
    if unit_system != "metric":
        return df_converted

    conv_map = UNIT_CONVERSIONS.get("us_to_metric", {})

    for col in df_converted.columns:
        # e.g. all “temp_air_degF” → apply “temp” conversion
        if (
            ("temp_air_degF" in col or "soil_temp_2in_degF" in col or
             "soil_temp_6in_degF" in col or col.startswith("T_"))
            and "temp" in conv_map
        ):
            df_converted[col] = df_converted[col].apply(conv_map["temp"])

        # precipitation columns, e.g. “precip_in”
        elif "precip_in" in col and "precip" in conv_map:
            df_converted[col] = df_converted[col].apply(conv_map["precip"])

        # irrigation columns: we only ever have “volume_gal” in the CSV; convert to “000 L”
        elif "volume_gal" in col and "irrigation" in conv_map:
            df_converted[col] = df_converted[col].apply(conv_map["irrigation"])

    return df_converted

def parse_sensor_column(col: str, unit_system: str) -> dict[str, str]:
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

def load_irrigation_events(strip: str, year: int) -> pd.DataFrame:
    """
    Reads biochar_app/data-processed/Harmonized_Irrigation_Data_{year}.csv,
    parses start_time/end_time/gallons, filters by west/east group,
    fixes any overnight events (end < start), and returns a DataFrame with
    columns: start, end, volume_gal (gal).
    """
    path = f"biochar_app/data-processed/Harmonized_Irrigation_Data_{year}.csv"
    if not os.path.exists(path):
        return pd.DataFrame(columns=["start", "end", "volume_gal"])

    df = pd.read_csv(path)
    df["start"]       = pd.to_datetime(df["start_time"], format="%m/%d/%y %H:%M")
    df["end"]         = pd.to_datetime(df["end_time"],   format="%m/%d/%y %H:%M")
    df["volume_gal"]  = pd.to_numeric(df["gallons"], errors="coerce").fillna(0)

    # Filter by strip group: west=S1/S2, east=S3/S4
    group = "west" if strip in ["S1", "S2"] else "east"
    df = df[df["location"] == group].copy()

    # Fix overnight events
    mask = df["end"] < df["start"]
    if mask.any():
        df.loc[mask, "end"] += pd.Timedelta(days=1)

    return df.loc[:, ["start", "end", "volume_gal"]].copy()

