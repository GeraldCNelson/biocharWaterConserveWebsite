# irrigation_analysis.py
"""
Irrigation response analysis utilities.

Purpose
-------
This module analyzes how soil-water sensors respond to irrigation events. It is
not the primary raw-to-clean irrigation ETL script.

Expected inputs
---------------
1. A 15-minute logger dataframe with a DatetimeIndex.
   Important columns include bottom-logger VWC variables such as:
       VWC_1_raw_S1_B
       VWC_2_raw_S1_B
       VWC_3_raw_S1_B

2. A cleaned irrigation-events dataframe, usually loaded through:
       biochar_app.scripts.data_loading.load_irrigation_data()

   Expected clean irrigation columns include:
       strip
       year
       start_timestamp
       end_timestamp
       gallons_strip
       gallons_group

Terminology
-----------
gallons_group
    Water delivered to a strip pair/group, such as S1_S2 or S3_S4.

gallons_strip
    Estimated water assigned to one individual strip. This is the preferred
    volume field for strip-level soil-water response analysis.

Main outputs
------------
The module can produce event-level and summary-level irrigation diagnostics,
including:
    baseline_vwc
    peak_vwc
    plateau_vwc
    time_to_peak_hours
    time_to_plateau_hours
    event_duration_hours
    avg_flow_gph_strip
    target_vwc
    runtime recommendations
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, TypedDict, cast

import numpy as np
import pandas as pd

from biochar_app.config.field_management_metadata import (
    PROFILE_AREA_SQFT,
)
profile_area_sqft = PROFILE_AREA_SQFT

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class PlateauConfig:
    search_hours_after_peak: float = 16.0
    slope_window_points: int = 4
    plateau_abs_slope_threshold: float = 0.12
    min_consecutive_plateau_points: int = 4
    plateau_summary_window_points: int = 4
    fallback_hours_after_peak: float = 4.0


@dataclass(frozen=True)
class EventSearchConfig:
    baseline_lookback_hours: float = 2.0
    peak_search_hours_after_start: float = 24.0
    min_peak_increase: float = 0.5


@dataclass(frozen=True)
class TargetConfig:
    k_std: float = 0.5
    min_events: int = 3
    upper_quantile_cap: float = 0.95


# ---------------------------------------------------------------------
# Typed structures
# ---------------------------------------------------------------------


class SensorMeta(TypedDict):
    variable: Optional[str]
    depth_index: Optional[str]
    strip_from_col: Optional[str]
    logger_position: Optional[str]
    is_valid_vwc_sensor: bool


# ---------------------------------------------------------------------
# Sensor helpers
# ---------------------------------------------------------------------


_VWC_SENSOR_RE = re.compile(
    r"^(?P<variable>VWC)_(?P<depth_index>[123])_raw_(?P<strip>S[1-4])_(?P<logger_position>[TMB])$"
)

DEPTH_INDEX_TO_INCHES: Dict[str, int] = {
    "1": 6,
    "2": 12,
    "3": 18,
}


def build_bottom_control_sensor_map() -> Dict[str, str]:
    return {
        "S1": "VWC_3_raw_S1_B",
        "S2": "VWC_3_raw_S2_B",
        "S3": "VWC_3_raw_S3_B",
        "S4": "VWC_3_raw_S4_B",
    }


def build_bottom_logger_profile_map() -> Dict[str, List[str]]:
    return {
        "S1": ["VWC_1_raw_S1_B", "VWC_2_raw_S1_B", "VWC_3_raw_S1_B"],
        "S2": ["VWC_1_raw_S2_B", "VWC_2_raw_S2_B", "VWC_3_raw_S2_B"],
        "S3": ["VWC_1_raw_S3_B", "VWC_2_raw_S3_B", "VWC_3_raw_S3_B"],
        "S4": ["VWC_1_raw_S4_B", "VWC_2_raw_S4_B", "VWC_3_raw_S4_B"],
    }


def build_bottom_profile_sensor_map(depth_index: int = 3) -> Dict[str, str]:
    if depth_index not in {1, 2, 3}:
        raise ValueError("depth_index must be 1, 2, or 3")

    return {
        "S1": f"VWC_{depth_index}_raw_S1_B",
        "S2": f"VWC_{depth_index}_raw_S2_B",
        "S3": f"VWC_{depth_index}_raw_S3_B",
        "S4": f"VWC_{depth_index}_raw_S4_B",
    }


def build_all_bottom_sensor_cols() -> List[str]:
    return [
        f"VWC_{depth_index}_raw_{strip}_B"
        for strip in ["S1", "S2", "S3", "S4"]
        for depth_index in [1, 2, 3]
    ]


def parse_vwc_sensor_column(sensor_col: str) -> SensorMeta:
    result: SensorMeta = {
        "variable": None,
        "depth_index": None,
        "strip_from_col": None,
        "logger_position": None,
        "is_valid_vwc_sensor": False,
    }

    match = _VWC_SENSOR_RE.match(sensor_col)
    if not match:
        return result

    result["variable"] = match.group("variable")
    result["depth_index"] = match.group("depth_index")
    result["strip_from_col"] = match.group("strip")
    result["logger_position"] = match.group("logger_position")
    result["is_valid_vwc_sensor"] = True
    return result


def _build_profile_swc_gal_cols(strip: str, logger_position: str) -> List[str]:
    return [
        f"SWC_vol_gal_{strip}_{logger_position}_1",
        f"SWC_vol_gal_{strip}_{logger_position}_2",
        f"SWC_vol_gal_{strip}_{logger_position}_3",
    ]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _is_missing(value: object) -> bool:
    return bool(pd.Series([value]).isna().iloc[0])


def _as_float_or_none(value: object) -> Optional[float]:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if _is_missing(num):
        return None
    return float(num)


def _coerce_optional_timestamp(value: object) -> Optional[pd.Timestamp]:
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        return None if _is_missing(value) else value

    ts = pd.to_datetime(cast(Any, value), errors="coerce")
    if _is_missing(ts):
        return None

    return pd.Timestamp(ts)


def _validate_datetime_index(df: pd.DataFrame) -> None:
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Input dataframe must have a DatetimeIndex.")
    if df.index.has_duplicates:
        raise ValueError("Input dataframe index contains duplicate timestamps.")
    if not df.index.is_monotonic_increasing:
        raise ValueError("Input dataframe index must be sorted ascending.")


def _coerce_datetime_column(events: pd.DataFrame, column: str) -> pd.Series:
    if column not in events.columns:
        raise KeyError(f"Required events column not found: {column}")
    return pd.to_datetime(events[column], errors="coerce")


def _hours_from_timedelta(td: pd.Timedelta) -> float:
    return td.total_seconds() / 3600.0


def _infer_step_minutes(index: pd.DatetimeIndex) -> float:
    diffs = index.to_series().diff().dropna()
    if diffs.empty:
        return float("nan")
    return float(diffs.dt.total_seconds().median() / 60.0)


def _run_lengths(mask: pd.Series) -> pd.Series:
    groups = (mask != mask.shift()).cumsum()
    lengths = mask.groupby(groups).transform("sum")
    return lengths.where(mask, 0)


def _group_iter(
    df: pd.DataFrame,
    group_cols: Sequence[str],
) -> Iterable[Tuple[Any, pd.DataFrame]]:
    if not group_cols:
        return [((), df)]
    return cast(
        Iterable[Tuple[Any, pd.DataFrame]],
        df.groupby(list(group_cols), dropna=False),
    )


def _safe_value_at_timestamp(
    df: pd.DataFrame,
    timestamp: Optional[pd.Timestamp],
    column: str,
) -> Optional[float]:
    if timestamp is None or column not in df.columns or timestamp not in df.index:
        return None

    return _as_float_or_none(df.at[timestamp, column])


# ---------------------------------------------------------------------
# Variable definitions
# ---------------------------------------------------------------------


def build_variable_definitions_table() -> pd.DataFrame:
    rows = [
        {
            "variable": "baseline_vwc",
            "definition": "Pre-irrigation reference VWC for one event.",
            "formula_or_rule": (
                "Median VWC over the lookback window before irrigation_start; "
                "baseline_time is the last timestamp in that lookback window."
            ),
        },
        {
            "variable": "peak_vwc",
            "definition": "Maximum post-irrigation VWC found in the search window for one event.",
            "formula_or_rule": "Maximum VWC after irrigation_start within the peak search window.",
        },
        {
            "variable": "peak_increase",
            "definition": "Increase from baseline to peak for one event.",
            "formula_or_rule": "peak_vwc - baseline_vwc",
        },
        {
            "variable": "plateau_vwc",
            "definition": "Estimated stabilized VWC below the maximum peak after irrigation for one event.",
            "formula_or_rule": (
                "Median VWC over the detected plateau window; if no flat segment is found, "
                "use the fallback window after peak."
            ),
        },
        {
            "variable": "time_to_peak_hours",
            "definition": "Elapsed time from irrigation_start to peak VWC for one event.",
            "formula_or_rule": "peak_time - irrigation_start, in hours",
        },
        {
            "variable": "time_to_plateau_hours",
            "definition": "Elapsed time from irrigation_start to plateau attainment for one event.",
            "formula_or_rule": "plateau_time - irrigation_start, in hours",
        },
        {
            "variable": "lag_after_irrigation_hr",
            "definition": "Additional time after irrigation shutoff before plateau is reached for one event.",
            "formula_or_rule": "time_to_plateau_hours - event_duration_hours",
        },
        {
            "variable": "event_duration_hours",
            "definition": "Length of time irrigation water was running for one event.",
            "formula_or_rule": "irrigation_end - irrigation_start, in hours",
        },
        {
            "variable": "gallons_group",
            "definition": "Water delivered to the active strip pair/group.",
            "formula_or_rule": "Read from clean irrigation events table.",
        },
        {
            "variable": "gallons_strip",
            "definition": "Water assigned to one individual strip.",
            "formula_or_rule": "gallons_group * strip_allocation_fraction",
        },
        {
            "variable": "avg_flow_gph_strip",
            "definition": "Average strip-level application flow rate during one irrigation event.",
            "formula_or_rule": "gallons_strip / event_duration_hours",
        },
        {
            "variable": "plateau_method",
            "definition": "Method used to estimate plateau_vwc for one event.",
            "formula_or_rule": (
                "flat_segment = sustained low-slope zone found; "
                "fallback_window = delayed post-peak summary window used; "
                "no_peak = no meaningful wetting peak detected."
            ),
        },
        {
            "variable": "depth_index",
            "definition": "Probe depth index in the logger naming convention.",
            "formula_or_rule": "1 = 6 in, 2 = 12 in, 3 = 18 in",
        },
        {
            "variable": "depth_inches",
            "definition": "Probe depth in inches.",
            "formula_or_rule": "Mapped from depth_index using DEPTH_INDEX_TO_INCHES[depth_index]",
        },
        {
            "variable": "event_storage_gal",
            "definition": "Change in estimated profile water storage during one event.",
            "formula_or_rule": "profile_plateau_storage_gal - profile_baseline_storage_gal",
        },
        {
            "variable": "efficiency_strip",
            "definition": "Fraction of strip-applied irrigation volume retained in measured profile storage.",
            "formula_or_rule": "event_storage_gal / gallons_strip",
        },
        {
            "variable": "estimated_loss_gal_strip",
            "definition": "Applied strip volume not accounted for as measured profile storage gain.",
            "formula_or_rule": "gallons_strip - event_storage_gal",
        },
        {
            "variable": "target_vwc",
            "definition": "Operational target VWC based on historical plateau responses.",
            "formula_or_rule": "min(mean_value + k_std * sd_value, quantile_cap)",
        },
        {
            "variable": "runtime_hours",
            "definition": "Recommended runtime in hours based on historical event response.",
            "formula_or_rule": "Median, mean, or p75 of time_to_plateau_hours within each group",
        },
        {
            "variable": "runtime_minutes",
            "definition": "Recommended runtime in minutes based on historical event response.",
            "formula_or_rule": "runtime_hours * 60",
        },
    ]

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------
# Event storage helpers
# ---------------------------------------------------------------------


def compute_event_storage_metrics(
    df: pd.DataFrame,
    sensor_meta: SensorMeta,
    baseline_time: Optional[pd.Timestamp],
    plateau_time: Optional[pd.Timestamp],
    gallons_strip: Optional[float],
) -> Dict[str, object]:
    strip_from_col = sensor_meta["strip_from_col"]
    logger_position = sensor_meta["logger_position"]

    if strip_from_col is None or logger_position is None:
        return {
            "profile_baseline_storage_gal": None,
            "profile_plateau_storage_gal": None,
            "event_storage_gal": None,
            "efficiency_strip": None,
            "estimated_loss_gal_strip": None,
        }

    swc_cols = _build_profile_swc_gal_cols(strip_from_col, logger_position)
    print("\n=== SWC DEBUG ===")
    print("sensor:", sensor_meta.get("sensor_col"))
    print("strip:", strip_from_col)
    print("logger_position:", logger_position)
    print("swc_cols:", swc_cols)

    existing_cols = [c for c in swc_cols if c in df.columns]
    missing_cols = [c for c in swc_cols if c not in df.columns]

    print("existing_cols:", existing_cols)
    print("missing_cols:", missing_cols)
    baseline_values = [
        value
        for col in swc_cols
        if (value := _safe_value_at_timestamp(df, baseline_time, col)) is not None
    ]
    plateau_values = [
        value
        for col in swc_cols
        if (value := _safe_value_at_timestamp(df, plateau_time, col)) is not None
    ]

    profile_baseline_storage_gal = float(sum(baseline_values)) if baseline_values else None
    profile_plateau_storage_gal = float(sum(plateau_values)) if plateau_values else None

    event_storage_gal: Optional[float] = None
    if profile_baseline_storage_gal is not None and profile_plateau_storage_gal is not None:
        event_storage_gal = profile_plateau_storage_gal - profile_baseline_storage_gal

    efficiency_strip: Optional[float] = None
    estimated_loss_gal_strip: Optional[float] = None

    if gallons_strip is not None and gallons_strip > 0 and event_storage_gal is not None:
        efficiency_strip = event_storage_gal / gallons_strip
        estimated_loss_gal_strip = gallons_strip - event_storage_gal

    return {
        "profile_baseline_storage_gal": profile_baseline_storage_gal,
        "profile_plateau_storage_gal": profile_plateau_storage_gal,
        "event_storage_gal": event_storage_gal,
        "efficiency_strip": efficiency_strip,
        "estimated_loss_gal_strip": estimated_loss_gal_strip,
    }


# ---------------------------------------------------------------------
# Core event logic
# ---------------------------------------------------------------------


def find_event_baseline(
    series: pd.Series,
    irrigation_start: pd.Timestamp,
    baseline_lookback_hours: float = 2.0,
) -> Tuple[Optional[float], Optional[pd.Timestamp]]:
    window_start = irrigation_start - pd.Timedelta(hours=baseline_lookback_hours)
    sub = series.loc[window_start:irrigation_start].dropna()

    if sub.empty:
        return None, None

    baseline_time = pd.Timestamp(sub.index[-1])
    return float(sub.median()), baseline_time


def find_event_peak(
    series: pd.Series,
    irrigation_start: pd.Timestamp,
    peak_search_hours_after_start: float = 24.0,
    min_peak_increase: float = 0.5,
    baseline_vwc: Optional[float] = None,
) -> Dict[str, object]:
    end = irrigation_start + pd.Timedelta(hours=peak_search_hours_after_start)
    sub = series.loc[irrigation_start:end].dropna()

    if sub.empty:
        return {
            "peak_vwc": None,
            "peak_time": None,
            "peak_increase": None,
            "peak_found": False,
        }

    peak_time = pd.Timestamp(sub.idxmax())
    peak_vwc = float(sub.loc[peak_time])
    peak_increase = None if baseline_vwc is None else peak_vwc - baseline_vwc

    peak_found = peak_increase is None or peak_increase >= min_peak_increase

    return {
        "peak_vwc": peak_vwc,
        "peak_time": peak_time,
        "peak_increase": peak_increase,
        "peak_found": peak_found,
    }


def find_post_peak_plateau(
    series: pd.Series,
    peak_time: pd.Timestamp,
    config: PlateauConfig,
) -> Dict[str, object]:
    search_end = peak_time + pd.Timedelta(hours=config.search_hours_after_peak)
    sub = series.loc[peak_time:search_end].dropna()

    min_needed = max(
        config.slope_window_points,
        config.min_consecutive_plateau_points,
        config.plateau_summary_window_points,
    )
    if len(sub) < min_needed:
        return {
            "plateau_vwc": None,
            "plateau_time": None,
            "plateau_method": "insufficient_data",
        }

    if not isinstance(sub.index, pd.DatetimeIndex):
        return {
            "plateau_vwc": None,
            "plateau_time": None,
            "plateau_method": "unknown_index",
        }

    step_minutes = _infer_step_minutes(sub.index)
    if np.isnan(step_minutes) or step_minutes <= 0:
        return {
            "plateau_vwc": None,
            "plateau_time": None,
            "plateau_method": "unknown_step",
        }

    smoothed = sub.rolling(
        config.slope_window_points,
        center=True,
        min_periods=1,
    ).mean()

    slope_per_hour = smoothed.diff() / (step_minutes / 60.0)
    flat_mask = slope_per_hour.abs() <= config.plateau_abs_slope_threshold
    flat_run_lengths = _run_lengths(flat_mask)
    qualifying = flat_mask & (flat_run_lengths >= config.min_consecutive_plateau_points)

    if qualifying.any():
        qualifying_index = qualifying[qualifying].index
        plateau_time = pd.Timestamp(qualifying_index[0])
        plateau_window = sub.loc[plateau_time:].iloc[: config.plateau_summary_window_points]

        if not plateau_window.empty:
            return {
                "plateau_vwc": float(plateau_window.median()),
                "plateau_time": plateau_time,
                "plateau_method": "flat_segment",
            }

    fallback_start = peak_time + pd.Timedelta(hours=config.fallback_hours_after_peak)
    fallback_window = sub.loc[fallback_start:].iloc[: config.plateau_summary_window_points]

    if fallback_window.empty:
        return {
            "plateau_vwc": None,
            "plateau_time": None,
            "plateau_method": "fallback_empty",
        }

    fallback_time = pd.Timestamp(fallback_window.index[0])
    return {
        "plateau_vwc": float(fallback_window.median()),
        "plateau_time": fallback_time,
        "plateau_method": "fallback_window",
    }


def analyze_single_event_sensor(
    df: pd.DataFrame,
    sensor_col: str,
    irrigation_start: pd.Timestamp,
    irrigation_end: Optional[pd.Timestamp] = None,
    gallons_strip: Optional[float] = None,
    gallons_group: Optional[float] = None,
    strip: Optional[str] = None,
    year: Optional[int] = None,
    event_id: Optional[object] = None,
    search_config: Optional[EventSearchConfig] = None,
    plateau_config: Optional[PlateauConfig] = None,
) -> Dict[str, object]:
    _validate_datetime_index(df)

    search_config = search_config or EventSearchConfig()
    plateau_config = plateau_config or PlateauConfig()

    if sensor_col not in df.columns:
        raise KeyError(f"Sensor column not found in dataframe: {sensor_col}")

    series = pd.to_numeric(df[sensor_col], errors="coerce")
    sensor_meta = parse_vwc_sensor_column(sensor_col)

    baseline_vwc, baseline_time = find_event_baseline(
        series=series,
        irrigation_start=irrigation_start,
        baseline_lookback_hours=search_config.baseline_lookback_hours,
    )

    peak_info = find_event_peak(
        series=series,
        irrigation_start=irrigation_start,
        peak_search_hours_after_start=search_config.peak_search_hours_after_start,
        min_peak_increase=search_config.min_peak_increase,
        baseline_vwc=baseline_vwc,
    )

    peak_time_obj = peak_info.get("peak_time")
    peak_found_obj = bool(peak_info.get("peak_found", False))

    plateau_info: Dict[str, object] = {
        "plateau_vwc": None,
        "plateau_time": None,
        "plateau_method": "no_peak",
    }

    if peak_found_obj and isinstance(peak_time_obj, pd.Timestamp):
        plateau_info = find_post_peak_plateau(
            series=series,
            peak_time=peak_time_obj,
            config=plateau_config,
        )

    time_to_peak_hours = (
        _hours_from_timedelta(peak_time_obj - irrigation_start)
        if isinstance(peak_time_obj, pd.Timestamp)
        else None
    )

    plateau_time_obj = plateau_info.get("plateau_time")
    time_to_plateau_hours = (
        _hours_from_timedelta(plateau_time_obj - irrigation_start)
        if isinstance(plateau_time_obj, pd.Timestamp)
        else None
    )

    event_duration_hours = (
        _hours_from_timedelta(irrigation_end - irrigation_start)
        if irrigation_end is not None
        else None
    )

    avg_flow_gph_strip = (
        gallons_strip / event_duration_hours
        if gallons_strip is not None and event_duration_hours is not None and event_duration_hours > 0
        else None
    )

    storage_metrics = compute_event_storage_metrics(
        df=df,
        sensor_meta=sensor_meta,
        baseline_time=baseline_time,
        plateau_time=plateau_time_obj if isinstance(plateau_time_obj, pd.Timestamp) else None,
        gallons_strip=gallons_strip,
    )

    return {
        "event_id": event_id,
        "strip": strip,
        "year": year,
        "sensor_col": sensor_col,
        "variable": sensor_meta["variable"],
        "depth_index": sensor_meta["depth_index"],
        "strip_from_col": sensor_meta["strip_from_col"],
        "logger_position": sensor_meta["logger_position"],
        "is_valid_vwc_sensor": sensor_meta["is_valid_vwc_sensor"],
        "irrigation_start": irrigation_start,
        "irrigation_end": irrigation_end,
        "gallons_group": gallons_group,
        "gallons_strip": gallons_strip,
        "event_duration_hours": event_duration_hours,
        "avg_flow_gph_strip": avg_flow_gph_strip,
        "baseline_time": baseline_time,
        "baseline_vwc": baseline_vwc,
        "peak_time": peak_info.get("peak_time"),
        "peak_vwc": peak_info.get("peak_vwc"),
        "peak_increase": peak_info.get("peak_increase"),
        "peak_found": peak_info.get("peak_found"),
        "plateau_time": plateau_info.get("plateau_time"),
        "plateau_vwc": plateau_info.get("plateau_vwc"),
        "plateau_method": plateau_info.get("plateau_method"),
        "time_to_peak_hours": time_to_peak_hours,
        "time_to_plateau_hours": time_to_plateau_hours,
        "profile_baseline_storage_gal": storage_metrics["profile_baseline_storage_gal"],
        "profile_plateau_storage_gal": storage_metrics["profile_plateau_storage_gal"],
        "event_storage_gal": storage_metrics["event_storage_gal"],
        "efficiency_strip": storage_metrics["efficiency_strip"],
        "estimated_loss_gal_strip": storage_metrics["estimated_loss_gal_strip"],
    }


def analyze_irrigation_events(
    df: pd.DataFrame,
    events: pd.DataFrame,
    sensor_cols: Sequence[str],
    start_col: str = "start",
    end_col: str = "end",
    gallons_strip_col: str = "gallons_strip",
    gallons_group_col: Optional[str] = "gallons_group",
    strip: Optional[str] = None,
    year: Optional[int] = None,
    event_id_col: Optional[str] = None,
    search_config: Optional[EventSearchConfig] = None,
    plateau_config: Optional[PlateauConfig] = None,
    layer_thickness_inches: float = 6.0,
) -> pd.DataFrame:
    """
    Analyze irrigation events for selected VWC sensors.

    Unit note
    ---------
    This function now corrects the old misleading profile-storage fields.

    The VWC-derived profile storage is first calculated as equivalent water
    depth over the instrumented profile:

        profile_storage_in = sum((VWC_pct / 100) * layer_thickness_inches)

    By default, each depth sensor is assumed to represent a 6-inch layer:
        depth 1 = 0-6 in
        depth 2 = 6-12 in
        depth 3 = 12-18 in

    If profile_area_sqft is provided, equivalent water depth is converted to
    gallons:

        gallons = (profile_storage_in / 12) * profile_area_sqft * 7.48051945

    """
    _validate_datetime_index(df)

    search_config = search_config or EventSearchConfig()
    plateau_config = plateau_config or PlateauConfig()

    events_local = events.copy()
    events_local[start_col] = _coerce_datetime_column(events_local, start_col)
    events_local[end_col] = _coerce_datetime_column(events_local, end_col)

    if gallons_strip_col not in events_local.columns:
        raise KeyError(
            f"Required strip-level irrigation volume column not found: {gallons_strip_col!r}"
        )

    events_local[gallons_strip_col] = pd.to_numeric(
        events_local[gallons_strip_col],
        errors="coerce",
    )

    if gallons_group_col and gallons_group_col in events_local.columns:
        events_local[gallons_group_col] = pd.to_numeric(
            events_local[gallons_group_col],
            errors="coerce",
        )
    else:
        gallons_group_col = None

    rows: List[Dict[str, object]] = []

    for i, ev in events_local.iterrows():
        irrigation_start = _coerce_optional_timestamp(ev[start_col])
        if irrigation_start is None:
            logger.warning("Skipping irrigation event with invalid start time: row=%s", i)
            continue

        irrigation_end = _coerce_optional_timestamp(ev[end_col])
        gallons_strip = _as_float_or_none(ev[gallons_strip_col])

        gallons_group = None
        if gallons_group_col is not None:
            gallons_group = _as_float_or_none(ev[gallons_group_col])

        event_id = ev[event_id_col] if event_id_col and event_id_col in ev else i

        for sensor_col in sensor_cols:
            rows.append(
                analyze_single_event_sensor(
                    df=df,
                    sensor_col=sensor_col,
                    irrigation_start=irrigation_start,
                    irrigation_end=irrigation_end,
                    gallons_strip=gallons_strip,
                    gallons_group=gallons_group,
                    strip=strip,
                    year=year,
                    event_id=event_id,
                    search_config=search_config,
                    plateau_config=plateau_config,
                )
            )

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)

    required_cols = {
        "event_id",
        "strip",
        "year",
        "logger_position",
        "depth_index",
        "baseline_vwc",
        "plateau_vwc",
        "gallons_strip",
    }

    if not required_cols.issubset(out.columns):
        return out

    out["depth_index"] = out["depth_index"].astype("string")
    out["baseline_vwc"] = pd.to_numeric(out["baseline_vwc"], errors="coerce")
    out["plateau_vwc"] = pd.to_numeric(out["plateau_vwc"], errors="coerce")
    out["gallons_strip"] = pd.to_numeric(out["gallons_strip"], errors="coerce")

    out["baseline_storage_in_layer"] = (
        out["baseline_vwc"] / 100.0 * layer_thickness_inches
    )
    out["plateau_storage_in_layer"] = (
        out["plateau_vwc"] / 100.0 * layer_thickness_inches
    )
    out["event_storage_in_layer"] = (
        out["plateau_storage_in_layer"] - out["baseline_storage_in_layer"]
    )

    group_cols = [
        "event_id",
        "strip",
        "year",
        "logger_position",
        "irrigation_start",
        "irrigation_end",
    ]

    profile = (
        out.groupby(group_cols, dropna=False)
        .agg(
            profile_baseline_storage_in=(
                "baseline_storage_in_layer",
                "sum",
            ),
            profile_plateau_storage_in=(
                "plateau_storage_in_layer",
                "sum",
            ),
            event_storage_in=(
                "event_storage_in_layer",
                "sum",
            ),
            n_profile_depths_used=(
                "event_storage_in_layer",
                lambda s: int(pd.to_numeric(s, errors="coerce").notna().sum()),
            ),
            gallons_strip_profile=(
                "gallons_strip",
                "first",
            ),
        )
        .reset_index()
    )

    if PROFILE_AREA_SQFT > 0:
        gallons_per_cubic_foot = 7.48051945

        profile["profile_area_sqft"] = float(PROFILE_AREA_SQFT)
        profile["profile_baseline_storage_gal"] = (
            profile["profile_baseline_storage_in"] / 12.0
            * float(PROFILE_AREA_SQFT)
            * gallons_per_cubic_foot
        )
        profile["profile_plateau_storage_gal"] = (
            profile["profile_plateau_storage_in"] / 12.0
            * float(PROFILE_AREA_SQFT)
            * gallons_per_cubic_foot
        )
        profile["event_storage_gal"] = (
            profile["event_storage_in"] / 12.0
            * float(PROFILE_AREA_SQFT)
            * gallons_per_cubic_foot
        )

        profile["efficiency_strip"] = (
            profile["event_storage_gal"] / profile["gallons_strip_profile"]
        )
        profile.loc[
            profile["gallons_strip_profile"] <= 0,
            "efficiency_strip",
        ] = pd.NA

        profile["estimated_loss_gal_strip"] = (
            profile["gallons_strip_profile"] - profile["event_storage_gal"]
        )
    else:
        profile["profile_area_sqft"] = pd.NA
        profile["profile_baseline_storage_gal"] = pd.NA
        profile["profile_plateau_storage_gal"] = pd.NA
        profile["event_storage_gal"] = pd.NA
        profile["efficiency_strip"] = pd.NA
        profile["estimated_loss_gal_strip"] = pd.NA

    replace_cols = [
        "profile_area_sqft",
        "profile_baseline_storage_in",
        "profile_plateau_storage_in",
        "event_storage_in",
        "n_profile_depths_used",
        "profile_baseline_storage_gal",
        "profile_plateau_storage_gal",
        "event_storage_gal",
        "efficiency_strip",
        "estimated_loss_gal_strip",
    ]

    out = out.drop(columns=[c for c in replace_cols if c in out.columns], errors="ignore")

    out = out.merge(
        profile[group_cols + replace_cols],
        on=group_cols,
        how="left",
    )

    return out


def analyze_bottom_logger_controls(
    df_15min: pd.DataFrame,
    strips: Sequence[str],
    year: int,
    strip_to_bottom_sensor: Optional[Dict[str, str]] = None,
    search_config: Optional[EventSearchConfig] = None,
    plateau_config: Optional[PlateauConfig] = None,
) -> pd.DataFrame:
    from biochar_app.scripts.data_loading import load_irrigation_data

    if strip_to_bottom_sensor is None:
        strip_to_bottom_sensor = build_bottom_control_sensor_map()

    all_results: List[pd.DataFrame] = []
    all_events = load_irrigation_data()

    required_cols = {
        "strip",
        "year",
        "start_timestamp",
        "end_timestamp",
        "gallons_strip",
    }
    missing = required_cols - set(all_events.columns)
    if missing:
        raise KeyError(
            f"Clean irrigation data is missing required columns: {sorted(missing)}"
        )

    for strip in strips:
        sensor_col = strip_to_bottom_sensor.get(strip)
        if not sensor_col or sensor_col not in df_15min.columns:
            logger.debug("Skipping %s: bottom sensor not available.", strip)
            continue

        select_cols = ["start_timestamp", "end_timestamp", "gallons_strip"]
        if "gallons_group" in all_events.columns:
            select_cols.append("gallons_group")
        if "event_id" in all_events.columns:
            select_cols.append("event_id")

        events = all_events.loc[
            (all_events["strip"] == strip) & (all_events["year"] == year),
            select_cols,
        ].copy()

        if events.empty:
            logger.debug("Skipping %s for %s: no irrigation events found.", strip, year)
            continue

        events = events.rename(
            columns={
                "start_timestamp": "start",
                "end_timestamp": "end",
            }
        )

        strip_results = analyze_irrigation_events(
            df=df_15min,
            events=events,
            sensor_cols=[sensor_col],
            start_col="start",
            end_col="end",
            gallons_strip_col="gallons_strip",
            gallons_group_col="gallons_group" if "gallons_group" in events.columns else None,
            strip=strip,
            year=year,
            event_id_col="event_id" if "event_id" in events.columns else None,
            search_config=search_config,
            plateau_config=plateau_config,
        )

        if not strip_results.empty:
            all_results.append(strip_results)

    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()


# ---------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------


def add_derived_event_fields(event_results: pd.DataFrame) -> pd.DataFrame:
    if event_results.empty:
        return event_results.copy()

    out = event_results.copy()

    if "depth_index" in out.columns:
        out["depth_index"] = out["depth_index"].astype("string")
        out["depth_inches"] = out["depth_index"].map(DEPTH_INDEX_TO_INCHES)
    else:
        out["depth_inches"] = pd.NA

    plateau_hours = (
        pd.to_numeric(out["time_to_plateau_hours"], errors="coerce")
        if "time_to_plateau_hours" in out.columns
        else pd.Series(pd.NA, index=out.index, dtype="Float64")
    )

    duration_hours = (
        pd.to_numeric(out["event_duration_hours"], errors="coerce")
        if "event_duration_hours" in out.columns
        else pd.Series(pd.NA, index=out.index, dtype="Float64")
    )

    gallons_strip = (
        pd.to_numeric(out["gallons_strip"], errors="coerce")
        if "gallons_strip" in out.columns
        else pd.Series(pd.NA, index=out.index, dtype="Float64")
    )

    out["lag_after_irrigation_hr"] = plateau_hours - duration_hours
    out["avg_flow_gph_strip"] = gallons_strip / duration_hours
    out.loc[duration_hours <= 0, "avg_flow_gph_strip"] = pd.NA

    return out


def build_event_debug_table(
    event_results: pd.DataFrame,
    decimals: int = 2,
) -> pd.DataFrame:
    if event_results.empty:
        return pd.DataFrame()

    out = add_derived_event_fields(event_results)

    keep_cols = [
        "event_id",
        "strip",
        "year",
        "sensor_col",
        "depth_index",
        "depth_inches",
        "logger_position",
        "irrigation_start",
        "irrigation_end",
        "gallons_group",
        "gallons_strip",
        "event_duration_hours",
        "avg_flow_gph_strip",
        "baseline_vwc",
        "peak_vwc",
        "peak_increase",
        "plateau_vwc",
        "plateau_method",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "lag_after_irrigation_hr",
        "profile_baseline_storage_gal",
        "profile_plateau_storage_gal",
        "event_storage_gal",
        "efficiency_strip",
        "estimated_loss_gal_strip",
    ]

    out = out[[c for c in keep_cols if c in out.columns]].copy()

    numeric_cols = out.select_dtypes(include=[np.number]).columns
    out[numeric_cols] = out[numeric_cols].round(decimals)

    return out


# ---------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------


def estimate_statistical_target(
    event_results: pd.DataFrame,
    value_col: str = "plateau_vwc",
    group_cols: Optional[Sequence[str]] = None,
    target_config: Optional[TargetConfig] = None,
) -> pd.DataFrame:
    target_config = target_config or TargetConfig()
    group_cols = list(group_cols or [])

    if value_col not in event_results.columns:
        raise KeyError(f"Column not found: {value_col}")

    df = event_results.copy()
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.loc[df[value_col].notna()].copy()

    rows: List[Dict[str, object]] = []

    for group_key, sub in _group_iter(df, group_cols):
        vals = pd.to_numeric(sub[value_col], errors="coerce").dropna()
        if vals.empty:
            continue

        n_events = int(vals.shape[0])
        mean_value = float(vals.mean())
        sd_value = float(vals.std(ddof=1)) if n_events > 1 else 0.0
        raw_target_value = mean_value + target_config.k_std * sd_value
        quantile_cap = float(vals.quantile(target_config.upper_quantile_cap))
        target_value = min(raw_target_value, quantile_cap)

        row: Dict[str, object] = {
            "n_events": n_events,
            "mean_value": mean_value,
            "sd_value": sd_value,
            "raw_target_value": raw_target_value,
            "quantile_cap": quantile_cap,
            "target_value": target_value,
            "target_is_trustworthy": n_events >= target_config.min_events,
            "source_value_col": value_col,
            "k_std": target_config.k_std,
        }

        if group_cols:
            key_tuple = group_key if isinstance(group_key, tuple) else (group_key,)
            for col_name, val in zip(group_cols, key_tuple):
                row[col_name] = val

        rows.append(row)

    return pd.DataFrame(rows)


def recommend_runtime_from_history(
    event_results: pd.DataFrame,
    target_time_col: str = "time_to_plateau_hours",
    group_cols: Optional[Sequence[str]] = None,
    min_events: int = 3,
    summary_stat: str = "median",
) -> pd.DataFrame:
    group_cols = list(group_cols or [])

    if target_time_col not in event_results.columns:
        raise KeyError(f"Column not found in event results: {target_time_col}")

    df = event_results.copy()
    df[target_time_col] = pd.to_numeric(df[target_time_col], errors="coerce")
    df = df.loc[df[target_time_col].notna()].copy()

    rows: List[Dict[str, object]] = []

    for group_key, sub in _group_iter(df, group_cols):
        vals = pd.to_numeric(sub[target_time_col], errors="coerce").dropna()
        n_events = int(vals.shape[0])
        if n_events == 0:
            continue

        if summary_stat == "median":
            runtime_hours = float(vals.median())
        elif summary_stat == "mean":
            runtime_hours = float(vals.mean())
        elif summary_stat == "p75":
            runtime_hours = float(vals.quantile(0.75))
        else:
            raise ValueError("summary_stat must be one of: 'median', 'mean', 'p75'")

        row: Dict[str, object] = {
            "n_events": n_events,
            "runtime_hours": runtime_hours,
            "runtime_minutes": runtime_hours * 60.0,
            "runtime_is_trustworthy": n_events >= min_events,
            "source_time_col": target_time_col,
            "summary_stat": summary_stat,
        }

        if group_cols:
            key_tuple = group_key if isinstance(group_key, tuple) else (group_key,)
            for col_name, val in zip(group_cols, key_tuple):
                row[col_name] = val

        rows.append(row)

    return pd.DataFrame(rows)


def summarize_targets_and_runtimes(
    event_results: pd.DataFrame,
    group_cols: Sequence[str] = ("strip", "sensor_col"),
    min_events: int = 3,
    k_std: float = 0.5,
    runtime_summary_stat: str = "median",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    targets = estimate_statistical_target(
        event_results=event_results,
        value_col="plateau_vwc",
        group_cols=list(group_cols),
        target_config=TargetConfig(k_std=k_std, min_events=min_events),
    )

    runtimes = recommend_runtime_from_history(
        event_results=event_results,
        target_time_col="time_to_plateau_hours",
        group_cols=list(group_cols),
        min_events=min_events,
        summary_stat=runtime_summary_stat,
    )

    return targets, runtimes


def build_depth_target_runtime_summary(
    event_results: pd.DataFrame,
    min_events: int = 3,
    k_std: float = 0.5,
) -> pd.DataFrame:
    if event_results.empty:
        return pd.DataFrame()

    df = add_derived_event_fields(event_results)
    group_cols = ["strip", "sensor_col", "depth_index", "depth_inches"]

    targets = estimate_statistical_target(
        event_results=df,
        value_col="plateau_vwc",
        group_cols=group_cols,
        target_config=TargetConfig(k_std=k_std, min_events=min_events),
    ).rename(
        columns={
            "n_events": "n_events_target",
            "target_value": "target_vwc",
        }
    )

    runtimes = recommend_runtime_from_history(
        event_results=df,
        target_time_col="time_to_plateau_hours",
        group_cols=group_cols,
        min_events=min_events,
        summary_stat="median",
    ).rename(
        columns={
            "n_events": "n_events_runtime",
        }
    )

    aux_rows: List[Dict[str, object]] = []

    for group_key, sub in df.groupby(group_cols, dropna=False):
        key_tuple = group_key if isinstance(group_key, tuple) else (group_key,)

        duration_vals = pd.to_numeric(sub["event_duration_hours"], errors="coerce").dropna()
        lag_vals = pd.to_numeric(sub["lag_after_irrigation_hr"], errors="coerce").dropna()
        flow_vals = pd.to_numeric(sub["avg_flow_gph_strip"], errors="coerce").dropna()

        row: Dict[str, object] = {
            col_name: val for col_name, val in zip(group_cols, key_tuple)
        }

        row["median_irrigation_duration_hours"] = (
            float(duration_vals.median()) if not duration_vals.empty else pd.NA
        )
        row["median_lag_after_irrigation_hr"] = (
            float(lag_vals.median()) if not lag_vals.empty else pd.NA
        )
        row["median_avg_flow_gph_strip"] = (
            float(flow_vals.median()) if not flow_vals.empty else pd.NA
        )

        aux_rows.append(row)

    aux = pd.DataFrame(aux_rows)

    out = targets.merge(runtimes, how="outer", on=group_cols)
    out = out.merge(aux, how="left", on=group_cols)

    preferred_cols = [
        "strip",
        "sensor_col",
        "depth_index",
        "depth_inches",
        "n_events_target",
        "target_vwc",
        "target_is_trustworthy",
        "n_events_runtime",
        "runtime_hours",
        "runtime_minutes",
        "runtime_is_trustworthy",
        "median_irrigation_duration_hours",
        "median_lag_after_irrigation_hr",
        "median_avg_flow_gph_strip",
        "source_value_col",
        "source_time_col",
        "k_std",
        "summary_stat",
    ]

    out = out[[c for c in preferred_cols if c in out.columns]].copy()

    numeric_cols = out.select_dtypes(include=[np.number]).columns
    out[numeric_cols] = out[numeric_cols].round(6)

    if "strip" in out.columns and "depth_inches" in out.columns:
        out = out.sort_values(["strip", "depth_inches"]).reset_index(drop=True)

    return out


# ---------------------------------------------------------------------
# Definitions helpers
# ---------------------------------------------------------------------


def build_variable_definitions_with_sources(
    output_dir: str | Path,
    year: int,
) -> pd.DataFrame:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_files = [
        f
        for f in output_path.glob(f"irrigation_*_{year}*.csv")
        if "variable_definitions" not in f.name
    ]

    records: List[Dict[str, object]] = []

    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, nrows=0)
        except Exception as exc:
            logger.warning("Skipping %s while building definitions: %s", csv_file, exc)
            continue

        for col in df.columns:
            records.append(
                {
                    "variable_name": col,
                    "source_csv": csv_file.name,
                }
            )

    source_df = pd.DataFrame(records)

    definitions_df = build_variable_definitions_table().copy()
    if not definitions_df.empty and "variable" in definitions_df.columns:
        definitions_df = definitions_df.rename(columns={"variable": "variable_name"})

    if source_df.empty:
        merged = pd.DataFrame(
            columns=["variable_name", "source_csv", "definition", "formula_or_rule"]
        )
    else:
        merged = source_df.merge(definitions_df, on="variable_name", how="left")
        merged = merged.sort_values(["source_csv", "variable_name"]).reset_index(drop=True)

    out_path = output_path / f"irrigation_variable_definitions_{year}_expanded.csv"
    merged.to_csv(out_path, index=False)

    logger.info("Saved expanded irrigation variable definitions to: %s", out_path)
    return merged