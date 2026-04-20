# irrigation_analysis.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, TypedDict, cast, Mapping

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

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


def build_variable_definitions_table() -> pd.DataFrame:
    """
    Definitions / formulas for variables written to the irrigation-analysis
    output CSV files.

    Keep this function as the single source of truth for variable definitions.
    The expanded definitions file should be built by merging CSV column names
    against this table.
    """
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
            "variable": "target_value",
            "definition": "Operational target VWC based on historical plateau responses within a group.",
            "formula_or_rule": "min(mean_value + k_std * sd_value, quantile_cap)",
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
            "variable": "avg_flow_gph",
            "definition": "Average application flow rate during one irrigation event.",
            "formula_or_rule": "volume_gal / event_duration_hours",
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
            "variable": "n_events",
            "definition": "Number of events contributing usable values to the grouped summary.",
            "formula_or_rule": "Count of non-missing source values within each group",
        },
        {
            "variable": "mean_value",
            "definition": "Mean historical value of the source variable used for target estimation.",
            "formula_or_rule": "Mean of source_value_col within each group",
        },
        {
            "variable": "sd_value",
            "definition": "Standard deviation of the historical source values used for target estimation.",
            "formula_or_rule": (
                "Standard deviation of source_value_col within each group with ddof=1; "
                "0.0 if n_events == 1"
            ),
        },
        {
            "variable": "raw_target_value",
            "definition": "Uncapped target calculated from the mean plus a fraction of the standard deviation.",
            "formula_or_rule": "mean_value + k_std * sd_value",
        },
        {
            "variable": "quantile_cap",
            "definition": "Upper cap applied so the target does not exceed an extreme historical value.",
            "formula_or_rule": "Quantile of source_value_col within each group using upper_quantile_cap",
        },
        {
            "variable": "target_is_trustworthy",
            "definition": "Flag indicating whether the target summary is based on at least the minimum number of events.",
            "formula_or_rule": "True when n_events >= min_events",
        },
        {
            "variable": "source_value_col",
            "definition": "Event-level variable used to compute the target summary.",
            "formula_or_rule": "Set in code to the grouped source variable, currently plateau_vwc",
        },
        {
            "variable": "k_std",
            "definition": "Multiplier applied to the standard deviation when building the raw target value.",
            "formula_or_rule": "raw_target_value = mean_value + k_std * sd_value",
        },
        {
            "variable": "rec_runtime_hours",
            "definition": "Recommended runtime in hours based on historical event response within a group.",
            "formula_or_rule": "Median of time_to_plateau_hours within each group",
        },
        {
            "variable": "rec_runtime_minutes",
            "definition": "Recommended runtime in minutes based on historical event response within a group.",
            "formula_or_rule": "rec_runtime_hours * 60",
        },
        {
            "variable": "rec_runtime_is_trustworthy",
            "definition": "Flag indicating whether the recommended runtime summary is based on at least the minimum number of events.",
            "formula_or_rule": "True when n_events >= min_events",
        },
        {
            "variable": "actual_runtime_hours",
            "definition": "Typical actual irrigation runtime in hours represented by the grouped event history.",
            "formula_or_rule": "Median of event_duration_hours within each group",
        },
        {
            "variable": "actual_runtime_minutes",
            "definition": "Typical actual irrigation runtime in minutes represented by the grouped event history.",
            "formula_or_rule": "actual_runtime_hours * 60",
        },
        {
            "variable": "source_time_col",
            "definition": "Event-level time variable used to compute the recommended runtime summary.",
            "formula_or_rule": "Set in code to the grouped source time variable, currently time_to_plateau_hours",
        },
        {
            "variable": "summary_stat",
            "definition": "Summary statistic used to convert event-level times into the reported runtime.",
            "formula_or_rule": "Set in code to median",
        },
        {
            "variable": "median_irrigation_duration_hours",
            "definition": "Median irrigation run duration for the grouped events.",
            "formula_or_rule": "Median of event_duration_hours within each group",
        },
        {
            "variable": "median_avg_flow_gph",
            "definition": "Median event-level average irrigation flow rate for the grouped events.",
            "formula_or_rule": (
                "Median of avg_flow_gph within each group; "
                "avg_flow_gph = volume_gal / event_duration_hours"
            ),
        },
        {
            "variable": "median_lag_after_irrigation_hr",
            "definition": "Median time between irrigation shutoff and plateau attainment for the grouped events.",
            "formula_or_rule": (
                "Median of lag_after_irrigation_hr within each group; "
                "lag_after_irrigation_hr = time_to_plateau_hours - event_duration_hours"
            ),
        },
        {
            "variable": "sensor_col",
            "definition": "Sensor column used for the grouped summary row.",
            "formula_or_rule": "Name of the logger variable analyzed for that row",
        },
        {
            "variable": "strip",
            "definition": "Experimental strip associated with the result row.",
            "formula_or_rule": "Strip identifier carried through event analysis and grouping",
        },
        {
            "variable": "year",
            "definition": "Calendar year of the analyzed irrigation events.",
            "formula_or_rule": "Assigned from the selected analysis year",
        },
    ]

    return pd.DataFrame(rows)


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
    cols: List[str] = []
    for strip in ["S1", "S2", "S3", "S4"]:
        for depth_index in [1, 2, 3]:
            cols.append(f"VWC_{depth_index}_raw_{strip}_B")
    return cols


def parse_vwc_sensor_column(sensor_col: str) -> SensorMeta:
    result: SensorMeta = {
        "variable": None,
        "depth_index": None,
        "strip_from_col": None,
        "logger_position": None,
        "is_valid_vwc_sensor": False,
    }

    if not isinstance(sensor_col, str):
        return result

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


def _safe_filename(text: str) -> str:
    text = re.sub(r"[^\w\-\.]+", "_", text.strip())
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def _fmt1(value: object) -> str:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        return "NA"
    return f"{float(num):.1f}"


def _coerce_optional_timestamp(value: object) -> Optional[pd.Timestamp]:
    """
    Safely coerce one scalar-like value to pd.Timestamp.
    Returns None if the value is missing or invalid.
    """
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value

    ts = pd.to_datetime(cast(Any, value), errors="coerce")
    if pd.isna(ts):
        return None

    return pd.Timestamp(ts)


def _datetime_index_to_mpl_nums(index: pd.Index) -> np.ndarray:
    """
    Convert a DatetimeIndex-like object into matplotlib date numbers.
    """
    dt_index = pd.DatetimeIndex(index)
    py_dates = list(dt_index.to_pydatetime())
    return np.asarray(mdates.date2num(py_dates), dtype=float)


def _timestamp_to_mpl_num(ts: pd.Timestamp) -> float:
    """
    Convert a single Timestamp to a matplotlib date number.
    """
    return float(mdates.date2num(ts.to_pydatetime()))


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
    if timestamp is None:
        return None
    if column not in df.columns:
        return None
    if timestamp not in df.index:
        return None

    value = df.at[timestamp, column]
    if pd.isna(value):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_plot_window_series(
    df: pd.DataFrame,
    sensor_col: str,
    irrigation_start: pd.Timestamp,
    hours_before: float,
    hours_after: float,
) -> pd.Series:
    plot_start = irrigation_start - pd.Timedelta(hours=hours_before)
    plot_end = irrigation_start + pd.Timedelta(hours=hours_after)

    series = pd.to_numeric(df[sensor_col], errors="coerce")
    sub = series.loc[plot_start:plot_end].dropna()
    return sub


def _prepare_plot_window_df(
    df: pd.DataFrame,
    start: pd.Timestamp | str,
    end: pd.Timestamp | str,
) -> pd.DataFrame:
    _validate_datetime_index(df)
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    return df.loc[start_ts:end_ts].copy()


def _collect_multidepth_cols(
    strip: str,
    logger_position: str = "B",
    depths: Sequence[int] = (1, 2, 3),
) -> List[Tuple[str, str]]:
    cols: List[Tuple[str, str]] = []
    for depth in depths:
        if depth not in {1, 2, 3}:
            raise ValueError("depth values must be 1, 2, or 3")
        inches = DEPTH_INDEX_TO_INCHES[str(depth)]
        cols.append((f"VWC_{depth}_raw_{strip}_{logger_position}", f"{inches} in"))
    return cols


def compute_event_plot_ylim(
    df: pd.DataFrame,
    event_results: pd.DataFrame,
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    strip_filter: Optional[Sequence[str]] = None,
    sensor_filter: Optional[Sequence[str]] = None,
    pad_fraction: float = 0.05,
) -> tuple[float, float] | None:
    """
    Compute one common y-axis range across a set of inspection plots.
    """
    if event_results.empty:
        return None

    work = event_results.copy()

    if strip_filter is not None:
        work = work[work["strip"].isin(strip_filter)].copy()

    if sensor_filter is not None:
        work = work[work["sensor_col"].isin(sensor_filter)].copy()

    mins: List[float] = []
    maxs: List[float] = []

    for _, row in work.iterrows():
        irrigation_start = _coerce_optional_timestamp(row.get("irrigation_start"))
        sensor_col_obj = row.get("sensor_col")
        sensor_col = str(sensor_col_obj) if sensor_col_obj is not None else ""

        if irrigation_start is None or sensor_col not in df.columns:
            continue

        sub = _get_plot_window_series(
            df=df,
            sensor_col=sensor_col,
            irrigation_start=irrigation_start,
            hours_before=hours_before,
            hours_after=hours_after,
        )

        if sub.empty:
            continue

        mins.append(float(sub.min()))
        maxs.append(float(sub.max()))

    if not mins or not maxs:
        return None

    ymin = min(mins)
    ymax = max(maxs)
    yrange = ymax - ymin

    if yrange <= 0:
        return ymin - 1.0, ymax + 1.0

    pad = yrange * pad_fraction
    return ymin - pad, ymax + pad


# ---------------------------------------------------------------------
# Event storage helpers
# ---------------------------------------------------------------------


def compute_event_storage_metrics(
    df: pd.DataFrame,
    sensor_meta: SensorMeta,
    baseline_time: Optional[pd.Timestamp],
    plateau_time: Optional[pd.Timestamp],
    volume_gal: Optional[float],
) -> Dict[str, object]:
    strip_from_col = sensor_meta["strip_from_col"]
    logger_position = sensor_meta["logger_position"]

    if strip_from_col is None or logger_position is None:
        return {
            "profile_baseline_storage_gal": None,
            "profile_plateau_storage_gal": None,
            "event_storage_gal": None,
            "efficiency": None,
            "estimated_loss_gal": None,
        }

    swc_cols = _build_profile_swc_gal_cols(strip_from_col, logger_position)

    baseline_values: List[float] = []
    plateau_values: List[float] = []

    for col in swc_cols:
        baseline_val = _safe_value_at_timestamp(df, baseline_time, col)
        plateau_val = _safe_value_at_timestamp(df, plateau_time, col)

        if baseline_val is not None:
            baseline_values.append(baseline_val)
        if plateau_val is not None:
            plateau_values.append(plateau_val)

    profile_baseline_storage_gal: Optional[float] = None
    if baseline_values:
        profile_baseline_storage_gal = float(sum(baseline_values))

    profile_plateau_storage_gal: Optional[float] = None
    if plateau_values:
        profile_plateau_storage_gal = float(sum(plateau_values))

    event_storage_gal: Optional[float] = None
    if (
        profile_baseline_storage_gal is not None
        and profile_plateau_storage_gal is not None
    ):
        event_storage_gal = profile_plateau_storage_gal - profile_baseline_storage_gal

    efficiency: Optional[float] = None
    estimated_loss_gal: Optional[float] = None

    if volume_gal is not None and volume_gal > 0 and event_storage_gal is not None:
        efficiency = event_storage_gal / volume_gal
        estimated_loss_gal = volume_gal - event_storage_gal

    return {
        "profile_baseline_storage_gal": profile_baseline_storage_gal,
        "profile_plateau_storage_gal": profile_plateau_storage_gal,
        "event_storage_gal": event_storage_gal,
        "efficiency": efficiency,
        "estimated_loss_gal": estimated_loss_gal,
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
    peak_increase: Optional[float] = None if baseline_vwc is None else peak_vwc - baseline_vwc

    peak_found = True
    if peak_increase is not None and peak_increase < min_peak_increase:
        peak_found = False

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
    volume_gal: Optional[float] = None,
    strip: Optional[str] = None,
    year: Optional[int] = None,
    event_id: Optional[object] = None,
    search_config: Optional[EventSearchConfig] = None,
    plateau_config: Optional[PlateauConfig] = None,
) -> Dict[str, object]:
    _validate_datetime_index(df)

    if search_config is None:
        search_config = EventSearchConfig()
    if plateau_config is None:
        plateau_config = PlateauConfig()

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

    plateau_info: Dict[str, object] = {
        "plateau_vwc": None,
        "plateau_time": None,
        "plateau_method": "no_peak",
    }

    peak_time_obj = peak_info.get("peak_time")
    peak_found_obj = peak_info.get("peak_found", False)

    if bool(peak_found_obj) and isinstance(peak_time_obj, pd.Timestamp):
        plateau_info = find_post_peak_plateau(
            series=series,
            peak_time=peak_time_obj,
            config=plateau_config,
        )

    time_to_peak_hours: Optional[float] = None
    if isinstance(peak_time_obj, pd.Timestamp):
        time_to_peak_hours = _hours_from_timedelta(peak_time_obj - irrigation_start)

    plateau_time_obj = plateau_info.get("plateau_time")
    time_to_plateau_hours: Optional[float] = None
    if isinstance(plateau_time_obj, pd.Timestamp):
        time_to_plateau_hours = _hours_from_timedelta(
            plateau_time_obj - irrigation_start
        )

    event_duration_hours: Optional[float] = None
    if irrigation_end is not None and pd.notna(irrigation_end):
        event_duration_hours = _hours_from_timedelta(irrigation_end - irrigation_start)

    storage_metrics = compute_event_storage_metrics(
        df=df,
        sensor_meta=sensor_meta,
        baseline_time=baseline_time,
        plateau_time=plateau_time_obj if isinstance(plateau_time_obj, pd.Timestamp) else None,
        volume_gal=volume_gal,
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
        "volume_gal": volume_gal,
        "event_duration_hours": event_duration_hours,
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
        "efficiency": storage_metrics["efficiency"],
        "estimated_loss_gal": storage_metrics["estimated_loss_gal"],
    }


def analyze_irrigation_events(
    df: pd.DataFrame,
    events: pd.DataFrame,
    sensor_cols: Sequence[str],
    start_col: str = "start",
    end_col: str = "end",
    volume_col: str = "volume_gal",
    strip: Optional[str] = None,
    year: Optional[int] = None,
    event_id_col: Optional[str] = None,
    search_config: Optional[EventSearchConfig] = None,
    plateau_config: Optional[PlateauConfig] = None,
) -> pd.DataFrame:
    _validate_datetime_index(df)

    if search_config is None:
        search_config = EventSearchConfig()
    if plateau_config is None:
        plateau_config = PlateauConfig()

    events_local = events.copy()
    events_local[start_col] = _coerce_datetime_column(events_local, start_col)
    events_local[end_col] = _coerce_datetime_column(events_local, end_col)

    if volume_col in events_local.columns:
        events_local[volume_col] = pd.to_numeric(events_local[volume_col], errors="coerce")
    else:
        events_local[volume_col] = np.nan

    rows: List[Dict[str, object]] = []

    for i, ev in events_local.iterrows():
        irrigation_start_obj = ev[start_col]
        irrigation_end_obj = ev[end_col]
        volume_obj = ev[volume_col]
        event_id = ev[event_id_col] if event_id_col and event_id_col in ev else i

        if pd.isna(irrigation_start_obj):
            continue

        irrigation_start = pd.Timestamp(irrigation_start_obj)

        irrigation_end: Optional[pd.Timestamp] = None
        if pd.notna(irrigation_end_obj):
            irrigation_end = pd.Timestamp(irrigation_end_obj)

        volume_gal: Optional[float] = None
        if pd.notna(volume_obj):
            volume_gal = float(volume_obj)

        for sensor_col in sensor_cols:
            rows.append(
                analyze_single_event_sensor(
                    df=df,
                    sensor_col=sensor_col,
                    irrigation_start=irrigation_start,
                    irrigation_end=irrigation_end,
                    volume_gal=volume_gal,
                    strip=strip,
                    year=year,
                    event_id=event_id,
                    search_config=search_config,
                    plateau_config=plateau_config,
                )
            )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


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

    for strip in strips:
        sensor_col = strip_to_bottom_sensor.get(strip)
        if not sensor_col:
            continue
        if sensor_col not in df_15min.columns:
            continue

        events = all_events.loc[
            (all_events["strip"] == strip) & (all_events["year"] == year),
            ["start_timestamp", "end_timestamp", "gallons"],
        ].copy()

        if events.empty:
            continue

        events = events.rename(
            columns={
                "start_timestamp": "start",
                "end_timestamp": "end",
                "gallons": "volume_gal",
            }
        )

        strip_results = analyze_irrigation_events(
            df=df_15min,
            events=events,
            sensor_cols=[sensor_col],
            start_col="start",
            end_col="end",
            volume_col="volume_gal",
            strip=strip,
            year=year,
            search_config=search_config,
            plateau_config=plateau_config,
        )

        if not strip_results.empty:
            all_results.append(strip_results)

    if not all_results:
        return pd.DataFrame()

    return pd.concat(all_results, ignore_index=True)


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

    if "time_to_plateau_hours" in out.columns:
        plateau_hours = pd.to_numeric(out["time_to_plateau_hours"], errors="coerce")
    else:
        plateau_hours = pd.Series(pd.NA, index=out.index, dtype="Float64")

    if "event_duration_hours" in out.columns:
        duration_hours = pd.to_numeric(out["event_duration_hours"], errors="coerce")
    else:
        duration_hours = pd.Series(pd.NA, index=out.index, dtype="Float64")

    if "volume_gal" in out.columns:
        volume_gal = pd.to_numeric(out["volume_gal"], errors="coerce")
    else:
        volume_gal = pd.Series(pd.NA, index=out.index, dtype="Float64")

    out["lag_after_irrigation_hr"] = plateau_hours - duration_hours
    out["avg_flow_gph"] = volume_gal / duration_hours
    out.loc[duration_hours <= 0, "avg_flow_gph"] = pd.NA

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
        "volume_gal",
        "event_duration_hours",
        "avg_flow_gph",
        "baseline_vwc",
        "peak_vwc",
        "peak_increase",
        "plateau_vwc",
        "plateau_method",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "lag_after_irrigation_hr",
    ]
    keep_cols = [c for c in keep_cols if c in out.columns]
    out = out[keep_cols].copy()

    numeric_cols = out.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        out[col] = out[col].round(decimals)

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
    if target_config is None:
        target_config = TargetConfig()
    if group_cols is None:
        group_cols = []

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
    if group_cols is None:
        group_cols = []

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
            "target_is_trustworthy": "target_is_trustworthy",
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
            "runtime_is_trustworthy": "runtime_is_trustworthy",
        }
    )

    aux_rows: List[Dict[str, object]] = []
    grouped = df.groupby(group_cols, dropna=False)

    for group_key, sub in grouped:
        key_tuple = group_key if isinstance(group_key, tuple) else (group_key,)

        duration_vals = pd.to_numeric(sub["event_duration_hours"], errors="coerce").dropna()
        lag_vals = pd.to_numeric(sub["lag_after_irrigation_hr"], errors="coerce").dropna()
        flow_vals = pd.to_numeric(sub["avg_flow_gph"], errors="coerce").dropna()

        row: Dict[str, object] = {}
        for col_name, val in zip(group_cols, key_tuple):
            row[col_name] = val

        row["median_irrigation_duration_hours"] = (
            float(duration_vals.median()) if not duration_vals.empty else pd.NA
        )
        row["median_lag_after_irrigation_hr"] = (
            float(lag_vals.median()) if not lag_vals.empty else pd.NA
        )
        row["median_avg_flow_gph"] = (
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
        "median_avg_flow_gph",
        "source_value_col",
        "source_time_col",
        "k_std",
        "summary_stat",
    ]
    preferred_cols = [c for c in preferred_cols if c in out.columns]
    out = out[preferred_cols].copy()

    numeric_cols = out.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        out[col] = out[col].round(6)

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
    """
    Build an expanded variable-definitions table by combining:
    1. all column names found in the irrigation numeric CSV outputs
    2. the source CSV each column came from
    3. canonical definitions/formulas from build_variable_definitions_table()

    This keeps build_variable_definitions_table() as the single source of truth
    and avoids gradual divergence between the base and expanded definitions files.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_files = [
        f for f in output_path.glob(f"irrigation_*_{year}*.csv")
        if "variable_definitions" not in f.name
    ]

    records: List[Dict[str, object]] = []

    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, nrows=0)
        except Exception as e:
            print(f"Skipping {csv_file}: {e}")
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

    print(f"Saved expanded definitions to: {out_path}")
    return merged


# ---------------------------------------------------------------------
# Visualization / Debugging Utilities
# ---------------------------------------------------------------------


def plot_irrigation_event_inspection(
    df: pd.DataFrame,
    event_row: pd.Series,
    output_path: Optional[str | Path] = None,
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    show: bool = False,
    y_limits: Optional[tuple[float, float]] = None,
    precip_col: Optional[str] = "precip_in",
) -> None:
    """
    Plot one irrigation event for visual inspection.

    Expected event_row fields
    -------------------------
    sensor_col
    irrigation_start
    irrigation_end
    baseline_time
    baseline_vwc
    peak_time
    peak_vwc
    plateau_time
    plateau_vwc
    strip
    year
    event_id
    plateau_method
    time_to_peak_hours
    time_to_plateau_hours
    event_duration_hours

    Notes
    -----
    baseline marker:
    - x = baseline_time (last timestamp in pre-irrigation lookback window)
    - y = baseline_vwc (median VWC over that lookback window)
    """
    _validate_datetime_index(df)

    sensor_col = str(event_row["sensor_col"])
    if sensor_col not in df.columns:
        raise KeyError(f"Sensor column not found in dataframe: {sensor_col}")

    irrigation_start = _coerce_optional_timestamp(event_row.get("irrigation_start"))
    irrigation_end = _coerce_optional_timestamp(event_row.get("irrigation_end"))
    baseline_time = _coerce_optional_timestamp(event_row.get("baseline_time"))
    peak_time = _coerce_optional_timestamp(event_row.get("peak_time"))
    plateau_time = _coerce_optional_timestamp(event_row.get("plateau_time"))

    if irrigation_start is None:
        raise ValueError("event_row is missing a valid irrigation_start")

    baseline_vwc = pd.to_numeric(pd.Series([event_row.get("baseline_vwc")]), errors="coerce").iloc[0]
    peak_vwc = pd.to_numeric(pd.Series([event_row.get("peak_vwc")]), errors="coerce").iloc[0]
    plateau_vwc = pd.to_numeric(pd.Series([event_row.get("plateau_vwc")]), errors="coerce").iloc[0]

    plot_start = irrigation_start - pd.Timedelta(hours=hours_before)
    plot_end = irrigation_start + pd.Timedelta(hours=hours_after)

    sub = _get_plot_window_series(
        df=df,
        sensor_col=sensor_col,
        irrigation_start=irrigation_start,
        hours_before=hours_before,
        hours_after=hours_after,
    )

    if sub.empty:
        raise ValueError(
            f"No data found for {sensor_col} between {plot_start} and {plot_end}"
        )

    strip = event_row.get("strip", "")
    year = event_row.get("year", "")
    event_id = event_row.get("event_id", "")
    plateau_method = event_row.get("plateau_method", "")
    duration_hr = event_row.get("event_duration_hours", pd.NA)
    t_peak_hr = event_row.get("time_to_peak_hours", pd.NA)
    t_plateau_hr = event_row.get("time_to_plateau_hours", pd.NA)

    fig, ax = plt.subplots(figsize=(13, 6))

    x_main = _datetime_index_to_mpl_nums(sub.index)
    y_main = np.asarray(sub.to_numpy(dtype=float), dtype=float)
    ax.plot(x_main, y_main, linewidth=1.8, label=sensor_col)

    ax2 = None
    if precip_col and precip_col in df.columns:
        precip = pd.to_numeric(df[precip_col], errors="coerce").loc[plot_start:plot_end].fillna(0)
        if not precip.empty and float(precip.max()) > 0:
            ax2 = ax.twinx()
            precip_x = _datetime_index_to_mpl_nums(precip.index)
            precip_y = np.asarray(precip.to_numpy(dtype=float), dtype=float)
            ax2.bar(precip_x, precip_y, width=0.009, alpha=0.18, label=precip_col)
            ax2.set_ylabel(precip_col)

    if irrigation_end is not None:
        ax.axvspan(
            _timestamp_to_mpl_num(irrigation_start),
            _timestamp_to_mpl_num(irrigation_end),
            alpha=0.15,
            label="irrigation window",
        )
        ax.axvline(
            _timestamp_to_mpl_num(irrigation_end),
            linestyle="--",
            linewidth=1.2,
            label="irrigation end",
        )

    ax.axvline(
        _timestamp_to_mpl_num(irrigation_start),
        linestyle="--",
        linewidth=1.2,
        label="irrigation start",
    )

    if baseline_time is not None and pd.notna(baseline_vwc):
        ax.scatter(
            [_timestamp_to_mpl_num(baseline_time)],
            [float(baseline_vwc)],
            s=55,
            marker="o",
            label="baseline",
            zorder=5,
        )

    if peak_time is not None and pd.notna(peak_vwc):
        ax.scatter(
            [_timestamp_to_mpl_num(peak_time)],
            [float(peak_vwc)],
            s=70,
            marker="^",
            label="peak",
            zorder=6,
        )

    if plateau_time is not None and pd.notna(plateau_vwc):
        ax.scatter(
            [_timestamp_to_mpl_num(plateau_time)],
            [float(plateau_vwc)],
            s=70,
            marker="s",
            label="plateau",
            zorder=6,
        )

    title = (
        f"Irrigation Event Inspection | strip={strip} | sensor={sensor_col} | "
        f"event_id={event_id} | year={year}"
    )
    subtitle = (
        f"duration_hr={_fmt1(duration_hr)}, "
        f"time_to_peak_hr={_fmt1(t_peak_hr)}, "
        f"time_to_plateau_hr={_fmt1(t_plateau_hr)}, "
        f"plateau_method={plateau_method}"
    )

    ax.set_title(title + "\n" + subtitle)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("VWC")
    ax.grid(True, alpha=0.3)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))

    if y_limits is not None:
        ax.set_ylim(*y_limits)

    handles1, labels1 = ax.get_legend_handles_labels()
    if ax2 is not None:
        handles2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(handles1 + handles2, labels1 + labels2, loc="best")
    else:
        ax.legend(loc="best")

    fig.autofmt_xdate()
    fig.tight_layout()

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    if show:
        plt.show()

    plt.close(fig)


def save_irrigation_event_inspection_plots(
    df: pd.DataFrame,
    event_results: pd.DataFrame,
    output_dir: str | Path,
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    strip_filter: Optional[Sequence[str]] = None,
    sensor_filter: Optional[Sequence[str]] = None,
    max_plots: Optional[int] = None,
    skip_no_peak: bool = False,
    use_common_y_axis: bool = True,
    precip_col: Optional[str] = "precip_in",
) -> pd.DataFrame:
    """
    Save a batch of irrigation event inspection plots.

    Returns
    -------
    DataFrame logging which plots were written.
    """
    if event_results.empty:
        return pd.DataFrame(
            columns=["event_id", "strip", "sensor_col", "output_file", "status"]
        )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    work = event_results.copy()

    if strip_filter is not None:
        work = work[work["strip"].isin(strip_filter)].copy()

    if sensor_filter is not None:
        work = work[work["sensor_col"].isin(sensor_filter)].copy()

    if skip_no_peak and "plateau_method" in work.columns:
        work = work[work["plateau_method"] != "no_peak"].copy()

    if max_plots is not None:
        work = work.head(max_plots).copy()

    y_limits = None
    if use_common_y_axis:
        y_limits = compute_event_plot_ylim(
            df=df,
            event_results=work,
            hours_before=hours_before,
            hours_after=hours_after,
        )

    log_rows: List[Dict[str, object]] = []

    for _, row in work.iterrows():
        event_id = row.get("event_id", "unknown")
        strip = row.get("strip", "unknown")
        sensor_col = row.get("sensor_col", "unknown")
        year = row.get("year", "unknown")

        filename = _safe_filename(
            f"{year}_{strip}_{sensor_col}_event_{event_id}.png"
        )
        output_file = out_dir / filename

        try:
            plot_irrigation_event_inspection(
                df=df,
                event_row=row,
                output_path=output_file,
                hours_before=hours_before,
                hours_after=hours_after,
                show=False,
                y_limits=y_limits,
                precip_col=precip_col,
            )
            status = "written"
        except Exception as e:
            status = f"failed: {e}"

        log_rows.append(
            {
                "event_id": event_id,
                "strip": strip,
                "sensor_col": sensor_col,
                "output_file": str(output_file),
                "status": status,
            }
        )

    return pd.DataFrame(log_rows)


def plot_event_multidepth(
    df: pd.DataFrame,
    cols: Sequence[Tuple[str, str]],
    start: pd.Timestamp | str,
    end: pd.Timestamp | str,
    event_id: Optional[object] = None,
    strip: Optional[str] = None,
    year: Optional[int] = None,
    irrigation_start: Optional[pd.Timestamp | str] = None,
    irrigation_end: Optional[pd.Timestamp | str] = None,
    peaks: Optional[Mapping[str, pd.Timestamp | str]] = None,
    baselines: Optional[Mapping[str, pd.Timestamp | str]] = None,
    plateaus: Optional[Mapping[str, pd.Timestamp | str]] = None,
    output_path: Optional[str | Path] = None,
    show: bool = False,
    precip_col: Optional[str] = "precip_in",
    y_limits: Optional[Tuple[float, float]] = None,
    title_prefix: str = "Event Multi-depth VWC",
) -> None:
    """
    Plot all requested depths on one shared axis for a given time window.

    Parameters
    ----------
    df
        Datetime-indexed dataframe.
    cols
        Sequence like:
            [("VWC_1_raw_S3_B", "6 in"), ("VWC_2_raw_S3_B", "12 in"), ...]
    start, end
        Plot window.
    peaks, baselines, plateaus
        Dict mapping sensor column -> timestamp.
    """
    sub = _prepare_plot_window_df(df, start=start, end=end)
    if sub.empty:
        raise ValueError("No data found in requested multi-depth plot window.")

    irrig_start_ts = _coerce_optional_timestamp(irrigation_start)
    irrig_end_ts = _coerce_optional_timestamp(irrigation_end)

    peak_ts_map = {
        k: ts for k, v in (peaks or {}).items()
        if (ts := _coerce_optional_timestamp(v)) is not None
    }
    baseline_ts_map = {
        k: ts for k, v in (baselines or {}).items()
        if (ts := _coerce_optional_timestamp(v)) is not None
    }
    plateau_ts_map = {
        k: ts for k, v in (plateaus or {}).items()
        if (ts := _coerce_optional_timestamp(v)) is not None
    }

    fig, ax = plt.subplots(figsize=(13, 6))
    ax2 = None

    if precip_col and precip_col in sub.columns:
        precip = pd.to_numeric(sub[precip_col], errors="coerce").fillna(0)
        if float(precip.max()) > 0:
            ax2 = ax.twinx()
            precip_x = _datetime_index_to_mpl_nums(precip.index)
            precip_y = np.asarray(precip.to_numpy(dtype=float), dtype=float)
            ax2.bar(
                precip_x,
                precip_y,
                width=0.009,
                alpha=0.18,
                label=precip_col,
            )
            ax2.set_ylabel(precip_col)

    plotted_any = False

    for sensor_col, label in cols:
        if sensor_col not in sub.columns:
            continue

        series = pd.to_numeric(sub[sensor_col], errors="coerce")
        x_vals = _datetime_index_to_mpl_nums(series.index)
        y_vals = np.asarray(series.to_numpy(dtype=float), dtype=float)
        ax.plot(x_vals, y_vals, linewidth=2.0, label=label)
        plotted_any = True

        baseline_time = baseline_ts_map.get(sensor_col)
        if baseline_time is not None and baseline_time in sub.index:
            baseline_val = pd.to_numeric(pd.Series([sub.at[baseline_time, sensor_col]]), errors="coerce").iloc[0]
            if pd.notna(baseline_val):
                ax.scatter(
                    [_timestamp_to_mpl_num(baseline_time)],
                    [float(baseline_val)],
                    s=45,
                    marker="o",
                    zorder=5,
                )

        peak_time = peak_ts_map.get(sensor_col)
        if peak_time is not None and peak_time in sub.index:
            peak_val = pd.to_numeric(pd.Series([sub.at[peak_time, sensor_col]]), errors="coerce").iloc[0]
            if pd.notna(peak_val):
                ax.scatter(
                    [_timestamp_to_mpl_num(peak_time)],
                    [float(peak_val)],
                    s=60,
                    marker="^",
                    zorder=6,
                )

        plateau_time = plateau_ts_map.get(sensor_col)
        if plateau_time is not None and plateau_time in sub.index:
            plateau_val = pd.to_numeric(pd.Series([sub.at[plateau_time, sensor_col]]), errors="coerce").iloc[0]
            if pd.notna(plateau_val):
                ax.scatter(
                    [_timestamp_to_mpl_num(plateau_time)],
                    [float(plateau_val)],
                    s=55,
                    marker="s",
                    zorder=6,
                )

    if not plotted_any:
        raise ValueError("None of the requested VWC columns were found in the plot window.")

    if irrig_start_ts is not None:
        ax.axvline(
            _timestamp_to_mpl_num(irrig_start_ts),
            linestyle="--",
            linewidth=1.2,
            label="irrigation start",
        )
    if irrig_end_ts is not None:
        ax.axvline(
            _timestamp_to_mpl_num(irrig_end_ts),
            linestyle="--",
            linewidth=1.2,
            label="irrigation end",
        )
    if irrig_start_ts is not None and irrig_end_ts is not None:
        ax.axvspan(
            _timestamp_to_mpl_num(irrig_start_ts),
            _timestamp_to_mpl_num(irrig_end_ts),
            alpha=0.15,
            label="irrigation window",
        )

    title_bits = [title_prefix]
    if strip is not None:
        title_bits.append(f"strip={strip}")
    if event_id is not None:
        title_bits.append(f"event_id={event_id}")
    if year is not None:
        title_bits.append(f"year={year}")

    ax.set_title(" | ".join(title_bits))
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("VWC (%)")
    ax.grid(True, alpha=0.3)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))

    if y_limits is not None:
        ax.set_ylim(*y_limits)

    handles1, labels1 = ax.get_legend_handles_labels()
    if ax2 is not None:
        handles2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(handles1 + handles2, labels1 + labels2, loc="best")
    else:
        ax.legend(loc="best")

    fig.autofmt_xdate()
    fig.tight_layout()

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")

    if show:
        plt.show()

    plt.close(fig)


def plot_event_multidepth_from_results(
    df: pd.DataFrame,
    event_results: pd.DataFrame,
    strip: str,
    event_id: object,
    logger_position: str = "B",
    depths: Sequence[int] = (1, 2, 3),
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    output_path: Optional[str | Path] = None,
    show: bool = False,
    precip_col: Optional[str] = "precip_in",
    y_limits: Optional[Tuple[float, float]] = None,
) -> None:
    """
    Build a multi-depth plot directly from event_results for one strip/event.

    This is the safer wrapper to use in practice because it:
    - pulls irrigation start/end from the event table
    - pulls baseline/peak/plateau timestamps by sensor column
    - automatically builds 6/12/18 inch bottom-logger columns
    """
    if event_results.empty:
        raise ValueError("event_results is empty.")

    work = event_results.copy()
    work = work[
        (work["strip"] == strip)
        & (work["event_id"] == event_id)
        & (work["logger_position"] == logger_position)
    ].copy()

    if work.empty:
        raise ValueError(
            f"No event_results rows found for strip={strip}, event_id={event_id}, logger_position={logger_position}"
        )

    first_row = work.iloc[0]
    irrigation_start = _coerce_optional_timestamp(first_row.get("irrigation_start"))
    irrigation_end = _coerce_optional_timestamp(first_row.get("irrigation_end"))
    year = first_row.get("year", None)

    if irrigation_start is None:
        raise ValueError("Selected event has no valid irrigation_start.")

    start = irrigation_start - pd.Timedelta(hours=hours_before)
    end = irrigation_start + pd.Timedelta(hours=hours_after)

    cols = _collect_multidepth_cols(
        strip=strip,
        logger_position=logger_position,
        depths=depths,
    )

    baselines: Dict[str, pd.Timestamp] = {}
    peaks: Dict[str, pd.Timestamp] = {}
    plateaus: Dict[str, pd.Timestamp] = {}

    for _, row in work.iterrows():
        sensor_col = str(row["sensor_col"])

        baseline_time = _coerce_optional_timestamp(row.get("baseline_time"))
        if baseline_time is not None:
            baselines[sensor_col] = baseline_time

        peak_time = _coerce_optional_timestamp(row.get("peak_time"))
        if peak_time is not None:
            peaks[sensor_col] = peak_time

        plateau_time = _coerce_optional_timestamp(row.get("plateau_time"))
        if plateau_time is not None:
            plateaus[sensor_col] = plateau_time

    plot_event_multidepth(
        df=df,
        cols=cols,
        start=start,
        end=end,
        event_id=event_id,
        strip=strip,
        year=year,
        irrigation_start=irrigation_start,
        irrigation_end=irrigation_end,
        peaks=peaks,
        baselines=baselines,
        plateaus=plateaus,
        output_path=output_path,
        show=show,
        precip_col=precip_col,
        y_limits=y_limits,
        title_prefix="Irrigation Event Multi-depth Inspection",
    )


def save_irrigation_event_multidepth_plots(
    df: pd.DataFrame,
    event_results: pd.DataFrame,
    output_dir: str | Path,
    strip_filter: Optional[Sequence[str]] = None,
    event_ids: Optional[Sequence[object]] = None,
    logger_position: str = "B",
    depths: Sequence[int] = (1, 2, 3),
    hours_before: float = 6.0,
    hours_after: float = 36.0,
    max_plots: Optional[int] = None,
    precip_col: Optional[str] = "precip_in",
    use_common_y_axis: bool = True,
) -> pd.DataFrame:
    """
    Save one multi-depth plot per (strip, event_id, logger_position).
    """
    if event_results.empty:
        return pd.DataFrame(
            columns=["event_id", "strip", "logger_position", "output_file", "status"]
        )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    work = event_results.copy()

    if strip_filter is not None:
        work = work[work["strip"].isin(strip_filter)].copy()

    if event_ids is not None:
        work = work[work["event_id"].isin(event_ids)].copy()

    work = work[work["logger_position"] == logger_position].copy()

    if work.empty:
        return pd.DataFrame(
            columns=["event_id", "strip", "logger_position", "output_file", "status"]
        )

    unique_events = (
        work[["strip", "event_id", "logger_position"]]
        .drop_duplicates()
        .sort_values(["strip", "event_id"])
        .reset_index(drop=True)
    )

    if max_plots is not None:
        unique_events = unique_events.head(max_plots).copy()

    y_limits: Optional[Tuple[float, float]] = None
    if use_common_y_axis:
        mins: List[float] = []
        maxs: List[float] = []

        for _, event_key in unique_events.iterrows():
            sub_rows = work[
                (work["strip"] == event_key["strip"])
                & (work["event_id"] == event_key["event_id"])
                & (work["logger_position"] == event_key["logger_position"])
            ].copy()

            if sub_rows.empty:
                continue

            irrig_start = _coerce_optional_timestamp(sub_rows.iloc[0]["irrigation_start"])
            if irrig_start is None:
                continue

            start = irrig_start - pd.Timedelta(hours=hours_before)
            end = irrig_start + pd.Timedelta(hours=hours_after)
            sub_df = _prepare_plot_window_df(df, start, end)

            for depth in depths:
                sensor_col = f"VWC_{depth}_raw_{event_key['strip']}_{logger_position}"
                if sensor_col in sub_df.columns:
                    series = pd.to_numeric(sub_df[sensor_col], errors="coerce").dropna()
                    if not series.empty:
                        mins.append(float(series.min()))
                        maxs.append(float(series.max()))

        if mins and maxs:
            ymin = min(mins)
            ymax = max(maxs)
            yrange = ymax - ymin
            if yrange <= 0:
                y_limits = (ymin - 1.0, ymax + 1.0)
            else:
                pad = 0.05 * yrange
                y_limits = (ymin - pad, ymax + pad)

    log_rows: List[Dict[str, object]] = []

    for _, event_key in unique_events.iterrows():
        strip = str(event_key["strip"])
        event_id = event_key["event_id"]

        filename = _safe_filename(
            f"{strip}_{logger_position}_multidepth_event_{event_id}.png"
        )
        output_file = out_dir / filename

        try:
            plot_event_multidepth_from_results(
                df=df,
                event_results=work,
                strip=strip,
                event_id=event_id,
                logger_position=logger_position,
                depths=depths,
                hours_before=hours_before,
                hours_after=hours_after,
                output_path=output_file,
                show=False,
                precip_col=precip_col,
                y_limits=y_limits,
            )
            status = "written"
        except Exception as e:
            status = f"failed: {e}"

        log_rows.append(
            {
                "event_id": event_id,
                "strip": strip,
                "logger_position": logger_position,
                "output_file": str(output_file),
                "status": status,
            }
        )

    return pd.DataFrame(log_rows)