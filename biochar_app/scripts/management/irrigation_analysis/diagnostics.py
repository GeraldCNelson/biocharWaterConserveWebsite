"""
Diagnostic and QA routines for irrigation analysis.
"""
from __future__ import annotations

from typing import Any, cast

import numpy as np
import pandas as pd

from biochar_app.config.experiment_config import LOGGER_GEOMETRY
from biochar_app.config.irrigation_config import MIN_PRECIP_IN
from biochar_app.scripts.management.irrigation_analysis.utils import (
    force_float,
    move_id_columns_left,
)

BATTERY_MIN_OK = 11.0
BATTERY_MAX_OK = 15.0
MIN_BOTTOM_RESPONSE_DELAY_HR = 0.5

def _logger_distance_ft(logger_position: object) -> float | None:
    code = str(logger_position).strip()
    meta = LOGGER_GEOMETRY.get(code, {})
    value = meta.get("distance_from_furrow_start_ft")
    return float(value) if value is not None else None

def add_vertical_velocity_fields(response_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Add vertical wetting-front velocity estimates to logger-level response table.
    Units: inches per minute.
    """
    if response_summary.empty:
        return response_summary

    out = response_summary.copy()

    pairs = [
        (6, 12),
        (12, 18),
        (6, 18),
    ]

    for upper, lower in pairs:
        upper_col = f"arrival_minutes_after_irrigation_start_{upper}in"
        lower_col = f"arrival_minutes_after_irrigation_start_{lower}in"
        out_col = f"vertical_velocity_{upper}_to_{lower}_in_per_min"

        if upper_col not in out.columns or lower_col not in out.columns:
            out[out_col] = pd.NA
            continue

        dt_min = (
            pd.to_numeric(out[lower_col], errors="coerce")
            - pd.to_numeric(out[upper_col], errors="coerce")
        )

        dz_in = float(lower - upper)

        out[out_col] = dz_in / dt_min
        out.loc[dt_min <= 0, out_col] = pd.NA

    return out

def build_irrigation_horizontal_advance_summary(
    arrival_times: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build one row per event × strip × depth summarizing movement along the furrow.

    Uses logger position arrival times:
      T = top / upstream
      M = middle
      B = bottom / downstream

    Velocity units: ft/min.
    """
    if arrival_times.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []

    work = arrival_times.copy()
    work["distance_from_furrow_start_ft"] = work["logger_position"].apply(
        _logger_distance_ft
    )

    group_cols = [
        "year",
        "event_id",
        "strip",
        "depth_inches",
    ]

    for keys, group in work.groupby(group_cols, dropna=False):
        year, event_id, strip, depth_inches = keys

        by_logger: dict[str, dict[str, object]] = {}

        for _, row in group.iterrows():
            loc = str(row["logger_position"]).strip()
            by_logger[loc] = {
                "arrival_min": pd.to_numeric(
                    pd.Series([row.get("arrival_minutes_after_irrigation_start")]),
                    errors="coerce",
                ).iloc[0],
                "alt_arrival_min": pd.to_numeric(
                    pd.Series([row.get("alt_arrival_minutes_after_irrigation_start")]),
                    errors="coerce",
                ).iloc[0],
                "distance_ft": row.get("distance_from_furrow_start_ft"),
                "irrigation_start": row.get("irrigation_start"),
                "irrigation_end": row.get("irrigation_end"),
                "gallons_strip": row.get("gallons_strip"),
                "event_duration_hours": row.get("event_duration_hours"),
            }

        def _val(loc: str, key: str) -> float | None:
            value = by_logger.get(loc, {}).get(key)
            parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
            return float(parsed) if pd.notna(parsed) else None

        def _meta(key: str) -> object:
            for loc in ["T", "M", "B"]:
                if loc in by_logger and key in by_logger[loc]:
                    return by_logger[loc][key]
            return pd.NA

        t_min = _val("T", "arrival_min")
        m_min = _val("M", "arrival_min")
        b_min = _val("B", "arrival_min")

        alt_t_min = _val("T", "alt_arrival_min")
        alt_m_min = _val("M", "alt_arrival_min")
        alt_b_min = _val("B", "alt_arrival_min")

        t_dist = _val("T", "distance_ft")
        m_dist = _val("M", "distance_ft")
        b_dist = _val("B", "distance_ft")

        def _delta_time(a: float | None, b: float | None) -> float | None:
            if a is None or b is None:
                return None
            return b - a

        def _velocity(
            d1: float | None,
            d2: float | None,
            t1: float | None,
            t2: float | None,
        ) -> float | None:
            if d1 is None or d2 is None or t1 is None or t2 is None:
                return None
            dt = t2 - t1
            if dt <= 0:
                return None
            return (d2 - d1) / dt

        t_to_m_min = _delta_time(t_min, m_min)
        m_to_b_min = _delta_time(m_min, b_min)
        t_to_b_min = _delta_time(t_min, b_min)

        alt_t_to_m_min = _delta_time(alt_t_min, alt_m_min)
        alt_m_to_b_min = _delta_time(alt_m_min, alt_b_min)
        alt_t_to_b_min = _delta_time(alt_t_min, alt_b_min)

        rows.append(
            {
                "year": year,
                "event_id": event_id,
                "strip": strip,
                "depth_inches": depth_inches,
                "irrigation_start": _meta("irrigation_start"),
                "irrigation_end": _meta("irrigation_end"),
                "gallons_strip": _meta("gallons_strip"),
                "event_duration_hours": _meta("event_duration_hours"),

                "distance_T_ft": t_dist,
                "distance_M_ft": m_dist,
                "distance_B_ft": b_dist,

                "arrival_T_min": t_min,
                "arrival_M_min": m_min,
                "arrival_B_min": b_min,
                "T_to_M_min": t_to_m_min,
                "M_to_B_min": m_to_b_min,
                "T_to_B_min": t_to_b_min,
                "T_to_M_ft_per_min": _velocity(t_dist, m_dist, t_min, m_min),
                "M_to_B_ft_per_min": _velocity(m_dist, b_dist, m_min, b_min),
                "T_to_B_ft_per_min": _velocity(t_dist, b_dist, t_min, b_min),

                "alt_arrival_T_min": alt_t_min,
                "alt_arrival_M_min": alt_m_min,
                "alt_arrival_B_min": alt_b_min,
                "alt_T_to_M_min": alt_t_to_m_min,
                "alt_M_to_B_min": alt_m_to_b_min,
                "alt_T_to_B_min": alt_t_to_b_min,
                "alt_T_to_M_ft_per_min": _velocity(
                    t_dist, m_dist, alt_t_min, alt_m_min
                ),
                "alt_M_to_B_ft_per_min": _velocity(
                    m_dist, b_dist, alt_m_min, alt_b_min
                ),
                "alt_T_to_B_ft_per_min": _velocity(
                    t_dist, b_dist, alt_t_min, alt_b_min
                ),
            }
        )

    out = pd.DataFrame(rows)

    if not out.empty:
        out = out.sort_values(
            ["year", "event_id", "strip", "depth_inches"]
        ).reset_index(drop=True)

    return force_float(move_id_columns_left(out))

def battery_col_for_sensor(sensor_col: str) -> str | None:
    parts = sensor_col.split("_raw_")
    if len(parts) != 2:
        return None
    return f"BattV_Min_{parts[1]}"

def battery_window_summary(
    df_15min: pd.DataFrame,
    battery_col: str | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
    vmin_ok: float = BATTERY_MIN_OK,
    vmax_ok: float = BATTERY_MAX_OK,
) -> dict[str, object]:
    empty: dict[str, object] = {
        "battery_col": battery_col,
        "battery_min_v": pd.NA,
        "battery_median_v": pd.NA,
        "battery_max_v": pd.NA,
        "battery_low_count": pd.NA,
        "battery_high_count": pd.NA,
        "battery_out_of_range_count": pd.NA,
        "battery_out_of_range_fraction": pd.NA,
        "flag_battery_low": False,
        "flag_battery_out_of_range": False,
    }

    if battery_col is None or battery_col not in df_15min.columns:
        return empty

    s = pd.to_numeric(df_15min[battery_col], errors="coerce").loc[start:end].dropna()
    if s.empty:
        return empty

    low_mask = s < vmin_ok
    high_mask = s > vmax_ok
    oor_mask = low_mask | high_mask

    return {
        "battery_col": battery_col,
        "battery_min_v": float(s.min()),
        "battery_median_v": float(s.median()),
        "battery_max_v": float(s.max()),
        "battery_low_count": int(low_mask.sum()),
        "battery_high_count": int(high_mask.sum()),
        "battery_out_of_range_count": int(oor_mask.sum()),
        "battery_out_of_range_fraction": float(oor_mask.mean()),
        "flag_battery_low": bool(low_mask.any()),
        "flag_battery_out_of_range": bool(oor_mask.any()),
    }

def detect_pre_start_response(
    df_15min: pd.DataFrame,
    event_results: pd.DataFrame,
    lookback_hours: float = 6.0,
    min_increase: float = 0.5,
    precip_col: str = "precip_in",
    min_precip_in: float = MIN_PRECIP_IN
) -> pd.DataFrame:
    if event_results.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []

    key_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "sensor_col",
        "depth_index",
        "depth_inches",
        "irrigation_start",
        "irrigation_end",
        "gallons_strip",
        "event_duration_hours",
        "bottom_response_delay_hr",
        "time_to_peak_hours",
        "time_to_plateau_hours",
    ]
    key_cols = [c for c in key_cols if c in event_results.columns]
    events = event_results[key_cols].drop_duplicates().copy()

    has_precip_col = precip_col in df_15min.columns

    for _, row in events.iterrows():
        sensor_col = str(row["sensor_col"])
        if sensor_col not in df_15min.columns:
            continue

        irrigation_start = pd.to_datetime(row["irrigation_start"], errors="coerce")
        if pd.isna(irrigation_start):
            continue

        irrigation_start = pd.Timestamp(irrigation_start)
        window_start = irrigation_start - pd.Timedelta(hours=lookback_hours)

        raw_sub = pd.to_numeric(df_15min[sensor_col], errors="coerce").loc[
            window_start:irrigation_start
        ]
        missing_vwc_fraction_pre_start = float(raw_sub.isna().mean()) if len(raw_sub) else 1.0
        sub = raw_sub.dropna()

        if len(sub) < 2:
            continue

        first_vwc = float(sub.iloc[0])
        last_pre_start_vwc = float(sub.iloc[-1])
        max_pre_start_vwc = float(sub.max())
        max_pre_start_time = pd.Timestamp(sub.idxmax())

        pre_start_increase = max_pre_start_vwc - first_vwc
        last_minus_first = last_pre_start_vwc - first_vwc
        flag_pre_start_response = pre_start_increase >= min_increase

        total_precip_in = 0.0
        max_precip_in = 0.0
        first_precip_time: pd.Timestamp | None = None
        last_precip_time: pd.Timestamp | None = None

        if has_precip_col:
            precip_window = (
                pd.to_numeric(df_15min[precip_col], errors="coerce")
                .loc[window_start:irrigation_start]
                .fillna(0)
            )

            positive_precip = precip_window[precip_window > 0]
            total_precip_in = float(precip_window.sum())
            max_precip_in = float(precip_window.max()) if not precip_window.empty else 0.0

            if not positive_precip.empty:
                first_precip_time = pd.Timestamp(positive_precip.index[0])
                last_precip_time = pd.Timestamp(positive_precip.index[-1])

        precip_in_window = total_precip_in >= min_precip_in
        likely_precip_driven_pre_start_response = (
            flag_pre_start_response and precip_in_window
        )

        battery_col = battery_col_for_sensor(sensor_col)

        battery_pre_start = battery_window_summary(
            df_15min=df_15min,
            battery_col=battery_col,
            start=window_start,
            end=irrigation_start,
        )

        irrigation_end_raw = row.get("irrigation_end")

        if irrigation_end_raw is None:
            irrigation_end_ts = irrigation_start
        else:
            parsed_end = pd.to_datetime(
                cast(Any, irrigation_end_raw),
                errors="coerce",
            )

            if pd.isna(parsed_end):
                irrigation_end_ts = irrigation_start
            else:
                irrigation_end_ts = pd.Timestamp(cast(Any, parsed_end))

        battery_event = battery_window_summary(
            df_15min=df_15min,
            battery_col=battery_col,
            start=irrigation_start,
            end=irrigation_end_ts,
        )

        possible_battery_or_logger_issue = bool(
            battery_pre_start["flag_battery_out_of_range"]
            or battery_event["flag_battery_out_of_range"]
            or missing_vwc_fraction_pre_start > 0.25
        )

        flag_unexplained_pre_start_response = bool(
            flag_pre_start_response
            and not likely_precip_driven_pre_start_response
            and not possible_battery_or_logger_issue
        )

        rows.append(
            {
                "flag_pre_start_response": flag_pre_start_response,
                "flag_unexplained_pre_start_response": flag_unexplained_pre_start_response,
                "likely_precip_driven_pre_start_response": likely_precip_driven_pre_start_response,
                "possible_battery_or_logger_issue": possible_battery_or_logger_issue,
                "year": row.get("year"),
                "strip_group": row.get("strip_group"),
                "location": row.get("location"),
                "strip": row.get("strip"),
                "event_id": row.get("event_id"),
                "sensor_col": sensor_col,
                "depth_index": row.get("depth_index"),
                "depth_inches": row.get("depth_inches"),
                "irrigation_start": irrigation_start,
                "irrigation_end": irrigation_end_ts,
                "window_start": window_start,
                "first_vwc": first_vwc,
                "last_pre_start_vwc": last_pre_start_vwc,
                "max_pre_start_vwc": max_pre_start_vwc,
                "max_pre_start_time": max_pre_start_time,
                "pre_start_increase": pre_start_increase,
                "last_minus_first": last_minus_first,
                "threshold": min_increase,
                "vwc_missing_fraction_pre_start_window": missing_vwc_fraction_pre_start,
                "precip_col": precip_col if has_precip_col else pd.NA,
                "precip_threshold_in": min_precip_in,
                "total_precip_in_pre_start_window": total_precip_in,
                "max_precip_in_pre_start_window": max_precip_in,
                "first_precip_time_pre_start_window": first_precip_time,
                "last_precip_time_pre_start_window": last_precip_time,
                "battery_col": battery_col,
                "battery_pre_start_min_v": battery_pre_start["battery_min_v"],
                "battery_pre_start_median_v": battery_pre_start["battery_median_v"],
                "battery_pre_start_max_v": battery_pre_start["battery_max_v"],
                "battery_pre_start_low_count": battery_pre_start["battery_low_count"],
                "battery_pre_start_high_count": battery_pre_start["battery_high_count"],
                "battery_pre_start_out_of_range_count": battery_pre_start["battery_out_of_range_count"],
                "battery_pre_start_out_of_range_fraction": battery_pre_start["battery_out_of_range_fraction"],
                "flag_battery_low_pre_start": battery_pre_start["flag_battery_low"],
                "flag_battery_out_of_range_pre_start": battery_pre_start["flag_battery_out_of_range"],
                "battery_event_min_v": battery_event["battery_min_v"],
                "battery_event_median_v": battery_event["battery_median_v"],
                "battery_event_max_v": battery_event["battery_max_v"],
                "battery_event_low_count": battery_event["battery_low_count"],
                "battery_event_high_count": battery_event["battery_high_count"],
                "battery_event_out_of_range_count": battery_event["battery_out_of_range_count"],
                "battery_event_out_of_range_fraction": battery_event["battery_out_of_range_fraction"],
                "flag_battery_low_event": battery_event["flag_battery_low"],
                "flag_battery_out_of_range_event": battery_event["flag_battery_out_of_range"],
                "bottom_response_delay_hr": row.get("bottom_response_delay_hr"),
                "time_to_peak_hours": row.get("time_to_peak_hours"),
                "time_to_plateau_hours": row.get("time_to_plateau_hours"),
                "event_duration_hours": row.get("event_duration_hours"),
                "gallons_strip": row.get("gallons_strip"),
            }
        )

    out = pd.DataFrame(rows)

    if not out.empty:
        numeric_cols = out.select_dtypes(include=["number"]).columns
        out[numeric_cols] = out[numeric_cols].round(4)

        out = out.sort_values(
            [
                "flag_unexplained_pre_start_response",
                "possible_battery_or_logger_issue",
                "likely_precip_driven_pre_start_response",
                "year",
                "strip_group",
                "strip",
                "irrigation_start",
            ],
            ascending=[False, False, False, True, True, True, True],
        ).reset_index(drop=True)

    return out

def classify_trustworthy_irrigation_events(
    pre_start_table: pd.DataFrame,
    min_bottom_response_delay_hr: float = MIN_BOTTOM_RESPONSE_DELAY_HR,
) -> pd.DataFrame:
    if pre_start_table.empty:
        return pd.DataFrame()

    out = pre_start_table.copy()

    for col in [
        "bottom_response_delay_hr",
        "event_duration_hours",
        "gallons_strip",
        "time_to_peak_hours",
        "time_to_plateau_hours",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    reasons: list[str] = []
    trustworthy: list[bool] = []

    for _, row in out.iterrows():
        fail_reasons: list[str] = []

        if bool(row.get("flag_unexplained_pre_start_response", False)):
            fail_reasons.append("unexplained_pre_start_response")

        if bool(row.get("possible_battery_or_logger_issue", False)):
            fail_reasons.append("possible_battery_or_logger_issue")

        bottom_delay = row.get("bottom_response_delay_hr")
        if pd.isna(bottom_delay):
            fail_reasons.append("missing_bottom_response_delay")
        elif float(bottom_delay) < min_bottom_response_delay_hr:
            fail_reasons.append("bottom_response_too_early")

        if pd.isna(row.get("gallons_strip")):
            fail_reasons.append("missing_gallons_strip")

        if pd.isna(row.get("event_duration_hours")):
            fail_reasons.append("missing_event_duration_hours")

        is_trustworthy = len(fail_reasons) == 0
        trustworthy.append(is_trustworthy)
        reasons.append("ok" if is_trustworthy else "; ".join(fail_reasons))

    out["trustworthy_event"] = trustworthy
    out["trustworthy_reason"] = reasons
    out["trustworthy_min_bottom_response_delay_hr"] = min_bottom_response_delay_hr

    keep_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "sensor_col",
        "depth_index",
        "depth_inches",
        "irrigation_start",
        "irrigation_end",
        "bottom_response_delay_hr",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "event_duration_hours",
        "gallons_strip",
        "flag_pre_start_response",
        "flag_unexplained_pre_start_response",
        "likely_precip_driven_pre_start_response",
        "possible_battery_or_logger_issue",
        "vwc_missing_fraction_pre_start_window",
        "battery_col",
        "battery_pre_start_min_v",
        "battery_event_min_v",
        "total_precip_in_pre_start_window",
        "trustworthy_event",
        "trustworthy_reason",
        "trustworthy_min_bottom_response_delay_hr",
    ]
    keep_cols = [c for c in keep_cols if c in out.columns]
    out = out[keep_cols].copy()

    out = out.sort_values(
        ["trustworthy_event", "year", "strip_group", "strip", "irrigation_start"],
        ascending=[False, True, True, True, True],
    ).reset_index(drop=True)

    return out

def build_arrival_order_diagnostics(
    arrival_times: pd.DataFrame,
) -> pd.DataFrame:
    if arrival_times.empty:
        return pd.DataFrame()

    def _as_float_or_none(value: object) -> float | None:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return float(parsed) if pd.notna(parsed) else None

    def _as_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        try:
            if bool(pd.Series([value]).isna().iloc[0]):
                return False
        except Exception:
            pass
        return str(value).strip().lower() in {"true", "1", "yes", "y"}

    def _depth_order_class(
        v6: float | None,
        v12: float | None,
        v18: float | None,
    ) -> str:
        if v6 is None or v12 is None or v18 is None:
            return "missing_depths"
        if v6 <= v12 <= v18:
            return "expected"
        if v12 < v6:
            return "12_before_6"
        if v18 < v12:
            return "18_before_12"
        if v18 < v6:
            return "18_before_6"
        return "other"

    def _logger_order_class(
        top: float | None,
        middle: float | None,
        bottom: float | None,
    ) -> str:
        if top is None or middle is None or bottom is None:
            return "missing_loggers"
        if top <= middle <= bottom:
            return "expected"
        if bottom < top or bottom < middle:
            return "bottom_before_top_or_middle"
        if middle < top:
            return "middle_before_top"
        return "other"

    rows: list[dict[str, object]] = []

    group_cols = ["year", "event_id", "strip", "logger_position"]

    for keys, group in arrival_times.groupby(group_cols, dropna=False):
        group = group.copy()

        arrival_lookup: dict[int, float | None] = {}
        alt_lookup: dict[int, float | None] = {}
        alt_before_start_lookup: dict[int, bool] = {}

        for _, row in group.iterrows():
            depth = int(row["depth_inches"])

            arrival_lookup[depth] = _as_float_or_none(
                row.get("arrival_minutes_after_irrigation_start")
            )
            alt_lookup[depth] = _as_float_or_none(
                row.get("alt_arrival_minutes_after_irrigation_start")
            )
            alt_before_start_lookup[depth] = _as_bool(
                row.get("alt_arrival_before_irrigation_start")
            )

        arrival_6 = arrival_lookup.get(6)
        arrival_12 = arrival_lookup.get(12)
        arrival_18 = arrival_lookup.get(18)

        alt_6 = alt_lookup.get(6)
        alt_12 = alt_lookup.get(12)
        alt_18 = alt_lookup.get(18)

        order_group = (
            group[
                pd.to_numeric(
                    group["arrival_minutes_after_irrigation_start"],
                    errors="coerce",
                ).notna()
            ]
            .sort_values("arrival_minutes_after_irrigation_start")
        )

        arrival_order = (
            "none"
            if order_group.empty
            else "→".join(order_group["depth_inches"].astype(int).astype(str))
        )

        alt_order_group = (
            group[
                pd.to_numeric(
                    group["alt_arrival_minutes_after_irrigation_start"],
                    errors="coerce",
                ).notna()
            ]
            .sort_values("alt_arrival_minutes_after_irrigation_start")
        )

        alt_arrival_order = (
            "none"
            if alt_order_group.empty
            else "→".join(alt_order_group["depth_inches"].astype(int).astype(str))
        )

        order_class = _depth_order_class(arrival_6, arrival_12, arrival_18)
        alt_order_class = _depth_order_class(alt_6, alt_12, alt_18)

        alt_before_start_depths = [
            str(depth)
            for depth, value in alt_before_start_lookup.items()
            if value
        ]

        all_alt_before_start = (
            bool(alt_before_start_lookup.get(6))
            and bool(alt_before_start_lookup.get(12))
            and bool(alt_before_start_lookup.get(18))
        )

        any_alt_before_start = bool(alt_before_start_depths)

        if order_class == "missing_depths" and all_alt_before_start:
            order_class = "all_depths_arrived_before_start"
        elif order_class == "missing_depths" and any_alt_before_start:
            order_class = "some_depths_arrived_before_start"

        if alt_order_class == "missing_depths" and any_alt_before_start:
            alt_order_class = "some_depths_arrived_before_start"

        rows.append(
            {
                "year": keys[0],
                "event_id": keys[1],
                "strip": keys[2],
                "logger_position": keys[3],
                "arrival_6in_min": arrival_6,
                "arrival_12in_min": arrival_12,
                "arrival_18in_min": arrival_18,
                "arrival_order": arrival_order,
                "expected_order": order_class == "expected",
                "order_class": order_class,
                "alt_arrival_6in_min": alt_6,
                "alt_arrival_12in_min": alt_12,
                "alt_arrival_18in_min": alt_18,
                "alt_arrival_order": alt_arrival_order,
                "alt_expected_order": alt_order_class == "expected",
                "alt_order_class": alt_order_class,
                "alt_before_start_depths": ",".join(alt_before_start_depths),
                "any_alt_before_start": any_alt_before_start,
                "all_alt_before_start": all_alt_before_start,
            }
        )

    out = pd.DataFrame(rows)

    if out.empty:
        return out

    logger_rows: list[dict[str, object]] = []

    for (year, event_id, strip), group in arrival_times.groupby(
        ["year", "event_id", "strip"],
        dropna=False,
    ):
        row_out: dict[str, object] = {
            "year": year,
            "event_id": event_id,
            "strip": strip,
        }

        any_bottom_before_upper = False
        any_alt_bottom_before_upper = False
        any_all_loggers_before_start = False

        for depth in [6, 12, 18]:
            depth_group = group[group["depth_inches"].astype(int).eq(depth)].copy()

            primary_by_logger: dict[str, float | None] = {}
            alt_by_logger: dict[str, float | None] = {}
            alt_before_by_logger: dict[str, bool] = {}

            for _, r in depth_group.iterrows():
                loc = str(r["logger_position"]).strip()

                primary_by_logger[loc] = _as_float_or_none(
                    r.get("arrival_minutes_after_irrigation_start")
                )
                alt_by_logger[loc] = _as_float_or_none(
                    r.get("alt_arrival_minutes_after_irrigation_start")
                )
                alt_before_by_logger[loc] = _as_bool(
                    r.get("alt_arrival_before_irrigation_start")
                )

            t = primary_by_logger.get("T")
            m = primary_by_logger.get("M")
            b = primary_by_logger.get("B")

            alt_t = alt_by_logger.get("T")
            alt_m = alt_by_logger.get("M")
            alt_b = alt_by_logger.get("B")

            primary_class = _logger_order_class(t, m, b)
            alt_class = _logger_order_class(alt_t, alt_m, alt_b)

            all_loggers_before_start = (
                bool(alt_before_by_logger.get("T"))
                and bool(alt_before_by_logger.get("M"))
                and bool(alt_before_by_logger.get("B"))
            )
            any_loggers_before_start = any(alt_before_by_logger.values())

            if primary_class == "missing_loggers" and all_loggers_before_start:
                primary_class = "all_loggers_arrived_before_start"
            elif primary_class == "missing_loggers" and any_loggers_before_start:
                primary_class = "some_loggers_arrived_before_start"

            if alt_class == "missing_loggers" and any_loggers_before_start:
                alt_class = "some_loggers_arrived_before_start"

            if primary_class == "bottom_before_top_or_middle":
                any_bottom_before_upper = True

            if alt_class == "bottom_before_top_or_middle":
                any_alt_bottom_before_upper = True

            if all_loggers_before_start:
                any_all_loggers_before_start = True

            row_out[f"arrival_{depth}in_T_min"] = t
            row_out[f"arrival_{depth}in_M_min"] = m
            row_out[f"arrival_{depth}in_B_min"] = b
            row_out[f"arrival_{depth}in_logger_order_class"] = primary_class

            row_out[f"alt_arrival_{depth}in_T_min"] = alt_t
            row_out[f"alt_arrival_{depth}in_M_min"] = alt_m
            row_out[f"alt_arrival_{depth}in_B_min"] = alt_b
            row_out[f"alt_arrival_{depth}in_logger_order_class"] = alt_class

        row_out["any_bottom_before_top_or_middle"] = any_bottom_before_upper
        row_out["any_alt_bottom_before_top_or_middle"] = any_alt_bottom_before_upper
        row_out["any_all_loggers_before_start"] = any_all_loggers_before_start

        logger_rows.append(row_out)

    logger_order = pd.DataFrame(logger_rows)

    out = out.merge(
        logger_order,
        on=["year", "event_id", "strip"],
        how="left",
    )

    out = out.sort_values(
        ["year", "event_id", "strip", "logger_position"]
    ).reset_index(drop=True)

    return out
