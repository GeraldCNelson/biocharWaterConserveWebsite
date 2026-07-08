from __future__ import annotations

import numpy as np
import pandas as pd

from biochar_app.config.irrigation_config import (
    ARRIVAL_RESPONSE_THRESHOLD_VWC,
    ALTERNATE_ARRIVAL_RESPONSE_THRESHOLD_VWC,
    EVENT_PLOT_HOURS_AFTER,
    EVENT_PLOT_HOURS_BEFORE,
)

def detect_sustained_baseline_arrival(
    vwc_series: pd.Series,
    baseline_vwc: float,
    irrigation_start: pd.Timestamp,
    response_threshold_vwc: float,
    min_persist_points: int = 4,
    min_followup_rise_vwc: float = 0.50,
) -> tuple[pd.Timestamp | None, float | None]:
    """
    Standard arrival:
    first time after irrigation start that VWC exceeds baseline by threshold
    and is followed by a sustained rise.

    With 15-min data:
      - min_persist_points=4 means about 1 hour.
      - min_followup_rise_vwc=0.50 means the VWC must rise at least another
        0.50 percentage points during that persistence window.

    This avoids treating small post-start drift as arrival.
    """
    clean = pd.to_numeric(vwc_series, errors="coerce").dropna()

    if clean.empty or len(clean) < min_persist_points:
        return None, None

    arrival_search = clean.loc[irrigation_start:]


    if arrival_search.empty or len(arrival_search) < min_persist_points:
        return None, None

    target_vwc = float(baseline_vwc) + float(response_threshold_vwc)
    candidates = arrival_search[arrival_search >= target_vwc]
    if candidates.empty:
        return None, None

    for candidate_time in candidates.index:
        candidate_pos = clean.index.get_loc(candidate_time)
        if isinstance(candidate_pos, slice) or not isinstance(candidate_pos, int):
            continue
        after = clean.iloc[candidate_pos : candidate_pos + min_persist_points]
        if len(after) < min_persist_points:
            continue
        if not (after >= target_vwc).all():
            continue
        candidate_vwc = float(clean.loc[candidate_time])
        followup_rise = float(after.iloc[-1]) - candidate_vwc
        if followup_rise < min_followup_rise_vwc:
            continue
        return pd.Timestamp(candidate_time), candidate_vwc

    return None, None

def detect_alt_arrival_from_vwc_step(
    vwc_series: pd.Series,
    response_threshold_vwc: float,
    min_persist_points: int = 4,
    min_total_rise_vwc: float | None = None,
) -> tuple[pd.Timestamp | None, float | None, float | None]:
    """
    Detect alternate arrival from the VWC trace itself.

    Alternate arrival is defined as the first increase of at least
    response_threshold_vwc that is followed by a sustained rise in VWC.
    """
    clean = pd.to_numeric(vwc_series, errors="coerce").dropna()

    if clean.empty or len(clean) < min_persist_points + 1:
        return None, None, None

    if min_total_rise_vwc is None:
        min_total_rise_vwc = response_threshold_vwc

    smoothed = clean.rolling(
        window=3,
        min_periods=1,
        center=True,
    ).median()

    step: pd.Series = pd.to_numeric(smoothed.diff(), errors="coerce").dropna()
    candidate_mask = step >= response_threshold_vwc
    candidates: pd.Series = step.loc[candidate_mask]

    if candidates.empty:
        return None, None, None

    for candidate_time in candidates.index:
        candidate_timestamp = pd.Timestamp(candidate_time)
        candidate_pos = clean.index.get_loc(candidate_timestamp)

        if isinstance(candidate_pos, slice) or not isinstance(candidate_pos, int):
            continue

        before_pos = max(candidate_pos - 1, 0)
        before_value = float(clean.iloc[before_pos])

        after = clean.iloc[candidate_pos : candidate_pos + min_persist_points]

        if len(after) < min_persist_points:
            continue

        if not (after >= before_value + min_total_rise_vwc).all():
            continue

        if float(after.iloc[-1]) < before_value + min_total_rise_vwc:
            continue

        candidate_value = float(clean.iloc[candidate_pos])
        step_value = float(candidates.loc[candidate_timestamp])

        return (
            candidate_timestamp,
            candidate_value,
            step_value,
        )

    return None, None, None

def build_irrigation_arrival_times(
    df_15min: pd.DataFrame,
    event_results: pd.DataFrame,
    response_threshold_vwc: float = ARRIVAL_RESPONSE_THRESHOLD_VWC,
    hours_before: float = EVENT_PLOT_HOURS_BEFORE,
    hours_after: float = EVENT_PLOT_HOURS_AFTER,
) -> pd.DataFrame:
    if event_results.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []

    events = (
        event_results[
            [
                "year",
                "strip_group",
                "location",
                "strip",
                "event_id",
                "sensor_col",
                "depth_index",
                "depth_inches",
                "logger_position",
                "irrigation_start",
                "irrigation_end",
                "baseline_vwc",
                "gallons_strip",
                "event_duration_hours",
            ]
        ]
        .drop_duplicates()
        .copy()
    )

    for _, row in events.iterrows():
        sensor_col = str(row["sensor_col"])
        if sensor_col not in df_15min.columns:
            continue

        irrigation_start = pd.to_datetime(row["irrigation_start"], errors="coerce")
        if pd.isna(irrigation_start):
            continue

        irrigation_start = pd.Timestamp(irrigation_start)
        irrigation_end = pd.to_datetime(row["irrigation_end"], errors="coerce")
        irrigation_end = (
            pd.Timestamp(irrigation_end)
            if not pd.isna(irrigation_end)
            else pd.NaT
        )

        plot_start = irrigation_start - pd.Timedelta(hours=hours_before)
        plot_end = irrigation_start + pd.Timedelta(hours=hours_after)

        vwc_series = (
            pd.to_numeric(df_15min[sensor_col], errors="coerce")
            .loc[plot_start:plot_end]
            .dropna()
        )

        if vwc_series.empty:
            continue

        (
            alt_arrival_time,
            alt_arrival_vwc,
            alt_arrival_step_vwc,
        ) = detect_alt_arrival_from_vwc_step(
            vwc_series=vwc_series,
            response_threshold_vwc=ALTERNATE_ARRIVAL_RESPONSE_THRESHOLD_VWC,
        )

        baseline_vwc = pd.to_numeric(
            pd.Series([row.get("baseline_vwc")]),
            errors="coerce",
        ).iloc[0]

        if pd.isna(baseline_vwc):
            pre = vwc_series.loc[:irrigation_start]
            baseline_vwc = pre.median() if not pre.empty else pd.NA

        response_time: pd.Timestamp | None = None
        response_vwc: float | None = None

        if pd.notna(baseline_vwc):
            response_time, response_vwc = detect_sustained_baseline_arrival(
                vwc_series=vwc_series,
                baseline_vwc=float(baseline_vwc),
                irrigation_start=irrigation_start,
                response_threshold_vwc=response_threshold_vwc,
                min_persist_points=4,
            )

        rows.append(
            {
                "year": row.get("year"),
                "strip_group": row.get("strip_group"),
                "location": row.get("location"),
                "strip": row.get("strip"),
                "event_id": row.get("event_id"),
                "sensor_col": sensor_col,
                "depth_index": row.get("depth_index"),
                "depth_inches": row.get("depth_inches"),
                "logger_position": row.get("logger_position"),
                "irrigation_start": irrigation_start,
                "irrigation_end": irrigation_end,
                "plot_start": plot_start,
                "plot_end": plot_end,
                "baseline_vwc": baseline_vwc,
                "response_threshold_vwc": response_threshold_vwc,
                "response_time": response_time,
                "response_vwc": response_vwc,
                "minutes_after_irrigation_start": (
                    (response_time - irrigation_start).total_seconds() / 60.0
                    if response_time is not None
                    else pd.NA
                ),
                "response_before_irrigation_start": (
                    response_time < irrigation_start
                    if response_time is not None
                    else pd.NA
                ),
                "gallons_strip": row.get("gallons_strip"),
                "event_duration_hours": row.get("event_duration_hours"),
                "alt_arrival_threshold_vwc": ALTERNATE_ARRIVAL_RESPONSE_THRESHOLD_VWC,
                "alt_arrival_time": alt_arrival_time,
                "alt_arrival_vwc": alt_arrival_vwc,
                "alt_arrival_step_vwc": alt_arrival_step_vwc,
                "alt_arrival_minutes_after_irrigation_start": (
                    (alt_arrival_time - irrigation_start).total_seconds() / 60.0
                    if alt_arrival_time is not None
                    else pd.NA
                ),
                "alt_arrival_before_irrigation_start": (
                    alt_arrival_time < irrigation_start
                    if alt_arrival_time is not None
                    else pd.NA
                ),
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        numeric_cols = out.select_dtypes(include=["number"]).columns
        out[numeric_cols] = out[numeric_cols].round(3)

        out = out.sort_values(
            [
                "year",
                "strip_group",
                "event_id",
                "strip",
                "logger_position",
                "depth_inches",
            ],
        ).reset_index(drop=True)

        out = out.rename(
            columns={
                "response_threshold_vwc": "arrival_threshold_vwc",
                "response_time": "arrival_time",
                "response_vwc": "arrival_vwc",
                "minutes_after_irrigation_start": "arrival_minutes_after_irrigation_start",
                "response_before_irrigation_start": "arrival_before_irrigation_start",
            }
        )

    return out