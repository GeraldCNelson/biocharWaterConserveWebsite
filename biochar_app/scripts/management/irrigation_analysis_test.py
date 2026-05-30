"""
biochar_app.scripts.management.irrigation_analysis_test.py
Purpose
-------
This script evaluates irrigation-event response dynamics using bottom-profile
soil moisture sensors (VWC) and irrigation management records.
"""

from pathlib import Path
import pandas as pd

from biochar_app.config.field_management_metadata import (
    #PROFILE_AREA_SQFT_BY_STRIP_LOGGER,
    PROFILE_AREA_SQFT,
    PROFILE_GALLONS_PER_INCH,
    STRIP_WIDTH_FT,
    STRIP_LENGTH_FT,
    LOGGER_POSITIONS_PER_STRIP,
    INCHES_WATER_TO_GALLONS_PER_SQFT,
    PROFILE_AREA_SQFT,
)
# Ballpark logger service area:
# strip width 46 ft × strip length 280 ft ÷ 3 logger positions.
profile_area_sqft = PROFILE_AREA_SQFT

from biochar_app.config.core import (
    SENSOR_DEPTH_CODES,
    SENSOR_DEPTH_VALUES,
    STRIPS,
    YEARS,
)
from biochar_app.scripts.data_loading import (
    load_irrigation_data,
    load_logger_data,
    prepare_irrigation_input,
)
from biochar_app.scripts.management.irrigation_analysis import (
    analyze_irrigation_events,
    build_variable_definitions_table,
    build_variable_definitions_with_sources,
    summarize_targets_and_runtimes,
)
from biochar_app.scripts.management.irrigation_plots import (
    save_irrigation_event_multidepth_plots,
)

BASE_DIR = Path(__file__).resolve().parents[3]
OUTPUT_DIR = BASE_DIR / "biochar_app" / "data-processed" / "management" / "irrigation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MULTIDEPTH_PLOT_DIR = OUTPUT_DIR / "irrigation_event_multidepth_plots"

VERBOSE = False

DEPTH_INDEX_TO_INCHES: dict[str, int] = {
    depth_code: int(SENSOR_DEPTH_VALUES[depth_code]["us"])
    for depth_code in SENSOR_DEPTH_CODES
}

BATTERY_MIN_OK = 11.0
BATTERY_MAX_OK = 13.0
MIN_BOTTOM_RESPONSE_DELAY_HR = 0.5


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
    empty = {
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


def build_bottom_logger_profile_map() -> dict[str, list[str]]:
    return {
        strip: [f"VWC_{depth_code}_raw_{strip}_B" for depth_code in SENSOR_DEPTH_CODES]
        for strip in STRIPS
    }


def prepare_15min_logger_data(year: int) -> pd.DataFrame:
    df_15min = load_logger_data(year=year, granularity="15min")

    if "timestamp" in df_15min.columns:
        df_15min["timestamp"] = pd.to_datetime(df_15min["timestamp"], errors="coerce")
        duplicate_count = int(df_15min["timestamp"].duplicated().sum())
        df_15min = prepare_irrigation_input(df_15min)

    elif isinstance(df_15min.index, pd.DatetimeIndex):
        duplicate_count = int(df_15min.index.duplicated().sum())
        df_15min.index = pd.to_datetime(df_15min.index, errors="coerce")
        df_15min = df_15min[~df_15min.index.isna()].copy()
        df_15min = df_15min.sort_index()
        df_15min = df_15min[~df_15min.index.duplicated(keep="last")].copy()

    else:
        raise ValueError(f"Could not find timestamp column or DatetimeIndex for year {year}")

    print(
        f"Year {year}: {len(df_15min):,} 15-min rows prepared "
        f"({duplicate_count} duplicate timestamps removed)."
    )

    if VERBOSE:
        print("Columns sample:", df_15min.columns.tolist()[:20])
        print("Index:", type(df_15min.index), df_15min.index.name)

    return df_15min


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

    peak_hours = (
        pd.to_numeric(out["time_to_peak_hours"], errors="coerce")
        if "time_to_peak_hours" in out.columns
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

    out["bottom_response_delay_hr"] = peak_hours
    out["lag_after_irrigation_hr"] = plateau_hours - duration_hours
    out["avg_flow_gph_strip"] = gallons_strip / duration_hours
    out.loc[duration_hours <= 0, "avg_flow_gph_strip"] = pd.NA

    return out


def attach_event_metadata(results: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return results

    out = results.copy()

    meta_cols = ["start", "end"]
    for col in ["event_id", "strip_group", "location"]:
        if col in events.columns:
            meta_cols.append(col)

    meta = events[meta_cols].drop_duplicates().copy()

    if "event_id" in meta.columns and "event_id" in out.columns:
        nonmissing_meta = meta[meta["event_id"].fillna("").astype(str).str.strip().ne("")]
        if not nonmissing_meta.empty:
            out = out.merge(
                nonmissing_meta.drop(columns=["start", "end"], errors="ignore"),
                on="event_id",
                how="left",
            )

    missing_strip_group = (
        "strip_group" not in out.columns
        or out["strip_group"].fillna("").astype(str).str.strip().eq("").any()
    )

    if missing_strip_group:
        merge_meta = meta.rename(
            columns={"start": "irrigation_start", "end": "irrigation_end"}
        )

        merge_cols = ["irrigation_start", "irrigation_end"]
        add_cols = [c for c in ["strip_group", "location"] if c in merge_meta.columns]

        if add_cols:
            merge_meta = merge_meta[merge_cols + add_cols].drop_duplicates()
            out = out.merge(
                merge_meta,
                on=merge_cols,
                how="left",
                suffixes=("", "_from_time"),
            )

            for col in add_cols:
                fallback_col = f"{col}_from_time"
                if fallback_col in out.columns:
                    if col in out.columns:
                        out[col] = out[col].where(out[col].notna(), out[fallback_col])
                        out = out.drop(columns=[fallback_col])
                    else:
                        out = out.rename(columns={fallback_col: col})

    return out


def analyze_bottom_logger_all_depths(
    df_15min: pd.DataFrame,
    strips: list[str],
    year: int,
    strip_to_profile_sensors: dict[str, list[str]],
) -> pd.DataFrame:
    all_results: list[pd.DataFrame] = []
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
        sensor_cols = strip_to_profile_sensors.get(strip, [])
        available_sensor_cols = [col for col in sensor_cols if col in df_15min.columns]

        if not available_sensor_cols:
            print(f"Skipping {strip}: no configured bottom-logger columns found.")
            continue

        select_cols = ["start_timestamp", "end_timestamp", "gallons_strip"]

        for optional_col in ["gallons_group", "event_id", "strip_group", "location"]:
            if optional_col in all_events.columns:
                select_cols.append(optional_col)

        events = all_events.loc[
            (all_events["strip"] == strip) & (all_events["year"] == year),
            select_cols,
        ].copy()

        if events.empty:
            print(f"Skipping {strip}: no irrigation events returned.")
            continue

        events = events.rename(
            columns={"start_timestamp": "start", "end_timestamp": "end"}
        )

        strip_results = analyze_irrigation_events(
            df=df_15min,
            events=events,
            sensor_cols=available_sensor_cols,
            start_col="start",
            end_col="end",
            gallons_strip_col="gallons_strip",
            gallons_group_col="gallons_group" if "gallons_group" in events.columns else None,
            strip=strip,
            year=year,
            event_id_col="event_id" if "event_id" in events.columns else None,
        )

        strip_results = attach_event_metadata(strip_results, events)

        if not strip_results.empty:
            all_results.append(strip_results)

    if not all_results:
        return pd.DataFrame()

    out = pd.concat(all_results, ignore_index=True)
    out = add_derived_event_fields(out)
    return out

def build_first_pass_water_balance_table(
    trustworthy_table: pd.DataFrame,
    event_results: pd.DataFrame,
) -> pd.DataFrame:
    if trustworthy_table.empty or event_results.empty:
        return pd.DataFrame()

    trusted = trustworthy_table[trustworthy_table["trustworthy_event"] == True].copy()
    if trusted.empty:
        return pd.DataFrame()

    merge_cols = ["year", "strip", "event_id", "sensor_col"]

    work = event_results.merge(
        trusted[merge_cols].drop_duplicates(),
        on=merge_cols,
        how="inner",
    )

    if work.empty:
        return pd.DataFrame()

    numeric_cols = [
        "gallons_strip",
        "event_storage_gal",
        "event_storage_in",
        "profile_area_sqft",
        "bottom_response_delay_hr",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "event_duration_hours",
    ]

    for col in numeric_cols:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    group_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "irrigation_start",
        "irrigation_end",
    ]
    group_cols = [c for c in group_cols if c in work.columns]

    out = (
        work.groupby(group_cols, dropna=False)
        .agg(
            n_trustworthy_depths=("sensor_col", "nunique"),
            mean_gallons_strip=("gallons_strip", "mean"),
            mean_event_storage_gal=("event_storage_gal", "mean"),
            mean_event_storage_in=("event_storage_in", "mean"),
            mean_profile_area_sqft=("profile_area_sqft", "mean"),
            mean_bottom_response_delay_hr=("bottom_response_delay_hr", "mean"),
            mean_time_to_peak_hours=("time_to_peak_hours", "mean"),
            mean_time_to_plateau_hours=("time_to_plateau_hours", "mean"),
            mean_event_duration_hours=("event_duration_hours", "mean"),
        )
        .reset_index()
    )

    out["estimated_surplus_gal_strip"] = (
        out["mean_gallons_strip"] - out["mean_event_storage_gal"]
    )

    out["estimated_surplus_fraction"] = (
        out["estimated_surplus_gal_strip"] / out["mean_gallons_strip"]
    )

    out.loc[out["mean_gallons_strip"] <= 0, "estimated_surplus_fraction"] = pd.NA

    numeric_out = out.select_dtypes(include=["number"]).columns
    out[numeric_out] = out[numeric_out].round(4)

    return out

def build_enhanced_event_debug_table(
    event_results: pd.DataFrame,
    decimals: int = 2,
) -> pd.DataFrame:
    if event_results.empty:
        return pd.DataFrame()

    keep_cols = [
        "event_id",
        "strip_group",
        "location",
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
        "bottom_response_delay_hr",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "lag_after_irrigation_hr",
        "profile_baseline_storage_gal",
        "profile_plateau_storage_gal",
        "event_storage_gal",
        "efficiency_strip",
        "estimated_loss_gal_strip",
    ]

    keep_cols = [c for c in keep_cols if c in event_results.columns]
    out = event_results[keep_cols].copy()

    numeric_cols = out.select_dtypes(include=["number"]).columns
    out[numeric_cols] = out[numeric_cols].round(decimals)

    return out


def build_enhanced_runtime_table(
    event_results: pd.DataFrame,
    min_events: int = 3,
) -> pd.DataFrame:
    if event_results.empty:
        return pd.DataFrame()

    required_group_cols = ["strip", "depth_inches"]
    df = event_results.copy()

    for col in ["time_to_plateau_hours", "event_duration_hours", "avg_flow_gph_strip"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    runtime_rows: list[dict[str, object]] = []

    for group_key, sub in df.groupby(required_group_cols, dropna=False):
        rec_runtime_vals = pd.to_numeric(
            sub["time_to_plateau_hours"], errors="coerce"
        ).dropna()

        n_events = int(rec_runtime_vals.shape[0])
        if n_events == 0:
            continue

        actual_runtime_vals = pd.to_numeric(
            sub["event_duration_hours"], errors="coerce"
        ).dropna()
        flow_vals = pd.to_numeric(sub["avg_flow_gph_strip"], errors="coerce").dropna()

        lag_vals = (
            pd.to_numeric(sub["lag_after_irrigation_hr"], errors="coerce").dropna()
            if "lag_after_irrigation_hr" in sub.columns
            else pd.Series(dtype="float64")
        )

        rec_runtime_hours = float(rec_runtime_vals.median())
        actual_runtime_hours = (
            float(actual_runtime_vals.median())
            if not actual_runtime_vals.empty
            else pd.NA
        )

        row: dict[str, object] = {
            "n_events": n_events,
            "rec_runtime_hours": rec_runtime_hours,
            "rec_runtime_minutes": rec_runtime_hours * 60.0,
            "rec_runtime_is_trustworthy": n_events >= min_events,
            "actual_runtime_hours": actual_runtime_hours,
            "actual_runtime_minutes": (
                actual_runtime_hours * 60.0
                if actual_runtime_hours is not pd.NA
                else pd.NA
            ),
            "source_time_col": "time_to_plateau_hours",
            "summary_stat": "median",
            "median_avg_flow_gph_strip": (
                float(flow_vals.median()) if not flow_vals.empty else pd.NA
            ),
            "median_lag_after_irrigation_hr": (
                float(lag_vals.median()) if not lag_vals.empty else pd.NA
            ),
        }

        if not isinstance(group_key, tuple):
            group_key = (group_key,)

        for col_name, value in zip(required_group_cols, group_key):
            row[col_name] = value

        runtime_rows.append(row)

    out = pd.DataFrame(runtime_rows)

    if not out.empty:
        numeric_cols = out.select_dtypes(include=["number"]).columns
        out[numeric_cols] = out[numeric_cols].round(6)

    return out


def detect_pre_start_response(
    df_15min: pd.DataFrame,
    event_results: pd.DataFrame,
    lookback_hours: float = 6.0,
    min_increase: float = 0.5,
    precip_col: str = "precip_in",
    min_precip_in: float = 0.01,
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

        irrigation_end = pd.to_datetime(row.get("irrigation_end"), errors="coerce")
        if pd.isna(irrigation_end):
            irrigation_end = irrigation_start
        irrigation_end = pd.Timestamp(irrigation_end)

        battery_event = battery_window_summary(
            df_15min=df_15min,
            battery_col=battery_col,
            start=irrigation_start,
            end=irrigation_end,
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
                "irrigation_end": irrigation_end,
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

def summarize_holding_capacity_from_trustworthy_events(
    trustworthy_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    Estimate logger-location holding capacity from trustworthy irrigation events.

    Uses trustworthy sensor/event/depth rows only. The main response variable is
    plateau_vwc if available; otherwise this table is mainly a timing/volume
    summary until plateau_vwc is carried into trustworthy_table.
    """
    if trustworthy_table.empty:
        return pd.DataFrame()

    df = trustworthy_table.copy()
    df = df[df["trustworthy_event"] == True].copy()

    if df.empty:
        return pd.DataFrame()

    numeric_cols = [
        "bottom_response_delay_hr",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "event_duration_hours",
        "gallons_strip",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    group_cols = ["strip_group", "location", "strip", "sensor_col", "depth_index", "depth_inches"]
    group_cols = [c for c in group_cols if c in df.columns]

    summary = (
        df.groupby(group_cols, dropna=False)
        .agg(
            n_trustworthy_events=("trustworthy_event", "size"),
            mean_bottom_response_delay_hr=("bottom_response_delay_hr", "mean"),
            sd_bottom_response_delay_hr=("bottom_response_delay_hr", "std"),
            mean_time_to_plateau_hours=("time_to_plateau_hours", "mean"),
            sd_time_to_plateau_hours=("time_to_plateau_hours", "std"),
            mean_event_duration_hours=("event_duration_hours", "mean"),
            sd_event_duration_hours=("event_duration_hours", "std"),
            mean_gallons_strip=("gallons_strip", "mean"),
            sd_gallons_strip=("gallons_strip", "std"),
        )
        .reset_index()
    )

    numeric_summary_cols = summary.select_dtypes(include=["number"]).columns
    summary[numeric_summary_cols] = summary[numeric_summary_cols].round(4)

    return summary

def force_float(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    numeric_cols = df.select_dtypes(include=["number"]).columns
    df[numeric_cols] = df[numeric_cols].astype(float)
    return df


def move_id_columns_left(df: pd.DataFrame) -> pd.DataFrame:
    left_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "sensor_col",
        "depth_index",
        "depth_inches",
    ]
    left_cols = [col for col in left_cols if col in df.columns]
    other_cols = [col for col in df.columns if col not in left_cols]
    return df[left_cols + other_cols]

def build_trustworthy_holding_capacity_summary(
    trustworthy_table: pd.DataFrame,
    event_results: pd.DataFrame,
) -> pd.DataFrame:
    if trustworthy_table.empty or event_results.empty:
        return pd.DataFrame()

    trusted = trustworthy_table[trustworthy_table["trustworthy_event"] == True].copy()
    if trusted.empty:
        return pd.DataFrame()

    merge_cols = ["year", "strip", "event_id", "sensor_col"]

    trusted_event_results = event_results.merge(
        trusted[merge_cols].drop_duplicates(),
        on=merge_cols,
        how="inner",
    )

    if trusted_event_results.empty:
        return pd.DataFrame()

    numeric_cols = [
        "bottom_response_delay_hr",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "event_duration_hours",
        "gallons_strip",
        "baseline_vwc",
        "peak_vwc",
        "peak_increase",
        "plateau_vwc",
        "profile_baseline_storage_gal",
        "profile_plateau_storage_gal",
        "event_storage_gal",
        "efficiency_strip",
        "estimated_loss_gal_strip",
    ]

    for col in numeric_cols:
        if col in trusted_event_results.columns:
            trusted_event_results[col] = pd.to_numeric(
                trusted_event_results[col],
                errors="coerce",
            )

    group_cols = [
        "strip_group",
        "location",
        "strip",
        "sensor_col",
        "depth_index",
        "depth_inches",
    ]
    group_cols = [c for c in group_cols if c in trusted_event_results.columns]

    summary = (
        trusted_event_results
        .groupby(group_cols, dropna=False)
        .agg(
            n_trustworthy_events=("event_id", "nunique"),

            mean_bottom_response_delay_hr=("bottom_response_delay_hr", "mean"),
            sd_bottom_response_delay_hr=("bottom_response_delay_hr", "std"),

            mean_time_to_peak_hours=("time_to_peak_hours", "mean"),
            sd_time_to_peak_hours=("time_to_peak_hours", "std"),

            mean_time_to_plateau_hours=("time_to_plateau_hours", "mean"),
            sd_time_to_plateau_hours=("time_to_plateau_hours", "std"),

            mean_event_duration_hours=("event_duration_hours", "mean"),
            sd_event_duration_hours=("event_duration_hours", "std"),

            mean_gallons_strip=("gallons_strip", "mean"),
            sd_gallons_strip=("gallons_strip", "std"),

            mean_baseline_vwc=("baseline_vwc", "mean"),
            sd_baseline_vwc=("baseline_vwc", "std"),

            mean_peak_vwc=("peak_vwc", "mean"),
            sd_peak_vwc=("peak_vwc", "std"),

            mean_peak_increase=("peak_increase", "mean"),
            sd_peak_increase=("peak_increase", "std"),

            mean_plateau_vwc=("plateau_vwc", "mean"),
            sd_plateau_vwc=("plateau_vwc", "std"),

            mean_profile_baseline_storage_gal=("profile_baseline_storage_gal", "mean"),
            sd_profile_baseline_storage_gal=("profile_baseline_storage_gal", "std"),

            mean_profile_plateau_storage_gal=("profile_plateau_storage_gal", "mean"),
            sd_profile_plateau_storage_gal=("profile_plateau_storage_gal", "std"),

            mean_event_storage_gal=("event_storage_gal", "mean"),
            sd_event_storage_gal=("event_storage_gal", "std"),

            mean_efficiency_strip=("efficiency_strip", "mean"),
            sd_efficiency_strip=("efficiency_strip", "std"),

            mean_estimated_loss_gal_strip=("estimated_loss_gal_strip", "mean"),
            sd_estimated_loss_gal_strip=("estimated_loss_gal_strip", "std"),
        )
        .reset_index()
    )


    summary["profile_area_sqft"] = PROFILE_AREA_SQFT
    summary["gallons_per_profile_inch"] = PROFILE_GALLONS_PER_INCH

    summary["mean_profile_baseline_storage_in"] = (
        summary["mean_profile_baseline_storage_gal"] / PROFILE_GALLONS_PER_INCH
    )
    summary["mean_profile_plateau_storage_in"] = (
        summary["mean_profile_plateau_storage_gal"] / PROFILE_GALLONS_PER_INCH
    )
    summary["mean_event_storage_in"] = (
        summary["mean_event_storage_gal"] / PROFILE_GALLONS_PER_INCH
    )

    summary["mean_profile_baseline_storage_gal_scaled"] = (
        summary["mean_profile_baseline_storage_in"] * PROFILE_GALLONS_PER_INCH
    )
    summary["mean_profile_plateau_storage_gal_scaled"] = (
        summary["mean_profile_plateau_storage_in"] * PROFILE_GALLONS_PER_INCH
    )
    summary["mean_event_storage_gal_scaled"] = (
        summary["mean_event_storage_in"] * PROFILE_GALLONS_PER_INCH
    )

    if "depth_index" in summary.columns:
        summary = summary.drop(columns=["depth_index"])

    numeric_out = summary.select_dtypes(include=["number"]).columns
    summary[numeric_out] = summary[numeric_out].round(4)

    return summary

def add_scaled_storage_fields(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return results.copy()

    out = results.copy()

    out["profile_area_sqft"] = PROFILE_AREA_SQFT
    out["gallons_per_profile_inch"] = (
        out["profile_area_sqft"] * INCHES_WATER_TO_GALLONS_PER_SQFT
    )

    baseline_vwc = pd.to_numeric(out["baseline_vwc"], errors="coerce")
    plateau_vwc = pd.to_numeric(out["plateau_vwc"], errors="coerce")
    depth_inches = pd.to_numeric(out["depth_inches"], errors="coerce")
    gallons_per_inch = pd.to_numeric(out["gallons_per_profile_inch"], errors="coerce")
    gallons_strip = pd.to_numeric(out["gallons_strip"], errors="coerce")

    out["profile_baseline_storage_in"] = baseline_vwc / 100.0 * depth_inches
    out["profile_plateau_storage_in"] = plateau_vwc / 100.0 * depth_inches
    out["event_storage_in"] = (
        out["profile_plateau_storage_in"] - out["profile_baseline_storage_in"]
    )

    out["profile_baseline_storage_gal_scaled"] = (
        out["profile_baseline_storage_in"] * gallons_per_inch
    )
    out["profile_plateau_storage_gal_scaled"] = (
        out["profile_plateau_storage_in"] * gallons_per_inch
    )
    out["event_storage_gal_scaled"] = (
        out["event_storage_in"] * gallons_per_inch
    )

    out["estimated_surplus_gal_strip"] = (
        gallons_strip - out["event_storage_gal_scaled"]
    )
    out["estimated_surplus_fraction"] = (
        out["estimated_surplus_gal_strip"] / gallons_strip
    )

    out.loc[gallons_strip <= 0, "estimated_surplus_fraction"] = pd.NA

    numeric_cols = out.select_dtypes(include=["number"]).columns
    out[numeric_cols] = out[numeric_cols].round(4)

    return out

def write_year_outputs(
    year: int,
    df_15min: pd.DataFrame,
    results: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    results = add_scaled_storage_fields(results)

    debug_table = build_enhanced_event_debug_table(results)
    targets, _ = summarize_targets_and_runtimes(results)
    runtimes = build_enhanced_runtime_table(results)
    definitions = build_variable_definitions_table()

    if not targets.empty and "depth_index" in results.columns:
        sensor_depth_lookup = (
            results[["sensor_col", "depth_index", "depth_inches"]]
            .drop_duplicates()
            .copy()
        )
        targets = targets.merge(sensor_depth_lookup, on="sensor_col", how="left")

    for table in [debug_table, targets, runtimes]:
        if not table.empty:
            table["year"] = year

    debug_table = force_float(move_id_columns_left(debug_table))
    targets = force_float(move_id_columns_left(targets))
    runtimes = force_float(move_id_columns_left(runtimes))

    debug_table.to_csv(
        OUTPUT_DIR / f"debug_irrigation_events_{year}_all_depths.csv",
        index=False,
        float_format="%.4f",
    )
    targets.to_csv(
        OUTPUT_DIR / f"irrigation_targets_{year}_all_depths.csv",
        index=False,
        float_format="%.4f",
    )
    runtimes.to_csv(
        OUTPUT_DIR / f"irrigation_runtimes_{year}_all_depths.csv",
        index=False,
        float_format="%.4f",
    )
    definitions.to_csv(
        OUTPUT_DIR / f"irrigation_variable_definitions_{year}.csv",
        index=False,
    )

    build_variable_definitions_with_sources(
        output_dir=str(OUTPUT_DIR),
        year=year,
    )

    plot_log_path = OUTPUT_DIR / f"irrigation_event_multidepth_plot_log_{year}.csv"

    multidepth_plot_log = save_irrigation_event_multidepth_plots(
        df=df_15min,
        event_results=results,
        output_dir=MULTIDEPTH_PLOT_DIR,
        strip_filter=["S1", "S2", "S3", "S4"],
        event_ids=None,
        logger_position="B",
        depths=(1, 2, 3),
        hours_before=4.0,
        hours_after=36.0,
        max_plots=None,
        precip_col="precip_in",
        use_common_y_axis=True,
    )

    multidepth_plot_log.to_csv(plot_log_path, index=False)

    pre_start_table = detect_pre_start_response(
        df_15min=df_15min,
        event_results=results,
        lookback_hours=6.0,
        min_increase=0.5,
        precip_col="precip_in",
        min_precip_in=0.01,
    )

    pre_start_table.to_csv(
        OUTPUT_DIR / f"irrigation_pre_start_response_flags_{year}.csv",
        index=False,
    )

    trustworthy_table = classify_trustworthy_irrigation_events(pre_start_table)

    trustworthy_table.to_csv(
        OUTPUT_DIR / f"trustworthy_irrigation_events_{year}.csv",
        index=False,
    )

    holding_capacity_table = build_trustworthy_holding_capacity_summary(
        trustworthy_table=trustworthy_table,
        event_results=results,
    )

    holding_capacity_table.to_csv(
        OUTPUT_DIR / f"trustworthy_holding_capacity_summary_{year}.csv",
        index=False,
    )

    water_balance_table = build_first_pass_water_balance_table(
        trustworthy_table=trustworthy_table,
        event_results=results,
    )

    water_balance_table.to_csv(
        OUTPUT_DIR / f"first_pass_water_balance_{year}.csv",
        index=False,
    )

    print(
        f"Year {year}: wrote debug, targets, runtimes, definitions, "
        f"plot log, pre-start flag, trustworthy-event, holding-capacity, "
        f"and first-pass water-balance CSVs."
    )

    return pre_start_table, trustworthy_table, holding_capacity_table, water_balance_table


def main() -> None:
    strip_to_profile_sensors = build_bottom_logger_profile_map()
    all_pre_start_flags: list[pd.DataFrame] = []
    all_trustworthy_tables: list[pd.DataFrame] = []
    all_holding_capacity_tables: list[pd.DataFrame] = []
    all_water_balance_tables: list[pd.DataFrame] = []

    for year in YEARS:
        print(f"\n================ YEAR {year} ================")

        df_15min = prepare_15min_logger_data(year)

        results = analyze_bottom_logger_all_depths(
            df_15min=df_15min,
            strips=STRIPS,
            year=year,
            strip_to_profile_sensors=strip_to_profile_sensors,
        )

        print("\n=== STORAGE DEBUG SAMPLE ===")
        print(
            results[
                [
                    "sensor_col",
                    "baseline_vwc",
                    "plateau_vwc",
                    "depth_inches",
                    "gallons_strip",
                    "profile_area_sqft",
                    "profile_baseline_storage_in",
                    "profile_plateau_storage_in",
                    "event_storage_in",
                    "profile_baseline_storage_gal",
                    "profile_plateau_storage_gal",
                    "event_storage_gal",
                ]
            ].head(10).to_string(index=False)
        )


        if results.empty:
            print(f"Year {year}: no irrigation-analysis results returned.")
            continue

        print(f"Year {year}: {len(results):,} event/sensor/depth result rows.")

        if VERBOSE:
            print("\n=== RESULTS COLUMNS ===")
            print(results.columns.tolist())
            debug_table = build_enhanced_event_debug_table(results)
            print("\n=== DEBUG TABLE SAMPLE ===")
            print(debug_table.head(40).to_string(index=False))

        (
            pre_start_table,
            trustworthy_table,
            holding_capacity_table,
            water_balance_table,
        ) = write_year_outputs(
            year=year,
            df_15min=df_15min,
            results=results,
        )

        if not pre_start_table.empty:
            all_pre_start_flags.append(pre_start_table)

        if not trustworthy_table.empty:
            all_trustworthy_tables.append(trustworthy_table)

        if not holding_capacity_table.empty:
            all_holding_capacity_tables.append(holding_capacity_table)

        if not water_balance_table.empty:
            all_water_balance_tables.append(water_balance_table)

    combined = (
        pd.concat(all_pre_start_flags, ignore_index=True)
        if all_pre_start_flags
        else pd.DataFrame()
    )

    combined_path = OUTPUT_DIR / "irrigation_pre_start_response_flags_all_years.csv"
    combined.to_csv(combined_path, index=False)

    combined_trustworthy = (
        pd.concat(all_trustworthy_tables, ignore_index=True)
        if all_trustworthy_tables
        else pd.DataFrame()
    )

    trustworthy_path = OUTPUT_DIR / "trustworthy_irrigation_events_all_years.csv"
    combined_trustworthy.to_csv(trustworthy_path, index=False)

    combined_holding_capacity = (
        pd.concat(all_holding_capacity_tables, ignore_index=True)
        if all_holding_capacity_tables
        else pd.DataFrame()
    )

    holding_capacity_path = OUTPUT_DIR / "trustworthy_holding_capacity_summary_all_years.csv"
    combined_holding_capacity.to_csv(holding_capacity_path, index=False)

    combined_water_balance = (
        pd.concat(all_water_balance_tables, ignore_index=True)
        if all_water_balance_tables
        else pd.DataFrame()
    )

    water_balance_path = OUTPUT_DIR / "first_pass_water_balance_all_years.csv"
    combined_water_balance.to_csv(water_balance_path, index=False)

    print("\n=== COMBINED PRE-START RESPONSE FILE ===")
    print(combined_path)

    print("\n=== COMBINED TRUSTWORTHY EVENT FILE ===")
    print(trustworthy_path)

    print("\n=== COMBINED HOLDING CAPACITY SUMMARY FILE ===")
    print(holding_capacity_path)

    print("\n=== COMBINED FIRST-PASS WATER BALANCE FILE ===")
    print(water_balance_path)

    if combined.empty:
        print("No pre-start response rows were produced.")
        return

    flagged = combined[combined["flag_pre_start_response"] == True].copy()
    unexplained = combined[
        combined["flag_unexplained_pre_start_response"] == True
    ].copy()
    precip_driven = combined[
        combined["likely_precip_driven_pre_start_response"] == True
    ].copy()
    battery_or_logger = combined[
        combined["possible_battery_or_logger_issue"] == True
    ].copy()

    trustworthy_count = 0
    untrustworthy_count = 0

    if not combined_trustworthy.empty and "trustworthy_event" in combined_trustworthy.columns:
        trustworthy_count = int((combined_trustworthy["trustworthy_event"] == True).sum())
        untrustworthy_count = int((combined_trustworthy["trustworthy_event"] == False).sum())

    print("\n=== PRE-START RESPONSE SUMMARY ===")
    print(f"Total pre-start rows: {len(combined):,}")
    print(f"Flagged pre-start responses: {len(flagged):,}")
    print(f"Likely precip-driven flagged responses: {len(precip_driven):,}")
    print(f"Possible battery/logger issue rows: {len(battery_or_logger):,}")
    print(f"Unexplained flagged responses: {len(unexplained):,}")

    print("\n=== TRUSTWORTHY EVENT SUMMARY ===")
    print(f"Trustworthy sensor/event/depth rows: {trustworthy_count:,}")
    print(f"Not trustworthy sensor/event/depth rows: {untrustworthy_count:,}")

    if not combined_holding_capacity.empty:
        print("\n=== HOLDING CAPACITY SUMMARY ===")
        print(f"Holding-capacity summary rows: {len(combined_holding_capacity):,}")

    if not combined_water_balance.empty:
        print("\n=== FIRST-PASS WATER BALANCE SUMMARY ===")
        print(f"Water-balance event rows: {len(combined_water_balance):,}")

        if "estimated_surplus_gal_strip" in combined_water_balance.columns:
            total_surplus = pd.to_numeric(
                combined_water_balance["estimated_surplus_gal_strip"],
                errors="coerce",
            ).sum()
            print(f"Total estimated surplus across included events: {total_surplus:,.0f} gal")

    if not unexplained.empty:
        print("\n=== UNEXPLAINED PRE-START RESPONSES ===")
        summary_cols = [
            "year",
            "strip_group",
            "strip",
            "event_id",
            "sensor_col",
            "irrigation_start",
            "pre_start_increase",
            "total_precip_in_pre_start_window",
            "battery_col",
            "battery_pre_start_min_v",
            "battery_event_min_v",
            "vwc_missing_fraction_pre_start_window",
            "gallons_strip",
        ]
        summary_cols = [c for c in summary_cols if c in unexplained.columns]
        print(unexplained[summary_cols].to_string(index=False))


if __name__ == "__main__":
    main()