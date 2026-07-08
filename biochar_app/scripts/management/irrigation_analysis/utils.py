import pandas as pd


from biochar_app.config.experiment_config import (
    STRIPS,
    SENSOR_DEPTH_CODES,
)

from biochar_app.scripts.management.estimate_irrigation_holding_capacity import (DEPTH_INDEX_TO_INCHES)

from biochar_app.scripts.data_loading import (load_logger_data, prepare_irrigation_input)

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


def round_for_reporting(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    round_0_cols = [
        "gallons_strip",
        "gallons_group",
        "baseline_storage_gal",
        "plateau_storage_gal",
        "event_storage_gal",
        "profile_baseline_storage_gal",
        "profile_plateau_storage_gal",
        "mean_gallons_strip",
        "sd_gallons_strip",
        "estimated_surplus_gal_strip",
    ]

    round_1_cols = [
        "zone_length_ft",
        "zone_area_sqft",
        "profile_area_sqft",
    ]

    round_2_cols = [
        "baseline_vwc",
        "plateau_vwc",
        "peak_vwc",
        "peak_increase",
        "depth_inches",
        "event_storage_in",
        "profile_baseline_storage_in",
        "profile_plateau_storage_in",
        "zone_gallons_per_inch",
        "estimated_surplus_fraction",
        "mean_storage_in",
        "median_storage_in",
        "max_storage_in",
        "p95_storage_in",
        "sd_storage_in",
    ]

    round_3_cols = [
        "cv_plateau_vwc",
        "efficiency_strip",
        "mean_efficiency_strip",
        "sd_efficiency_strip",
        "flow_storage_corr",
    ]

    for col in round_0_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(0)

    for col in round_1_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(1)

    for col in round_2_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

    for col in round_3_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(3)

    return out


def build_bottom_logger_profile_map() -> dict[str, list[str]]:
    return {
        strip: [f"VWC_{depth_code}_raw_{strip}_B" for depth_code in SENSOR_DEPTH_CODES]
        for strip in STRIPS
    }


def prepare_15min_logger_data(year: int, VERBOSE=None) -> pd.DataFrame:
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
