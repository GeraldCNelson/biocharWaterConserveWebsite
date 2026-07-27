"""
Holding-capacity, storage, and water-balance helpers for irrigation analysis.
"""
from __future__ import annotations

from typing import Any, cast

import numpy as np
import pandas as pd

from biochar_app.config.core import SENSOR_DEPTH_CODES
from biochar_app.config.field_management_metadata import (
    INCHES_WATER_TO_GALLONS_PER_SQFT,
    PROFILE_AREA_SQFT,
    PROFILE_GALLONS_PER_INCH,
    ZONE_AREAS_SQFT_BY_STRIP,
    ZONE_GALLONS_PER_INCH_BY_STRIP,
    ZONE_LENGTHS_FT_BY_STRIP,
)

from biochar_app.scripts.management.irrigation_analysis.utils import (
    force_float,
    move_id_columns_left,
    round_for_reporting,
)

def build_event_storage_by_event(zone_df: pd.DataFrame) -> pd.DataFrame:
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()
    df["event_storage_gal"] = pd.to_numeric(df["event_storage_gal"], errors="coerce")
    df["gallons_strip"] = pd.to_numeric(df["gallons_strip"], errors="coerce")

    index_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "irrigation_start",
        "irrigation_end",
        "gallons_strip",
        "event_duration_hours",
        "avg_flow_gph_strip",
    ]

    index_cols = [c for c in index_cols if c in df.columns]

    event_summary = (
        df.pivot_table(
            index=index_cols,
            columns="logger_position",
            values="event_storage_gal",
            aggfunc="sum",
        )
        .reset_index()
        .rename(
            columns={
                "T": "top_storage_gal",
                "M": "middle_storage_gal",
                "B": "bottom_storage_gal",
            }
        )
    )

    for col in ["top_storage_gal", "middle_storage_gal", "bottom_storage_gal"]:
        if col not in event_summary.columns:
            event_summary[col] = pd.NA

    storage_cols = ["top_storage_gal", "middle_storage_gal", "bottom_storage_gal"]

    event_summary["total_storage_gal"] = event_summary[storage_cols].sum(
        axis=1,
        min_count=1,
    )

    event_summary["capture_fraction"] = (
        event_summary["total_storage_gal"] / event_summary["gallons_strip"]
    )

    event_summary["estimated_tailwater_gal"] = (
        event_summary["gallons_strip"] - event_summary["total_storage_gal"]
    )

    return round_for_reporting(event_summary)

def build_zone_storage_summary(zone_df: pd.DataFrame) -> pd.DataFrame:
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()
    df["event_storage_gal"] = pd.to_numeric(df["event_storage_gal"], errors="coerce")
    df["event_storage_in"] = pd.to_numeric(df["event_storage_in"], errors="coerce")

    summary = (
        df.groupby(["year", "strip", "logger_position"], dropna=False)
        .agg(
            n_events=("event_id", "nunique"),
            mean_storage_gal=("event_storage_gal", "mean"),
            median_storage_gal=("event_storage_gal", "median"),
            max_storage_gal=("event_storage_gal", "max"),
            p95_storage_gal=("event_storage_gal", lambda s: float(np.nanpercentile(s, 95))),
            sd_storage_gal=("event_storage_gal", "std"),
            mean_storage_in=("event_storage_in", "mean"),
            median_storage_in=("event_storage_in", "median"),
            max_storage_in=("event_storage_in", "max"),
            p95_storage_in=("event_storage_in", lambda s: float(np.nanpercentile(s, 95))),
            sd_storage_in=("event_storage_in", "std"),
        )
        .reset_index()
    )
    
    

    return round_for_reporting(summary)

def build_flow_storage_correlation_summary(zone_df: pd.DataFrame) -> pd.DataFrame:
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()
    df["event_storage_in"] = pd.to_numeric(df["event_storage_in"], errors="coerce")
    df["avg_flow_gph_strip"] = pd.to_numeric(df["avg_flow_gph_strip"], errors="coerce")

    rows = []

    for (year, strip, position), sub in df.groupby(
        ["year", "strip", "logger_position"],
        dropna=False,
    ):
        valid = sub[["event_storage_in", "avg_flow_gph_strip"]].dropna()

        corr: float | pd._libs.missing.NAType | None = None
        if len(valid) >= 3:
            corr = valid["event_storage_in"].corr(valid["avg_flow_gph_strip"])

        rows.append(
            {
                "year": year,
                "strip": strip,
                "logger_position": position,
                "n_events": len(valid),
                "flow_storage_corr": corr,
            }
        )

    return round_for_reporting(pd.DataFrame(rows))

def build_zone_ordering_frequency(zone_df: pd.DataFrame) -> pd.DataFrame:
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()
    df["event_storage_gal"] = pd.to_numeric(df["event_storage_gal"], errors="coerce")

    pivot = (
        df.pivot_table(
            index=["year", "strip", "event_id"],
            columns="logger_position",
            values="event_storage_gal",
            aggfunc="sum",
        )
        .reset_index()
    )

    for col in ["T", "M", "B"]:
        if col not in pivot.columns:
            pivot[col] = pd.NA

    pivot = pivot.dropna(subset=["T", "M", "B"]).copy()

    def ordering(row: pd.Series) -> str:
        values = {
            "T": row["T"],
            "M": row["M"],
            "B": row["B"],
        }
        return ">".join(
            sorted(values, key=lambda k: values[k], reverse=True)
        )

    pivot["zone_ordering"] = pivot.apply(ordering, axis=1)

    freq = (
        pivot.groupby(["zone_ordering"], dropna=False)
        .size()
        .reset_index(name="n_events")
        .sort_values("n_events", ascending=False)
    )

    freq["pct_events"] = 100.0 * freq["n_events"] / freq["n_events"].sum()

    return round_for_reporting(freq)

def build_zone_anomaly_diagnostics(zone_df: pd.DataFrame) -> pd.DataFrame:
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()
    df["event_storage_gal"] = pd.to_numeric(df["event_storage_gal"], errors="coerce")

    index_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "irrigation_start",
        "gallons_strip",
        "event_duration_hours",
        "avg_flow_gph_strip",
    ]

    index_cols = [c for c in index_cols if c in df.columns]

    pivot = (
        df.pivot_table(
            index=index_cols,
            columns="logger_position",
            values="event_storage_gal",
            aggfunc="sum",
        )
        .reset_index()
    )

    for col in ["T", "M", "B"]:
        if col not in pivot.columns:
            pivot[col] = pd.NA

    pivot = pivot.rename(
        columns={
            "T": "top_storage_gal",
            "M": "middle_storage_gal",
            "B": "bottom_storage_gal",
        }
    )

    pivot["s2_bottom_largest"] = (
        (pivot["strip"] == "S2")
        & (pivot["bottom_storage_gal"] > pivot["top_storage_gal"])
        & (pivot["bottom_storage_gal"] > pivot["middle_storage_gal"])
    )

    pivot["s3_middle_low"] = (
        (pivot["strip"] == "S3")
        & (pivot["middle_storage_gal"] < pivot["top_storage_gal"])
        & (pivot["middle_storage_gal"] < pivot["bottom_storage_gal"])
    )

    out = pivot[
        pivot["s2_bottom_largest"] | pivot["s3_middle_low"]
    ].copy()

    return round_for_reporting(out)

def build_event_storage_by_zone(event_results: pd.DataFrame) -> pd.DataFrame:
    if event_results.empty:
        return pd.DataFrame()

    df = event_results.copy()

    df = df[df["logger_position"].isin(["T", "M", "B"])].copy()

    numeric_cols = [
        "profile_baseline_storage_gal",
        "profile_plateau_storage_gal",
        "event_storage_gal",
        "event_storage_in",
        "gallons_strip",
        "event_duration_hours",
        "avg_flow_gph_strip",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    zone_rows = []

    group_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "irrigation_start",
        "irrigation_end",
        "logger_position",
        "gallons_strip",
        "event_duration_hours",
        "avg_flow_gph_strip",
    ]

    for keys, sub in df.groupby(group_cols, dropna=False):
        row = dict(zip(group_cols, keys))
        strip = str(row["strip"])
        zone = str(row["logger_position"])

        if strip not in ZONE_LENGTHS_FT_BY_STRIP:
            continue
        if zone not in ZONE_LENGTHS_FT_BY_STRIP[strip]:
            continue

        baseline_gal = sub["profile_baseline_storage_gal"].sum(min_count=1)
        plateau_gal = sub["profile_plateau_storage_gal"].sum(min_count=1)
        storage_gal = sub["event_storage_gal"].sum(min_count=1)

        zone_gal_per_in = ZONE_GALLONS_PER_INCH_BY_STRIP[strip][zone]
        storage_in = storage_gal / zone_gal_per_in if pd.notna(storage_gal) else pd.NA

        zone_rows.append(
            {
                **row,
                "zone": zone,
                "zone_label": {"T": "top", "M": "middle", "B": "bottom"}[zone],
                "zone_length_ft": ZONE_LENGTHS_FT_BY_STRIP[strip][zone],
                "zone_area_sqft": ZONE_AREAS_SQFT_BY_STRIP[strip][zone],
                "zone_gallons_per_inch": zone_gal_per_in,
                "baseline_storage_gal": baseline_gal,
                "plateau_storage_gal": plateau_gal,
                "event_storage_gal": storage_gal,
                "event_storage_in": storage_in,
                "n_depths": sub["sensor_col"].nunique(),
            }
        )

    out = pd.DataFrame(zone_rows)

    if out.empty:
        return out

    round_2 = [
        "event_duration_hours",
        "event_storage_in",
        "zone_length_ft",
        "zone_area_sqft",
        "zone_gallons_per_inch",
    ]

    round_0 = [
        "gallons_strip",
        "avg_flow_gph_strip",
        "baseline_storage_gal",
        "plateau_storage_gal",
        "event_storage_gal",
    ]

    for col in round_2:
        if col in out.columns:
            out[col] = out[col].round(2)

    for col in round_0:
        if col in out.columns:
            out[col] = out[col].round(0)

    return out

def add_response_delta_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    delta_specs = [
        ("baseline_vwc", "peak_vwc", "delta_vwc"), #% so no units
        ("baseline_swc_gal", "peak_swc_gal", "delta_swc_gal"),
        ("baseline_swc_L", "peak_swc_L", "delta_swc_L"),
    ]

    for baseline_col, peak_col, delta_col in delta_specs:
        if baseline_col in out.columns and peak_col in out.columns:
            out[delta_col] = (
                pd.to_numeric(out[peak_col], errors="coerce")
                - pd.to_numeric(out[baseline_col], errors="coerce")
            )

    return out

def build_first_pass_water_balance_table(
    trustworthy_table: pd.DataFrame,
    event_results: pd.DataFrame,
) -> pd.DataFrame:
    if trustworthy_table.empty or event_results.empty:
        return pd.DataFrame()

    trusted = trustworthy_table[trustworthy_table["trustworthy_event"].fillna(False)].copy()
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

def summarize_holding_capacity_from_trustworthy_events(
    trustworthy_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    Estimate logger-location holding capacity from trustworthy irrigation events.

    Uses trustworthy sensor/event/depth rows only. Plateau VWC statistics are
    used as the main field-capacity / holding-capacity estimate.
    """
    if trustworthy_table.empty:
        return pd.DataFrame()

    df = trustworthy_table.copy()
    df = df[df["trustworthy_event"].fillna(False)].copy()

    if df.empty:
        return pd.DataFrame()

    numeric_cols = [
        "bottom_response_delay_hr",
        "time_to_peak_hours",
        "time_to_plateau_hours",
        "event_duration_hours",
        "gallons_strip",
        "plateau_vwc",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    group_cols = [
        "strip_group",
        "location",
        "strip",
        "sensor_col",
        "depth_index",
        "depth_inches",
    ]
    group_cols = [c for c in group_cols if c in df.columns]

    agg_dict = {
        "n_trustworthy_events": ("trustworthy_event", "size"),
        "mean_bottom_response_delay_hr": ("bottom_response_delay_hr", "mean"),
        "sd_bottom_response_delay_hr": ("bottom_response_delay_hr", "std"),
        "mean_time_to_plateau_hours": ("time_to_plateau_hours", "mean"),
        "sd_time_to_plateau_hours": ("time_to_plateau_hours", "std"),
        "mean_event_duration_hours": ("event_duration_hours", "mean"),
        "sd_event_duration_hours": ("event_duration_hours", "std"),
        "mean_gallons_strip": ("gallons_strip", "mean"),
        "sd_gallons_strip": ("gallons_strip", "std"),
    }

    if "plateau_vwc" in df.columns:
        agg_dict.update(
            {
                "mean_plateau_vwc": ("plateau_vwc", "mean"),
                "sd_plateau_vwc": ("plateau_vwc", "std"),
                "min_plateau_vwc": ("plateau_vwc", "min"),
                "max_plateau_vwc": ("plateau_vwc", "max"),
            }
        )

    summary = (
        df.groupby(group_cols, dropna=False)
        .agg(**cast(Any, agg_dict))
        .reset_index()
    )

    if {"mean_plateau_vwc", "sd_plateau_vwc"}.issubset(summary.columns):
        summary["cv_plateau_vwc"] = (
            summary["sd_plateau_vwc"] / summary["mean_plateau_vwc"]
        )

        def capacity_confidence(row: pd.Series) -> str:
            n = row.get("n_trustworthy_events")
            sd = row.get("sd_plateau_vwc")

            if pd.isna(n) or pd.isna(sd):
                return "low"
            if n >= 3 and sd <= 3:
                return "high"
            if n >= 2 and sd <= 5:
                return "medium"
            return "low"

        summary["capacity_confidence"] = summary.apply(
            capacity_confidence,
            axis=1,
        )

    numeric_summary_cols = summary.select_dtypes(include=["number"]).columns
    summary[numeric_summary_cols] = summary[numeric_summary_cols].round(2)
    print(summary.columns.tolist())
    return summary

def build_trustworthy_holding_capacity_summary(
    trustworthy_table: pd.DataFrame,
    event_results: pd.DataFrame,
) -> pd.DataFrame:
    if trustworthy_table.empty or event_results.empty:
        return pd.DataFrame()

    trusted = trustworthy_table[trustworthy_table["trustworthy_event"].fillna(False)].copy()
    if trusted.empty:
        return pd.DataFrame()

    merge_cols = ["year", "strip", "event_id", "sensor_col"]

    trusted_event_results = event_results.merge(
        trusted[merge_cols].drop_duplicates(),
        on=merge_cols,
        how="inner",
    )

    print(len(event_results))
    print(len(trusted_event_results))

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
        trusted_event_results.groupby(group_cols, dropna=False)
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
            min_plateau_vwc=("plateau_vwc", "min"),
            max_plateau_vwc=("plateau_vwc", "max"),

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
