"""
biochar_app.scripts.management.estimate_irrigation_holding_capacity.py

Purpose
-------
Evaluate irrigation-event soil-water response using 15-minute logger VWC data
and strip-level irrigation management records.

The script currently supports two related workflows:

1. Bottom-logger workflow
   Uses bottom logger positions only. This is the older/stable workflow used to
   estimate irrigation response timing, plateau VWC, trustworthy events, holding
   capacity, and first-pass water balance summaries.

2. Three-zone workflow
   Uses all three logger positions in each strip:
   - T = top influence zone
   - M = middle influence zone
   - B = bottom influence zone

   The three-zone workflow estimates event water storage by logger influence
   zone using zone-specific field areas and gallons-per-inch conversion factors.
   It is intended to help identify spatial differences in irrigation response
   along each strip and estimate where applied irrigation water is being stored
   or potentially leaving the strip as tailwater/runoff.

Important caution
-----------------
Water-balance and holding-capacity results depend on correctly aligned
timestamps. Known or suspected timestamp problems, especially for S3M in 2025
and possibly 2026, must be diagnosed before using these outputs for scientific
interpretation.

Main outputs
------------
- event-level debug tables
- irrigation target and runtime tables
- pre-start response diagnostics
- trustworthy-event classifications
- holding-capacity summaries
- first-pass water-balance tables
- zone-level event storage summaries
- zone anomaly diagnostics
- diagnostic plots

# Increase in VWC (%) used by the primary arrival detector.
# This detector begins searching at the recorded irrigation start time.
ARRIVAL_RESPONSE_THRESHOLD_VWC = 0.25

# Increase in VWC (%) used by the alternate arrival detector.
# This detector scans the entire trace and is intended to identify
# possible wetting that occurred before the recorded irrigation start.
# A larger threshold reduces false detections from noise or gradual drift.
ALTERNATE_ARRIVAL_RESPONSE_THRESHOLD_VWC = 0.50

# TODO: Continue refactoring the irrigation analysis workflow.
#
# Remaining work:
# - separate intermediate and final analysis products
# - reduce responsibilities of this script by moving reusable routines
#   into smaller modules
# - distinguish diagnostic outputs from publication-quality outputs
# - integrate water-deficit estimation with the holding-capacity workflow
#   once the methodology is finalized
"""

from pathlib import Path
import pandas as pd
from typing import Any
import matplotlib.pyplot as plt
import numpy as np


from matplotlib.figure import Figure
from typing import cast

from biochar_app.config.experiment_config import LOGGER_GEOMETRY
from biochar_app.config.field_management_metadata import (
    PROFILE_GALLONS_PER_INCH,
    #STRIP_WIDTH_FT,
    #STRIP_LENGTH_FT,
    #LOGGER_POSITIONS_PER_STRIP,
    INCHES_WATER_TO_GALLONS_PER_SQFT,
    PROFILE_AREA_SQFT,
    ZONE_LENGTHS_FT_BY_STRIP,
    ZONE_AREAS_SQFT_BY_STRIP,
    ZONE_GALLONS_PER_INCH_BY_STRIP,
)

from biochar_app.config.experiment_config import (LOGGER_LOCATIONS)
# Ballpark logger service area:
# strip width 46 ft × strip length 280 ft ÷ 3 logger positions.

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
from biochar_app.scripts.management.irrigation_analysis.irrigation_response_analysis import (
     analyze_irrigation_events,
     build_variable_definitions_table,
     build_variable_definitions_with_sources,
     summarize_targets_and_runtimes,
)

from biochar_app.scripts.management.irrigation_analysis.plotting import (
    save_irrigation_event_multidepth_plots,
)

from biochar_app.config.irrigation_config import (
    EVENT_PLOT_HOURS_BEFORE,
    EVENT_PLOT_HOURS_AFTER,
    MULTIDEPTH_PLOT_DIR,
    TIMESTAMP_DIAGNOSTICS_DIR,
    ARRIVAL_RESPONSE_THRESHOLD_VWC,
    ALTERNATE_ARRIVAL_RESPONSE_THRESHOLD_VWC,
    MIN_PRECIP_IN,
)

from biochar_app.config.paths import (
    HOLDING_CAPACITY_DIR,
    IRRIGATION_DIAGNOSTICS_DIR,
    IRRIGATION_FIGURES_DIR,
)

profile_area_sqft = PROFILE_AREA_SQFT

for d in [
    HOLDING_CAPACITY_DIR,
    IRRIGATION_DIAGNOSTICS_DIR,
    IRRIGATION_FIGURES_DIR,
    MULTIDEPTH_PLOT_DIR,
    TIMESTAMP_DIAGNOSTICS_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)

VERBOSE = False

DEPTH_INDEX_TO_INCHES: dict[str, int] = {
    depth_code: int(SENSOR_DEPTH_VALUES[depth_code]["us"])
    for depth_code in SENSOR_DEPTH_CODES
}


BATTERY_MIN_OK = 11.0
BATTERY_MAX_OK = 15.0
MIN_BOTTOM_RESPONSE_DELAY_HR = 0.5

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


def plot_mean_storage_depth_by_zone_by_year(zone_df: pd.DataFrame, HOLDING_CAPACITY_DIR: Path) -> None:
    if zone_df.empty:
        return

    figure_dir = HOLDING_CAPACITY_DIR / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)

    df = zone_df.copy()
    df["event_storage_in"] = pd.to_numeric(df["event_storage_in"], errors="coerce")

    for year, sub in df.groupby("year"):
        summary = (
            sub.groupby(["logger_position", "strip"])["event_storage_in"]
            .mean()
            .unstack("strip")
            .reindex(LOGGER_LOCATIONS)
        )

        ax = summary.plot(kind="bar", figsize=(8, 5))
        ax.set_xlabel("Logger influence zone")
        ax.set_ylabel("Mean water stored (in)")
        ax.set_title(f"Mean water stored by logger influence zone and strip, {year}")
        ax.set_xticklabels(["Top", "Middle", "Bottom"], rotation=0)
        ax.legend(title="Strip")

        ax.text(
            0.01,
            -0.22,
            "Water stored is expressed as equivalent water depth over each logger influence zone, "
            "derived from 6, 12, and 18 in VWC sensors.",
            transform=ax.transAxes,
            fontsize=8,
            va="top",
        )


        out_path = figure_dir / f"mean_water_stored_in_by_zone_{year}.png"
        fig = cast(Figure, ax.figure)
        fig.tight_layout()
        fig.savefig(out_path, dpi=300, bbox_inches="tight")
        plt.close(fig)


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


def plot_mean_storage_by_zone(zone_df: pd.DataFrame, HOLDING_CAPACITY_DIR: Path) -> None:
    if zone_df.empty:
        return

    figure_dir = HOLDING_CAPACITY_DIR / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)

    df = zone_df.copy()
    df["event_storage_gal"] = pd.to_numeric(df["event_storage_gal"], errors="coerce")

    summary = (
        df.groupby(["logger_position", "strip"])["event_storage_gal"]
        .mean()
        .unstack("strip")
        .reindex(["T", "M", "B"])
    )

    ax = summary.plot(kind="bar", figsize=(8, 5))
    ax.set_xlabel("Logger position")
    ax.set_ylabel("Mean event storage (gal)")
    ax.set_title("Mean event storage by logger position and strip")
    ax.set_xticklabels(["Top", "Middle", "Bottom"], rotation=0)
    ax.legend(title="Strip")
    

    out_path = figure_dir / "mean_event_storage_by_zone_all_years.png"
    fig = cast(Figure, ax.figure)
    fig.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_mean_storage_by_zone_by_year(zone_df: pd.DataFrame, HOLDING_CAPACITY_DIR: Path) -> None:
    if zone_df.empty:
        return

    figure_dir = HOLDING_CAPACITY_DIR / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)

    df = zone_df.copy()
    df["event_storage_gal"] = pd.to_numeric(df["event_storage_gal"], errors="coerce")

    for year, sub in df.groupby("year"):
        summary = (
            sub.groupby(["logger_position", "strip"])["event_storage_gal"]
            .mean()
            .unstack("strip")
            .reindex(LOGGER_LOCATIONS)
        )

        ax = summary.plot(kind="bar", figsize=(8, 5))
        ax.set_xlabel("Logger position")
        ax.set_ylabel("Mean event storage (gal)")
        ax.set_title(f"Mean event storage by logger position and strip, {year}")
        ax.set_xticklabels(["Top", "Middle", "Bottom"], rotation=0)
        ax.legend(title="Strip")

        out_path = figure_dir / f"mean_event_storage_by_zone_{year}.png"
        fig = cast(Figure, ax.figure)
        fig.tight_layout()
        fig.savefig(out_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
        
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
def build_irrigation_event_response_summary(
    arrival_times: pd.DataFrame,
    event_results: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build one summary row per logger position for each irrigation event.

    Uses one row per event × strip × logger_position and pivots depth-level
    values into 6in / 12in / 18in columns.

    Adds:
      - VWC response by depth
      - SWC response by depth
      - upper-zone vs deep-zone storage partitioning
      - storage efficiency
      - vertical arrival timing spread
    """
    if arrival_times.empty or event_results.empty:
        return pd.DataFrame()

    desired_cols = [
        "year",
        "event_id",
        "strip",
        "logger_position",
        "sensor_col",
        "depth_index",
        "depth_inches",
        "irrigation_start",
        "irrigation_end",
        "event_duration_hours",
        "gallons_strip",
        "avg_flow_gph_strip",

        "baseline_vwc",
        "pre_vwc",
        "target_vwc",
        "peak_vwc",
        "delta_vwc",

        # SWC, canonical analysis names
        "baseline_swc_gal",
        "peak_swc_gal",
        "delta_swc_gal",
        "baseline_swc_L",
        "peak_swc_L",
        "delta_swc_L",

        "pre_storage_gal",
        "target_storage_gal",
        "delta_storage_gal",
    ]

    available_cols = [c for c in desired_cols if c in event_results.columns]
    summary = event_results[available_cols].copy()

    # Normalize baseline/pre-irrigation VWC naming.
    if "pre_vwc" not in summary.columns and "baseline_vwc" in summary.columns:
        summary["pre_vwc"] = summary["baseline_vwc"]

    arrival_desired_cols = [
        "event_id",
        "strip",
        "sensor_col",
        "logger_position",
        "arrival_time",
        "arrival_minutes_after_irrigation_start",
        "arrival_vwc",
        "arrival_threshold_vwc",
        "alt_arrival_time",
        "alt_arrival_minutes_after_irrigation_start",
        "alt_arrival_vwc",
        "alt_arrival_threshold_vwc",
    ]

    arrival_available_cols = [
        c for c in arrival_desired_cols if c in arrival_times.columns
    ]

    summary = summary.merge(
        arrival_times[arrival_available_cols],
        on=["event_id", "strip", "sensor_col", "logger_position"],
        how="left",
    )

    id_cols = [
        c
        for c in [
            "year",
            "event_id",
            "strip",
            "logger_position",
            "irrigation_start",
            "irrigation_end",
            "event_duration_hours",
            "gallons_strip",
            "avg_flow_gph_strip",
        ]
        if c in summary.columns
    ]

    value_cols = [
        c
        for c in [
            "pre_vwc",
            "target_vwc",
            "peak_vwc",
            "delta_vwc",

            "baseline_swc_gal",
            "peak_swc_gal",
            "delta_swc_gal",
            "baseline_swc_L",
            "peak_swc_L",
            "delta_swc_L",

            "pre_storage_gal",
            "target_storage_gal",
            "delta_storage_gal",

            "arrival_minutes_after_irrigation_start",
            "arrival_vwc",
            "alt_arrival_minutes_after_irrigation_start",
            "alt_arrival_vwc",
        ]
        if c in summary.columns
    ]

    if not id_cols or "depth_inches" not in summary.columns or not value_cols:
        return force_float(move_id_columns_left(summary))

    wide = summary.pivot_table(
        index=id_cols,
        columns="depth_inches",
        values=value_cols,
        aggfunc="first",
    ).sort_index(axis=1)

    wide.columns = [
        f"{name}_{int(depth)}in"
        for name, depth in wide.columns.to_flat_index()
    ]

    wide = wide.reset_index()

    # ------------------------------------------------------------------
    # Antecedent VWC profile summary
    # ------------------------------------------------------------------
    pre_cols = [c for c in wide.columns if c.startswith("pre_vwc_")]
    if pre_cols:
        wide["pre_profile_vwc_mean"] = wide[pre_cols].mean(axis=1)
        wide["pre_profile_vwc_min"] = wide[pre_cols].min(axis=1)
        wide["pre_profile_vwc_max"] = wide[pre_cols].max(axis=1)

    # ------------------------------------------------------------------
    # VWC response by depth: peak - pre
    # ------------------------------------------------------------------
    for depth_code in SENSOR_DEPTH_CODES:
        depth_inches = DEPTH_INDEX_TO_INCHES[depth_code]

        pre_col = f"pre_vwc_{depth_inches}in"
        peak_col = f"peak_vwc_{depth_inches}in"
        delta_col = f"delta_vwc_{depth_inches}in"

        if pre_col in wide.columns and peak_col in wide.columns:
            wide[delta_col] = (
                pd.to_numeric(wide[peak_col], errors="coerce")
                - pd.to_numeric(wide[pre_col], errors="coerce")
            )

    delta_vwc_cols = [c for c in wide.columns if c.startswith("delta_vwc_")]
    if delta_vwc_cols:
        wide["delta_profile_vwc_mean"] = wide[delta_vwc_cols].mean(axis=1)

    # ------------------------------------------------------------------
    # SWC response by depth: peak - baseline
    # ------------------------------------------------------------------
    for depth_code in SENSOR_DEPTH_CODES:
        depth_inches = DEPTH_INDEX_TO_INCHES[depth_code]

        baseline_col = f"baseline_swc_gal_{depth_inches}in"
        peak_col = f"peak_swc_gal_{depth_inches}in"
        delta_col = f"delta_swc_gal_{depth_inches}in"

        if baseline_col in wide.columns and peak_col in wide.columns:
            wide[delta_col] = (
                pd.to_numeric(wide[peak_col], errors="coerce")
                - pd.to_numeric(wide[baseline_col], errors="coerce")
            )

        baseline_col_L = f"baseline_swc_L_{depth_inches}in"
        peak_col_L = f"peak_swc_L_{depth_inches}in"
        delta_col_L = f"delta_swc_L_{depth_inches}in"

        if baseline_col_L in wide.columns and peak_col_L in wide.columns:
            wide[delta_col_L] = (
                pd.to_numeric(wide[peak_col_L], errors="coerce")
                - pd.to_numeric(wide[baseline_col_L], errors="coerce")
            )

    # ------------------------------------------------------------------
    # Profile SWC summaries
    # ------------------------------------------------------------------
    swc_pre_cols = [
        c for c in wide.columns if c.startswith("baseline_swc_gal_")
    ]
    if swc_pre_cols:
        wide["pre_profile_swc_gal"] = wide[swc_pre_cols].sum(
            axis=1,
            min_count=1,
        )

    swc_peak_cols = [
        c for c in wide.columns if c.startswith("peak_swc_gal_")
    ]
    if swc_peak_cols:
        wide["peak_profile_swc_gal"] = wide[swc_peak_cols].sum(
            axis=1,
            min_count=1,
        )

    swc_delta_cols = [
        c for c in wide.columns if c.startswith("delta_swc_gal_")
    ]
    if swc_delta_cols:
        wide["total_delta_swc_gal"] = wide[swc_delta_cols].sum(
            axis=1,
            min_count=1,
        )

    # ------------------------------------------------------------------
    # Upper-zone vs deep-zone storage partitioning
    # Treat 6 + 12 in as upper/root-zone response and 18 in as deep response.
    # ------------------------------------------------------------------
    upper_cols = [
        c
        for c in [
            "delta_swc_gal_6in",
            "delta_swc_gal_12in",
        ]
        if c in wide.columns
    ]

    if upper_cols:
        wide["upper_zone_delta_swc_gal"] = wide[upper_cols].sum(
            axis=1,
            min_count=1,
        )

    if "delta_swc_gal_18in" in wide.columns:
        wide["deep_zone_delta_swc_gal"] = wide["delta_swc_gal_18in"]

    if (
        "upper_zone_delta_swc_gal" in wide.columns
        or "deep_zone_delta_swc_gal" in wide.columns
    ):
        upper = pd.to_numeric(
            wide.get("upper_zone_delta_swc_gal", pd.Series(np.nan, index=wide.index)),
            errors="coerce",
        )
        deep = pd.to_numeric(
            wide.get("deep_zone_delta_swc_gal", pd.Series(np.nan, index=wide.index)),
            errors="coerce",
        )

        wide["total_partition_delta_swc_gal"] = (
            pd.concat([upper, deep], axis=1).sum(axis=1, min_count=1)
        )

        total = pd.to_numeric(
            wide["total_partition_delta_swc_gal"],
            errors="coerce",
        )

        wide["upper_storage_fraction"] = np.where(
            total > 0,
            upper / total,
            np.nan,
        )

        wide["deep_storage_fraction"] = np.where(
            total > 0,
            deep / total,
            np.nan,
        )

    # ------------------------------------------------------------------
    # Storage efficiency: stored SWC response divided by applied water.
    # ------------------------------------------------------------------
    if "total_partition_delta_swc_gal" in wide.columns and "gallons_strip" in wide.columns:
        total_delta = pd.to_numeric(
            wide["total_partition_delta_swc_gal"],
            errors="coerce",
        )
        applied = pd.to_numeric(wide["gallons_strip"], errors="coerce")

        wide["storage_efficiency"] = np.where(
            applied > 0,
            total_delta / applied,
            np.nan,
        )

    # Keep older storage summary if present.
    delta_storage_cols = [
        c for c in wide.columns if c.startswith("delta_storage_gal_")
    ]
    if delta_storage_cols:
        wide["delta_storage_profile_gal"] = wide[delta_storage_cols].sum(
            axis=1,
            min_count=1,
        )

    # ------------------------------------------------------------------
    # Arrival timing summary
    # ------------------------------------------------------------------
    arrival_cols = [
        c
        for c in wide.columns
        if c.startswith("arrival_minutes_after_irrigation_start_")
    ]
    if arrival_cols:
        wide["first_arrival_min"] = wide[arrival_cols].min(axis=1)
        wide["last_arrival_min"] = wide[arrival_cols].max(axis=1)
        wide["vertical_spread_min"] = (
            wide["last_arrival_min"] - wide["first_arrival_min"]
        )

    return force_float(move_id_columns_left(wide))

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

def analyze_loggers_all_depths(
    df_15min: pd.DataFrame,
    strips: list[str],
    year: int,
    logger_positions: list[str] | None = None,
) -> pd.DataFrame:

    if logger_positions is None:
        logger_positions = ["T", "M", "B"]

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

        sensor_cols: list[str] = []

        for position in logger_positions:
            for depth in (1, 2, 3):
                col = f"VWC_{depth}_raw_{strip}_{position}"

                if col in df_15min.columns:
                    sensor_cols.append(col)

        if not sensor_cols:
            print(
                f"Skipping {strip}: "
                f"no matching sensors for positions {logger_positions}"
            )
            continue

        select_cols = [
            "start_timestamp",
            "end_timestamp",
            "gallons_strip",
        ]

        for optional_col in [
            "gallons_group",
            "event_id",
            "strip_group",
            "location",
        ]:
            if optional_col in all_events.columns:
                select_cols.append(optional_col)

        events = all_events.loc[
            (all_events["strip"] == strip)
            & (all_events["year"] == year),
            select_cols,
        ].copy()

        if events.empty:
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
            sensor_cols=sensor_cols,
            start_col="start",
            end_col="end",
            gallons_strip_col="gallons_strip",
            gallons_group_col=(
                "gallons_group"
                if "gallons_group" in events.columns
                else None
            ),
            strip=strip,
            year=year,
            event_id_col=(
                "event_id"
                if "event_id" in events.columns
                else None
            ),
        )
        strip_results = attach_event_metadata(
            strip_results,
            events,
        )

        print("\n=== STRIP RESULTS COLUMNS AFTER DELTA FIELDS ===")
        print(strip_results.columns.tolist())
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
    plot_results: pd.DataFrame | None = None,
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
        HOLDING_CAPACITY_DIR / f"debug_irrigation_events_{year}_all_depths.csv",
        index=False,
        float_format="%.2f",
    )

    targets.to_csv(
        HOLDING_CAPACITY_DIR / f"irrigation_targets_{year}_all_depths.csv",
        index=False,
        float_format="%.2f",
    )

    runtimes.to_csv(
        HOLDING_CAPACITY_DIR / f"irrigation_runtimes_{year}_all_depths.csv",
        index=False,
        float_format="%.2f",
    )

    definitions.to_csv(
        HOLDING_CAPACITY_DIR / f"irrigation_variable_definitions_{year}.csv",
        index=False,
    )

    build_variable_definitions_with_sources(
        output_dir=str(HOLDING_CAPACITY_DIR),
        year=year,
    )

    if plot_results is None:
        plot_results = results

    plot_results = plot_results.copy()

    arrival_times = build_irrigation_arrival_times(
        df_15min=df_15min,
        event_results=plot_results,
        response_threshold_vwc=ARRIVAL_RESPONSE_THRESHOLD_VWC,
        hours_before=EVENT_PLOT_HOURS_BEFORE,
        hours_after=EVENT_PLOT_HOURS_AFTER,
    )
    
    response_summary = build_irrigation_event_response_summary(
        arrival_times=arrival_times,
        event_results=results,
    )

    response_summary["distance_from_furrow_start_ft"] = response_summary[
        "logger_position"
    ].apply(_logger_distance_ft)

    response_summary = add_vertical_velocity_fields(response_summary)

    response_summary.to_csv(
        IRRIGATION_DIAGNOSTICS_DIR / f"irrigation_event_response_summary_{year}.csv",
        index=False,
        float_format="%.2f",
    )

    horizontal_advance = build_irrigation_horizontal_advance_summary(
        arrival_times=arrival_times,
    )

    horizontal_advance.to_csv(
        IRRIGATION_DIAGNOSTICS_DIR / f"irrigation_horizontal_advance_summary_{year}.csv",
        index=False,
        float_format="%.2f",
    )

    print(f"\n=== ARRIVAL TIMES COLUMNS BEFORE CSV WRITE ({year}) ===")
    print(arrival_times.columns.tolist())

    arrival_times.to_csv(
        IRRIGATION_DIAGNOSTICS_DIR / f"irrigation_arrival_times_{year}.csv",
        index=False,
    )

    arrival_order_table = build_arrival_order_diagnostics(
        arrival_times=arrival_times,
    )

    arrival_order_table.to_csv(
        IRRIGATION_DIAGNOSTICS_DIR / f"arrival_order_diagnostics_{year}.csv",
        index=False,
    )

    print(f"\n=== ARRIVAL ORDER SUMMARY ({year}) ===")
    if not arrival_order_table.empty:
        print(arrival_order_table["order_class"].value_counts(dropna=False).to_string())

    print(f"\n=== ARRIVAL ORDER BY LOGGER POSITION ({year}) ===")
    if not arrival_order_table.empty:
        print(
            arrival_order_table.groupby(["logger_position", "order_class"])
            .size()
            .unstack(fill_value=0)
            .to_string()
        )

    print(f"\n=== ARRIVAL ORDER BY STRIP ({year}) ===")
    if not arrival_order_table.empty:
        print(
            arrival_order_table.groupby(["strip", "order_class"])
            .size()
            .unstack(fill_value=0)
            .to_string()
        )

    if not arrival_order_table.empty:
        missing_depth_rows = arrival_order_table[
            arrival_order_table["order_class"].eq("missing_depths")
        ]

        print(f"\n=== MISSING DEPTHS BY STRIP / LOGGER ({year}) ===")
        if missing_depth_rows.empty:
            print("None")
        else:
            print(
                missing_depth_rows.groupby(["strip", "logger_position"])
                .size()
                .unstack(fill_value=0)
                .to_string()
            )

            detail_cols = [
                "strip",
                "logger_position",
                "event_id",
                "arrival_6in_min",
                "arrival_12in_min",
                "arrival_18in_min",
                "alt_before_start_depths",
            ]

            print(f"\n=== MISSING DEPTH ARRIVAL DETAILS ({year}) ===")
            print(
                missing_depth_rows[detail_cols]
                .sort_values(["strip", "logger_position", "event_id"])
                .to_string(index=False)
            )

    if not arrival_order_table.empty:
        logger_order_flag_rows = arrival_order_table[
            arrival_order_table["any_bottom_before_top_or_middle"].eq(True)
            | arrival_order_table["any_alt_bottom_before_top_or_middle"].eq(True)
        ]

        print(f"\n=== LOGGER ORDER FLAGS ({year}) ===")
        if logger_order_flag_rows.empty:
            print("None")
        else:
            logger_order_cols = [
                "strip",
                "event_id",
                "logger_position",
                "arrival_6in_logger_order_class",
                "arrival_12in_logger_order_class",
                "arrival_18in_logger_order_class",
                "alt_arrival_6in_logger_order_class",
                "alt_arrival_12in_logger_order_class",
                "alt_arrival_18in_logger_order_class",
            ]
            print(
                logger_order_flag_rows[logger_order_cols]
                .sort_values(["strip", "event_id", "logger_position"])
                .to_string(index=False)
            )

    if not arrival_times.empty:
        arrival_cols = [
            "event_id",
            "strip",
            "sensor_col",
            "logger_position",
            "arrival_time",
            "arrival_vwc",
            "arrival_threshold_vwc",
        ]

        plot_results = plot_results.merge(
            arrival_times[arrival_cols],
            on=["event_id", "strip", "sensor_col", "logger_position"],
            how="left",
        )

    plot_logs: list[pd.DataFrame] = []

    print("\n=== PLOT RESULTS LOGGER POSITION COUNTS ===")
    print(
        plot_results["logger_position"]
        .astype(str)
        .value_counts(dropna=False)
        .sort_index()
        .to_string()
    )

    print("\n=== PLOT RESULTS STRIP / LOGGER COUNTS ===")
    print(
        plot_results.groupby(["strip", "logger_position"], dropna=False)
        .size()
        .reset_index(name="n")
        .to_string(index=False)
    )

    print("\nLOGGER_LOCATIONS used for plot generation:")
    print(LOGGER_LOCATIONS)

    for logger_position_raw in LOGGER_LOCATIONS:
        logger_position = str(logger_position_raw).strip()

        position_plot_dir = MULTIDEPTH_PLOT_DIR / logger_position
        position_plot_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nWriting {logger_position} plots to:")
        print(position_plot_dir)

        plot_log = save_irrigation_event_multidepth_plots(
            df=df_15min,
            event_results=plot_results,
            output_dir=position_plot_dir,
            strip_filter=STRIPS,
            event_ids=None,
            logger_position=logger_position,
            depths=tuple(int(d) for d in SENSOR_DEPTH_CODES),
            hours_before=EVENT_PLOT_HOURS_BEFORE,
            hours_after=EVENT_PLOT_HOURS_AFTER,
            max_plots=None,
            precip_col="precip_in",
            use_common_y_axis=True,
        )

        print(f"{logger_position}: {len(plot_log)} plot-log rows returned")

        if not plot_log.empty:
            print(plot_log["status"].value_counts(dropna=False).to_string())

            if "output_file" in plot_log.columns:
                print(
                    f"{logger_position} example file:\n"
                    f"{plot_log.iloc[0]['output_file']}"
                )

            plot_log["logger_position_requested"] = logger_position
            plot_logs.append(plot_log)

    multidepth_plot_log = (
        pd.concat(plot_logs, ignore_index=True)
        if plot_logs
        else pd.DataFrame()
    )

    plot_log_path = IRRIGATION_FIGURES_DIR / f"irrigation_event_multidepth_plot_log_{year}.csv"
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
        IRRIGATION_DIAGNOSTICS_DIR / f"irrigation_pre_start_response_flags_{year}.csv",
        index=False,
    )

    trustworthy_table = classify_trustworthy_irrigation_events(pre_start_table)

    trustworthy_table.to_csv(
        IRRIGATION_DIAGNOSTICS_DIR / f"trustworthy_irrigation_events_{year}.csv",
        index=False,
    )

    holding_capacity_table = build_trustworthy_holding_capacity_summary(
        trustworthy_table=trustworthy_table,
        event_results=results,
    )

    holding_capacity_table.to_csv(
        HOLDING_CAPACITY_DIR / f"trustworthy_holding_capacity_summary_{year}.csv",
        index=False,
    )

    water_balance_table = build_first_pass_water_balance_table(
        trustworthy_table=trustworthy_table,
        event_results=results,
    )

    water_balance_table.to_csv(
        HOLDING_CAPACITY_DIR / f"first_pass_water_balance_{year}.csv",
        index=False,
    )

    print(
        f"Year {year}: wrote holding-capacity tables, diagnostic tables, "
        f"plot log, arrival table, and multidepth event plots."
    )

    return (
        pre_start_table,
        trustworthy_table,
        holding_capacity_table,
        water_balance_table,
    )

def main() -> None:
    all_pre_start_flags: list[pd.DataFrame] = []
    all_trustworthy_tables: list[pd.DataFrame] = []
    all_holding_capacity_tables: list[pd.DataFrame] = []
    all_water_balance_tables: list[pd.DataFrame] = []
    all_event_storage_zone_tables: list[pd.DataFrame] = []

    for year in YEARS:
        print(f"\n================ YEAR {year} ================")

        df_15min = prepare_15min_logger_data(year)

        # Existing, stable workflow: bottom loggers only.
        bottom_results = analyze_loggers_all_depths(
            df_15min=df_15min,
            strips=STRIPS,
            year=year,
            logger_positions=["B"],
        )

        if bottom_results.empty:
            print(f"Year {year}: no bottom-logger irrigation-analysis results returned.")
            continue

        # New zone-storage workflow: top, middle, and bottom loggers.
        all_logger_results = analyze_loggers_all_depths(
            df_15min=df_15min,
            strips=STRIPS,
            year=year,
            logger_positions=LOGGER_LOCATIONS,
        )

        s3m_2025_debug = all_logger_results.loc[
            (all_logger_results["strip"].astype(str).str.strip() == "S3")
            & (all_logger_results["logger_position"].astype(str).str.strip() == "M")
            & (all_logger_results["year"].astype(str).str.strip() == "2025")
        ].copy()

# TODO (timestamp-debug):
# Remove or generalize this once the S3M 2025 timestamp issue is resolved.
        s3m_2025_debug_path = (
            TIMESTAMP_DIAGNOSTICS_DIR / "s3m_2025_peak_plateau_debug.csv"
        )
        s3m_2025_debug.to_csv(s3m_2025_debug_path, index=False)
        print(f"Wrote S3M 2025 peak/plateau debug: {s3m_2025_debug_path}")

        if not all_logger_results.empty:
            zone_storage_table = build_event_storage_by_zone(all_logger_results)

            if not zone_storage_table.empty:
                all_event_storage_zone_tables.append(zone_storage_table)

        print("\n=== STORAGE DEBUG SAMPLE, ALL LOGGERS ===")
        debug_cols = [
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
        debug_cols = [c for c in debug_cols if c in all_logger_results.columns]

        if debug_cols:
            debug_sample = round_for_reporting(all_logger_results[debug_cols].head(10))
            print(debug_sample.to_string(index=False))

        print(f"Year {year}: {len(bottom_results):,} bottom event/sensor/depth result rows.")
        print(f"Year {year}: {len(all_logger_results):,} all-logger event/sensor/depth result rows.")

        if VERBOSE:
            print("\n=== BOTTOM RESULTS COLUMNS ===")
            print(bottom_results.columns.tolist())
            debug_table = build_enhanced_event_debug_table(bottom_results)
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
            results=bottom_results,
            plot_results=all_logger_results,
        )

        print("\n=== YEAR OUTPUT STATUS ===")
        print(f"year: {year}")
        print(f"bottom_results:         {bottom_results.shape}")
        print(f"all_logger_results:     {all_logger_results.shape}")
        print(f"pre_start_table:        {pre_start_table.shape}")
        print(f"trustworthy_table:      {trustworthy_table.shape}")
        print(f"holding_capacity_table: {holding_capacity_table.shape}")
        print(f"water_balance_table:    {water_balance_table.shape}")

        if not trustworthy_table.empty and "trustworthy_event" in trustworthy_table.columns:
            print("\ntrustworthy_event counts:")
            print(
                trustworthy_table["trustworthy_event"]
                .value_counts(dropna=False)
                .to_string()
            )

        if not water_balance_table.empty:
            print("\nwater_balance_table by year / strip_group / strip:")
            print(
                water_balance_table
                .groupby(["year", "strip_group", "strip"], dropna=False)
                .size()
                .reset_index(name="n_rows")
                .to_string(index=False)
            )

            print("\nwater_balance_table date range:")
            print(
                f"{water_balance_table['irrigation_start'].min()} "
                f"to {water_balance_table['irrigation_start'].max()}"
            )

        if not pre_start_table.empty:
            all_pre_start_flags.append(pre_start_table)

        if not trustworthy_table.empty:
            all_trustworthy_tables.append(trustworthy_table)

        if not holding_capacity_table.empty:
            holding_capacity_table["year"] = year
            all_holding_capacity_tables.append(holding_capacity_table)

        if not water_balance_table.empty:
            all_water_balance_tables.append(water_balance_table)

        if not trustworthy_table.empty and "trustworthy_reason" in trustworthy_table.columns:
            print("\ntrustworthy_reason counts:")
            print(
                trustworthy_table["trustworthy_reason"]
                .value_counts(dropna=False)
                .to_string()
            )

    combined_zone_storage = (
        pd.concat(all_event_storage_zone_tables, ignore_index=True)
        if all_event_storage_zone_tables
        else pd.DataFrame()
    )

    zone_storage_path = HOLDING_CAPACITY_DIR / "event_storage_by_zone.csv"
    combined_zone_storage = round_for_reporting(combined_zone_storage)
    combined_zone_storage.to_csv(zone_storage_path, index=False)

    event_storage_by_event = build_event_storage_by_event(combined_zone_storage)
    event_storage_by_event_path = HOLDING_CAPACITY_DIR / "event_storage_by_event.csv"
    event_storage_by_event.to_csv(event_storage_by_event_path, index=False)

    zone_storage_summary = build_zone_storage_summary(combined_zone_storage)
    zone_storage_summary_path = HOLDING_CAPACITY_DIR / "zone_storage_summary.csv"
    zone_storage_summary.to_csv(zone_storage_summary_path, index=False)

    flow_storage_correlation = build_flow_storage_correlation_summary(
        combined_zone_storage
    )
    flow_storage_correlation_path = HOLDING_CAPACITY_DIR / "flow_storage_correlation_summary.csv"
    flow_storage_correlation.to_csv(flow_storage_correlation_path, index=False)

    plot_mean_storage_depth_by_zone_by_year(combined_zone_storage, HOLDING_CAPACITY_DIR)

    print(f"Wrote flow-storage correlation summary: {flow_storage_correlation_path}")

    zone_ordering_frequency = build_zone_ordering_frequency(combined_zone_storage)
    zone_ordering_frequency_path = HOLDING_CAPACITY_DIR / "zone_ordering_frequency.csv"
    zone_ordering_frequency.to_csv(zone_ordering_frequency_path, index=False)

    zone_anomaly_diagnostics = build_zone_anomaly_diagnostics(combined_zone_storage)
    zone_anomaly_diagnostics_path = HOLDING_CAPACITY_DIR / "zone_anomaly_diagnostics.csv"
    zone_anomaly_diagnostics.to_csv(zone_anomaly_diagnostics_path, index=False)

    plot_mean_storage_by_zone(combined_zone_storage, HOLDING_CAPACITY_DIR)
    plot_mean_storage_by_zone_by_year(combined_zone_storage, HOLDING_CAPACITY_DIR)

    print(f"Wrote zone storage table: {zone_storage_path}")
    print(f"Wrote event storage by event table: {event_storage_by_event_path}")
    print(f"Wrote zone storage summary: {zone_storage_summary_path}")
    print(f"Wrote zone ordering frequency: {zone_ordering_frequency_path}")
    print(f"Wrote zone anomaly diagnostics: {zone_anomaly_diagnostics_path}")

    combined = (
        pd.concat(all_pre_start_flags, ignore_index=True)
        if all_pre_start_flags
        else pd.DataFrame()
    )

    combined_path = HOLDING_CAPACITY_DIR / "irrigation_pre_start_response_flags_all_years.csv"
    combined.to_csv(combined_path, index=False)

    combined_trustworthy = (
        pd.concat(all_trustworthy_tables, ignore_index=True)
        if all_trustworthy_tables
        else pd.DataFrame()
    )

    trustworthy_path = HOLDING_CAPACITY_DIR / "trustworthy_irrigation_events_all_years.csv"
    combined_trustworthy.to_csv(trustworthy_path, index=False)

    combined_holding_capacity = (
        pd.concat(all_holding_capacity_tables, ignore_index=True)
        if all_holding_capacity_tables
        else pd.DataFrame()
    )

    if {
        "mean_plateau_vwc",
        "sd_plateau_vwc",
    }.issubset(combined_holding_capacity.columns):
        combined_holding_capacity["cv_plateau_vwc"] = (
            combined_holding_capacity["sd_plateau_vwc"]
            / combined_holding_capacity["mean_plateau_vwc"]
        )

        def capacity_confidence(row: pd.Series) -> str:
            n = row.get("n_trustworthy_events")
            cv = row.get("cv_plateau_vwc")

            if pd.isna(n) or pd.isna(cv):
                return "low"
            if n >= 5 and cv <= 0.10:
                return "high"
            if n >= 3 and cv <= 0.20:
                return "medium"
            return "low"

        combined_holding_capacity["capacity_confidence"] = (
            combined_holding_capacity.apply(capacity_confidence, axis=1)
        )

        round_3_cols = ["cv_plateau_vwc"]
        round_2_cols = [
            "mean_plateau_vwc",
            "sd_plateau_vwc",
            "min_plateau_vwc",
            "max_plateau_vwc",
            "sd_gallons_strip",
            "mean_baseline_vwc",
            "sd_baseline_vwc",
            "mean_peak_vwc",
            "sd_peak_vwc",
            "mean_peak_increase",
        ]

        for col in round_3_cols:
            if col in combined_holding_capacity.columns:
                combined_holding_capacity[col] = combined_holding_capacity[col].round(3)

        for col in round_2_cols:
            if col in combined_holding_capacity.columns:
                combined_holding_capacity[col] = combined_holding_capacity[col].round(2)

    combined_holding_capacity = round_for_reporting(combined_holding_capacity)

    holding_capacity_path = HOLDING_CAPACITY_DIR / "trustworthy_holding_capacity_summary_all_years.csv"
    combined_holding_capacity.to_csv(holding_capacity_path, index=False)

    combined_water_balance = (
        pd.concat(all_water_balance_tables, ignore_index=True)
        if all_water_balance_tables
        else pd.DataFrame()
    )

    combined_water_balance = round_for_reporting(combined_water_balance)

    water_balance_path = HOLDING_CAPACITY_DIR / "first_pass_water_balance_all_years.csv"
    combined_water_balance.to_csv(water_balance_path, index=False)

    print("\n=== COMBINED ZONE STORAGE FILE ===")
    print(zone_storage_path)

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

    flagged = combined[combined["flag_pre_start_response"].fillna(False)].copy()
    unexplained = combined[
        combined["flag_unexplained_pre_start_response"].fillna(False)
    ].copy()
    precip_driven = combined[
        combined["likely_precip_driven_pre_start_response"].fillna(False)
    ].copy()
    battery_or_logger = combined[
        combined["possible_battery_or_logger_issue"].fillna(False)
    ].copy()

    trustworthy_count = 0
    untrustworthy_count = 0

    if (
        not combined_trustworthy.empty
        and "trustworthy_event" in combined_trustworthy.columns
    ):
        trustworthy_mask = combined_trustworthy["trustworthy_event"].fillna(False)

        trustworthy_count = int(trustworthy_mask.sum())
        untrustworthy_count = int((~trustworthy_mask).sum())

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

    if not combined_zone_storage.empty:
        print("\n=== EVENT STORAGE BY ZONE SUMMARY ===")
        print(f"Zone-storage rows: {len(combined_zone_storage):,}")

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