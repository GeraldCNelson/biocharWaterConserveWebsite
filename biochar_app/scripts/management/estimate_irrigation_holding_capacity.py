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

import pandas as pd
import numpy as np
from pathlib import Path

from biochar_app.config.field_management_metadata import (
    PROFILE_AREA_SQFT,
)

from biochar_app.config.experiment_config import (LOGGER_LOCATIONS)
# Ballpark logger service area:
# strip width 46 ft × strip length 280 ft ÷ 3 logger positions.

from biochar_app.config.core import (
    STRIPS,
    YEARS,
)

from biochar_app.config.experiment_config import (
    SENSOR_DEPTH_CODES,
    SENSOR_DEPTH_INDEX_TO_INCHES,
)

from biochar_app.scripts.data_loading import (
    load_irrigation_data,
)

from biochar_app.scripts.management.irrigation_analysis.irrigation_response_analysis import (
     analyze_irrigation_events,
     build_variable_definitions_table,
     build_variable_definitions_with_sources,
     summarize_targets_and_runtimes,
)

from biochar_app.scripts.management.irrigation_analysis.plotting import (
    save_irrigation_event_multidepth_plots,
    plot_mean_storage_depth_by_zone_by_year,
    plot_mean_storage_by_zone,
    plot_mean_storage_by_zone_by_year,
)

from biochar_app.scripts.management.irrigation_analysis.diagnostics import (
    _logger_distance_ft,
    add_vertical_velocity_fields,
    build_arrival_order_diagnostics,
    build_irrigation_horizontal_advance_summary,
    classify_trustworthy_irrigation_events,
    detect_pre_start_response,
)

from biochar_app.config.irrigation_config import (
    get_irrigation_analysis_options,
    EVENT_PLOT_HOURS_BEFORE,
    EVENT_PLOT_HOURS_AFTER,
    MULTIDEPTH_PLOT_DIR,
    TIMESTAMP_DIAGNOSTICS_DIR,
    ARRIVAL_RESPONSE_THRESHOLD_VWC,
)

from biochar_app.config.paths import (
    irrigation_analysis_paths,
    ensure_analysis_output_directories,
)

from biochar_app.scripts.management.irrigation_analysis.holding_capacity import (
    add_scaled_storage_fields,
    build_event_storage_by_event,
    build_event_storage_by_zone,
    build_first_pass_water_balance_table,
    build_flow_storage_correlation_summary,
    build_trustworthy_holding_capacity_summary,
    build_zone_anomaly_diagnostics,
    build_zone_ordering_frequency,
    build_zone_storage_summary,
)

from biochar_app.scripts.management.irrigation_analysis.arrival import (
    build_irrigation_arrival_times,
)

from biochar_app.scripts.management.irrigation_analysis.utils import (
    add_derived_event_fields,
    attach_event_metadata,
    force_float,
    move_id_columns_left,
    prepare_15min_logger_data,
)

from biochar_app.scripts.management.irrigation_analysis.utils import (
    round_for_reporting,
)

ANALYSIS_OPTIONS = get_irrigation_analysis_options()
ANALYSIS_VARIANT = ANALYSIS_OPTIONS.output_variant
IRRIGATION_INPUT_CSV = ANALYSIS_OPTIONS.input_csv

ANALYSIS_PATHS = irrigation_analysis_paths(
    ANALYSIS_OPTIONS.output_variant
)

IRRIGATION_ANALYSIS_DIR = ANALYSIS_PATHS["root"]
HOLDING_CAPACITY_DIR = ANALYSIS_PATHS["holding_capacity"]
IRRIGATION_DIAGNOSTICS_DIR = ANALYSIS_PATHS["diagnostics"]
IRRIGATION_FIGURES_DIR = ANALYSIS_PATHS["figures"]
IRRIGATION_REPORTS_DIR = ANALYSIS_PATHS["reports"]


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

BATTERY_MIN_OK = 11.0
BATTERY_MAX_OK = 15.0
MIN_BOTTOM_RESPONSE_DELAY_HR = 0.5

        
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
        depth_inches = SENSOR_DEPTH_INDEX_TO_INCHES[depth_code]

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
        depth_inches = SENSOR_DEPTH_INDEX_TO_INCHES[depth_code]

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


def analyze_loggers_all_depths(
    df_15min: pd.DataFrame,
    irrigation_events: pd.DataFrame,
    strips: list[str],
    year: int,
    logger_positions: list[str] | None = None,
) -> pd.DataFrame:
    """
    Analyze irrigation responses for all available depths and selected loggers.

    Parameters
    ----------
    df_15min
        Prepared 15-minute logger data with a DatetimeIndex.

    irrigation_events
        Canonically loaded strip-level irrigation events. The active analysis
        variant determines which irrigation CSV is loaded before this function
        is called.

    strips
        Strip identifiers to analyze.

    year
        Calendar year to analyze.

    logger_positions
        Logger positions to include. Defaults to top, middle, and bottom.
    """
    if logger_positions is None:
        logger_positions = ["T", "M", "B"]

    all_results: list[pd.DataFrame] = []

    all_events = irrigation_events

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
            "Clean irrigation data is missing required columns: "
            f"{sorted(missing)}"
        )

    for strip in strips:
        sensor_cols: list[str] = []

        for position in logger_positions:
            for depth in (1, 2, 3):
                col = (
                    f"VWC_{depth}_raw_"
                    f"{strip}_{position}"
                )

                if col in df_15min.columns:
                    sensor_cols.append(col)

        if not sensor_cols:
            print(
                f"Skipping {strip}: "
                f"no matching sensors for positions "
                f"{logger_positions}"
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
            "gallons_source",
            "gallons_estimated",
            "meter_volume_shared_between_groups",
            "meter_volume_allocation_method",
            "correction_applied",
            "correction_code",
        ]:
            if optional_col in all_events.columns:
                select_cols.append(optional_col)

        events = all_events.loc[
            (
                all_events["strip"].eq(strip)
                & all_events["year"].eq(year)
            ),
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

        # print(
        #     "\n=== STRIP RESULTS COLUMNS "
        #     "AFTER DELTA FIELDS ==="
        # )
        # print(strip_results.columns.tolist())

        if not strip_results.empty:
            all_results.append(strip_results)

    if not all_results:
        return pd.DataFrame()

    out = pd.concat(
        all_results,
        ignore_index=True,
    )

    out = add_derived_event_fields(out)

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

def write_year_outputs(
    year: int,
    df_15min: pd.DataFrame,
    results: pd.DataFrame,
    plot_results: pd.DataFrame | None = None,
    *,
    diagnostics_dir: Path = IRRIGATION_DIAGNOSTICS_DIR,
    holding_capacity_dir: Path = HOLDING_CAPACITY_DIR,
    figures_dir: Path = IRRIGATION_FIGURES_DIR,
    reports_dir: Path = IRRIGATION_REPORTS_DIR,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    diagnostics_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    holding_capacity_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    figures_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    reports_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    multidepth_plot_dir = (
        figures_dir
        / "event_multidepth"
    )

    multidepth_plot_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

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
        holding_capacity_dir / f"debug_irrigation_events_{year}_all_depths.csv",
        index=False,
        float_format="%.2f",
    )

    targets.to_csv(
        holding_capacity_dir / f"irrigation_targets_{year}_all_depths.csv",
        index=False,
        float_format="%.2f",
    )

    runtimes.to_csv(
        holding_capacity_dir / f"irrigation_runtimes_{year}_all_depths.csv",
        index=False,
        float_format="%.2f",
    )

    definitions.to_csv(
        reports_dir / f"irrigation_variable_definitions_{year}.csv",
        index=False,
    )

    build_variable_definitions_with_sources(
        output_dir=str(reports_dir),
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
        diagnostics_dir / f"irrigation_event_response_summary_{year}.csv",
        index=False,
        float_format="%.2f",
    )

    horizontal_advance = build_irrigation_horizontal_advance_summary(
        arrival_times=arrival_times,
    )

    horizontal_advance.to_csv(
        diagnostics_dir / f"irrigation_horizontal_advance_summary_{year}.csv",
        index=False,
        float_format="%.2f",
    )

    print(f"\n=== ARRIVAL TIMES COLUMNS BEFORE CSV WRITE ({year}) ===")
    print(arrival_times.columns.tolist())

    arrival_times.to_csv(
        diagnostics_dir / f"irrigation_arrival_times_{year}.csv",
        index=False,
    )

    arrival_order_table = build_arrival_order_diagnostics(
        arrival_times=arrival_times,
    )

    arrival_order_table.to_csv(
        diagnostics_dir / f"arrival_order_diagnostics_{year}.csv",
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

        position_plot_dir = multidepth_plot_dir / logger_position
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

    plot_log_path = (
            figures_dir
            / f"irrigation_event_multidepth_plot_log_{year}.csv"
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
        diagnostics_dir / f"irrigation_pre_start_response_flags_{year}.csv",
        index=False,
    )

    trustworthy_table = classify_trustworthy_irrigation_events(pre_start_table)

    trustworthy_table.to_csv(
        diagnostics_dir / f"trustworthy_irrigation_events_{year}.csv",
        index=False,
    )

    holding_capacity_table = build_trustworthy_holding_capacity_summary(
        trustworthy_table=trustworthy_table,
        event_results=results,
    )

    holding_capacity_table.to_csv(
        holding_capacity_dir / f"trustworthy_holding_capacity_summary_{year}.csv",
        index=False,
    )

    water_balance_table = build_first_pass_water_balance_table(
        trustworthy_table=trustworthy_table,
        event_results=results,
    )

    water_balance_table.to_csv(
        holding_capacity_dir / f"first_pass_water_balance_{year}.csv",
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

def _append_if_not_empty(collection: list[pd.DataFrame], df: pd.DataFrame) -> None:
    if not df.empty:
        collection.append(df)

def _write_zone_storage_outputs(
    all_event_storage_zone_tables: list[pd.DataFrame],
    *,
    holding_capacity_dir: Path,
) -> pd.DataFrame:
    combined_zone_storage = (
        pd.concat(
            all_event_storage_zone_tables,
            ignore_index=True,
        )
        if all_event_storage_zone_tables
        else pd.DataFrame()
    )

    combined_zone_storage = round_for_reporting(
        combined_zone_storage
    )

    holding_capacity_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    zone_storage_path = (
        holding_capacity_dir
        / "event_storage_by_zone.csv"
    )

    combined_zone_storage.to_csv(
        zone_storage_path,
        index=False,
    )

    build_event_storage_by_event(
        combined_zone_storage
    ).to_csv(
        holding_capacity_dir
        / "event_storage_by_event.csv",
        index=False,
    )

    build_zone_storage_summary(
        combined_zone_storage
    ).to_csv(
        holding_capacity_dir
        / "zone_storage_summary.csv",
        index=False,
    )

    build_flow_storage_correlation_summary(
        combined_zone_storage
    ).to_csv(
        holding_capacity_dir
        / "flow_storage_correlation_summary.csv",
        index=False,
    )

    build_zone_ordering_frequency(
        combined_zone_storage
    ).to_csv(
        holding_capacity_dir
        / "zone_ordering_frequency.csv",
        index=False,
    )

    build_zone_anomaly_diagnostics(
        combined_zone_storage
    ).to_csv(
        holding_capacity_dir
        / "zone_anomaly_diagnostics.csv",
        index=False,
    )

    plot_mean_storage_depth_by_zone_by_year(
        combined_zone_storage,
        holding_capacity_dir,
    )

    plot_mean_storage_by_zone(
        combined_zone_storage,
        holding_capacity_dir,
    )

    plot_mean_storage_by_zone_by_year(
        combined_zone_storage,
        holding_capacity_dir,
    )

    return combined_zone_storage

def _write_combined_year_outputs(
    all_pre_start_flags: list[pd.DataFrame],
    all_trustworthy_tables: list[pd.DataFrame],
    all_holding_capacity_tables: list[pd.DataFrame],
    all_water_balance_tables: list[pd.DataFrame],
    *,
    diagnostics_dir: Path,
    holding_capacity_dir: Path,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    diagnostics_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    holding_capacity_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    frames = [df for df in all_pre_start_flags if df is not None and not df.empty]

    combined_pre_start = (
        pd.concat(
            frames,
            ignore_index=True,
        )
        if frames
        else pd.DataFrame()
    )

    combined_pre_start.to_csv(
        diagnostics_dir
        / "irrigation_pre_start_response_flags_all_years.csv",
        index=False,
    )

    trustworthy_frames = [
        df for df in all_trustworthy_tables if df is not None and not df.empty
    ]

    combined_trustworthy = (
        pd.concat(
            trustworthy_frames,
            ignore_index=True,
        )
        if trustworthy_frames
        else pd.DataFrame()
    )

    combined_trustworthy.to_csv(
        diagnostics_dir
        / "trustworthy_irrigation_events_all_years.csv",
        index=False,
    )

    combined_holding_capacity = (
        pd.concat(
            all_holding_capacity_tables,
            ignore_index=True,
        )
        if all_holding_capacity_tables
        else pd.DataFrame()
    )

    combined_holding_capacity = round_for_reporting(
        combined_holding_capacity
    )

    combined_holding_capacity.to_csv(
        holding_capacity_dir
        / "trustworthy_holding_capacity_summary_all_years.csv",
        index=False,
    )

    combined_water_balance = (
        pd.concat(
            all_water_balance_tables,
            ignore_index=True,
        )
        if all_water_balance_tables
        else pd.DataFrame()
    )

    combined_water_balance = round_for_reporting(
        combined_water_balance
    )

    combined_water_balance.to_csv(
        holding_capacity_dir
        / "first_pass_water_balance_all_years.csv",
        index=False,
    )

    return (
        combined_pre_start,
        combined_trustworthy,
        combined_holding_capacity,
        combined_water_balance,
    )


def main() -> None:
    ensure_analysis_output_directories(ANALYSIS_PATHS)

    if not IRRIGATION_INPUT_CSV.exists():
        raise FileNotFoundError(
            "Selected irrigation analysis input file was not found: "
            f"{IRRIGATION_INPUT_CSV}"
        )

    irrigation_events = load_irrigation_data(IRRIGATION_INPUT_CSV)

    if irrigation_events.empty:
        raise ValueError(
            "Selected irrigation analysis input produced no usable events: "
            f"{IRRIGATION_INPUT_CSV}"
        )

    print(f"\n=== IRRIGATION ANALYSIS: {ANALYSIS_OPTIONS.report_label.upper()} ===")
    print(f"Irrigation input: {IRRIGATION_INPUT_CSV}")
    print(f"Loaded irrigation rows: {len(irrigation_events):,}")
    print(f"Output root: {IRRIGATION_ANALYSIS_DIR}")
    print(f"Description: {ANALYSIS_OPTIONS.description}")

    all_pre_start_flags: list[pd.DataFrame] = []
    all_trustworthy_tables: list[pd.DataFrame] = []
    all_holding_capacity_tables: list[pd.DataFrame] = []
    all_water_balance_tables: list[pd.DataFrame] = []
    all_event_storage_zone_tables: list[pd.DataFrame] = []

    for year in YEARS:
        print(f"\n================ YEAR {year} ================")

        df_15min = prepare_15min_logger_data(year)

        bottom_results = analyze_loggers_all_depths(
            df_15min=df_15min,
            irrigation_events=irrigation_events,
            strips=STRIPS,
            year=year,
            logger_positions=["B"],
        )
        if bottom_results.empty:
            print(
                f"Year {year}: no bottom-logger "
                "irrigation-analysis results returned."
            )
            continue

        all_logger_results = analyze_loggers_all_depths(
            df_15min=df_15min,
            irrigation_events=irrigation_events,
            strips=STRIPS,
            year=year,
            logger_positions=LOGGER_LOCATIONS,
        )

        if not all_logger_results.empty:
            zone_storage_table = (
                build_event_storage_by_zone(
                    all_logger_results
                )
            )

            _append_if_not_empty(
                all_event_storage_zone_tables,
                zone_storage_table,
            )

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
            diagnostics_dir=(
                IRRIGATION_DIAGNOSTICS_DIR
            ),
            holding_capacity_dir=(
                HOLDING_CAPACITY_DIR
            ),
            figures_dir=(
                IRRIGATION_FIGURES_DIR
            ),
            reports_dir=(
                IRRIGATION_REPORTS_DIR
            ),
        )

        _append_if_not_empty(
            all_pre_start_flags,
            pre_start_table,
        )

        _append_if_not_empty(
            all_trustworthy_tables,
            trustworthy_table,
        )

        _append_if_not_empty(
            all_holding_capacity_tables,
            holding_capacity_table,
        )

        _append_if_not_empty(
            all_water_balance_tables,
            water_balance_table,
        )

        print(
            f"Year {year}: "
            f"bottom rows={bottom_results.shape}, "
            f"all-logger rows="
            f"{all_logger_results.shape}"
        )

    combined_zone_storage = (
        _write_zone_storage_outputs(
            all_event_storage_zone_tables,
            holding_capacity_dir=(
                HOLDING_CAPACITY_DIR
            ),
        )
    )

    (
        combined_pre_start,
        combined_trustworthy,
        combined_holding_capacity,
        combined_water_balance,
    ) = _write_combined_year_outputs(
        all_pre_start_flags=(
            all_pre_start_flags
        ),
        all_trustworthy_tables=(
            all_trustworthy_tables
        ),
        all_holding_capacity_tables=(
            all_holding_capacity_tables
        ),
        all_water_balance_tables=(
            all_water_balance_tables
        ),
        diagnostics_dir=(
            IRRIGATION_DIAGNOSTICS_DIR
        ),
        holding_capacity_dir=(
            HOLDING_CAPACITY_DIR
        ),
    )

    print(
        "\n=== "
        f"{ANALYSIS_OPTIONS.report_label.upper()} "
        "IRRIGATION ANALYSIS COMPLETE ==="
    )
    print(
        f"Irrigation source: "
        f"{IRRIGATION_INPUT_CSV}"
    )
    print(
        f"Output root: "
        f"{IRRIGATION_ANALYSIS_DIR}"
    )
    print(
        f"DEPTH_INDEX_TO_INCHES: "
        f"{SENSOR_DEPTH_INDEX_TO_INCHES}"
    )
    print(
        f"Zone-storage rows: "
        f"{len(combined_zone_storage):,}"
    )
    print(
        f"Pre-start rows: "
        f"{len(combined_pre_start):,}"
    )
    print(
        f"Trustworthy-event rows: "
        f"{len(combined_trustworthy):,}"
    )
    print(
        f"Holding-capacity rows: "
        f"{len(combined_holding_capacity):,}"
    )
    print(
        f"Water-balance rows: "
        f"{len(combined_water_balance):,}"
    )

if __name__ == "__main__":
    main()
