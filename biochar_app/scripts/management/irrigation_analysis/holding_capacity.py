"""
Holding-capacity, storage, and water-balance helpers for irrigation analysis.

The storage workflow distinguishes three spatial scales:

1. Sensor scale
   Existing ETL SWC values such as ``baseline_swc_gal``,
   ``peak_swc_gal``, and ``delta_swc_gal`` describe the standardized
   soil volume represented by an individual sensor calculation.

2. Logger influence-zone scale
   Each logger position represents one field zone:
       T = top
       M = middle
       B = bottom

   Each sensor depth is treated as representing one non-overlapping soil layer.
   Zone storage is estimated from VWC change, represented layer thickness,
   and the mapped field area for that logger influence zone.

3. Whole-strip scale
   Top-, middle-, and bottom-zone storage estimates are summed to estimate
   the irrigation-event change in soil-water storage for the full strip
   within the measured 0-18 inch profile.

Important
---------
Whole-strip applied irrigation water must not be compared directly with
single-sensor or single-depth storage values.

The residual:

    applied irrigation
    - estimated 0-18 inch whole-strip storage

is reported as unretained water in the measured soil profile. It is not a
runoff measurement. Response of the bottom-position 6-inch sensor is retained
separately as a hydraulic timing diagnostic.
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pandas as pd

from biochar_app.config.core import SENSOR_DEPTH_CODES

from biochar_app.config.experiment_config import (
    LOGGER_LOCATION_MAPPING,
    REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX,
    ZONE_LABELS,
)

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


def build_event_storage_by_event(
    zone_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build one whole-strip storage row per irrigation event.

    Whole-strip 0-18 inch storage is reported only when all three logger
    influence zones have complete valid 0-18 inch storage profiles.

    Incomplete or invalid zones remain visible diagnostically, but they do not
    produce a scientific whole-strip storage estimate. This prevents missing or
    bad sensor responses from artificially lowering estimated storage and
    inflating the apparent water-balance residual.
    """
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()

    required_cols = {
        "logger_position",
        "estimated_zone_storage_gal_0_18in",
        "gallons_strip",
    }

    missing = (
        required_cols
        - set(
            df.columns
        )
    )

    if missing:
        raise KeyError(
            "Zone-storage table is missing required column(s): "
            f"{sorted(missing)}"
        )

    df[
        "estimated_zone_storage_gal_0_18in"
    ] = pd.to_numeric(
        df[
            "estimated_zone_storage_gal_0_18in"
        ],
        errors="coerce",
    )

    df[
        "gallons_strip"
    ] = pd.to_numeric(
        df[
            "gallons_strip"
        ],
        errors="coerce",
    )

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

    index_cols = [
        col
        for col
        in index_cols
        if col
        in df.columns
    ]

    event_summary = (
        df.pivot_table(
            index=index_cols,
            columns="logger_position",
            values=(
                "estimated_zone_storage_gal_0_18in"
            ),
            aggfunc="first",
        )
        .reset_index()
        .rename(
            columns={
                "T": (
                    "top_zone_storage_gal_0_18in"
                ),
                "M": (
                    "middle_zone_storage_gal_0_18in"
                ),
                "B": (
                    "bottom_zone_storage_gal_0_18in"
                ),
            }
        )
    )

    zone_storage_cols = [
        "top_zone_storage_gal_0_18in",
        "middle_zone_storage_gal_0_18in",
        "bottom_zone_storage_gal_0_18in",
    ]

    for col in zone_storage_cols:
        if col not in event_summary.columns:
            event_summary[col] = pd.NA

        event_summary[col] = pd.to_numeric(
            event_summary[col],
            errors="coerce",
        )

    # ------------------------------------------------------------------
    # Zone coverage.
    #
    # Because build_event_storage_by_zone() now returns NA for a zone whose
    # internal 6/12/18-inch profile is not valid, non-NA zone counts are now
    # scientifically meaningful.
    # ------------------------------------------------------------------
    event_summary[
        "n_zones_with_valid_storage"
    ] = (
        event_summary[
            zone_storage_cols
        ]
        .notna()
        .sum(
            axis=1
        )
    )

    event_summary[
        "complete_three_zone_coverage"
    ] = (
        event_summary[
            "n_zones_with_valid_storage"
        ]
        .eq(3)
    )

    # Backward-compatible alias.
    event_summary[
        "n_zones_with_storage"
    ] = event_summary[
        "n_zones_with_valid_storage"
    ]

    # ------------------------------------------------------------------
    # Whole-strip storage.
    #
    # Never sum a partial spatial profile into a whole-strip estimate.
    # ------------------------------------------------------------------
    zone_sum = (
        event_summary[
            zone_storage_cols
        ]
        .sum(
            axis=1,
            min_count=3,
        )
    )

    event_summary[
        "estimated_storage_gal_strip_0_18in"
    ] = np.where(
        event_summary[
            "complete_three_zone_coverage"
        ],
        zone_sum,
        np.nan,
    )

    applied = pd.to_numeric(
        event_summary[
            "gallons_strip"
        ],
        errors="coerce",
    )

    storage = pd.to_numeric(
        event_summary[
            "estimated_storage_gal_strip_0_18in"
        ],
        errors="coerce",
    )

    # ------------------------------------------------------------------
    # Water-balance residual.
    # ------------------------------------------------------------------
    event_summary[
        "water_balance_residual_gal_strip"
    ] = (
        applied
        - storage
    )

    event_summary[
        "storage_exceeds_applied_water"
    ] = np.where(
        storage.notna()
        & applied.notna(),
        storage
        > applied,
        False,
    )

    # Water not accounted for as increased 0-18 inch storage.
    #
    # Negative residual is clipped to zero for this descriptive quantity,
    # while water_balance_residual_gal_strip retains the signed difference.
    event_summary[
        "unretained_gal_strip"
    ] = np.where(
        storage.notna()
        & applied.notna(),
        np.maximum(
            applied
            - storage,
            0.0,
        ),
        np.nan,
    )

    event_summary[
        "estimated_storage_fraction_0_18in"
    ] = np.where(
        applied
        > 0,
        storage
        / applied,
        np.nan,
    )

    event_summary[
        "unretained_fraction"
    ] = np.where(
        applied
        > 0,
        (
            event_summary[
                "unretained_gal_strip"
            ]
            / applied
        ),
        np.nan,
    )

    # Explicit scientific-availability flag.
    event_summary[
        "whole_strip_storage_estimate_available"
    ] = (
        event_summary[
            "complete_three_zone_coverage"
        ]
        & event_summary[
            "estimated_storage_gal_strip_0_18in"
        ].notna()
    )

    return round_for_reporting(
        event_summary
    )


def build_zone_storage_summary(
    zone_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Summarize event storage by year, strip, and logger influence zone.
    """
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()

    storage_gal_col = (
        "estimated_zone_storage_gal_0_18in"
    )

    storage_in_col = (
        "estimated_zone_storage_in_0_18in"
    )

    if storage_gal_col not in df.columns:
        raise KeyError(
            f"Missing required column {storage_gal_col!r}."
        )

    if storage_in_col not in df.columns:
        raise KeyError(
            f"Missing required column {storage_in_col!r}."
        )

    df[storage_gal_col] = pd.to_numeric(
        df[storage_gal_col],
        errors="coerce",
    )

    df[storage_in_col] = pd.to_numeric(
        df[storage_in_col],
        errors="coerce",
    )

    summary = (
        df.groupby(
            [
                "year",
                "strip",
                "logger_position",
            ],
            dropna=False,
        )
        .agg(
            n_events=(
                "event_id",
                "nunique",
            ),
            mean_storage_gal=(
                storage_gal_col,
                "mean",
            ),
            median_storage_gal=(
                storage_gal_col,
                "median",
            ),
            max_storage_gal=(
                storage_gal_col,
                "max",
            ),
            p95_storage_gal=(
                storage_gal_col,
                lambda s: float(
                    np.nanpercentile(
                        s,
                        95,
                    )
                ),
            ),
            sd_storage_gal=(
                storage_gal_col,
                "std",
            ),
            mean_storage_in=(
                storage_in_col,
                "mean",
            ),
            median_storage_in=(
                storage_in_col,
                "median",
            ),
            max_storage_in=(
                storage_in_col,
                "max",
            ),
            p95_storage_in=(
                storage_in_col,
                lambda s: float(
                    np.nanpercentile(
                        s,
                        95,
                    )
                ),
            ),
            sd_storage_in=(
                storage_in_col,
                "std",
            ),
        )
        .reset_index()
    )

    return round_for_reporting(
        summary
    )


def build_flow_storage_correlation_summary(
    zone_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Summarize the relationship between average irrigation flow rate and
    estimated 0-18 inch zone storage.

    This is descriptive only. It does not establish that flow rate caused
    differences in storage or arrival behavior.
    """
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()

    storage_in_col = (
        "estimated_zone_storage_in_0_18in"
    )

    if storage_in_col not in df.columns:
        raise KeyError(
            f"Missing required column {storage_in_col!r}."
        )

    df[storage_in_col] = pd.to_numeric(
        df[storage_in_col],
        errors="coerce",
    )

    df["avg_flow_gph_strip"] = pd.to_numeric(
        df["avg_flow_gph_strip"],
        errors="coerce",
    )

    rows = []

    for (
        year,
        strip,
        position,
    ), sub in df.groupby(
        [
            "year",
            "strip",
            "logger_position",
        ],
        dropna=False,
    ):
        valid = sub[
            [
                storage_in_col,
                "avg_flow_gph_strip",
            ]
        ].dropna()

        corr: (
            float
            | pd._libs.missing.NAType
            | None
        ) = None

        if len(valid) >= 3:
            corr = valid[
                storage_in_col
            ].corr(
                valid[
                    "avg_flow_gph_strip"
                ]
            )

        rows.append(
            {
                "year": year,
                "strip": strip,
                "logger_position": position,
                "n_events": len(valid),
                "flow_storage_corr": corr,
            }
        )

    return round_for_reporting(
        pd.DataFrame(rows)
    )


def build_zone_ordering_frequency(
    zone_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Summarize the frequency of relative top/middle/bottom zone-storage
    ordering across complete events.
    """
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()

    storage_col = (
        "estimated_zone_storage_gal_0_18in"
    )

    if storage_col not in df.columns:
        raise KeyError(
            f"Missing required column {storage_col!r}."
        )

    df[storage_col] = pd.to_numeric(
        df[storage_col],
        errors="coerce",
    )

    pivot = (
        df.pivot_table(
            index=[
                "year",
                "strip",
                "event_id",
            ],
            columns="logger_position",
            values=storage_col,
            aggfunc="first",
        )
        .reset_index()
    )

    for col in ["T", "M", "B"]:
        if col not in pivot.columns:
            pivot[col] = pd.NA

    pivot = pivot.dropna(
        subset=["T", "M", "B"]
    ).copy()

    def ordering(
        row: pd.Series,
    ) -> str:
        values = {
            "T": row["T"],
            "M": row["M"],
            "B": row["B"],
        }

        return ">".join(
            sorted(
                values,
                key=lambda key: values[key],
                reverse=True,
            )
        )

    pivot["zone_ordering"] = pivot.apply(
        ordering,
        axis=1,
    )

    freq = (
        pivot.groupby(
            ["zone_ordering"],
            dropna=False,
        )
        .size()
        .reset_index(
            name="n_events"
        )
        .sort_values(
            "n_events",
            ascending=False,
        )
    )

    if not freq.empty:
        freq["pct_events"] = (
            100.0
            * freq["n_events"]
            / freq["n_events"].sum()
        )

    return round_for_reporting(
        freq
    )


def build_zone_anomaly_diagnostics(
    zone_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Identify selected historical zone-storage patterns that were previously
    flagged for closer review.

    These are diagnostics, not automatic evidence of invalid irrigation events.
    """
    if zone_df.empty:
        return pd.DataFrame()

    df = zone_df.copy()

    storage_col = (
        "estimated_zone_storage_gal_0_18in"
    )

    if storage_col not in df.columns:
        raise KeyError(
            f"Missing required column {storage_col!r}."
        )

    df[storage_col] = pd.to_numeric(
        df[storage_col],
        errors="coerce",
    )

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

    index_cols = [
        c for c in index_cols
        if c in df.columns
    ]

    pivot = (
        df.pivot_table(
            index=index_cols,
            columns="logger_position",
            values=storage_col,
            aggfunc="first",
        )
        .reset_index()
    )

    for col in ["T", "M", "B"]:
        if col not in pivot.columns:
            pivot[col] = pd.NA

    pivot = pivot.rename(
        columns={
            "T": "top_zone_storage_gal_0_18in",
            "M": "middle_zone_storage_gal_0_18in",
            "B": "bottom_zone_storage_gal_0_18in",
        }
    )

    pivot["s2_bottom_largest"] = (
        pivot["strip"].eq("S2")
        & (
            pivot[
                "bottom_zone_storage_gal_0_18in"
            ]
            > pivot[
                "top_zone_storage_gal_0_18in"
            ]
        )
        & (
            pivot[
                "bottom_zone_storage_gal_0_18in"
            ]
            > pivot[
                "middle_zone_storage_gal_0_18in"
            ]
        )
    )

    pivot["s3_middle_low"] = (
        pivot["strip"].eq("S3")
        & (
            pivot[
                "middle_zone_storage_gal_0_18in"
            ]
            < pivot[
                "top_zone_storage_gal_0_18in"
            ]
        )
        & (
            pivot[
                "middle_zone_storage_gal_0_18in"
            ]
            < pivot[
                "bottom_zone_storage_gal_0_18in"
            ]
        )
    )

    out = pivot[
        pivot["s2_bottom_largest"]
        | pivot["s3_middle_low"]
    ].copy()

    return round_for_reporting(
        out
    )


def build_event_storage_by_zone(
    event_results: pd.DataFrame,
) -> pd.DataFrame:
    """
    Estimate 0-18 inch irrigation-induced soil-water storage by logger zone.

    Each sensor represents one non-overlapping soil layer:

        depth_index 1 -> 0-6 in
        depth_index 2 -> 6-12 in
        depth_index 3 -> 12-18 in

    A depth contributes to the scientific zone-storage estimate only when:

    - baseline VWC is available
    - plateau VWC is available
    - plateau method is not ``no_peak``
    - plateau VWC is greater than or equal to baseline VWC

    The last condition prevents a post-event recession or sensor artifact from
    being interpreted as negative irrigation storage.

    IMPORTANT
    ---------
    A zone-level 0-18 inch storage estimate is considered available only when
    all three represented layers have valid storage estimates.

    Partial layer sums are retained separately for diagnostics, but must not be
    used for whole-strip water-balance or unretained-water estimation.
    """
    if event_results.empty:
        return pd.DataFrame()

    df = event_results.copy()

    df = df[
        df["logger_position"].isin(["T", "M", "B"])
    ].copy()

    required_cols = {
        "strip",
        "logger_position",
        "depth_index",
        "baseline_vwc",
        "plateau_vwc",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise KeyError(
            "Event results are missing columns required for zone-storage "
            f"calculation: {sorted(missing)}"
        )

    numeric_cols = [
        "baseline_vwc",
        "plateau_vwc",
        "peak_vwc",
        "peak_increase",
        "depth_index",
        "depth_inches",
        "gallons_strip",
        "event_duration_hours",
        "avg_flow_gph_strip",
        "baseline_swc_gal",
        "peak_swc_gal",
        "delta_swc_gal",
        "baseline_swc_L",
        "peak_swc_L",
        "delta_swc_L",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce",
            )

    # ------------------------------------------------------------------
    # Represented soil-layer thickness.
    # ------------------------------------------------------------------
    df["represented_layer_thickness_in"] = (
        df["depth_index"].map(
            REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX
        )
    )

    # ------------------------------------------------------------------
    # Raw layer water depths.
    #
    # These are retained even when the event-response estimate is later
    # classified as invalid.
    # ------------------------------------------------------------------
    df["baseline_layer_storage_in"] = (
        df["baseline_vwc"]
        / 100.0
        * df["represented_layer_thickness_in"]
    )

    df["plateau_layer_storage_in"] = (
        df["plateau_vwc"]
        / 100.0
        * df["represented_layer_thickness_in"]
    )

    df["event_layer_storage_in_raw"] = (
        df["plateau_layer_storage_in"]
        - df["baseline_layer_storage_in"]
    )

    # ------------------------------------------------------------------
    # Depth-level storage validity.
    # ------------------------------------------------------------------
    baseline_available = df["baseline_vwc"].notna()
    plateau_available = df["plateau_vwc"].notna()

    if "plateau_method" in df.columns:
        plateau_method = (
            df["plateau_method"]
            .astype("string")
            .str.strip()
        )

        usable_plateau_method = (
            plateau_method.notna()
            & ~plateau_method.eq("no_peak")
        )
    else:
        usable_plateau_method = plateau_available

    plateau_not_below_baseline = (
        df["plateau_vwc"]
        >= df["baseline_vwc"]
    )

    df["depth_storage_valid"] = (
        baseline_available
        & plateau_available
        & usable_plateau_method
        & plateau_not_below_baseline
    )

    # ------------------------------------------------------------------
    # Explicit reason for invalidity.
    #
    # This is useful for diagnosing whether a failed zone is due to:
    # - missing/no response
    # - fallback/plateau recession below baseline
    # - some future unexpected condition
    # ------------------------------------------------------------------
    df["depth_storage_invalid_reason"] = "valid"

    df.loc[
        ~baseline_available,
        "depth_storage_invalid_reason",
    ] = "missing_baseline"

    df.loc[
        baseline_available
        & ~plateau_available,
        "depth_storage_invalid_reason",
    ] = "missing_plateau_or_no_peak"

    if "plateau_method" in df.columns:
        df.loc[
            baseline_available
            & plateau_method.eq("no_peak"),
            "depth_storage_invalid_reason",
        ] = "no_peak"

    df.loc[
        baseline_available
        & plateau_available
        & usable_plateau_method
        & ~plateau_not_below_baseline,
        "depth_storage_invalid_reason",
    ] = "plateau_below_baseline"

    # Scientific storage contribution.
    #
    # Invalid depths are NA rather than zero. Zero would imply that a valid
    # measurement showed no storage response, which is not what happened.
    df["valid_event_layer_storage_in"] = np.where(
        df["depth_storage_valid"],
        df["event_layer_storage_in_raw"],
        np.nan,
    )

    # Expected profile depths from configuration.
    expected_depth_indices = {
        int(depth_code)
        for depth_code in SENSOR_DEPTH_CODES
    }

    # ------------------------------------------------------------------
    # Event / strip / zone grouping.
    # ------------------------------------------------------------------
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

    group_cols = [
        col
        for col in group_cols
        if col in df.columns
    ]

    zone_rows: list[dict[str, object]] = []

    for keys, sub in df.groupby(
        group_cols,
        dropna=False,
    ):
        if not isinstance(keys, tuple):
            keys = (keys,)

        row = dict(
            zip(
                group_cols,
                keys,
            )
        )

        strip = str(
            row["strip"]
        )

        zone = str(
            row["logger_position"]
        )

        if strip not in ZONE_LENGTHS_FT_BY_STRIP:
            continue

        if zone not in ZONE_LENGTHS_FT_BY_STRIP[strip]:
            continue

        zone_gallons_per_inch = (
            ZONE_GALLONS_PER_INCH_BY_STRIP[
                strip
            ][
                zone
            ]
        )

        # --------------------------------------------------------------
        # Sensor rows physically present.
        # --------------------------------------------------------------
        observed_depth_indices = sorted(
            pd.to_numeric(
                sub["depth_index"],
                errors="coerce",
            )
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )

        complete_sensor_profile = (
            set(observed_depth_indices)
            == expected_depth_indices
        )

        # --------------------------------------------------------------
        # Valid storage depths.
        # --------------------------------------------------------------
        valid_sub = sub[
            sub["depth_storage_valid"]
            .fillna(False)
        ].copy()

        valid_depth_indices = sorted(
            pd.to_numeric(
                valid_sub["depth_index"],
                errors="coerce",
            )
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )

        complete_valid_profile = (
            set(valid_depth_indices)
            == expected_depth_indices
        )

        # --------------------------------------------------------------
        # Partial valid-profile diagnostics.
        #
        # These are useful for understanding what was observed but are NOT
        # permitted into whole-strip water balance.
        # --------------------------------------------------------------
        partial_valid_storage_in = (
            valid_sub[
                "valid_event_layer_storage_in"
            ]
            .sum(
                min_count=1
            )
        )

        partial_valid_storage_gal = (
            partial_valid_storage_in
            * zone_gallons_per_inch
            if pd.notna(
                partial_valid_storage_in
            )
            else pd.NA
        )

        # --------------------------------------------------------------
        # Scientific 0-18 inch estimate.
        #
        # Only available when all three represented layers are valid.
        # --------------------------------------------------------------
        if complete_valid_profile:
            baseline_storage_in = (
                valid_sub[
                    "baseline_layer_storage_in"
                ]
                .sum(
                    min_count=1
                )
            )

            plateau_storage_in = (
                valid_sub[
                    "plateau_layer_storage_in"
                ]
                .sum(
                    min_count=1
                )
            )

            event_storage_in = (
                valid_sub[
                    "valid_event_layer_storage_in"
                ]
                .sum(
                    min_count=1
                )
            )

            baseline_storage_gal = (
                baseline_storage_in
                * zone_gallons_per_inch
            )

            plateau_storage_gal = (
                plateau_storage_in
                * zone_gallons_per_inch
            )

            event_storage_gal = (
                event_storage_in
                * zone_gallons_per_inch
            )

        else:
            baseline_storage_in = pd.NA
            plateau_storage_in = pd.NA
            event_storage_in = pd.NA

            baseline_storage_gal = pd.NA
            plateau_storage_gal = pd.NA
            event_storage_gal = pd.NA

        # --------------------------------------------------------------
        # Invalid-depth detail.
        # --------------------------------------------------------------
        invalid_depth_rows = sub[
            ~sub["depth_storage_valid"]
            .fillna(False)
        ].copy()

        invalid_depth_indices = sorted(
            pd.to_numeric(
                invalid_depth_rows[
                    "depth_index"
                ],
                errors="coerce",
            )
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )

        invalid_reasons = sorted(
            invalid_depth_rows[
                "depth_storage_invalid_reason"
            ]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        # --------------------------------------------------------------
        # Existing sensor-scale SWC diagnostics.
        # --------------------------------------------------------------
        sensor_scale_baseline_swc_gal = (
            sub[
                "baseline_swc_gal"
            ].sum(
                min_count=1
            )
            if "baseline_swc_gal"
            in sub.columns
            else pd.NA
        )

        sensor_scale_peak_swc_gal = (
            sub[
                "peak_swc_gal"
            ].sum(
                min_count=1
            )
            if "peak_swc_gal"
            in sub.columns
            else pd.NA
        )

        sensor_scale_delta_swc_gal = (
            sub[
                "delta_swc_gal"
            ].sum(
                min_count=1
            )
            if "delta_swc_gal"
            in sub.columns
            else pd.NA
        )

        sensor_scale_baseline_swc_L = (
            sub[
                "baseline_swc_L"
            ].sum(
                min_count=1
            )
            if "baseline_swc_L"
            in sub.columns
            else pd.NA
        )

        sensor_scale_peak_swc_L = (
            sub[
                "peak_swc_L"
            ].sum(
                min_count=1
            )
            if "peak_swc_L"
            in sub.columns
            else pd.NA
        )

        sensor_scale_delta_swc_L = (
            sub[
                "delta_swc_L"
            ].sum(
                min_count=1
            )
            if "delta_swc_L"
            in sub.columns
            else pd.NA
        )

        zone_rows.append(
            {
                **row,

                "zone": zone,

                "zone_label": (
                    ZONE_LABELS[
                        zone
                    ]
                ),

                "zone_length_ft": (
                    ZONE_LENGTHS_FT_BY_STRIP[
                        strip
                    ][
                        zone
                    ]
                ),

                "zone_area_sqft": (
                    ZONE_AREAS_SQFT_BY_STRIP[
                        strip
                    ][
                        zone
                    ]
                ),

                "zone_gallons_per_inch": (
                    zone_gallons_per_inch
                ),

                # Physical sensor coverage.
                "n_depth_rows": int(
                    sub[
                        "depth_index"
                    ].nunique()
                ),

                "observed_depth_indices": (
                    ",".join(
                        str(value)
                        for value
                        in observed_depth_indices
                    )
                ),

                "complete_sensor_0_18in_profile": (
                    complete_sensor_profile
                ),

                # Valid storage coverage.
                "n_valid_storage_depths": int(
                    valid_sub[
                        "depth_index"
                    ].nunique()
                ),

                "valid_storage_depth_indices": (
                    ",".join(
                        str(value)
                        for value
                        in valid_depth_indices
                    )
                ),

                "invalid_storage_depth_indices": (
                    ",".join(
                        str(value)
                        for value
                        in invalid_depth_indices
                    )
                ),

                "invalid_storage_reasons": (
                    ";".join(
                        invalid_reasons
                    )
                ),

                "complete_valid_0_18in_storage_profile": (
                    complete_valid_profile
                ),

                "zone_storage_estimate_available": (
                    complete_valid_profile
                ),

                # Partial diagnostics only.
                "partial_valid_storage_in": (
                    partial_valid_storage_in
                ),

                "partial_valid_storage_gal": (
                    partial_valid_storage_gal
                ),

                # Scientific 0-18 inch estimate.
                "baseline_zone_storage_in_0_18in": (
                    baseline_storage_in
                ),

                "plateau_zone_storage_in_0_18in": (
                    plateau_storage_in
                ),

                "estimated_zone_storage_in_0_18in": (
                    event_storage_in
                ),

                "baseline_zone_storage_gal_0_18in": (
                    baseline_storage_gal
                ),

                "plateau_zone_storage_gal_0_18in": (
                    plateau_storage_gal
                ),

                "estimated_zone_storage_gal_0_18in": (
                    event_storage_gal
                ),

                # Sensor-cylinder diagnostics.
                "sensor_scale_baseline_swc_gal": (
                    sensor_scale_baseline_swc_gal
                ),

                "sensor_scale_peak_swc_gal": (
                    sensor_scale_peak_swc_gal
                ),

                "sensor_scale_delta_swc_gal": (
                    sensor_scale_delta_swc_gal
                ),

                "sensor_scale_baseline_swc_L": (
                    sensor_scale_baseline_swc_L
                ),

                "sensor_scale_peak_swc_L": (
                    sensor_scale_peak_swc_L
                ),

                "sensor_scale_delta_swc_L": (
                    sensor_scale_delta_swc_L
                ),
            }
        )

    out = pd.DataFrame(
        zone_rows
    )

    if out.empty:
        return out

    numeric_cols = (
        out
        .select_dtypes(
            include=["number"]
        )
        .columns
    )

    out[
        numeric_cols
    ] = out[
        numeric_cols
    ].round(4)

    return out

def add_response_delta_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add simple response (peak - baseline) fields.

    These are sensor-scale response metrics and are independent of the
    zone-storage calculations.
    """
    if df.empty:
        return df

    out = df.copy()

    delta_specs = [
        ("baseline_vwc", "peak_vwc", "delta_vwc"),
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
    zone_storage_table: pd.DataFrame,
    arrival_times: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a first-pass whole-strip irrigation water balance.

    Storage basis
    -------------
    Whole-strip 0-18 inch storage is calculated from the previously built
    three-zone storage table:

        top zone
        + middle zone
        + bottom zone

    Only irrigation events that pass the established trustworthy-event QC
    workflow are retained.

    Bottom-arrival interpretation
    -----------------------------
    Surface runoff from the downstream end of a strip is assumed to be
    physically impossible before irrigation water reaches that end of the
    field.

    The primary response of the bottom-position 6-inch sensor is used as the
    field indicator that irrigation water has reached the downstream portion
    of the strip.

    Bottom arrival is retained as an independent hydraulic timing diagnostic.
    It does not determine whether the water-balance residual is available.

    Water-balance interpretation
    ----------------------------
    The difference between applied irrigation and estimated increase in
    0-18 inch soil-water storage is:

        unretained_gal_strip

    This residual can include:

    - surface runoff / tailwater
    - continuing infiltration
    - storage below 18 inches
    - lateral redistribution
    - measurement and spatial-representation error

    It is not a runoff measurement. ``unretained`` is intentionally concise;
    the metadata and eligibility fields preserve this full interpretation.
    """

    if (
        trustworthy_table.empty
        or zone_storage_table.empty
    ):
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Identify trustworthy irrigation events.
    #
    # trustworthy_table is sensor/depth level. For the water balance we need
    # one event-level key so the corresponding T/M/B zone-storage records can
    # be retained.
    # ------------------------------------------------------------------
    if "trustworthy_event" not in trustworthy_table.columns:
        raise KeyError(
            "trustworthy_table is missing required column "
            "'trustworthy_event'."
        )

    event_key_cols = [
        "year",
        "strip",
        "event_id",
    ]

    missing_trusted_keys = (
        set(event_key_cols)
        - set(trustworthy_table.columns)
    )

    if missing_trusted_keys:
        raise KeyError(
            "trustworthy_table is missing event key column(s): "
            f"{sorted(missing_trusted_keys)}"
        )

    missing_zone_keys = (
        set(event_key_cols)
        - set(zone_storage_table.columns)
    )

    if missing_zone_keys:
        raise KeyError(
            "zone_storage_table is missing event key column(s): "
            f"{sorted(missing_zone_keys)}"
        )

    event_qc = trustworthy_table.copy()
    event_qc["_qc_pass"] = event_qc["trustworthy_event"].fillna(False)

    def combine_qc_reasons(values: pd.Series) -> str:
        reasons = sorted(
            {
                reason
                for value in values.dropna().astype(str)
                for reason in value.split("; ")
                if reason and reason != "ok"
            }
        )
        return "ok" if not reasons else "; ".join(reasons)

    event_eligibility = (
        event_qc.groupby(event_key_cols, dropna=False)
        .agg(
            event_qc_eligible=("_qc_pass", "all"),
            event_qc_reason=("trustworthy_reason", combine_qc_reasons),
        )
        .reset_index()
    )

    # ------------------------------------------------------------------
    # Keep all zones for classified events, including QC failures, so this
    # output is also the complete event inclusion/exclusion list.
    #
    # Trustworthy-event QC currently originates from the established
    # bottom-logger workflow. Once an event is accepted, however, all
    # available T/M/B zone-storage records are retained for whole-strip
    # accounting.
    # ------------------------------------------------------------------
    classified_zone_storage = (
        zone_storage_table.merge(
            event_eligibility[event_key_cols],
            on=event_key_cols,
            how="inner",
        )
    )

    if classified_zone_storage.empty:
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Convert zone rows to one whole-strip row per irrigation event.
    #
    # build_event_storage_by_event() provides:
    #
    #   top_zone_storage_gal_0_18in
    #   middle_zone_storage_gal_0_18in
    #   bottom_zone_storage_gal_0_18in
    #   estimated_storage_gal_strip_0_18in
    #   estimated_storage_fraction_0_18in
    #   water_balance_residual_gal_strip
    #   unretained_gal_strip
    #   unretained_fraction
    # ------------------------------------------------------------------
    out = build_event_storage_by_event(
        classified_zone_storage
    )

    if out.empty:
        return pd.DataFrame()

    out = out.merge(event_eligibility, on=event_key_cols, how="left")

    # ------------------------------------------------------------------
    # Determine primary bottom 6-inch arrival.
    # ------------------------------------------------------------------
    if arrival_times is None or arrival_times.empty:
        bottom_arrivals = pd.DataFrame(
            columns=[
                *event_key_cols,
                "bottom_6in_arrival_time",
                "bottom_6in_arrival_delay_hr",
            ]
        )

    else:
        bottom = arrival_times.copy()

        for col in event_key_cols:
            if col not in bottom.columns:
                raise KeyError(
                    "arrival_times is missing required event key "
                    f"column {col!r}."
                )

        # Bottom logger only.
        if "logger_position" in bottom.columns:
            bottom = bottom[
                bottom[
                    "logger_position"
                ]
                .astype(str)
                .str.strip()
                .eq("B")
            ].copy()

        # 6-inch sensor only.
        #
        # Prefer depth_inches when available. Fall back to depth_index or
        # sensor_col for compatibility with older arrival-table structures.
        if "depth_inches" in bottom.columns:
            depth_inches = pd.to_numeric(
                bottom["depth_inches"],
                errors="coerce",
            )

            bottom = bottom[
                depth_inches.eq(6)
            ].copy()

        elif "depth_index" in bottom.columns:
            depth_index = pd.to_numeric(
                bottom["depth_index"],
                errors="coerce",
            )

            bottom = bottom[
                depth_index.eq(1)
            ].copy()

        elif "sensor_col" in bottom.columns:
            bottom = bottom[
                bottom[
                    "sensor_col"
                ]
                .astype(str)
                .str.match(
                    r"^VWC_1_raw_.*_B$",
                    na=False,
                )
            ].copy()

        else:
            raise KeyError(
                "arrival_times has no depth_inches, depth_index, "
                "or sensor_col column that can identify the bottom "
                "6-inch sensor."
            )

        # Primary arrival only. The alternate detector remains diagnostic
        # and is not used for the water-balance interpretation.
        if "arrival_time" in bottom.columns:
            bottom[
                "arrival_time"
            ] = pd.to_datetime(
                bottom["arrival_time"],
                errors="coerce",
            )

        if (
            "arrival_minutes_after_irrigation_start"
            in bottom.columns
        ):
            bottom[
                "bottom_6in_arrival_delay_hr"
            ] = (
                pd.to_numeric(
                    bottom[
                        "arrival_minutes_after_irrigation_start"
                    ],
                    errors="coerce",
                )
                / 60.0
            )

        elif (
            "arrival_time" in bottom.columns
            and "irrigation_start" in bottom.columns
        ):
            irrigation_start = pd.to_datetime(
                bottom["irrigation_start"],
                errors="coerce",
            )

            bottom[
                "bottom_6in_arrival_delay_hr"
            ] = (
                (
                    bottom["arrival_time"]
                    - irrigation_start
                )
                .dt.total_seconds()
                / 3600.0
            )

        else:
            bottom[
                "bottom_6in_arrival_delay_hr"
            ] = pd.NA

        if "arrival_time" in bottom.columns:
            bottom[
                "bottom_6in_arrival_time"
            ] = bottom["arrival_time"]
        else:
            bottom[
                "bottom_6in_arrival_time"
            ] = pd.NaT

        bottom_arrivals = (
            bottom[
                [
                    *event_key_cols,
                    "bottom_6in_arrival_time",
                    "bottom_6in_arrival_delay_hr",
                ]
            ]
            .sort_values(
                [
                    *event_key_cols,
                    "bottom_6in_arrival_time",
                ]
            )
            .drop_duplicates(
                subset=event_key_cols,
                keep="first",
            )
            .reset_index(
                drop=True
            )
        )

    out = out.merge(
        bottom_arrivals,
        on=event_key_cols,
        how="left",
    )

    # ------------------------------------------------------------------
    # Numeric inputs.
    # ------------------------------------------------------------------
    applied = pd.to_numeric(
        out["gallons_strip"],
        errors="coerce",
    )

    duration = pd.to_numeric(
        out["event_duration_hours"],
        errors="coerce",
    )

    flow = pd.to_numeric(
        out["avg_flow_gph_strip"],
        errors="coerce",
    )

    bottom_delay = pd.to_numeric(
        out["bottom_6in_arrival_delay_hr"],
        errors="coerce",
    )

    # ------------------------------------------------------------------
    # Bottom-arrival timing diagnostics.
    #
    # These describe when the downstream portion of the field responded.
    # They are retained for interpretation but are NOT used as a volumetric
    # cap on unretained water.
    # ------------------------------------------------------------------
    out[
        "bottom_6in_response_observed"
    ] = bottom_delay.notna()

    out[
        "bottom_6in_arrival_before_irrigation_end"
    ] = (
        bottom_delay.notna()
        & duration.notna()
        & (bottom_delay <= duration)
    )

    out[
        "bottom_6in_arrival_after_irrigation_end"
    ] = (
        bottom_delay.notna()
        & duration.notna()
        & (bottom_delay > duration)
    )

    out[
        "post_bottom_6in_arrival_runtime_hr"
    ] = np.where(
        bottom_delay.notna()
        & duration.notna(),
        np.maximum(
            duration - bottom_delay,
            0.0,
        ),
        np.nan,
    )

    out[
        "post_bottom_6in_arrival_applied_gal"
    ] = (
        pd.to_numeric(
            out[
                "post_bottom_6in_arrival_runtime_hr"
            ],
            errors="coerce",
        )
        * flow
    )

    out[
        "post_bottom_6in_arrival_applied_fraction"
    ] = np.where(
        applied > 0,
        (
            out[
                "post_bottom_6in_arrival_applied_gal"
            ]
            / applied
        ),
        np.nan,
    )

    # ------------------------------------------------------------------
    # Unretained-water eligibility.
    #
    # The residual was calculated above for complete three-zone storage
    # estimates. Bottom arrival remains diagnostic and is not an eligibility
    # condition because the residual is not interpreted as measured runoff.
    # ------------------------------------------------------------------
    out[
        "unretained_available"
    ] = (
        out["event_qc_eligible"].fillna(False)
        & out["complete_three_zone_coverage"].fillna(False)
        & applied.gt(0)
        & out["unretained_gal_strip"].notna()
    )
    out["holding_capacity_eligible"] = out[
        "unretained_available"
    ]
    profile_failure = "incomplete_three_zone_storage_or_missing_applied_water"
    out["holding_capacity_reason"] = np.where(
        ~out["event_qc_eligible"].fillna(False),
        out["event_qc_reason"].fillna("event_qc_failed"),
        np.where(out["holding_capacity_eligible"], "ok", profile_failure),
    )
    out["unretained_eligible"] = out["unretained_available"]
    out["unretained_reason"] = out["holding_capacity_reason"]

    # ------------------------------------------------------------------
    # Reporting conveniences.
    # ------------------------------------------------------------------
    out[
        "estimated_storage_percent"
    ] = (
        100.0
        * pd.to_numeric(
            out[
                "estimated_storage_fraction_0_18in"
            ],
            errors="coerce",
        )
    )

    out[
        "unretained_percent"
    ] = (
        100.0
        * pd.to_numeric(
            out[
                "unretained_fraction"
            ],
            errors="coerce",
        )
    )

    numeric_cols = (
        out
        .select_dtypes(
            include=["number"]
        )
        .columns
    )

    out[
        numeric_cols
    ] = out[
        numeric_cols
    ].round(4)

    return move_id_columns_left(
        force_float(
            out
        )
    )

def build_biochar_performance_summary(
    water_balance: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """
    Build compact biochar irrigation-performance summaries.

    Treatment structure
    -------------------
    S1 = biochar, S2 = control
    S3 = biochar, S4 = control

    S1/S2 and S3/S4 are analyzed as matched irrigation pairs.

    Performance dimensions
    ----------------------
    1. 0-18 inch soil-water storage.
    2. Water not retained as increased measured-profile storage.
    3. Bottom 6-inch arrival timing as an independent hydraulic indicator.

    Important interpretation
    ------------------------
    ``unretained_gal`` and ``unretained_pct`` are water-balance residuals,
    not measured runoff.

    Whole-strip storage comparisons require a valid complete three-zone
    storage estimate for both strips in a matched event.

    Returns
    -------
    strip_summary
        Four-row quick-look table, one row per strip.

    pair_summary
        Two-row matched biochar-vs-control summary.

    matched_events
        One row per matched irrigation event, showing biochar and control
        values side-by-side.

    pair_year_summary
        Matched treatment effects summarized separately by year and pair.

    Sign convention
    ---------------
    storage_diff_gal:
        biochar - control
        Positive = more storage in biochar strip.

    unretained_diff_gal:
        biochar - control
        Negative = less unretained water in the biochar strip.

    arrival_diff_hr:
        biochar - control
        Positive = later downstream arrival in biochar strip.
    """
    if water_balance.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty

    df = water_balance.copy()

    required_cols = {
        "year",
        "strip",
        "event_id",
        "gallons_strip",
        "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise KeyError(
            "water_balance is missing required column(s): "
            f"{sorted(missing)}"
        )

    # ------------------------------------------------------------------
    # Experiment metadata
    # ------------------------------------------------------------------
    treatment_by_strip = {
        "S1": "biochar",
        "S2": "control",
        "S3": "biochar",
        "S4": "control",
    }

    pair_by_strip = {
        "S1": "S1_S2",
        "S2": "S1_S2",
        "S3": "S3_S4",
        "S4": "S3_S4",
    }

    biochar_strip_by_pair = {
        "S1_S2": "S1",
        "S3_S4": "S3",
    }

    control_strip_by_pair = {
        "S1_S2": "S2",
        "S3_S4": "S4",
    }

    irrigation_regime_by_pair = {
        "S1_S2": "monthly",
        "S3_S4": "biweekly",
    }

    strip_order = [
        "S1",
        "S2",
        "S3",
        "S4",
    ]

    pair_order = [
        "S1_S2",
        "S3_S4",
    ]

    df["strip"] = (
        df["strip"]
        .astype("string")
        .str.strip()
    )

    df = df[
        df["strip"].isin(
            treatment_by_strip
        )
    ].copy()

    if df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty

    df["treatment"] = (
        df["strip"]
        .map(treatment_by_strip)
    )

    df["pair"] = (
        df["strip"]
        .map(pair_by_strip)
    )

    df["regime"] = (
        df["pair"]
        .map(irrigation_regime_by_pair)
    )

    # ------------------------------------------------------------------
    # Normalize important numeric fields
    # ------------------------------------------------------------------
    numeric_cols = [
        "year",
        "gallons_strip",
        "event_duration_hours",
        "avg_flow_gph_strip",
        "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in",
        "unretained_gal_strip",
        "unretained_fraction",
        "bottom_6in_arrival_delay_hr",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce",
            )

    df["year"] = pd.to_numeric(
        df["year"],
        errors="coerce",
    ).astype("Int64")

    if "irrigation_start" in df.columns:
        df["irrigation_start"] = pd.to_datetime(
            df["irrigation_start"],
            errors="coerce",
        )

    # ------------------------------------------------------------------
    # Valid whole-strip storage
    # ------------------------------------------------------------------
    if "complete_three_zone_coverage" in df.columns:
        storage_available = (
            df["complete_three_zone_coverage"]
            .fillna(False)
            .astype(bool)
        )
    else:
        storage_available = (
            df[
                "estimated_storage_gal_strip_0_18in"
            ].notna()
        )

    storage_available = (
        storage_available
        & df[
            "estimated_storage_gal_strip_0_18in"
        ].notna()
    )

    df["storage_ok"] = storage_available

    # ------------------------------------------------------------------
    # Unretained-water availability
    # ------------------------------------------------------------------
    if "unretained_available" in df.columns:
        unretained_available = (
            df[
                "unretained_available"
            ]
            .fillna(False)
            .astype(bool)
        )

    elif "unretained_gal_strip" in df.columns:
        unretained_available = (
            df[
                "unretained_gal_strip"
            ].notna()
        )

    else:
        unretained_available = pd.Series(
            False,
            index=df.index,
            dtype=bool,
        )

    unretained_available = (
        unretained_available
        & storage_available
    )

    if "unretained_gal_strip" in df.columns:
        unretained_available = (
            unretained_available
            & df[
                "unretained_gal_strip"
            ].notna()
        )

    df["unretained_ok"] = unretained_available

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------
    def numeric_values(
        frame: pd.DataFrame,
        column: str,
    ) -> pd.Series:
        if (
            frame.empty
            or column not in frame.columns
        ):
            return pd.Series(
                dtype="float64"
            )

        return pd.to_numeric(
            frame[column],
            errors="coerce",
        ).dropna()

    def mean_value(
        frame: pd.DataFrame,
        column: str,
    ) -> float | object:
        values = numeric_values(
            frame,
            column,
        )

        if values.empty:
            return pd.NA

        return float(
            values.mean()
        )

    def median_value(
        frame: pd.DataFrame,
        column: str,
    ) -> float | object:
        values = numeric_values(
            frame,
            column,
        )

        if values.empty:
            return pd.NA

        return float(
            values.median()
        )

    def pct_true(
        values: pd.Series,
    ) -> float | object:
        values = values.dropna()

        if values.empty:
            return pd.NA

        return (
            100.0
            * float(
                values.astype(bool).mean()
            )
        )

    def compact_round(
        frame: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Apply reporting-specific rounding.

        Gallons:
            0 decimals

        Percentages, fractions, and hours:
            2 decimals

        Counts / years:
            integer-like values preserved
        """
        if frame.empty:
            return frame.copy()

        out = frame.copy()

        for col in out.columns:
            if col in {
                "year",
                "n_events",
                "n_storage",
                "n_unretained",
                "n_matched",
                "n_storage_better",
                "n_lower_unretained",
            }:
                continue

            if (
                "_gal" in col
                or col.endswith("gal")
            ):
                out[col] = pd.to_numeric(
                    out[col],
                    errors="coerce",
                ).round(0)

            elif (
                "_pct" in col
                or "_hr" in col
                or "_fraction" in col
            ):
                out[col] = pd.to_numeric(
                    out[col],
                    errors="coerce",
                ).round(2)

            elif pd.api.types.is_numeric_dtype(
                out[col]
            ):
                out[col] = pd.to_numeric(
                    out[col],
                    errors="coerce",
                ).round(2)

        return out

    # ==================================================================
    # 1. QUICK-LOOK STRIP SUMMARY
    # ==================================================================
    strip_rows: list[
        dict[str, object]
    ] = []

    for strip in strip_order:
        sub = df[
            df["strip"].eq(strip)
        ].copy()

        if sub.empty:
            continue

        storage_sub = sub[
            sub["storage_ok"]
        ].copy()

        unretained_sub = sub[
            sub["unretained_ok"]
        ].copy()

        n_events = int(
            sub["event_id"].nunique()
        )

        n_storage = int(
            storage_sub[
                "event_id"
            ].nunique()
        )

        n_unretained = int(
            unretained_sub[
                "event_id"
            ].nunique()
        )

        pair = pair_by_strip[
            strip
        ]

        strip_rows.append(
            {
                "strip": strip,
                "treatment": (
                    treatment_by_strip[
                        strip
                    ]
                ),
                "pair": pair,
                "regime": (
                    irrigation_regime_by_pair[
                        pair
                    ]
                ),

                "n_events": n_events,
                "n_storage": n_storage,
                "storage_available_pct": (
                    100.0
                    * n_storage
                    / n_events
                    if n_events > 0
                    else pd.NA
                ),
                "n_unretained": n_unretained,

                "applied_gal": (
                    mean_value(
                        storage_sub,
                        "gallons_strip",
                    )
                ),

                "storage_gal": (
                    mean_value(
                        storage_sub,
                        "estimated_storage_gal_strip_0_18in",
                    )
                ),

                "storage_pct": (
                    100.0
                    * mean_value(
                        storage_sub,
                        "estimated_storage_fraction_0_18in",
                    )
                    if not storage_sub.empty
                    else pd.NA
                ),

                "unretained_gal": (
                    mean_value(
                        unretained_sub,
                        "unretained_gal_strip",
                    )
                ),

                "unretained_pct": (
                    100.0
                    * mean_value(
                        unretained_sub,
                        "unretained_fraction",
                    )
                    if not unretained_sub.empty
                    else pd.NA
                ),

                "arrival_hr": (
                    mean_value(
                        sub,
                        "bottom_6in_arrival_delay_hr",
                    )
                ),
            }
        )

    strip_summary = compact_round(
        pd.DataFrame(
            strip_rows
        )
    )

    # ==================================================================
    # 2. BUILD EVENT-LEVEL MATCHED DATA
    # ==================================================================
    matched_rows: list[
        dict[str, object]
    ] = []

    for pair in pair_order:
        pair_df = df[
            df["pair"].eq(pair)
        ].copy()

        if pair_df.empty:
            continue

        biochar_strip = (
            biochar_strip_by_pair[
                pair
            ]
        )

        control_strip = (
            control_strip_by_pair[
                pair
            ]
        )

        event_ids = (
            pair_df[
                "event_id"
            ]
            .dropna()
            .drop_duplicates()
        )

        for event_id in event_ids:
            event = pair_df[
                pair_df[
                    "event_id"
                ].eq(event_id)
            ].copy()

            biochar = event[
                event[
                    "strip"
                ].eq(
                    biochar_strip
                )
            ]

            control = event[
                event[
                    "strip"
                ].eq(
                    control_strip
                )
            ]

            if (
                biochar.empty
                or control.empty
            ):
                continue

            b = biochar.iloc[0]
            c = control.iloc[0]

            b_storage_ok = bool(
                b["storage_ok"]
            )

            c_storage_ok = bool(
                c["storage_ok"]
            )

            matched_storage_ok = (
                b_storage_ok
                and c_storage_ok
            )

            b_unretained_ok = bool(
                b["unretained_ok"]
            )

            c_unretained_ok = bool(
                c["unretained_ok"]
            )

            matched_unretained_ok = (
                b_unretained_ok
                and c_unretained_ok
            )

            year = b.get(
                "year",
                c.get(
                    "year",
                    pd.NA,
                ),
            )

            irrigation_start = b.get(
                "irrigation_start",
                c.get(
                    "irrigation_start",
                    pd.NaT,
                ),
            )

            applied_gal = pd.to_numeric(
                pd.Series(
                    [
                        b.get(
                            "gallons_strip"
                        ),
                        c.get(
                            "gallons_strip"
                        ),
                    ]
                ),
                errors="coerce",
            ).mean()

            b_storage_gal = (
                pd.to_numeric(
                    b.get(
                        "estimated_storage_gal_strip_0_18in"
                    ),
                    errors="coerce",
                )
                if b_storage_ok
                else np.nan
            )

            c_storage_gal = (
                pd.to_numeric(
                    c.get(
                        "estimated_storage_gal_strip_0_18in"
                    ),
                    errors="coerce",
                )
                if c_storage_ok
                else np.nan
            )

            b_storage_pct = (
                100.0
                * pd.to_numeric(
                    b.get(
                        "estimated_storage_fraction_0_18in"
                    ),
                    errors="coerce",
                )
                if b_storage_ok
                else np.nan
            )

            c_storage_pct = (
                100.0
                * pd.to_numeric(
                    c.get(
                        "estimated_storage_fraction_0_18in"
                    ),
                    errors="coerce",
                )
                if c_storage_ok
                else np.nan
            )

            b_unretained_gal = (
                pd.to_numeric(
                    b.get(
                        "unretained_gal_strip"
                    ),
                    errors="coerce",
                )
                if matched_unretained_ok
                else np.nan
            )

            c_unretained_gal = (
                pd.to_numeric(
                    c.get(
                        "unretained_gal_strip"
                    ),
                    errors="coerce",
                )
                if matched_unretained_ok
                else np.nan
            )

            b_unretained_pct = (
                100.0
                * pd.to_numeric(
                    b.get(
                        "unretained_fraction"
                    ),
                    errors="coerce",
                )
                if matched_unretained_ok
                else np.nan
            )

            c_unretained_pct = (
                100.0
                * pd.to_numeric(
                    c.get(
                        "unretained_fraction"
                    ),
                    errors="coerce",
                )
                if matched_unretained_ok
                else np.nan
            )

            b_arrival = pd.to_numeric(
                b.get(
                    "bottom_6in_arrival_delay_hr"
                ),
                errors="coerce",
            )

            c_arrival = pd.to_numeric(
                c.get(
                    "bottom_6in_arrival_delay_hr"
                ),
                errors="coerce",
            )

            matched_rows.append(
                {
                    "year": year,
                    "date": (
                        irrigation_start.date()
                        if pd.notna(
                            irrigation_start
                        )
                        else pd.NaT
                    ),
                    "pair": pair,
                    "regime": (
                        irrigation_regime_by_pair[
                            pair
                        ]
                    ),
                    "event_id": event_id,
                    "biochar": (
                        biochar_strip
                    ),
                    "control": (
                        control_strip
                    ),
                    "applied_gal": (
                        applied_gal
                    ),

                    "bio_storage_ok": (
                        b_storage_ok
                    ),
                    "ctrl_storage_ok": (
                        c_storage_ok
                    ),
                    "matched_storage_ok": (
                        matched_storage_ok
                    ),

                    "bio_storage_gal": (
                        b_storage_gal
                    ),
                    "ctrl_storage_gal": (
                        c_storage_gal
                    ),
                    "storage_diff_gal": (
                        b_storage_gal
                        - c_storage_gal
                        if matched_storage_ok
                        else np.nan
                    ),

                    "bio_storage_pct": (
                        b_storage_pct
                    ),
                    "ctrl_storage_pct": (
                        c_storage_pct
                    ),
                    "storage_diff_pct": (
                        b_storage_pct
                        - c_storage_pct
                        if matched_storage_ok
                        else np.nan
                    ),

                    "matched_unretained_ok": (
                        matched_unretained_ok
                    ),

                    "bio_unretained_gal": (
                        b_unretained_gal
                    ),
                    "ctrl_unretained_gal": (
                        c_unretained_gal
                    ),
                    "unretained_diff_gal": (
                        b_unretained_gal
                        - c_unretained_gal
                        if matched_unretained_ok
                        else np.nan
                    ),

                    "bio_unretained_pct": (
                        b_unretained_pct
                    ),
                    "ctrl_unretained_pct": (
                        c_unretained_pct
                    ),
                    "unretained_diff_pct": (
                        b_unretained_pct
                        - c_unretained_pct
                        if matched_unretained_ok
                        else np.nan
                    ),

                    "bio_arrival_hr": (
                        b_arrival
                    ),
                    "ctrl_arrival_hr": (
                        c_arrival
                    ),
                    "arrival_diff_hr": (
                        b_arrival
                        - c_arrival
                        if (
                            pd.notna(
                                b_arrival
                            )
                            and pd.notna(
                                c_arrival
                            )
                        )
                        else np.nan
                    ),
                }
            )

    matched_events = pd.DataFrame(
        matched_rows
    )

    if not matched_events.empty:
        matched_events = (
            matched_events
            .sort_values(
                [
                    "year",
                    "pair",
                    "date",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        matched_events = compact_round(
            matched_events
        )

    # ==================================================================
    # 3. TWO-ROW MATCHED PAIR SUMMARY
    # ==================================================================
    pair_rows: list[
        dict[str, object]
    ] = []

    for pair in pair_order:
        sub = matched_events[
            matched_events[
                "pair"
            ].eq(pair)
        ].copy()

        if sub.empty:
            continue

        storage_sub = sub[
            sub[
                "matched_storage_ok"
            ].fillna(False)
        ].copy()

        unretained_sub = sub[
            sub[
                "matched_unretained_ok"
            ].fillna(False)
        ].copy()

        arrival_sub = sub[
            sub[
                "arrival_diff_hr"
            ].notna()
        ].copy()

        storage_diff = numeric_values(
            storage_sub,
            "storage_diff_gal",
        )

        unretained_diff = numeric_values(
            unretained_sub,
            "unretained_diff_gal",
        )

        pair_rows.append(
            {
                "pair": pair,
                "regime": (
                    irrigation_regime_by_pair[
                        pair
                    ]
                ),
                "biochar": (
                    biochar_strip_by_pair[
                        pair
                    ]
                ),
                "control": (
                    control_strip_by_pair[
                        pair
                    ]
                ),

                "n_matched": int(
                    len(
                        storage_sub
                    )
                ),

                "bio_storage_gal": (
                    mean_value(
                        storage_sub,
                        "bio_storage_gal",
                    )
                ),
                "ctrl_storage_gal": (
                    mean_value(
                        storage_sub,
                        "ctrl_storage_gal",
                    )
                ),
                "storage_diff_gal": (
                    mean_value(
                        storage_sub,
                        "storage_diff_gal",
                    )
                ),
                "storage_diff_pct": (
                    mean_value(
                        storage_sub,
                        "storage_diff_pct",
                    )
                ),

                "n_storage_better": int(
                    (
                        storage_diff
                        > 0
                    ).sum()
                ),
                "storage_better_pct": (
                    100.0
                    * (
                        storage_diff
                        > 0
                    ).mean()
                    if not storage_diff.empty
                    else pd.NA
                ),

                "n_unretained": int(
                    len(
                        unretained_sub
                    )
                ),
                "bio_unretained_gal": (
                    mean_value(
                        unretained_sub,
                        "bio_unretained_gal",
                    )
                ),
                "ctrl_unretained_gal": (
                    mean_value(
                        unretained_sub,
                        "ctrl_unretained_gal",
                    )
                ),
                "unretained_diff_gal": (
                    mean_value(
                        unretained_sub,
                        "unretained_diff_gal",
                    )
                ),
                "unretained_diff_pct": (
                    mean_value(
                        unretained_sub,
                        "unretained_diff_pct",
                    )
                ),

                "n_lower_unretained": int(
                    (
                        unretained_diff
                        < 0
                    ).sum()
                ),
                "lower_unretained_pct": (
                    100.0
                    * (
                        unretained_diff
                        < 0
                    ).mean()
                    if not unretained_diff.empty
                    else pd.NA
                ),

                "n_arrival": int(
                    len(
                        arrival_sub
                    )
                ),
                "bio_arrival_hr": (
                    mean_value(
                        arrival_sub,
                        "bio_arrival_hr",
                    )
                ),
                "ctrl_arrival_hr": (
                    mean_value(
                        arrival_sub,
                        "ctrl_arrival_hr",
                    )
                ),
                "arrival_diff_hr": (
                    mean_value(
                        arrival_sub,
                        "arrival_diff_hr",
                    )
                ),
            }
        )

    pair_summary = compact_round(
        pd.DataFrame(
            pair_rows
        )
    )

    # ==================================================================
    # 4. MATCHED PERFORMANCE BY YEAR
    # ==================================================================
    year_rows: list[
        dict[str, object]
    ] = []

    if not matched_events.empty:
        for (
            year,
            pair,
        ), sub in matched_events.groupby(
            [
                "year",
                "pair",
            ],
            dropna=False,
        ):
            storage_sub = sub[
                sub[
                    "matched_storage_ok"
                ].fillna(False)
            ].copy()

            unretained_sub = sub[
                sub[
                    "matched_unretained_ok"
                ].fillna(False)
            ].copy()

            arrival_sub = sub[
                sub[
                    "arrival_diff_hr"
                ].notna()
            ].copy()

            storage_diff = numeric_values(
                storage_sub,
                "storage_diff_gal",
            )

            unretained_diff = numeric_values(
                unretained_sub,
                "unretained_diff_gal",
            )

            year_rows.append(
                {
                    "year": year,
                    "pair": pair,
                    "regime": (
                        irrigation_regime_by_pair.get(
                            pair,
                            pd.NA,
                        )
                    ),

                    "n_matched": int(
                        len(
                            storage_sub
                        )
                    ),

                    "bio_storage_gal": (
                        mean_value(
                            storage_sub,
                            "bio_storage_gal",
                        )
                    ),
                    "ctrl_storage_gal": (
                        mean_value(
                            storage_sub,
                            "ctrl_storage_gal",
                        )
                    ),
                    "storage_diff_gal": (
                        mean_value(
                            storage_sub,
                            "storage_diff_gal",
                        )
                    ),
                    "storage_diff_pct": (
                        mean_value(
                            storage_sub,
                            "storage_diff_pct",
                        )
                    ),
                    "storage_better_pct": (
                        100.0
                        * (
                            storage_diff
                            > 0
                        ).mean()
                        if not storage_diff.empty
                        else pd.NA
                    ),

                    "n_unretained": int(
                        len(
                            unretained_sub
                        )
                    ),
                    "unretained_diff_gal": (
                        mean_value(
                            unretained_sub,
                            "unretained_diff_gal",
                        )
                    ),
                    "unretained_diff_pct": (
                        mean_value(
                            unretained_sub,
                            "unretained_diff_pct",
                        )
                    ),
                    "lower_unretained_pct": (
                        100.0
                        * (
                            unretained_diff
                            < 0
                        ).mean()
                        if not unretained_diff.empty
                        else pd.NA
                    ),

                    "n_arrival": int(
                        len(
                            arrival_sub
                        )
                    ),
                    "bio_arrival_hr": (
                        mean_value(
                            arrival_sub,
                            "bio_arrival_hr",
                        )
                    ),
                    "ctrl_arrival_hr": (
                        mean_value(
                            arrival_sub,
                            "ctrl_arrival_hr",
                        )
                    ),
                    "arrival_diff_hr": (
                        mean_value(
                            arrival_sub,
                            "arrival_diff_hr",
                        )
                    ),
                }
            )

    pair_year_summary = (
        pd.DataFrame(
            year_rows
        )
    )

    if not pair_year_summary.empty:
        pair_year_summary = (
            pair_year_summary
            .sort_values(
                [
                    "year",
                    "pair",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        pair_year_summary = compact_round(
            pair_year_summary
        )

    return (
        strip_summary,
        pair_summary,
        matched_events,
        pair_year_summary,
    )
    """
    Build compact biochar irrigation-performance summaries.

    Treatment structure
    -------------------
    S1 = biochar, S2 = control
    S3 = biochar, S4 = control

    S1/S2 and S3/S4 are analyzed as matched irrigation pairs.

    Performance dimensions
    ----------------------
    1. 0-18 inch soil-water storage.
    2. Water not retained as increased measured-profile storage.
    3. Bottom 6-inch arrival timing as an independent hydraulic indicator.

    Important interpretation
    ------------------------
    ``unretained_gal`` and ``unretained_pct`` are water-balance residuals,
    not measured runoff.

    Whole-strip storage comparisons require a valid complete three-zone
    storage estimate for both strips in a matched event.

    Returns
    -------
    strip_summary
        Four-row quick-look table, one row per strip.

    pair_summary
        Two-row matched biochar-vs-control summary.

    matched_events
        One row per matched irrigation event, showing biochar and control
        values side-by-side.

    pair_year_summary
        Matched treatment effects summarized separately by year and pair.

    Sign convention
    ---------------
    storage_diff_gal:
        biochar - control
        Positive = more storage in biochar strip.

    unretained_diff_gal:
        biochar - control
        Negative = less unretained water in biochar strip.

    arrival_diff_hr:
        biochar - control
        Positive = later downstream arrival in biochar strip.
    """
    if water_balance.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty

    df = water_balance.copy()

    required_cols = {
        "year",
        "strip",
        "event_id",
        "gallons_strip",
        "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise KeyError(
            "water_balance is missing required column(s): "
            f"{sorted(missing)}"
        )

    # ------------------------------------------------------------------
    # Experiment metadata
    # ------------------------------------------------------------------
    treatment_by_strip = {
        "S1": "biochar",
        "S2": "control",
        "S3": "biochar",
        "S4": "control",
    }

    pair_by_strip = {
        "S1": "S1_S2",
        "S2": "S1_S2",
        "S3": "S3_S4",
        "S4": "S3_S4",
    }

    biochar_strip_by_pair = {
        "S1_S2": "S1",
        "S3_S4": "S3",
    }

    control_strip_by_pair = {
        "S1_S2": "S2",
        "S3_S4": "S4",
    }

    irrigation_regime_by_pair = {
        "S1_S2": "monthly",
        "S3_S4": "biweekly",
    }

    strip_order = [
        "S1",
        "S2",
        "S3",
        "S4",
    ]

    pair_order = [
        "S1_S2",
        "S3_S4",
    ]

    df["strip"] = (
        df["strip"]
        .astype("string")
        .str.strip()
    )

    df = df[
        df["strip"].isin(
            treatment_by_strip
        )
    ].copy()

    if df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty

    df["treatment"] = (
        df["strip"]
        .map(treatment_by_strip)
    )

    df["pair"] = (
        df["strip"]
        .map(pair_by_strip)
    )

    df["regime"] = (
        df["pair"]
        .map(irrigation_regime_by_pair)
    )

    # ------------------------------------------------------------------
    # Normalize important numeric fields
    # ------------------------------------------------------------------
    numeric_cols = [
        "year",
        "gallons_strip",
        "event_duration_hours",
        "avg_flow_gph_strip",
        "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in",
        "unretained_gal_strip",
        "unretained_fraction",
        "bottom_6in_arrival_delay_hr",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce",
            )

    df["year"] = pd.to_numeric(
        df["year"],
        errors="coerce",
    ).astype("Int64")

    if "irrigation_start" in df.columns:
        df["irrigation_start"] = pd.to_datetime(
            df["irrigation_start"],
            errors="coerce",
        )

    # ------------------------------------------------------------------
    # Valid whole-strip storage
    # ------------------------------------------------------------------
    if "complete_three_zone_coverage" in df.columns:
        storage_available = (
            df["complete_three_zone_coverage"]
            .fillna(False)
            .astype(bool)
        )
    else:
        storage_available = (
            df[
                "estimated_storage_gal_strip_0_18in"
            ].notna()
        )

    storage_available = (
        storage_available
        & df[
            "estimated_storage_gal_strip_0_18in"
        ].notna()
    )

    df["storage_ok"] = storage_available

    # ------------------------------------------------------------------
    # Unretained-water availability
    # ------------------------------------------------------------------
    if "unretained_available" in df.columns:
        unretained_available = (
            df[
                "unretained_available"
            ]
            .fillna(False)
            .astype(bool)
        )

    elif "unretained_gal_strip" in df.columns:
        unretained_available = (
            df[
                "unretained_gal_strip"
            ].notna()
        )

    else:
        unretained_available = pd.Series(
            False,
            index=df.index,
            dtype=bool,
        )

    unretained_available = (
        unretained_available
        & storage_available
    )

    if "unretained_gal_strip" in df.columns:
        unretained_available = (
            unretained_available
            & df[
                "unretained_gal_strip"
            ].notna()
        )

    df["unretained_ok"] = unretained_available

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------
    def numeric_values(
        frame: pd.DataFrame,
        column: str,
    ) -> pd.Series:
        if (
            frame.empty
            or column not in frame.columns
        ):
            return pd.Series(
                dtype="float64"
            )

        return pd.to_numeric(
            frame[column],
            errors="coerce",
        ).dropna()

    def mean_value(
        frame: pd.DataFrame,
        column: str,
    ) -> float | object:
        values = numeric_values(
            frame,
            column,
        )

        if values.empty:
            return pd.NA

        return float(
            values.mean()
        )

    def median_value(
        frame: pd.DataFrame,
        column: str,
    ) -> float | object:
        values = numeric_values(
            frame,
            column,
        )

        if values.empty:
            return pd.NA

        return float(
            values.median()
        )

    def pct_true(
        values: pd.Series,
    ) -> float | object:
        values = values.dropna()

        if values.empty:
            return pd.NA

        return (
            100.0
            * float(
                values.astype(bool).mean()
            )
        )

    def compact_round(
        frame: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Apply reporting-specific rounding.

        Gallons:
            0 decimals

        Percentages, fractions, and hours:
            2 decimals

        Counts / years:
            integer-like values preserved
        """
        if frame.empty:
            return frame.copy()

        out = frame.copy()

        for col in out.columns:
            if col in {
                "year",
                "n_events",
                "n_storage",
                "n_unretained",
                "n_matched",
                "n_storage_better",
                "n_lower_unretained",
            }:
                continue

            if (
                "_gal" in col
                or col.endswith("gal")
            ):
                out[col] = pd.to_numeric(
                    out[col],
                    errors="coerce",
                ).round(0)

            elif (
                "_pct" in col
                or "_hr" in col
                or "_fraction" in col
            ):
                out[col] = pd.to_numeric(
                    out[col],
                    errors="coerce",
                ).round(2)

            elif pd.api.types.is_numeric_dtype(
                out[col]
            ):
                out[col] = pd.to_numeric(
                    out[col],
                    errors="coerce",
                ).round(2)

        return out

    # ==================================================================
    # 1. QUICK-LOOK STRIP SUMMARY
    # ==================================================================
    strip_rows: list[
        dict[str, object]
    ] = []

    for strip in strip_order:
        sub = df[
            df["strip"].eq(strip)
        ].copy()

        if sub.empty:
            continue

        storage_sub = sub[
            sub["storage_ok"]
        ].copy()

        unretained_sub = sub[
            sub["unretained_ok"]
        ].copy()

        n_events = int(
            sub["event_id"].nunique()
        )

        n_storage = int(
            storage_sub[
                "event_id"
            ].nunique()
        )

        n_unretained = int(
            unretained_sub[
                "event_id"
            ].nunique()
        )

        pair = pair_by_strip[
            strip
        ]

        strip_rows.append(
            {
                "strip": strip,
                "treatment": (
                    treatment_by_strip[
                        strip
                    ]
                ),
                "pair": pair,
                "regime": (
                    irrigation_regime_by_pair[
                        pair
                    ]
                ),

                "n_events": n_events,
                "n_storage": n_storage,
                "storage_available_pct": (
                    100.0
                    * n_storage
                    / n_events
                    if n_events > 0
                    else pd.NA
                ),
                "n_unretained": n_unretained,

                "applied_gal": (
                    mean_value(
                        storage_sub,
                        "gallons_strip",
                    )
                ),

                "storage_gal": (
                    mean_value(
                        storage_sub,
                        "estimated_storage_gal_strip_0_18in",
                    )
                ),

                "storage_pct": (
                    100.0
                    * mean_value(
                        storage_sub,
                        "estimated_storage_fraction_0_18in",
                    )
                    if not storage_sub.empty
                    else pd.NA
                ),

                "unretained_gal": (
                    mean_value(
                        unretained_sub,
                        "unretained_gal_strip",
                    )
                ),

                "unretained_pct": (
                    100.0
                    * mean_value(
                        unretained_sub,
                        "unretained_fraction",
                    )
                    if not unretained_sub.empty
                    else pd.NA
                ),

                "arrival_hr": (
                    mean_value(
                        sub,
                        "bottom_6in_arrival_delay_hr",
                    )
                ),
            }
        )

    strip_summary = compact_round(
        pd.DataFrame(
            strip_rows
        )
    )

    # ==================================================================
    # 2. BUILD EVENT-LEVEL MATCHED DATA
    # ==================================================================
    matched_rows: list[
        dict[str, object]
    ] = []

    for pair in pair_order:
        pair_df = df[
            df["pair"].eq(pair)
        ].copy()

        if pair_df.empty:
            continue

        biochar_strip = (
            biochar_strip_by_pair[
                pair
            ]
        )

        control_strip = (
            control_strip_by_pair[
                pair
            ]
        )

        event_ids = (
            pair_df[
                "event_id"
            ]
            .dropna()
            .drop_duplicates()
        )

        for event_id in event_ids:
            event = pair_df[
                pair_df[
                    "event_id"
                ].eq(event_id)
            ].copy()

            biochar = event[
                event[
                    "strip"
                ].eq(
                    biochar_strip
                )
            ]

            control = event[
                event[
                    "strip"
                ].eq(
                    control_strip
                )
            ]

            if (
                biochar.empty
                or control.empty
            ):
                continue

            b = biochar.iloc[0]
            c = control.iloc[0]

            b_storage_ok = bool(
                b["storage_ok"]
            )

            c_storage_ok = bool(
                c["storage_ok"]
            )

            matched_storage_ok = (
                b_storage_ok
                and c_storage_ok
            )

            b_unretained_ok = bool(
                b["unretained_ok"]
            )

            c_unretained_ok = bool(
                c["unretained_ok"]
            )

            matched_unretained_ok = (
                b_unretained_ok
                and c_unretained_ok
            )

            year = b.get(
                "year",
                c.get(
                    "year",
                    pd.NA,
                ),
            )

            irrigation_start = b.get(
                "irrigation_start",
                c.get(
                    "irrigation_start",
                    pd.NaT,
                ),
            )

            applied_gal = pd.to_numeric(
                pd.Series(
                    [
                        b.get(
                            "gallons_strip"
                        ),
                        c.get(
                            "gallons_strip"
                        ),
                    ]
                ),
                errors="coerce",
            ).mean()

            b_storage_gal = (
                pd.to_numeric(
                    b.get(
                        "estimated_storage_gal_strip_0_18in"
                    ),
                    errors="coerce",
                )
                if b_storage_ok
                else np.nan
            )

            c_storage_gal = (
                pd.to_numeric(
                    c.get(
                        "estimated_storage_gal_strip_0_18in"
                    ),
                    errors="coerce",
                )
                if c_storage_ok
                else np.nan
            )

            b_storage_pct = (
                100.0
                * pd.to_numeric(
                    b.get(
                        "estimated_storage_fraction_0_18in"
                    ),
                    errors="coerce",
                )
                if b_storage_ok
                else np.nan
            )

            c_storage_pct = (
                100.0
                * pd.to_numeric(
                    c.get(
                        "estimated_storage_fraction_0_18in"
                    ),
                    errors="coerce",
                )
                if c_storage_ok
                else np.nan
            )

            b_unretained_gal = (
                pd.to_numeric(
                    b.get(
                        "unretained_gal_strip"
                    ),
                    errors="coerce",
                )
                if matched_unretained_ok
                else np.nan
            )

            c_unretained_gal = (
                pd.to_numeric(
                    c.get(
                        "unretained_gal_strip"
                    ),
                    errors="coerce",
                )
                if matched_unretained_ok
                else np.nan
            )

            b_unretained_pct = (
                100.0
                * pd.to_numeric(
                    b.get(
                        "unretained_fraction"
                    ),
                    errors="coerce",
                )
                if matched_unretained_ok
                else np.nan
            )

            c_unretained_pct = (
                100.0
                * pd.to_numeric(
                    c.get(
                        "unretained_fraction"
                    ),
                    errors="coerce",
                )
                if matched_unretained_ok
                else np.nan
            )

            b_arrival = pd.to_numeric(
                b.get(
                    "bottom_6in_arrival_delay_hr"
                ),
                errors="coerce",
            )

            c_arrival = pd.to_numeric(
                c.get(
                    "bottom_6in_arrival_delay_hr"
                ),
                errors="coerce",
            )

            matched_rows.append(
                {
                    "year": year,
                    "date": (
                        irrigation_start.date()
                        if pd.notna(
                            irrigation_start
                        )
                        else pd.NaT
                    ),
                    "pair": pair,
                    "regime": (
                        irrigation_regime_by_pair[
                            pair
                        ]
                    ),
                    "event_id": event_id,
                    "biochar": (
                        biochar_strip
                    ),
                    "control": (
                        control_strip
                    ),
                    "applied_gal": (
                        applied_gal
                    ),

                    "bio_storage_ok": (
                        b_storage_ok
                    ),
                    "ctrl_storage_ok": (
                        c_storage_ok
                    ),
                    "matched_storage_ok": (
                        matched_storage_ok
                    ),

                    "bio_storage_gal": (
                        b_storage_gal
                    ),
                    "ctrl_storage_gal": (
                        c_storage_gal
                    ),
                    "storage_diff_gal": (
                        b_storage_gal
                        - c_storage_gal
                        if matched_storage_ok
                        else np.nan
                    ),

                    "bio_storage_pct": (
                        b_storage_pct
                    ),
                    "ctrl_storage_pct": (
                        c_storage_pct
                    ),
                    "storage_diff_pct": (
                        b_storage_pct
                        - c_storage_pct
                        if matched_storage_ok
                        else np.nan
                    ),

                    "matched_unretained_ok": (
                        matched_unretained_ok
                    ),

                    "bio_unretained_gal": (
                        b_unretained_gal
                    ),
                    "ctrl_unretained_gal": (
                        c_unretained_gal
                    ),
                    "unretained_diff_gal": (
                        b_unretained_gal
                        - c_unretained_gal
                        if matched_unretained_ok
                        else np.nan
                    ),

                    "bio_unretained_pct": (
                        b_unretained_pct
                    ),
                    "ctrl_unretained_pct": (
                        c_unretained_pct
                    ),
                    "unretained_diff_pct": (
                        b_unretained_pct
                        - c_unretained_pct
                        if matched_unretained_ok
                        else np.nan
                    ),

                    "bio_arrival_hr": (
                        b_arrival
                    ),
                    "ctrl_arrival_hr": (
                        c_arrival
                    ),
                    "arrival_diff_hr": (
                        b_arrival
                        - c_arrival
                        if (
                            pd.notna(
                                b_arrival
                            )
                            and pd.notna(
                                c_arrival
                            )
                        )
                        else np.nan
                    ),
                }
            )

    matched_events = pd.DataFrame(
        matched_rows
    )

    if not matched_events.empty:
        matched_events = (
            matched_events
            .sort_values(
                [
                    "year",
                    "pair",
                    "date",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        matched_events = compact_round(
            matched_events
        )

    # ==================================================================
    # 3. TWO-ROW MATCHED PAIR SUMMARY
    # ==================================================================
    pair_rows: list[
        dict[str, object]
    ] = []

    for pair in pair_order:
        sub = matched_events[
            matched_events[
                "pair"
            ].eq(pair)
        ].copy()

        if sub.empty:
            continue

        storage_sub = sub[
            sub[
                "matched_storage_ok"
            ].fillna(False)
        ].copy()

        unretained_sub = sub[
            sub[
                "matched_unretained_ok"
            ].fillna(False)
        ].copy()

        arrival_sub = sub[
            sub[
                "arrival_diff_hr"
            ].notna()
        ].copy()

        storage_diff = numeric_values(
            storage_sub,
            "storage_diff_gal",
        )

        unretained_diff = numeric_values(
            unretained_sub,
            "unretained_diff_gal",
        )

        pair_rows.append(
            {
                "pair": pair,
                "regime": (
                    irrigation_regime_by_pair[
                        pair
                    ]
                ),
                "biochar": (
                    biochar_strip_by_pair[
                        pair
                    ]
                ),
                "control": (
                    control_strip_by_pair[
                        pair
                    ]
                ),

                "n_matched": int(
                    len(
                        storage_sub
                    )
                ),

                "bio_storage_gal": (
                    mean_value(
                        storage_sub,
                        "bio_storage_gal",
                    )
                ),
                "ctrl_storage_gal": (
                    mean_value(
                        storage_sub,
                        "ctrl_storage_gal",
                    )
                ),
                "storage_diff_gal": (
                    mean_value(
                        storage_sub,
                        "storage_diff_gal",
                    )
                ),
                "storage_diff_pct": (
                    mean_value(
                        storage_sub,
                        "storage_diff_pct",
                    )
                ),

                "n_storage_better": int(
                    (
                        storage_diff
                        > 0
                    ).sum()
                ),
                "storage_better_pct": (
                    100.0
                    * (
                        storage_diff
                        > 0
                    ).mean()
                    if not storage_diff.empty
                    else pd.NA
                ),

                "n_unretained": int(
                    len(
                        unretained_sub
                    )
                ),
                "bio_unretained_gal": (
                    mean_value(
                        unretained_sub,
                        "bio_unretained_gal",
                    )
                ),
                "ctrl_unretained_gal": (
                    mean_value(
                        unretained_sub,
                        "ctrl_unretained_gal",
                    )
                ),
                "unretained_diff_gal": (
                    mean_value(
                        unretained_sub,
                        "unretained_diff_gal",
                    )
                ),
                "unretained_diff_pct": (
                    mean_value(
                        unretained_sub,
                        "unretained_diff_pct",
                    )
                ),

                "n_lower_unretained": int(
                    (
                        unretained_diff
                        < 0
                    ).sum()
                ),
                "lower_unretained_pct": (
                    100.0
                    * (
                        unretained_diff
                        < 0
                    ).mean()
                    if not unretained_diff.empty
                    else pd.NA
                ),

                "n_arrival": int(
                    len(
                        arrival_sub
                    )
                ),
                "bio_arrival_hr": (
                    mean_value(
                        arrival_sub,
                        "bio_arrival_hr",
                    )
                ),
                "ctrl_arrival_hr": (
                    mean_value(
                        arrival_sub,
                        "ctrl_arrival_hr",
                    )
                ),
                "arrival_diff_hr": (
                    mean_value(
                        arrival_sub,
                        "arrival_diff_hr",
                    )
                ),
            }
        )

    pair_summary = compact_round(
        pd.DataFrame(
            pair_rows
        )
    )

    # ==================================================================
    # 4. MATCHED PERFORMANCE BY YEAR
    # ==================================================================
    year_rows: list[
        dict[str, object]
    ] = []

    if not matched_events.empty:
        for (
            year,
            pair,
        ), sub in matched_events.groupby(
            [
                "year",
                "pair",
            ],
            dropna=False,
        ):
            storage_sub = sub[
                sub[
                    "matched_storage_ok"
                ].fillna(False)
            ].copy()

            unretained_sub = sub[
                sub[
                    "matched_unretained_ok"
                ].fillna(False)
            ].copy()

            arrival_sub = sub[
                sub[
                    "arrival_diff_hr"
                ].notna()
            ].copy()

            storage_diff = numeric_values(
                storage_sub,
                "storage_diff_gal",
            )

            unretained_diff = numeric_values(
                unretained_sub,
                "unretained_diff_gal",
            )

            year_rows.append(
                {
                    "year": year,
                    "pair": pair,
                    "regime": (
                        irrigation_regime_by_pair.get(
                            pair,
                            pd.NA,
                        )
                    ),

                    "n_matched": int(
                        len(
                            storage_sub
                        )
                    ),

                    "bio_storage_gal": (
                        mean_value(
                            storage_sub,
                            "bio_storage_gal",
                        )
                    ),
                    "ctrl_storage_gal": (
                        mean_value(
                            storage_sub,
                            "ctrl_storage_gal",
                        )
                    ),
                    "storage_diff_gal": (
                        mean_value(
                            storage_sub,
                            "storage_diff_gal",
                        )
                    ),
                    "storage_diff_pct": (
                        mean_value(
                            storage_sub,
                            "storage_diff_pct",
                        )
                    ),
                    "storage_better_pct": (
                        100.0
                        * (
                            storage_diff
                            > 0
                        ).mean()
                        if not storage_diff.empty
                        else pd.NA
                    ),

                    "n_unretained": int(
                        len(
                            unretained_sub
                        )
                    ),
                    "unretained_diff_gal": (
                        mean_value(
                            unretained_sub,
                            "unretained_diff_gal",
                        )
                    ),
                    "unretained_diff_pct": (
                        mean_value(
                            unretained_sub,
                            "unretained_diff_pct",
                        )
                    ),
                    "lower_unretained_pct": (
                        100.0
                        * (
                            unretained_diff
                            < 0
                        ).mean()
                        if not unretained_diff.empty
                        else pd.NA
                    ),

                    "n_arrival": int(
                        len(
                            arrival_sub
                        )
                    ),
                    "bio_arrival_hr": (
                        mean_value(
                            arrival_sub,
                            "bio_arrival_hr",
                        )
                    ),
                    "ctrl_arrival_hr": (
                        mean_value(
                            arrival_sub,
                            "ctrl_arrival_hr",
                        )
                    ),
                    "arrival_diff_hr": (
                        mean_value(
                            arrival_sub,
                            "arrival_diff_hr",
                        )
                    ),
                }
            )

    pair_year_summary = (
        pd.DataFrame(
            year_rows
        )
    )

    if not pair_year_summary.empty:
        pair_year_summary = (
            pair_year_summary
            .sort_values(
                [
                    "year",
                    "pair",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        pair_year_summary = compact_round(
            pair_year_summary
        )

    return (
        strip_summary,
        pair_summary,
        matched_events,
        pair_year_summary,
    )


def summarize_holding_capacity_from_trustworthy_events(
    trustworthy_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    Estimate logger-location holding capacity from trustworthy irrigation events.

    This is a sensor-response / holding-capacity summary, not a whole-strip
    water-balance calculation.

    Trustworthy sensor/event/depth rows are summarized using plateau VWC as the
    primary estimate of post-irrigation soil-water condition.
    """
    if trustworthy_table.empty:
        return pd.DataFrame()

    df = trustworthy_table.copy()

    df = df[
        df["trustworthy_event"].fillna(False)
    ].copy()

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
            df[col] = pd.to_numeric(
                df[col],
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

    group_cols = [
        c for c in group_cols
        if c in df.columns
    ]

    agg_dict = {
        "n_trustworthy_events": (
            "trustworthy_event",
            "size",
        ),
        "mean_bottom_response_delay_hr": (
            "bottom_response_delay_hr",
            "mean",
        ),
        "sd_bottom_response_delay_hr": (
            "bottom_response_delay_hr",
            "std",
        ),
        "mean_time_to_plateau_hours": (
            "time_to_plateau_hours",
            "mean",
        ),
        "sd_time_to_plateau_hours": (
            "time_to_plateau_hours",
            "std",
        ),
        "mean_event_duration_hours": (
            "event_duration_hours",
            "mean",
        ),
        "sd_event_duration_hours": (
            "event_duration_hours",
            "std",
        ),
        "mean_gallons_strip": (
            "gallons_strip",
            "mean",
        ),
        "sd_gallons_strip": (
            "gallons_strip",
            "std",
        ),
    }

    if "plateau_vwc" in df.columns:
        agg_dict.update(
            {
                "mean_plateau_vwc": (
                    "plateau_vwc",
                    "mean",
                ),
                "sd_plateau_vwc": (
                    "plateau_vwc",
                    "std",
                ),
                "min_plateau_vwc": (
                    "plateau_vwc",
                    "min",
                ),
                "max_plateau_vwc": (
                    "plateau_vwc",
                    "max",
                ),
            }
        )

    summary = (
        df.groupby(
            group_cols,
            dropna=False,
        )
        .agg(
            **cast(
                Any,
                agg_dict,
            )
        )
        .reset_index()
    )

    if {
        "mean_plateau_vwc",
        "sd_plateau_vwc",
    }.issubset(
        summary.columns
    ):
        summary["cv_plateau_vwc"] = (
            summary["sd_plateau_vwc"]
            / summary["mean_plateau_vwc"]
        )

        def capacity_confidence(
            row: pd.Series,
        ) -> str:
            n = row.get(
                "n_trustworthy_events"
            )

            sd = row.get(
                "sd_plateau_vwc"
            )

            if pd.isna(n) or pd.isna(sd):
                return "low"

            if n >= 3 and sd <= 3:
                return "high"

            if n >= 2 and sd <= 5:
                return "medium"

            return "low"

        summary[
            "capacity_confidence"
        ] = summary.apply(
            capacity_confidence,
            axis=1,
        )

    numeric_summary_cols = (
        summary
        .select_dtypes(
            include=["number"]
        )
        .columns
    )

    summary[
        numeric_summary_cols
    ] = summary[
        numeric_summary_cols
    ].round(2)

    return summary


def build_trustworthy_holding_capacity_summary(
    trustworthy_table: pd.DataFrame,
    event_results: pd.DataFrame,
) -> pd.DataFrame:
    """
    Summarize trustworthy bottom-logger irrigation responses by sensor depth.

    This table describes observed logger-scale holding-capacity behavior.
    It should not be interpreted as whole-strip stored-water volume.

    Whole-strip 0-18 inch storage is calculated separately from the three
    logger influence zones in ``build_event_storage_by_zone`` and
    ``build_event_storage_by_event``.
    """
    if (
        trustworthy_table.empty
        or event_results.empty
    ):
        return pd.DataFrame()

    trusted = trustworthy_table[
        trustworthy_table[
            "trustworthy_event"
        ].fillna(False)
    ].copy()

    if trusted.empty:
        return pd.DataFrame()

    merge_cols = [
        "year",
        "strip",
        "event_id",
        "sensor_col",
    ]

    trusted_event_results = (
        event_results.merge(
            trusted[
                merge_cols
            ].drop_duplicates(),
            on=merge_cols,
            how="inner",
        )
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

        # Legacy/logger-scale storage fields retained for diagnostic
        # compatibility. These must not be interpreted as whole-strip
        # storage.
        "profile_baseline_storage_gal",
        "profile_plateau_storage_gal",
        "event_storage_gal",
        "efficiency_strip",
        "estimated_loss_gal_strip",

        # New explicit layer-scale fields.
        "baseline_layer_storage_in",
        "plateau_layer_storage_in",
        "event_layer_storage_in",
        "baseline_layer_storage_gal_scaled",
        "plateau_layer_storage_gal_scaled",
        "event_layer_storage_gal_scaled",

        # Existing sensor-cylinder SWC fields.
        "baseline_swc_gal",
        "peak_swc_gal",
        "delta_swc_gal",
    ]

    for col in numeric_cols:
        if col in trusted_event_results.columns:
            trusted_event_results[
                col
            ] = pd.to_numeric(
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

    group_cols = [
        c for c in group_cols
        if c in trusted_event_results.columns
    ]

    agg_spec: dict[str, tuple[str, str]] = {
        "n_trustworthy_events": (
            "event_id",
            "nunique",
        ),

        "mean_bottom_response_delay_hr": (
            "bottom_response_delay_hr",
            "mean",
        ),
        "sd_bottom_response_delay_hr": (
            "bottom_response_delay_hr",
            "std",
        ),

        "mean_time_to_peak_hours": (
            "time_to_peak_hours",
            "mean",
        ),
        "sd_time_to_peak_hours": (
            "time_to_peak_hours",
            "std",
        ),

        "mean_time_to_plateau_hours": (
            "time_to_plateau_hours",
            "mean",
        ),
        "sd_time_to_plateau_hours": (
            "time_to_plateau_hours",
            "std",
        ),

        "mean_event_duration_hours": (
            "event_duration_hours",
            "mean",
        ),
        "sd_event_duration_hours": (
            "event_duration_hours",
            "std",
        ),

        "mean_gallons_strip": (
            "gallons_strip",
            "mean",
        ),
        "sd_gallons_strip": (
            "gallons_strip",
            "std",
        ),

        "mean_baseline_vwc": (
            "baseline_vwc",
            "mean",
        ),
        "sd_baseline_vwc": (
            "baseline_vwc",
            "std",
        ),

        "mean_peak_vwc": (
            "peak_vwc",
            "mean",
        ),
        "sd_peak_vwc": (
            "peak_vwc",
            "std",
        ),

        "mean_peak_increase": (
            "peak_increase",
            "mean",
        ),
        "sd_peak_increase": (
            "peak_increase",
            "std",
        ),

        "mean_plateau_vwc": (
            "plateau_vwc",
            "mean",
        ),
        "sd_plateau_vwc": (
            "plateau_vwc",
            "std",
        ),
        "min_plateau_vwc": (
            "plateau_vwc",
            "min",
        ),
        "max_plateau_vwc": (
            "plateau_vwc",
            "max",
        ),
    }

    optional_agg_specs = {
        "mean_event_layer_storage_in": (
            "event_layer_storage_in",
            "mean",
        ),
        "sd_event_layer_storage_in": (
            "event_layer_storage_in",
            "std",
        ),

        "mean_event_layer_storage_gal_scaled": (
            "event_layer_storage_gal_scaled",
            "mean",
        ),
        "sd_event_layer_storage_gal_scaled": (
            "event_layer_storage_gal_scaled",
            "std",
        ),

        "mean_sensor_scale_baseline_swc_gal": (
            "baseline_swc_gal",
            "mean",
        ),
        "mean_sensor_scale_peak_swc_gal": (
            "peak_swc_gal",
            "mean",
        ),
        "mean_sensor_scale_delta_swc_gal": (
            "delta_swc_gal",
            "mean",
        ),
    }

    for output_col, spec in (
        optional_agg_specs.items()
    ):
        source_col = spec[0]

        if (
            source_col
            in trusted_event_results.columns
        ):
            agg_spec[
                output_col
            ] = spec

    summary = (
        trusted_event_results
        .groupby(
            group_cols,
            dropna=False,
        )
        .agg(
            **cast(
                Any,
                agg_spec,
            )
        )
        .reset_index()
    )

    # These describe the standardized diagnostic profile area used by the
    # legacy/logger-scale calculations. They are not zone areas.
    summary[
        "diagnostic_profile_area_sqft"
    ] = PROFILE_AREA_SQFT

    summary[
        "diagnostic_profile_gallons_per_inch"
    ] = PROFILE_GALLONS_PER_INCH

    if (
        "mean_event_layer_storage_in"
        in summary.columns
    ):
        summary[
            "mean_event_layer_storage_gal_check"
        ] = (
            summary[
                "mean_event_layer_storage_in"
            ]
            * PROFILE_GALLONS_PER_INCH
        )

    numeric_out = (
        summary
        .select_dtypes(
            include=["number"]
        )
        .columns
    )

    summary[
        numeric_out
    ] = summary[
        numeric_out
    ].round(4)

    return summary


def add_scaled_storage_fields(
    results: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add explicit layer-scale storage fields to irrigation event results.

    Each sensor depth represents one non-overlapping soil layer whose thickness
    is defined in ``REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX``.

    Current field interpretation
    ----------------------------
        depth index 1 -> 0-6 inch layer
        depth index 2 -> 6-12 inch layer
        depth index 3 -> 12-18 inch layer

    The fields created here are logger/sensor-scale diagnostics.

    Whole-zone and whole-strip storage must instead be calculated with
    ``build_event_storage_by_zone`` and ``build_event_storage_by_event``,
    because those functions apply the actual logger influence-zone areas.

    Existing ETL SWC fields are retained separately and provide an independent
    sensor-scale check on the VWC-derived storage response.
    """
    if results.empty:
        return results.copy()

    out = results.copy()

    required_cols = {
        "baseline_vwc",
        "plateau_vwc",
        "depth_index",
    }

    missing = (
        required_cols
        - set(out.columns)
    )

    if missing:
        raise KeyError(
            "Cannot calculate layer storage because "
            f"required columns are missing: {sorted(missing)}"
        )

    baseline_vwc = pd.to_numeric(
        out["baseline_vwc"],
        errors="coerce",
    )

    plateau_vwc = pd.to_numeric(
        out["plateau_vwc"],
        errors="coerce",
    )

    depth_index = pd.to_numeric(
        out["depth_index"],
        errors="coerce",
    )

    out[
        "represented_layer_thickness_in"
    ] = depth_index.map(
        REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX
    )

    missing_layer_mask = (
        out[
            "represented_layer_thickness_in"
        ].isna()
        & depth_index.notna()
    )

    if missing_layer_mask.any():
        bad_depths = sorted(
            depth_index[
                missing_layer_mask
            ]
            .dropna()
            .unique()
            .tolist()
        )

        raise ValueError(
            "No represented soil-layer thickness is configured "
            f"for depth index value(s): {bad_depths}"
        )

    out[
        "diagnostic_profile_area_sqft"
    ] = PROFILE_AREA_SQFT

    out[
        "diagnostic_profile_gallons_per_inch"
    ] = (
        PROFILE_AREA_SQFT
        * INCHES_WATER_TO_GALLONS_PER_SQFT
    )

    layer_thickness = pd.to_numeric(
        out[
            "represented_layer_thickness_in"
        ],
        errors="coerce",
    )

    diagnostic_gallons_per_inch = (
        pd.to_numeric(
            out[
                "diagnostic_profile_gallons_per_inch"
            ],
            errors="coerce",
        )
    )

    # --------------------------------------------------------------
    # Water depth represented by this one sensor layer
    # --------------------------------------------------------------
    out[
        "baseline_layer_storage_in"
    ] = (
        baseline_vwc
        / 100.0
        * layer_thickness
    )

    out[
        "plateau_layer_storage_in"
    ] = (
        plateau_vwc
        / 100.0
        * layer_thickness
    )

    out[
        "event_layer_storage_in"
    ] = (
        out[
            "plateau_layer_storage_in"
        ]
        - out[
            "baseline_layer_storage_in"
        ]
    )

    # --------------------------------------------------------------
    # Diagnostic gallons using the standardized profile area
    #
    # These are NOT whole-zone gallons.
    # --------------------------------------------------------------
    out[
        "baseline_layer_storage_gal_scaled"
    ] = (
        out[
            "baseline_layer_storage_in"
        ]
        * diagnostic_gallons_per_inch
    )

    out[
        "plateau_layer_storage_gal_scaled"
    ] = (
        out[
            "plateau_layer_storage_in"
        ]
        * diagnostic_gallons_per_inch
    )

    out[
        "event_layer_storage_gal_scaled"
    ] = (
        out[
            "event_layer_storage_in"
        ]
        * diagnostic_gallons_per_inch
    )

    # --------------------------------------------------------------
    # Preserve legacy field names temporarily because existing debug and
    # reporting code still references them.
    #
    # IMPORTANT:
    # These aliases now describe ONE represented sensor layer, not a
    # cumulative 0-depth profile.
    # --------------------------------------------------------------
    out[
        "profile_area_sqft"
    ] = out[
        "diagnostic_profile_area_sqft"
    ]

    out[
        "gallons_per_profile_inch"
    ] = out[
        "diagnostic_profile_gallons_per_inch"
    ]

    out[
        "profile_baseline_storage_in"
    ] = out[
        "baseline_layer_storage_in"
    ]

    out[
        "profile_plateau_storage_in"
    ] = out[
        "plateau_layer_storage_in"
    ]

    out[
        "event_storage_in"
    ] = out[
        "event_layer_storage_in"
    ]

    out[
        "profile_baseline_storage_gal_scaled"
    ] = out[
        "baseline_layer_storage_gal_scaled"
    ]

    out[
        "profile_plateau_storage_gal_scaled"
    ] = out[
        "plateau_layer_storage_gal_scaled"
    ]

    out[
        "event_storage_gal_scaled"
    ] = out[
        "event_layer_storage_gal_scaled"
    ]

    # Do NOT calculate a strip-level surplus/runoff here.
    #
    # A single sensor layer cannot be compared with whole-strip applied
    # irrigation volume. Whole-strip water balance is calculated only after
    # top, middle, and bottom influence-zone storage has been combined.

    numeric_cols = (
        out
        .select_dtypes(
            include=["number"]
        )
        .columns
    )

    out[
        numeric_cols
    ] = out[
        numeric_cols
    ].round(4)

    return out
