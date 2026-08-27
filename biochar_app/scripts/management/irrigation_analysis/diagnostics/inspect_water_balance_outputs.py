#!/usr/bin/env python3
"""
inspect_water_balance_outputs.py

Run:
    python biochar_app/scripts/management/irrigation_analysis/diagnostics/inspect_water_balance_outputs.py

Purpose
-------
Inspect the outputs of the irrigation whole-strip water-balance analysis
without modifying the production analysis pipeline.

The script reads:

    data-processed/management/irrigation/analysis/holding_capacity/
    first_pass_water_balance_all_years.csv

and produces compact diagnostic summaries describing:

1. Overall 0-18 inch storage behavior
2. Unretained-water residuals
3. Bottom 6-inch irrigation-arrival timing
4. Strip-level differences
5. Top / middle / bottom zone contributions
6. Relationships with irrigation volume and flow rate
7. Events with the largest unexplained water-balance residuals
8. Basic internal consistency checks
9. Events where estimated storage exceeds applied irrigation
10. Events with incomplete three-zone coverage
11. Events with negative zone-storage values
12. Strip summaries restricted to complete three-zone events
13. Comparison of all-event vs complete-three-zone strip summaries

Important interpretation
------------------------
``unretained_gal_strip`` is NOT measured runoff.

It is the portion of applied irrigation not represented by increased
0-18 inch soil-water storage for an eligible event with complete three-zone
coverage. Bottom 6-inch response is reported separately as a timing
diagnostic.

That residual may also include:
- storage below 18 inches
- continuing infiltration
- lateral redistribution
- spatial sampling limitations
- measurement/model error

Outputs
-------
Written to:

    data-processed/management/irrigation/analysis/diagnostics/
    water_balance_inspection/

Files:
    water_balance_overall_summary.csv
    water_balance_strip_summary.csv
    water_balance_zone_summary.csv
    water_balance_arrival_summary.csv
    water_balance_correlations.csv
    water_balance_correlations_by_strip.csv
    water_balance_largest_residuals.csv
    water_balance_consistency_checks.csv
    water_balance_storage_exceeds_applied_events.csv
    water_balance_incomplete_zone_events.csv
    water_balance_negative_zone_storage_events.csv
    water_balance_complete_zone_strip_summary.csv
    water_balance_complete_vs_all_summary.csv
"""

from pathlib import Path

import numpy as np
import pandas as pd


# ----------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------

BIOCHAR_APP_DIR = Path(__file__).resolve().parents[4]

ANALYSIS_DIR = (
    BIOCHAR_APP_DIR
    / "data-processed"
    / "management"
    / "irrigation"
    / "analysis"
)

INPUT_CSV = (
    ANALYSIS_DIR
    / "holding_capacity"
    / "first_pass_water_balance_all_years.csv"
)

OUTPUT_DIR = (
    ANALYSIS_DIR
    / "diagnostics"
    / "water_balance_inspection"
)


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

ZONE_STORAGE_COLS = [
    "top_zone_storage_gal_0_18in",
    "middle_zone_storage_gal_0_18in",
    "bottom_zone_storage_gal_0_18in",
]

KEY_NUMERIC_COLS = [
    "gallons_strip",
    "event_duration_hours",
    "avg_flow_gph_strip",
    *ZONE_STORAGE_COLS,
    "n_zones_with_storage",
    "estimated_storage_gal_strip_0_18in",
    "water_balance_residual_gal_strip",
    "unretained_gal_strip",
    "estimated_storage_fraction_0_18in",
    "unretained_fraction",
    "bottom_6in_arrival_delay_hr",
    "post_bottom_6in_arrival_runtime_hr",
    "post_bottom_6in_arrival_applied_gal",
    "post_bottom_6in_arrival_applied_fraction",
    "estimated_storage_percent",
    "unretained_percent",
]

CORRELATION_COLS = [
    "gallons_strip",
    "avg_flow_gph_strip",
    "estimated_storage_gal_strip_0_18in",
    "estimated_storage_fraction_0_18in",
    "unretained_gal_strip",
    "unretained_fraction",
    "bottom_6in_arrival_delay_hr",
]

LARGEST_RESIDUAL_N = 20


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def require_columns(
    df: pd.DataFrame,
    columns: list[str],
    *,
    table_name: str,
) -> None:
    """Raise a clear error if required columns are missing."""

    missing = [
        col
        for col in columns
        if col not in df.columns
    ]

    if missing:
        raise KeyError(
            f"{table_name} is missing required column(s): "
            f"{missing}"
        )


def numeric_if_present(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Convert listed columns to numeric where present."""

    out = df.copy()

    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(
                out[col],
                errors="coerce",
            )

    return out


def round_numeric(
    df: pd.DataFrame,
    decimals: int = 2,
) -> pd.DataFrame:
    """Round numeric reporting columns."""

    out = df.copy()

    numeric_cols = out.select_dtypes(
        include=["number"]
    ).columns

    out[numeric_cols] = out[numeric_cols].round(
        decimals
    )

    return out


def print_section(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def bool_series_if_present(
    df: pd.DataFrame,
    column: str,
    *,
    default: bool = False,
) -> pd.Series:
    """
    Return a boolean Series for a column when present.

    Missing values are filled with ``default``. If the column is absent,
    a constant Series aligned to the dataframe index is returned.
    """

    if column not in df.columns:
        return pd.Series(
            default,
            index=df.index,
            dtype="bool",
        )

    return (
        df[column]
        .fillna(default)
        .astype(bool)
    )


# ----------------------------------------------------------------------
# Load data
# ----------------------------------------------------------------------

def load_water_balance() -> pd.DataFrame:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            "Water-balance input file not found:\n"
            f"{INPUT_CSV}"
        )

    df = pd.read_csv(
        INPUT_CSV,
        low_memory=False,
    )

    if df.empty:
        raise ValueError(
            f"Water-balance input is empty: {INPUT_CSV}"
        )

    require_columns(
        df,
        [
            "year",
            "strip",
            "event_id",
            "gallons_strip",
            "estimated_storage_gal_strip_0_18in",
            "unretained_gal_strip",
            "estimated_storage_fraction_0_18in",
        ],
        table_name="first_pass_water_balance_all_years.csv",
    )

    df = numeric_if_present(
        df,
        KEY_NUMERIC_COLS,
    )

    if "year" in df.columns:
        df["year"] = pd.to_numeric(
            df["year"],
            errors="coerce",
        ).astype("Int64")

    for col in [
        "irrigation_start",
        "irrigation_end",
        "bottom_6in_arrival_time",
    ]:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col],
                errors="coerce",
            )

    return df


# ----------------------------------------------------------------------
# Overall summary
# ----------------------------------------------------------------------

def build_overall_summary(
    df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    metrics = {
        "applied_irrigation_gal":
            "gallons_strip",

        "estimated_storage_gal_0_18in":
            "estimated_storage_gal_strip_0_18in",

        "estimated_storage_fraction_0_18in":
            "estimated_storage_fraction_0_18in",

        "unretained_gal":
            "unretained_gal_strip",

        "unretained_fraction":
            "unretained_fraction",

        "bottom_6in_arrival_delay_hr":
            "bottom_6in_arrival_delay_hr",

        "unretained_gal_strip":
            "unretained_gal_strip",

        "unretained_fraction":
            "unretained_fraction",
    }

    for metric_name, col in metrics.items():
        if col not in df.columns:
            continue

        s = pd.to_numeric(
            df[col],
            errors="coerce",
        ).dropna()

        if s.empty:
            continue

        rows.append(
            {
                "metric": metric_name,
                "n": int(s.count()),
                "mean": s.mean(),
                "median": s.median(),
                "min": s.min(),
                "p25": s.quantile(0.25),
                "p75": s.quantile(0.75),
                "max": s.max(),
                "std": s.std(),
            }
        )

    return round_numeric(
        pd.DataFrame(rows)
    )


# ----------------------------------------------------------------------
# Strip summary
# ----------------------------------------------------------------------

def build_strip_summary(
    df: pd.DataFrame,
) -> pd.DataFrame:
    agg_spec: dict[str, tuple[str, str]] = {
        "n_events": (
            "event_id",
            "nunique",
        ),
        "mean_applied_gal": (
            "gallons_strip",
            "mean",
        ),
        "mean_flow_gph": (
            "avg_flow_gph_strip",
            "mean",
        ),
        "mean_storage_gal_0_18in": (
            "estimated_storage_gal_strip_0_18in",
            "mean",
        ),
        "median_storage_gal_0_18in": (
            "estimated_storage_gal_strip_0_18in",
            "median",
        ),
        "mean_storage_fraction_0_18in": (
            "estimated_storage_fraction_0_18in",
            "mean",
        ),
        "mean_unretained_gal": (
            "unretained_gal_strip",
            "mean",
        ),
    }

    optional_specs = {
        "mean_unretained_gal_strip": (
            "unretained_gal_strip",
            "mean",
        ),
        "mean_unretained_fraction": (
            "unretained_fraction",
            "mean",
        ),
        "mean_bottom_6in_arrival_delay_hr": (
            "bottom_6in_arrival_delay_hr",
            "mean",
        ),
    }

    for output_col, spec in optional_specs.items():
        source_col = spec[0]

        if source_col in df.columns:
            agg_spec[output_col] = spec

    out = (
        df.groupby(
            "strip",
            dropna=False,
        )
        .agg(**agg_spec)
        .reset_index()
    )

    return round_numeric(out)


# ----------------------------------------------------------------------
# Zone summary
# ----------------------------------------------------------------------

def build_zone_summary(
    df: pd.DataFrame,
) -> pd.DataFrame:
    require_columns(
        df,
        ZONE_STORAGE_COLS,
        table_name="water balance",
    )

    rows: list[dict[str, object]] = []

    zone_map = {
        "top": "top_zone_storage_gal_0_18in",
        "middle": "middle_zone_storage_gal_0_18in",
        "bottom": "bottom_zone_storage_gal_0_18in",
    }

    for strip, sub in df.groupby(
        "strip",
        dropna=False,
    ):
        for zone, col in zone_map.items():
            values = pd.to_numeric(
                sub[col],
                errors="coerce",
            ).dropna()

            rows.append(
                {
                    "strip": strip,
                    "zone": zone,
                    "n_events": int(values.count()),
                    "mean_storage_gal_0_18in": (
                        values.mean()
                    ),
                    "median_storage_gal_0_18in": (
                        values.median()
                    ),
                    "min_storage_gal_0_18in": (
                        values.min()
                    ),
                    "max_storage_gal_0_18in": (
                        values.max()
                    ),
                    "sd_storage_gal_0_18in": (
                        values.std()
                    ),
                }
            )

    return round_numeric(
        pd.DataFrame(rows)
    )


# ----------------------------------------------------------------------
# Arrival summary
# ----------------------------------------------------------------------

def build_arrival_summary(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if "bottom_6in_arrival_delay_hr" not in df.columns:
        return pd.DataFrame()

    out = (
        df.groupby(
            "strip",
            dropna=False,
        )
        .agg(
            n_events=(
                "event_id",
                "nunique",
            ),
            n_bottom_arrivals=(
                "bottom_6in_arrival_delay_hr",
                "count",
            ),
            mean_bottom_arrival_hr=(
                "bottom_6in_arrival_delay_hr",
                "mean",
            ),
            median_bottom_arrival_hr=(
                "bottom_6in_arrival_delay_hr",
                "median",
            ),
            min_bottom_arrival_hr=(
                "bottom_6in_arrival_delay_hr",
                "min",
            ),
            max_bottom_arrival_hr=(
                "bottom_6in_arrival_delay_hr",
                "max",
            ),
        )
        .reset_index()
    )

    if (
        "bottom_6in_arrival_before_irrigation_end"
        in df.columns
    ):
        timing_counts = (
            df.groupby("strip")[
                "bottom_6in_arrival_before_irrigation_end"
            ]
            .sum()
            .rename(
                "n_arrivals_before_irrigation_end"
            )
            .reset_index()
        )

        out = out.merge(
            timing_counts,
            on="strip",
            how="left",
        )

    if (
        "bottom_6in_arrival_after_irrigation_end"
        in df.columns
    ):
        timing_counts = (
            df.groupby("strip")[
                "bottom_6in_arrival_after_irrigation_end"
            ]
            .sum()
            .rename(
                "n_arrivals_after_irrigation_end"
            )
            .reset_index()
        )

        out = out.merge(
            timing_counts,
            on="strip",
            how="left",
        )

    return round_numeric(out)


# ----------------------------------------------------------------------
# Correlations
# ----------------------------------------------------------------------

def build_correlation_table(
    df: pd.DataFrame,
) -> pd.DataFrame:
    cols = [
        col
        for col in CORRELATION_COLS
        if col in df.columns
    ]

    if len(cols) < 2:
        return pd.DataFrame()

    corr = (
        df[cols]
        .apply(
            pd.to_numeric,
            errors="coerce",
        )
        .corr()
    )

    out = (
        corr
        .stack()
        .rename("correlation")
        .reset_index()
        .rename(
            columns={
                "level_0": "variable_1",
                "level_1": "variable_2",
            }
        )
    )

    out = out[
        out["variable_1"]
        < out["variable_2"]
    ].copy()

    out["abs_correlation"] = (
        out["correlation"].abs()
    )

    out = out.sort_values(
        "abs_correlation",
        ascending=False,
    )

    return round_numeric(
        out,
        decimals=3,
    )


def build_correlations_by_strip(
    df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    pairs = [
        (
            "gallons_strip",
            "estimated_storage_gal_strip_0_18in",
        ),
        (
            "avg_flow_gph_strip",
            "estimated_storage_gal_strip_0_18in",
        ),
        (
            "avg_flow_gph_strip",
            "bottom_6in_arrival_delay_hr",
        ),
        (
            "gallons_strip",
            "unretained_gal_strip",
        ),
    ]

    for strip, sub in df.groupby(
        "strip",
        dropna=False,
    ):
        for x_col, y_col in pairs:
            if (
                x_col not in sub.columns
                or y_col not in sub.columns
            ):
                continue

            valid = sub[
                [x_col, y_col]
            ].apply(
                pd.to_numeric,
                errors="coerce",
            ).dropna()

            corr = np.nan

            if len(valid) >= 3:
                corr = valid[
                    x_col
                ].corr(
                    valid[y_col]
                )

            rows.append(
                {
                    "strip": strip,
                    "variable_1": x_col,
                    "variable_2": y_col,
                    "n_events": len(valid),
                    "correlation": corr,
                }
            )

    return round_numeric(
        pd.DataFrame(rows),
        decimals=3,
    )


# ----------------------------------------------------------------------
# Largest residual events
# ----------------------------------------------------------------------

def build_largest_residuals(
    df: pd.DataFrame,
) -> pd.DataFrame:
    keep_cols = [
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
        *ZONE_STORAGE_COLS,
        "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in",
        "unretained_gal_strip",
        "unretained_fraction",
        "bottom_6in_arrival_time",
        "bottom_6in_arrival_delay_hr",
        "bottom_6in_arrival_before_irrigation_end",
        "bottom_6in_arrival_after_irrigation_end",
        "unretained_gal_strip",
        "unretained_fraction",
    ]

    keep_cols = [
        col
        for col in keep_cols
        if col in df.columns
    ]

    out = (
        df[keep_cols]
        .sort_values(
            "unretained_gal_strip",
            ascending=False,
        )
        .head(
            LARGEST_RESIDUAL_N
        )
        .copy()
    )

    return round_numeric(out)


# ----------------------------------------------------------------------
# New focused QC tables
# ----------------------------------------------------------------------

def build_storage_exceeds_applied_events(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if "storage_exceeds_applied_water" not in df.columns:
        return pd.DataFrame()

    flag = bool_series_if_present(
        df,
        "storage_exceeds_applied_water",
    )

    out = df.loc[flag].copy()

    keep_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "irrigation_start",
        "irrigation_end",
        "gallons_strip",
        *ZONE_STORAGE_COLS,
        "n_zones_with_storage",
        "complete_three_zone_coverage",
        "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in",
        "water_balance_residual_gal_strip",
        "unretained_gal_strip",
    ]

    keep_cols = [
        col
        for col in keep_cols
        if col in out.columns
    ]

    return round_numeric(
        out[keep_cols].copy()
    )


def build_incomplete_zone_events(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if "complete_three_zone_coverage" not in df.columns:
        return pd.DataFrame()

    complete = bool_series_if_present(
        df,
        "complete_three_zone_coverage",
    )

    out = df.loc[~complete].copy()

    keep_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "irrigation_start",
        "irrigation_end",
        "gallons_strip",
        "n_zones_with_storage",
        "complete_three_zone_coverage",
        *ZONE_STORAGE_COLS,
        "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in",
        "unretained_gal_strip",
        "unretained_fraction",
    ]

    keep_cols = [
        col
        for col in keep_cols
        if col in out.columns
    ]

    return round_numeric(
        out[keep_cols].copy()
    )


def build_negative_zone_storage_events(
    df: pd.DataFrame,
) -> pd.DataFrame:
    require_columns(
        df,
        ZONE_STORAGE_COLS,
        table_name="water balance",
    )

    negative_mask = (
        df[ZONE_STORAGE_COLS]
        .apply(
            pd.to_numeric,
            errors="coerce",
        )
        .lt(0)
        .any(axis=1)
    )

    out = df.loc[
        negative_mask
    ].copy()

    keep_cols = [
        "year",
        "strip_group",
        "location",
        "strip",
        "event_id",
        "irrigation_start",
        "irrigation_end",
        "gallons_strip",
        *ZONE_STORAGE_COLS,
        "n_zones_with_storage",
        "complete_three_zone_coverage",
        "estimated_storage_gal_strip_0_18in",
        "estimated_storage_fraction_0_18in",
        "unretained_gal_strip",
    ]

    keep_cols = [
        col
        for col in keep_cols
        if col in out.columns
    ]

    return round_numeric(
        out[keep_cols].copy()
    )


def build_complete_zone_strip_summary(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if "complete_three_zone_coverage" not in df.columns:
        return pd.DataFrame()

    complete = bool_series_if_present(
        df,
        "complete_three_zone_coverage",
    )

    work = df.loc[
        complete
    ].copy()

    if work.empty:
        return pd.DataFrame()

    agg_spec: dict[str, tuple[str, str]] = {
        "n_events": (
            "event_id",
            "nunique",
        ),
        "mean_applied_gal": (
            "gallons_strip",
            "mean",
        ),
        "mean_storage_gal_0_18in": (
            "estimated_storage_gal_strip_0_18in",
            "mean",
        ),
        "median_storage_gal_0_18in": (
            "estimated_storage_gal_strip_0_18in",
            "median",
        ),
        "mean_storage_fraction_0_18in": (
            "estimated_storage_fraction_0_18in",
            "mean",
        ),
        "mean_unretained_gal": (
            "unretained_gal_strip",
            "mean",
        ),
    }

    optional_specs = {
        "mean_unretained_gal_strip": (
            "unretained_gal_strip",
            "mean",
        ),
        "mean_unretained_fraction": (
            "unretained_fraction",
            "mean",
        ),
        "mean_bottom_6in_arrival_delay_hr": (
            "bottom_6in_arrival_delay_hr",
            "mean",
        ),
    }

    for output_col, spec in optional_specs.items():
        source_col = spec[0]

        if source_col in work.columns:
            agg_spec[output_col] = spec

    out = (
        work.groupby(
            "strip",
            dropna=False,
        )
        .agg(**agg_spec)
        .reset_index()
    )

    return round_numeric(out)


def build_complete_vs_all_summary(
    all_summary: pd.DataFrame,
    complete_summary: pd.DataFrame,
) -> pd.DataFrame:
    if (
        all_summary.empty
        or complete_summary.empty
    ):
        return pd.DataFrame()

    all_keep = [
        "strip",
        "n_events",
        "mean_storage_gal_0_18in",
        "mean_storage_fraction_0_18in",
        "mean_unretained_gal",
    ]

    all_keep = [
        col
        for col in all_keep
        if col in all_summary.columns
    ]

    complete_keep = [
        "strip",
        "n_events",
        "mean_storage_gal_0_18in",
        "mean_storage_fraction_0_18in",
        "mean_unretained_gal",
    ]

    complete_keep = [
        col
        for col in complete_keep
        if col in complete_summary.columns
    ]

    left = all_summary[
        all_keep
    ].copy()

    right = complete_summary[
        complete_keep
    ].copy()

    left = left.rename(
        columns={
            "n_events":
                "all_events",
            "mean_storage_gal_0_18in":
                "all_mean_storage_gal_0_18in",
            "mean_storage_fraction_0_18in":
                "all_mean_storage_fraction_0_18in",
            "mean_unretained_gal":
                "all_mean_unretained_gal",
        }
    )

    right = right.rename(
        columns={
            "n_events":
                "complete_events",
            "mean_storage_gal_0_18in":
                "complete_mean_storage_gal_0_18in",
            "mean_storage_fraction_0_18in":
                "complete_mean_storage_fraction_0_18in",
            "mean_unretained_gal":
                "complete_mean_unretained_gal",
        }
    )

    out = left.merge(
        right,
        on="strip",
        how="left",
    )

    if {
        "all_mean_storage_fraction_0_18in",
        "complete_mean_storage_fraction_0_18in",
    }.issubset(out.columns):
        out[
            "storage_fraction_difference_complete_minus_all"
        ] = (
            out[
                "complete_mean_storage_fraction_0_18in"
            ]
            - out[
                "all_mean_storage_fraction_0_18in"
            ]
        )

    if {
        "all_mean_storage_gal_0_18in",
        "complete_mean_storage_gal_0_18in",
    }.issubset(out.columns):
        out[
            "storage_gal_difference_complete_minus_all"
        ] = (
            out[
                "complete_mean_storage_gal_0_18in"
            ]
            - out[
                "all_mean_storage_gal_0_18in"
            ]
        )

    return round_numeric(out)


# ----------------------------------------------------------------------
# Internal consistency checks
# ----------------------------------------------------------------------

def build_consistency_checks(
    df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    if all(
        col in df.columns
        for col in ZONE_STORAGE_COLS
    ):
        zone_sum = df[
            ZONE_STORAGE_COLS
        ].sum(
            axis=1,
            min_count=1,
        )

        reported = pd.to_numeric(
            df[
                "estimated_storage_gal_strip_0_18in"
            ],
            errors="coerce",
        )

        difference = zone_sum - reported

        rows.append(
            {
                "check": (
                    "zone_sum_equals_reported_strip_storage"
                ),
                "n_rows": len(df),
                "n_failed": int(
                    (
                        difference.abs()
                        > 1.0
                    ).sum()
                ),
                "max_abs_difference": (
                    difference.abs().max()
                ),
            }
        )

    if (
        "storage_exceeds_applied_water"
        in df.columns
    ):
        flag = bool_series_if_present(
            df,
            "storage_exceeds_applied_water",
        )

        rows.append(
            {
                "check": (
                    "storage_does_not_exceed_applied_water"
                ),
                "n_rows": len(df),
                "n_failed": int(
                    flag.sum()
                ),
                "max_abs_difference": pd.NA,
            }
        )

    if (
        "complete_three_zone_coverage"
        in df.columns
    ):
        complete = bool_series_if_present(
            df,
            "complete_three_zone_coverage",
        )

        rows.append(
            {
                "check": (
                    "complete_three_zone_coverage"
                ),
                "n_rows": len(df),
                "n_failed": int(
                    (~complete).sum()
                ),
                "max_abs_difference": pd.NA,
            }
        )

    return round_numeric(
        pd.DataFrame(rows)
    )


# ----------------------------------------------------------------------
# Reporting
# ----------------------------------------------------------------------

def write_csv(
    df: pd.DataFrame,
    filename: str,
) -> None:
    path = OUTPUT_DIR / filename

    df.to_csv(
        path,
        index=False,
    )

    print(
        f"Wrote {len(df):,} rows: {path}"
    )


def main() -> None:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    df = load_water_balance()

    print_section(
        "WATER BALANCE INPUT"
    )

    print(f"Input: {INPUT_CSV}")
    print(f"Rows: {len(df):,}")

    if "year" in df.columns:
        print(
            "Years:",
            sorted(
                df["year"]
                .dropna()
                .unique()
                .tolist()
            ),
        )

    if "strip" in df.columns:
        print(
            "Events by strip:"
        )
        print(
            df.groupby("strip")
            .size()
            .rename("n_events")
            .to_string()
        )

    # ------------------------------------------------------------------
    # Build summaries
    # ------------------------------------------------------------------
    overall = build_overall_summary(
        df
    )

    strip_summary = build_strip_summary(
        df
    )

    zone_summary = build_zone_summary(
        df
    )

    arrival_summary = build_arrival_summary(
        df
    )

    correlations = build_correlation_table(
        df
    )

    correlations_by_strip = (
        build_correlations_by_strip(
            df
        )
    )

    largest_residuals = (
        build_largest_residuals(
            df
        )
    )

    storage_exceeds = (
        build_storage_exceeds_applied_events(
            df
        )
    )

    incomplete_zone_events = (
        build_incomplete_zone_events(
            df
        )
    )

    negative_zone_events = (
        build_negative_zone_storage_events(
            df
        )
    )

    complete_zone_strip_summary = (
        build_complete_zone_strip_summary(
            df
        )
    )

    complete_vs_all = (
        build_complete_vs_all_summary(
            strip_summary,
            complete_zone_strip_summary,
        )
    )

    consistency = build_consistency_checks(
        df
    )

    # ------------------------------------------------------------------
    # Quick QC
    # ------------------------------------------------------------------
    print_section(
        "QUICK QC"
    )

    complete_mask = bool_series_if_present(
        df,
        "complete_three_zone_coverage",
    )

    bottom_arrival_count = (
        int(
            pd.to_numeric(
                df.get(
                    "bottom_6in_arrival_delay_hr",
                    pd.Series(
                        np.nan,
                        index=df.index,
                    ),
                ),
                errors="coerce",
            )
            .notna()
            .sum()
        )
    )

    print(
        f"Events ....................................... "
        f"{len(df):,}"
    )

    print(
        f"Complete three-zone events ................... "
        f"{int(complete_mask.sum()):,}"
    )

    print(
        f"Incomplete three-zone events ................. "
        f"{len(incomplete_zone_events):,}"
    )

    print(
        f"Negative-zone events ......................... "
        f"{len(negative_zone_events):,}"
    )

    print(
        f"Storage > applied ............................ "
        f"{len(storage_exceeds):,}"
    )

    print(
        f"Bottom 6-in arrival observed ................. "
        f"{bottom_arrival_count:,}"
    )

    print(
        f"Missing bottom 6-in arrival .................. "
        f"{len(df) - bottom_arrival_count:,}"
    )

    # ------------------------------------------------------------------
    # Console summaries
    # ------------------------------------------------------------------
    print_section(
        "OVERALL WATER-BALANCE SUMMARY"
    )
    print(
        overall.to_string(
            index=False
        )
    )

    print_section(
        "STRIP SUMMARY"
    )
    print(
        strip_summary.to_string(
            index=False
        )
    )

    print_section(
        "ZONE STORAGE SUMMARY"
    )
    print(
        zone_summary.to_string(
            index=False
        )
    )

    print_section(
        "BOTTOM 6-INCH ARRIVAL SUMMARY"
    )
    print(
        arrival_summary.to_string(
            index=False
        )
    )

    print_section(
        "STRONGEST OVERALL CORRELATIONS"
    )

    if correlations.empty:
        print(
            "No correlations available."
        )
    else:
        print(
            correlations
            .head(15)
            .to_string(
                index=False
            )
        )

    print_section(
        f"LARGEST {LARGEST_RESIDUAL_N} "
        "WATER-BALANCE RESIDUALS"
    )

    print(
        largest_residuals.to_string(
            index=False
        )
    )

    print_section(
        "STORAGE EXCEEDS APPLIED WATER"
    )

    if storage_exceeds.empty:
        print(
            "None."
        )
    else:
        print(
            storage_exceeds.to_string(
                index=False
            )
        )

    print_section(
        "INCOMPLETE THREE-ZONE COVERAGE"
    )

    if incomplete_zone_events.empty:
        print(
            "None."
        )
    else:
        print(
            incomplete_zone_events.to_string(
                index=False
            )
        )

    print_section(
        "NEGATIVE ZONE STORAGE EVENTS"
    )

    if negative_zone_events.empty:
        print(
            "None."
        )
    else:
        print(
            negative_zone_events.to_string(
                index=False
            )
        )

    print_section(
        "COMPLETE THREE-ZONE EVENTS ONLY"
    )

    if complete_zone_strip_summary.empty:
        print(
            "No complete three-zone events available."
        )
    else:
        print(
            complete_zone_strip_summary.to_string(
                index=False
            )
        )

    print_section(
        "ALL EVENTS VS COMPLETE THREE-ZONE EVENTS"
    )

    if complete_vs_all.empty:
        print(
            "Comparison unavailable."
        )
    else:
        print(
            complete_vs_all.to_string(
                index=False
            )
        )

    print_section(
        "INTERNAL CONSISTENCY CHECKS"
    )

    print(
        consistency.to_string(
            index=False
        )
    )

    # ------------------------------------------------------------------
    # Write diagnostic CSVs
    # ------------------------------------------------------------------
    print_section(
        "WRITING DIAGNOSTIC OUTPUTS"
    )

    write_csv(
        overall,
        "water_balance_overall_summary.csv",
    )

    write_csv(
        strip_summary,
        "water_balance_strip_summary.csv",
    )

    write_csv(
        zone_summary,
        "water_balance_zone_summary.csv",
    )

    write_csv(
        arrival_summary,
        "water_balance_arrival_summary.csv",
    )

    write_csv(
        correlations,
        "water_balance_correlations.csv",
    )

    write_csv(
        correlations_by_strip,
        "water_balance_correlations_by_strip.csv",
    )

    write_csv(
        largest_residuals,
        "water_balance_largest_residuals.csv",
    )

    write_csv(
        consistency,
        "water_balance_consistency_checks.csv",
    )

    write_csv(
        storage_exceeds,
        "water_balance_storage_exceeds_applied_events.csv",
    )

    write_csv(
        incomplete_zone_events,
        "water_balance_incomplete_zone_events.csv",
    )

    write_csv(
        negative_zone_events,
        "water_balance_negative_zone_storage_events.csv",
    )

    write_csv(
        complete_zone_strip_summary,
        "water_balance_complete_zone_strip_summary.csv",
    )

    write_csv(
        complete_vs_all,
        "water_balance_complete_vs_all_summary.csv",
    )

    print_section(
        "WATER BALANCE INSPECTION COMPLETE"
    )

    print(
        f"Output directory:\n{OUTPUT_DIR}"
    )


if __name__ == "__main__":
    main()
