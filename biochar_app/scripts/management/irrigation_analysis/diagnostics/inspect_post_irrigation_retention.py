#!/usr/bin/env python3
"""
inspect_post_irrigation_retention.py -- version 2

Diagnostic analysis for Phase 2 of the irrigation project.

Purpose
-------
Phase 1 measures irrigation-event storage response and potential surplus water.

Phase 2 asks a different question:

    Does biochar help retain water in the 0-18 inch soil profile
    during the drying period between irrigation events?

This script is intentionally diagnostic. It does not modify the production
irrigation-analysis pipeline.

For each irrigation interval and strip, the script:

1. Loads continuous 15-minute VWC logger data.
2. Computes actual 0-18 inch equivalent soil-water depth for each logger
   position from the three CS650 VWC sensors.
3. Averages valid T/M/B logger profiles to obtain a strip-level profile-water
   time series.
4. Follows that time series from irrigation end until the next irrigation.
5. Excludes intervals that cross calendar years or exceed 45 days.  This
   conservative rule removes cross-season/winter gaps without inventing crop
   phenology dates that are not present in the irrigation record.
6. Defines a redistributed post-irrigation reference state as the median
   profile water from 24 through 36 hours after irrigation end.  This robust
   window is late enough for the initial wetting front to redistribute and is
   less sensitive than a single timestamp to logger noise or timing offsets.
7. Reports absolute water at standardized checkpoints:
       +24 hours
       +72 hours
       +7 days
       halfway to next irrigation
       immediately before next irrigation
8. Accumulates processed 15-minute CoAgMet precipitation from irrigation end
   to 24 hours, 72 hours, 7 days, half interval, and next irrigation.
9. Calculates:
       change from the redistributed reference state
       fraction of reference water remaining
       drying/depletion rate
       area under the storage curve
10. Writes diagnostic CSV files and compact matched-pair summaries.

Important interpretation
------------------------
This script measures actual water present in the upper 18 inches of soil.

It is NOT the same quantity as Phase 1 event storage:

    plateau storage - pre-irrigation storage

The Phase 2 quantity is intended to characterize persistence of soil water
between irrigations.

Initial implementation
----------------------
The script uses VWC directly:

    water_depth_in_layer =
        VWC_percent / 100 * layer_thickness_inches

Each sensor represents a nominal six-inch soil layer.

Therefore:

    profile_water_in_0_18in =
        6-in layer water
        + 12-in layer water
        + 18-in layer water

A logger profile is considered valid only when all three depth measurements
are available.

The strip profile is initially calculated as the mean of all valid T/M/B
logger profiles at a timestamp.

This equal-position averaging is deliberate for the diagnostic stage.
If the trajectories look scientifically reasonable, a later production
version can use explicit spatial-zone weighting.

Run
---
From the project root:

    python -u \
    biochar_app/scripts/management/irrigation_analysis/diagnostics/inspect_post_irrigation_retention.py

Outputs
-------
Written under:

    data-processed/management/irrigation/analysis/diagnostics/
    post_irrigation_retention/

Files:

    post_irrigation_retention_long.csv
    post_irrigation_retention_checkpoints.csv
    post_irrigation_retention_event_summary.csv
    post_irrigation_retention_pair_summary.csv
    post_irrigation_retention_excluded_intervals.csv
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd


# ======================================================================
# Paths
# ======================================================================

BIOCHAR_APP_DIR: Final[Path] = Path(__file__).resolve().parents[4]

DATA_PROCESSED_DIR: Final[Path] = (
    BIOCHAR_APP_DIR
    / "data-processed"
)

PARQUET_DIR: Final[Path] = (
    DATA_PROCESSED_DIR
    / "parquet"
)

IRRIGATION_DIR: Final[Path] = (
    DATA_PROCESSED_DIR
    / "management"
    / "irrigation"
)

IRRIGATION_CSV: Final[Path] = (
    IRRIGATION_DIR
    / "irrigation_clean.csv"
)

OUTPUT_DIR: Final[Path] = (
    IRRIGATION_DIR
    / "analysis"
    / "diagnostics"
    / "post_irrigation_retention"
)


# ======================================================================
# Configuration
# ======================================================================

YEARS: Final[tuple[int, ...]] = (
    2023,
    2024,
    2025,
    2026,
)

STRIPS: Final[tuple[str, ...]] = (
    "S1",
    "S2",
    "S3",
    "S4",
)

LOGGER_POSITIONS: Final[tuple[str, ...]] = (
    "T",
    "M",
    "B",
)

DEPTH_INDEX_TO_INCHES: Final[dict[int, int]] = {
    1: 6,
    2: 12,
    3: 18,
}

LAYER_THICKNESS_IN: Final[float] = 6.0

CHECKPOINT_HOURS: Final[dict[str, float]] = {
    "24h": 24.0,
    "72h": 72.0,
    "7d": 24.0 * 7.0,
}

# A conservative season boundary.  Intervals must also begin and end in the
# same calendar year.  Together these rules remove winter/cross-season gaps.
MAX_INTERVAL_DAYS: Final[float] = 45.0

# Post-wetting redistribution reference.  The median over a window is more
# robust than using irrigation_end or one nominal timestamp.
REFERENCE_WINDOW_START_HOURS: Final[float] = 24.0
REFERENCE_WINDOW_END_HOURS: Final[float] = 36.0
MIN_REFERENCE_OBSERVATIONS: Final[int] = 8

# Maximum timestamp separation allowed when selecting a value for a
# standardized checkpoint.
CHECKPOINT_TOLERANCE_MINUTES: Final[float] = 45.0

# For "pre-next-irrigation", use a point shortly before the next irrigation
# begins rather than one exactly on the boundary.
PRE_NEXT_OFFSET_MINUTES: Final[float] = 15.0

# Require at least this many logger positions at a timestamp before calculating
# the strip-level average.
MIN_VALID_LOGGER_PROFILES: Final[int] = 2


# ======================================================================
# General helpers
# ======================================================================

def print_section(title: str) -> None:
    print()
    print("=" * 88)
    print(title)
    print("=" * 88)


def require_columns(
    df: pd.DataFrame,
    columns: list[str],
    *,
    table_name: str,
) -> None:
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


def first_existing_column(
    df: pd.DataFrame,
    candidates: list[str],
) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col

    return None


def write_csv(
    df: pd.DataFrame,
    filename: str,
) -> None:
    path = OUTPUT_DIR / filename

    df.to_csv(
        path,
        index=False,
        float_format="%.4f",
    )

    print(
        f"Wrote {len(df):,} rows: {path}"
    )


# ======================================================================
# Irrigation-event loading
# ======================================================================

def load_irrigation_events() -> pd.DataFrame:
    if not IRRIGATION_CSV.exists():
        raise FileNotFoundError(
            f"Irrigation file not found:\n{IRRIGATION_CSV}"
        )

    df = pd.read_csv(
        IRRIGATION_CSV,
        low_memory=False,
    )

    if df.empty:
        raise ValueError(
            f"Irrigation file is empty:\n{IRRIGATION_CSV}"
        )

    start_col = first_existing_column(
        df,
        [
            "irrigation_start",
            "start_timestamp",
            "start_datetime",
            "start_time",
        ],
    )

    end_col = first_existing_column(
        df,
        [
            "irrigation_end",
            "end_timestamp",
            "end_datetime",
            "end_time",
        ],
    )

    if start_col is None or end_col is None:
        raise KeyError(
            "Could not identify irrigation start/end columns.\n"
            f"Columns found:\n{df.columns.tolist()}"
        )

    strip_group_col = first_existing_column(
        df,
        [
            "strip_group",
            "group",
            "irrigation_group",
        ],
    )

    if strip_group_col is None:
        raise KeyError(
            "Could not identify strip-group column.\n"
            f"Columns found:\n{df.columns.tolist()}"
        )

    strip_col = first_existing_column(
        df,
        ["strip", "Strip"],
    )

    out = df.copy()

    out["irrigation_start"] = pd.to_datetime(
        out[start_col],
        errors="coerce",
    )

    out["irrigation_end"] = pd.to_datetime(
        out[end_col],
        errors="coerce",
    )

    out["strip_group"] = (
        out[strip_group_col]
        .astype(str)
        .str.strip()
    )

    out = out[
        out["irrigation_start"].notna()
        & out["irrigation_end"].notna()
    ].copy()

    out["year"] = (
        out["irrigation_start"]
        .dt.year
        .astype("Int64")
    )

    out = out[
        out["year"].isin(YEARS)
    ].copy()

    # One irrigation event becomes one row per strip.
    rows: list[dict[str, object]] = []

    for _, row in out.iterrows():
        strip_group = str(
            row["strip_group"]
        ).strip()

        row_strip = (
            str(row[strip_col]).strip()
            if strip_col is not None and pd.notna(row[strip_col])
            else ""
        )

        # The canonical clean file is already one row per strip.  Retain the
        # group-expansion fallback for older group-level irrigation files.
        if row_strip in STRIPS:
            event_strips = (row_strip,)
        elif strip_group == "S1_S2":
            event_strips = ("S1", "S2")
        elif strip_group == "S3_S4":
            event_strips = ("S3", "S4")
        else:
            continue

        irrigation_start = row[
            "irrigation_start"
        ]

        irrigation_end = row[
            "irrigation_end"
        ]

        event_id = row.get(
            "event_id",
            pd.NA,
        )

        if pd.isna(event_id):
            event_id = (
                f"{irrigation_start:%Y-%m-%d}_"
                f"{strip_group}"
            )

        for strip in event_strips:
            rows.append(
                {
                    "year": int(
                        irrigation_start.year
                    ),
                    "strip_group": strip_group,
                    "strip": strip,
                    "event_id": str(
                        event_id
                    ),
                    "irrigation_start": (
                        irrigation_start
                    ),
                    "irrigation_end": (
                        irrigation_end
                    ),
                }
            )

    events = pd.DataFrame(
        rows
    )

    if events.empty:
        raise ValueError(
            "No usable irrigation events were found."
        )

    events = (
        events
        .sort_values(
            [
                "strip",
                "irrigation_start",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    # Determine the next irrigation for each strip.
    events["next_irrigation_start"] = (
        events
        .groupby(
            "strip"
        )[
            "irrigation_start"
        ]
        .shift(-1)
    )

    events[
        "hours_to_next_irrigation"
    ] = (
        (
            events[
                "next_irrigation_start"
            ]
            - events[
                "irrigation_end"
            ]
        )
        .dt.total_seconds()
        / 3600.0
    )

    interval_days = events["hours_to_next_irrigation"] / 24.0
    same_year = (
        events["next_irrigation_start"].dt.year
        == events["irrigation_end"].dt.year
    )
    events["retention_interval_eligible"] = (
        events["next_irrigation_start"].notna()
        & same_year
        & interval_days.gt(0.0)
        & interval_days.le(MAX_INTERVAL_DAYS)
    )
    events["retention_exclusion_reason"] = np.select(
        [
            events["next_irrigation_start"].isna(),
            ~same_year.fillna(False),
            interval_days.le(0.0),
            interval_days.gt(MAX_INTERVAL_DAYS),
        ],
        [
            "no_next_irrigation",
            "cross_calendar_year",
            "nonpositive_interval",
            f"interval_over_{MAX_INTERVAL_DAYS:g}_days",
        ],
        default="",
    )

    return events


# ======================================================================
# Parquet loading
# ======================================================================

def find_raw_logger_parquet(
    year: int,
) -> Path:
    """
    Locate the raw logger parquet for one year.

    Expected canonical naming is something like:

        2025_raw_logger.parquet

    The recursive fallback makes the diagnostic tolerant of minor directory
    changes while avoiding ratio files.
    """

    preferred = [
        PARQUET_DIR
        / f"{year}_raw_logger.parquet",
        PARQUET_DIR
        / "raw"
        / f"{year}_raw_logger.parquet",
    ]

    for path in preferred:
        if path.exists():
            return path

    candidates = [
        path
        for path in PARQUET_DIR.rglob(
            f"*{year}*raw*logger*.parquet"
        )
        if "ratio" not in path.name.lower()
    ]

    if not candidates:
        raise FileNotFoundError(
            "Could not locate raw logger parquet "
            f"for {year} under:\n{PARQUET_DIR}"
        )

    candidates = sorted(
        candidates,
        key=lambda p: (
            len(p.parts),
            len(p.name),
        ),
    )

    return candidates[0]


def load_logger_year(
    year: int,
) -> pd.DataFrame:
    path = find_raw_logger_parquet(
        year
    )

    print(
        f"Loading {year}: {path}"
    )

    df = pd.read_parquet(
        path
    )

    if df.empty:
        raise ValueError(
            f"Logger parquet is empty: {path}"
        )

    if isinstance(
        df.index,
        pd.DatetimeIndex,
    ):
        out = df.copy()

        out["timestamp"] = (
            out.index
        )

    else:
        timestamp_col = first_existing_column(
            df,
            [
                "timestamp",
                "TIMESTAMP",
                "datetime",
                "DateTime",
            ],
        )

        if timestamp_col is None:
            raise KeyError(
                "Could not identify logger timestamp "
                f"column in {path}."
            )

        out = df.copy()

        out["timestamp"] = pd.to_datetime(
            out[timestamp_col],
            errors="coerce",
        )

    out["timestamp"] = pd.to_datetime(
        out["timestamp"],
        errors="coerce",
    )

    out = out[
        out["timestamp"].notna()
    ].copy()

    out = (
        out
        .sort_values(
            "timestamp"
        )
        .drop_duplicates(
            subset="timestamp",
            keep="last",
        )
        .reset_index(
            drop=True
        )
    )

    return out


def find_weather_parquet(year: int) -> Path:
    """Locate the canonical processed 15-minute CoAgMet parquet."""
    preferred = (
        PARQUET_DIR / "summary" / "weather" / "15min"
        / f"{year}_15min.parquet"
    )
    if preferred.exists():
        return preferred

    candidates = sorted(
        PARQUET_DIR.rglob(f"*weather*/*{year}*15min*.parquet")
    )
    if not candidates:
        raise FileNotFoundError(
            f"Could not locate processed 15-minute CoAgMet data for {year} "
            f"under:\n{PARQUET_DIR}"
        )
    return candidates[0]


def load_weather_year(year: int) -> pd.DataFrame:
    """Load cleaned precipitation increments; do not fetch external data."""
    path = find_weather_parquet(year)
    weather = pd.read_parquet(path)
    if "timestamp" not in weather.columns:
        weather = weather.reset_index()
    require_columns(
        weather,
        ["timestamp", "precip_in"],
        table_name=str(path),
    )
    out = weather[["timestamp", "precip_in"]].copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out["precip_in"] = pd.to_numeric(out["precip_in"], errors="coerce")
    out.loc[out["precip_in"].lt(0), "precip_in"] = np.nan
    return (
        out.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .drop_duplicates("timestamp", keep="last")
        .reset_index(drop=True)
    )


def accumulated_precipitation(
    weather_df: pd.DataFrame,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> float:
    """Sum CoAgMet precipitation increments in the half-open [start, end)."""
    if weather_df.empty or pd.isna(start) or pd.isna(end) or end <= start:
        return np.nan
    window = weather_df.loc[
        weather_df["timestamp"].ge(start)
        & weather_df["timestamp"].lt(end),
        "precip_in",
    ]
    return float(window.sum(min_count=1)) if not window.empty else np.nan


# ======================================================================
# Profile-water calculation
# ======================================================================

def build_logger_profile_water(
    logger_df: pd.DataFrame,
    *,
    strip: str,
    logger_position: str,
) -> pd.DataFrame:
    """
    Calculate actual 0-18 inch profile water for one logger.

    The raw VWC columns are expected to be:

        VWC_1_raw_S1_T
        VWC_2_raw_S1_T
        VWC_3_raw_S1_T

    where depth indices 1, 2, 3 correspond to 6, 12, 18 inches.
    """

    vwc_cols = {
        depth_index: (
            f"VWC_{depth_index}_raw_"
            f"{strip}_{logger_position}"
        )
        for depth_index in (
            1,
            2,
            3,
        )
    }

    missing = [
        col
        for col in vwc_cols.values()
        if col not in logger_df.columns
    ]

    if missing:
        return pd.DataFrame()

    out = logger_df[
        [
            "timestamp",
            *vwc_cols.values(),
        ]
    ].copy()

    for col in vwc_cols.values():
        out[col] = pd.to_numeric(
            out[col],
            errors="coerce",
        )

    # Physical plausibility screen.
    for col in vwc_cols.values():
        out.loc[
            (
                out[col] < 0
            )
            | (
                out[col] > 100
            ),
            col,
        ] = np.nan

    out[
        "n_valid_depths"
    ] = (
        out[
            list(
                vwc_cols.values()
            )
        ]
        .notna()
        .sum(
            axis=1
        )
    )

    # Require all three depth measurements.
    complete = (
        out[
            "n_valid_depths"
        ]
        .eq(3)
    )

    profile_water = pd.Series(
        np.nan,
        index=out.index,
        dtype=float,
    )

    profile_water.loc[
        complete
    ] = (
        (
            out.loc[
                complete,
                list(
                    vwc_cols.values()
                ),
            ]
            / 100.0
        )
        * LAYER_THICKNESS_IN
    ).sum(
        axis=1
    )

    out[
        "profile_water_in_0_18in"
    ] = profile_water

    out[
        "strip"
    ] = strip

    out[
        "logger_position"
    ] = logger_position

    keep_cols = [
        "timestamp",
        "strip",
        "logger_position",
        "n_valid_depths",
        "profile_water_in_0_18in",
    ]

    return out[
        keep_cols
    ]


def build_strip_profile_water(
    logger_df: pd.DataFrame,
    *,
    strip: str,
) -> pd.DataFrame:
    """
    Build strip-level actual 0-18 inch soil-water storage.

    For this first diagnostic version, valid T/M/B logger profile-water values
    are averaged equally.
    """

    frames: list[
        pd.DataFrame
    ] = []

    for logger_position in (
        LOGGER_POSITIONS
    ):
        profile = (
            build_logger_profile_water(
                logger_df,
                strip=strip,
                logger_position=(
                    logger_position
                ),
            )
        )

        if not profile.empty:
            frames.append(
                profile
            )

    if not frames:
        return pd.DataFrame()

    long = pd.concat(
        frames,
        ignore_index=True,
    )

    pivot = (
        long
        .pivot_table(
            index="timestamp",
            columns="logger_position",
            values=(
                "profile_water_in_0_18in"
            ),
            aggfunc="mean",
        )
        .reset_index()
    )

    for position in (
        LOGGER_POSITIONS
    ):
        if position not in pivot.columns:
            pivot[position] = np.nan

    pivot[
        "n_valid_logger_profiles"
    ] = (
        pivot[
            list(
                LOGGER_POSITIONS
            )
        ]
        .notna()
        .sum(
            axis=1
        )
    )

    pivot[
        "strip_profile_water_in_0_18in"
    ] = (
        pivot[
            list(
                LOGGER_POSITIONS
            )
        ]
        .mean(
            axis=1,
            skipna=True,
        )
    )

    pivot.loc[
        pivot[
            "n_valid_logger_profiles"
        ]
        < MIN_VALID_LOGGER_PROFILES,
        "strip_profile_water_in_0_18in",
    ] = np.nan

    pivot[
        "strip"
    ] = strip

    pivot = pivot.rename(
        columns={
            "T": "top_profile_water_in",
            "M": "middle_profile_water_in",
            "B": "bottom_profile_water_in",
        }
    )

    return pivot[
        [
            "timestamp",
            "strip",
            "top_profile_water_in",
            "middle_profile_water_in",
            "bottom_profile_water_in",
            "n_valid_logger_profiles",
            "strip_profile_water_in_0_18in",
        ]
    ]


# ======================================================================
# Event-interval extraction
# ======================================================================

def extract_event_retention_series(
    profile_df: pd.DataFrame,
    event: pd.Series,
) -> pd.DataFrame:
    irrigation_end = event[
        "irrigation_end"
    ]

    next_irrigation_start = event[
        "next_irrigation_start"
    ]

    # We need a real drying interval.
    if pd.isna(
        next_irrigation_start
    ):
        return pd.DataFrame()

    if (
        next_irrigation_start
        <= irrigation_end
    ):
        return pd.DataFrame()

    sub = profile_df[
        (
            profile_df[
                "timestamp"
            ]
            >= irrigation_end
        )
        & (
            profile_df[
                "timestamp"
            ]
            < next_irrigation_start
        )
    ].copy()

    if sub.empty:
        return pd.DataFrame()

    sub[
        "year"
    ] = int(
        event["year"]
    )

    sub[
        "strip_group"
    ] = event[
        "strip_group"
    ]

    sub[
        "event_id"
    ] = event[
        "event_id"
    ]

    sub[
        "irrigation_start"
    ] = event[
        "irrigation_start"
    ]

    sub[
        "irrigation_end"
    ] = irrigation_end

    sub[
        "next_irrigation_start"
    ] = next_irrigation_start

    sub[
        "hours_since_irrigation_end"
    ] = (
        (
            sub[
                "timestamp"
            ]
            - irrigation_end
        )
        .dt.total_seconds()
        / 3600.0
    )

    interval_hours = (
        (
            next_irrigation_start
            - irrigation_end
        )
        .total_seconds()
        / 3600.0
    )

    sub[
        "irrigation_interval_hours"
    ] = interval_hours

    sub[
        "interval_fraction"
    ] = (
        sub[
            "hours_since_irrigation_end"
        ]
        / interval_hours
    )

    return sub


# ======================================================================
# Checkpoint selection
# ======================================================================

def nearest_checkpoint_row(
    event_df: pd.DataFrame,
    *,
    target_timestamp: pd.Timestamp,
) -> pd.Series | None:
    if event_df.empty:
        return None

    working = event_df[
        event_df[
            "strip_profile_water_in_0_18in"
        ].notna()
    ].copy()

    if working.empty:
        return None

    working[
        "_time_difference_minutes"
    ] = (
        (
            working[
                "timestamp"
            ]
            - target_timestamp
        )
        .abs()
        .dt.total_seconds()
        / 60.0
    )

    idx = (
        working[
            "_time_difference_minutes"
        ]
        .idxmin()
    )

    row = working.loc[
        idx
    ]

    if (
        row[
            "_time_difference_minutes"
        ]
        > CHECKPOINT_TOLERANCE_MINUTES
    ):
        return None

    return row


def build_event_checkpoints(
    event_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    if event_df.empty:
        return pd.DataFrame()

    first = event_df.iloc[
        0
    ]

    irrigation_end = first[
        "irrigation_end"
    ]

    next_irrigation_start = first[
        "next_irrigation_start"
    ]

    interval_hours = first[
        "irrigation_interval_hours"
    ]

    targets: list[
        tuple[str, pd.Timestamp]
    ] = []

    for label, hours in (
        CHECKPOINT_HOURS.items()
    ):
        target = (
            irrigation_end
            + pd.Timedelta(
                hours=hours
            )
        )

        if target < next_irrigation_start:
            targets.append(
                (
                    label,
                    target,
                )
            )

    halfway_target = (
        irrigation_end
        + (
            next_irrigation_start
            - irrigation_end
        )
        / 2
    )

    targets.append(
        (
            "half_interval",
            halfway_target,
        )
    )

    pre_next_target = (
        next_irrigation_start
        - pd.Timedelta(
            minutes=(
                PRE_NEXT_OFFSET_MINUTES
            )
        )
    )

    targets.append(
        (
            "pre_next_irrigation",
            pre_next_target,
        )
    )

    rows: list[
        dict[str, object]
    ] = []

    reference_window_start = irrigation_end + pd.Timedelta(
        hours=REFERENCE_WINDOW_START_HOURS
    )
    reference_window_end = irrigation_end + pd.Timedelta(
        hours=REFERENCE_WINDOW_END_HOURS
    )
    reference_values = event_df.loc[
        event_df["timestamp"].ge(reference_window_start)
        & event_df["timestamp"].le(reference_window_end),
        "strip_profile_water_in_0_18in",
    ].dropna()
    reference_water = (
        float(reference_values.median())
        if len(reference_values) >= MIN_REFERENCE_OBSERVATIONS
        else np.nan
    )

    for label, target in targets:
        selected = (
            nearest_checkpoint_row(
                event_df,
                target_timestamp=target,
            )
        )

        row: dict[
            str,
            object
        ] = {
            "year": first[
                "year"
            ],
            "strip_group": first[
                "strip_group"
            ],
            "strip": first[
                "strip"
            ],
            "event_id": first[
                "event_id"
            ],
            "irrigation_start": first[
                "irrigation_start"
            ],
            "irrigation_end": (
                irrigation_end
            ),
            "next_irrigation_start": (
                next_irrigation_start
            ),
            "irrigation_interval_hours": (
                interval_hours
            ),
            "checkpoint": label,
            "target_timestamp": target,
            "precip_since_irrigation_end_in": accumulated_precipitation(
                weather_df,
                start=irrigation_end,
                end=target,
            ),
            "reference_window_start": reference_window_start,
            "reference_window_end": reference_window_end,
            "reference_n_observations": len(reference_values),
            "reference_water_in": reference_water,
        }

        if selected is None:
            row.update(
                {
                    "observed_timestamp": (
                        pd.NaT
                    ),
                    "hours_since_irrigation_end": (
                        np.nan
                    ),
                    "interval_fraction": (
                        np.nan
                    ),
                    "strip_profile_water_in_0_18in": (
                        np.nan
                    ),
                    "top_profile_water_in": (
                        np.nan
                    ),
                    "middle_profile_water_in": (
                        np.nan
                    ),
                    "bottom_profile_water_in": (
                        np.nan
                    ),
                    "n_valid_logger_profiles": (
                        np.nan
                    ),
                }
            )

        else:
            row.update(
                {
                    "observed_timestamp": (
                        selected[
                            "timestamp"
                        ]
                    ),
                    "hours_since_irrigation_end": (
                        selected[
                            "hours_since_irrigation_end"
                        ]
                    ),
                    "interval_fraction": (
                        selected[
                            "interval_fraction"
                        ]
                    ),
                    "strip_profile_water_in_0_18in": (
                        selected[
                            "strip_profile_water_in_0_18in"
                        ]
                    ),
                    "top_profile_water_in": (
                        selected[
                            "top_profile_water_in"
                        ]
                    ),
                    "middle_profile_water_in": (
                        selected[
                            "middle_profile_water_in"
                        ]
                    ),
                    "bottom_profile_water_in": (
                        selected[
                            "bottom_profile_water_in"
                        ]
                    ),
                    "n_valid_logger_profiles": (
                        selected[
                            "n_valid_logger_profiles"
                        ]
                    ),
                }
            )

        rows.append(
            row
        )

    out = pd.DataFrame(
        rows
    )

    out[
        "change_from_reference_in"
    ] = (
        out[
            "strip_profile_water_in_0_18in"
        ]
        - out[
            "reference_water_in"
        ]
    )

    out[
        "fraction_of_reference_water_remaining"
    ] = np.where(
        out[
            "reference_water_in"
        ]
        > 0,
        (
            out[
                "strip_profile_water_in_0_18in"
            ]
            / out[
                "reference_water_in"
            ]
        ),
        np.nan,
    )

    return out


# ======================================================================
# Event summaries
# ======================================================================

def trapezoid_auc(
    hours: pd.Series,
    water: pd.Series,
) -> float:
    valid = pd.DataFrame(
        {
            "hours": pd.to_numeric(
                hours,
                errors="coerce",
            ),
            "water": pd.to_numeric(
                water,
                errors="coerce",
            ),
        }
    ).dropna()

    if len(valid) < 2:
        return np.nan

    valid = valid.sort_values(
        "hours"
    )

    return float(
        np.trapezoid(
            y=valid[
                "water"
            ].to_numpy(),
            x=valid[
                "hours"
            ].to_numpy(),
        )
    )


def build_event_summary(
    event_df: pd.DataFrame,
    checkpoints: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> dict[str, object] | None:
    if event_df.empty:
        return None

    first = event_df.iloc[
        0
    ]

    valid = event_df[
        event_df[
            "strip_profile_water_in_0_18in"
        ].notna()
    ].copy()

    if valid.empty:
        return None

    interval_hours = float(
        first[
            "irrigation_interval_hours"
        ]
    )

    auc_water_in_hours = trapezoid_auc(
        valid[
            "hours_since_irrigation_end"
        ],
        valid[
            "strip_profile_water_in_0_18in"
        ],
    )

    # Normalize AUC by interval duration so units become average inches
    # of water present during the interval.
    mean_interval_water_in = (
        auc_water_in_hours
        / interval_hours
        if (
            np.isfinite(
                auc_water_in_hours
            )
            and interval_hours > 0
        )
        else np.nan
    )

    cp = checkpoints.set_index(
        "checkpoint"
    )

    def cp_value(
        label: str,
        col: str,
    ) -> float:
        if label not in cp.index:
            return np.nan

        value = cp.loc[
            label,
            col,
        ]

        if isinstance(
            value,
            pd.Series,
        ):
            value = value.iloc[
                0
            ]

        return pd.to_numeric(
            pd.Series(
                [value]
            ),
            errors="coerce",
        ).iloc[
            0
        ]

    reference_water = cp_value("24h", "reference_water_in")
    reference_n = cp_value("24h", "reference_n_observations")

    pre_next_water = cp_value(
        "pre_next_irrigation",
        "strip_profile_water_in_0_18in",
    )

    depletion_in = (
        reference_water
        - pre_next_water
        if (
            np.isfinite(
                reference_water
            )
            and np.isfinite(
                pre_next_water
            )
        )
        else np.nan
    )

    depletion_in_per_day = (
        depletion_in
        / (
            interval_hours
            / 24.0
        )
        if (
            np.isfinite(
                depletion_in
            )
            and interval_hours > 0
        )
        else np.nan
    )

    return {
        "year": first[
            "year"
        ],
        "strip_group": first[
            "strip_group"
        ],
        "strip": first[
            "strip"
        ],
        "event_id": first[
            "event_id"
        ],
        "irrigation_start": first[
            "irrigation_start"
        ],
        "irrigation_end": first[
            "irrigation_end"
        ],
        "next_irrigation_start": first[
            "next_irrigation_start"
        ],
        "interval_days": (
            interval_hours
            / 24.0
        ),
        "n_timeseries_points": len(
            valid
        ),
        "reference_window_start_hours": REFERENCE_WINDOW_START_HOURS,
        "reference_window_end_hours": REFERENCE_WINDOW_END_HOURS,
        "reference_n_observations": reference_n,
        "reference_water_in": reference_water,
        "water_24h_in": cp_value(
            "24h",
            "strip_profile_water_in_0_18in",
        ),
        "water_72h_in": cp_value(
            "72h",
            "strip_profile_water_in_0_18in",
        ),
        "water_7d_in": cp_value(
            "7d",
            "strip_profile_water_in_0_18in",
        ),
        "water_half_interval_in": cp_value(
            "half_interval",
            "strip_profile_water_in_0_18in",
        ),
        "water_pre_next_in": (
            pre_next_water
        ),
        "fraction_remaining_24h": (
            cp_value(
                "24h",
                "fraction_of_reference_water_remaining",
            )
        ),
        "fraction_remaining_72h": (
            cp_value(
                "72h",
                "fraction_of_reference_water_remaining",
            )
        ),
        "fraction_remaining_7d": (
            cp_value(
                "7d",
                "fraction_of_reference_water_remaining",
            )
        ),
        "fraction_remaining_pre_next": (
            cp_value(
                "pre_next_irrigation",
                "fraction_of_reference_water_remaining",
            )
        ),
        "depletion_reference_to_pre_next_in": (
            depletion_in
        ),
        "depletion_in_per_day": (
            depletion_in_per_day
        ),
        "auc_water_in_hours": (
            auc_water_in_hours
        ),
        "mean_interval_water_in": (
            mean_interval_water_in
        ),
        "precip_0_24h_in": cp_value(
            "24h", "precip_since_irrigation_end_in"
        ),
        "precip_0_72h_in": cp_value(
            "72h", "precip_since_irrigation_end_in"
        ),
        "precip_0_7d_in": cp_value(
            "7d", "precip_since_irrigation_end_in"
        ),
        "precip_0_half_interval_in": cp_value(
            "half_interval", "precip_since_irrigation_end_in"
        ),
        "precip_0_next_irrigation_in": accumulated_precipitation(
            weather_df,
            start=first["irrigation_end"],
            end=first["next_irrigation_start"],
        ),
    }


# ======================================================================
# Pair summaries
# ======================================================================

def treatment_name(
    strip: str,
) -> str:
    return (
        "biochar"
        if strip in {
            "S1",
            "S3",
        }
        else "control"
    )


def build_pair_summary(
    event_summary: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compare matched biochar/control intervals.

    Matching is based on strip_group plus irrigation_start because the two
    strips within a treatment pair receive the same irrigation event.
    """

    if event_summary.empty:
        return pd.DataFrame()

    working = event_summary.copy()

    working[
        "treatment"
    ] = working[
        "strip"
    ].map(
        treatment_name
    )

    metrics = [
        "reference_water_in",
        "water_24h_in",
        "water_72h_in",
        "water_7d_in",
        "water_half_interval_in",
        "water_pre_next_in",
        "fraction_remaining_72h",
        "fraction_remaining_7d",
        "fraction_remaining_pre_next",
        "depletion_in_per_day",
        "mean_interval_water_in",
    ]

    metric_metadata = {
        "reference_water_in": ("Redistributed reference water", "in"),
        "water_24h_in": ("Profile water at 24 h", "in"),
        "water_72h_in": ("Profile water at 72 h", "in"),
        "water_7d_in": ("Profile water at 7 d", "in"),
        "water_half_interval_in": ("Profile water at half interval", "in"),
        "water_pre_next_in": ("Profile water before next irrigation", "in"),
        "fraction_remaining_72h": ("Reference fraction remaining at 72 h", "fraction"),
        "fraction_remaining_7d": ("Reference fraction remaining at 7 d", "fraction"),
        "fraction_remaining_pre_next": ("Reference fraction remaining pre-next", "fraction"),
        "depletion_in_per_day": ("Reference-to-pre-next depletion rate", "in/day"),
        "mean_interval_water_in": ("Mean profile water over interval", "in"),
    }

    rows: list[
        dict[str, object]
    ] = []

    pair_definitions = {
        "S1_S2": (
            "S1",
            "S2",
            "monthly",
        ),
        "S3_S4": (
            "S3",
            "S4",
            "biweekly",
        ),
    }

    for (
        pair,
        (
            biochar_strip,
            control_strip,
            regime,
        ),
    ) in pair_definitions.items():

        pair_df = working[
            working[
                "strip_group"
            ]
            .eq(
                pair
            )
        ].copy()

        bio = pair_df[
            pair_df[
                "strip"
            ]
            .eq(
                biochar_strip
            )
        ].copy()

        ctrl = pair_df[
            pair_df[
                "strip"
            ]
            .eq(
                control_strip
            )
        ].copy()

        merged = bio.merge(
            ctrl,
            on=[
                "year",
                "strip_group",
                "irrigation_start",
            ],
            suffixes=(
                "_biochar",
                "_control",
            ),
            how="inner",
        )

        if merged.empty:
            continue

        for metric in metrics:
            bio_col = (
                f"{metric}_biochar"
            )

            ctrl_col = (
                f"{metric}_control"
            )

            if (
                bio_col
                not in merged.columns
                or ctrl_col
                not in merged.columns
            ):
                continue

            valid = merged[
                [
                    bio_col,
                    ctrl_col,
                ]
            ].apply(
                pd.to_numeric,
                errors="coerce",
            ).dropna()

            if valid.empty:
                continue

            differences = (
                valid[
                    bio_col
                ]
                - valid[
                    ctrl_col
                ]
            )

            rows.append(
                {
                    "pair": pair,
                    "regime": regime,
                    "metric": metric,
                    "metric_label": metric_metadata[metric][0],
                    "unit": metric_metadata[metric][1],
                    "n_matched": len(
                        valid
                    ),
                    "biochar_mean": valid[
                        bio_col
                    ].mean(),
                    "control_mean": valid[
                        ctrl_col
                    ].mean(),
                    "biochar_minus_control": (
                        differences.mean()
                    ),
                    "median_paired_difference": differences.median(),
                    "biochar_higher_pct": (
                        100.0
                        * (
                            differences
                            > 0
                        ).mean()
                    ),
                }
            )

    return pd.DataFrame(
        rows
    )


# ======================================================================
# Main
# ======================================================================

def main() -> None:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print_section(
        "POST-IRRIGATION RETENTION DIAGNOSTIC"
    )

    print(
        f"Irrigation source:\n{IRRIGATION_CSV}"
    )

    print(
        f"\nOutput directory:\n{OUTPUT_DIR}"
    )

    events = (
        load_irrigation_events()
    )

    excluded_intervals = events.loc[
        ~events["retention_interval_eligible"]
    ].copy()
    eligible_events = events.loc[
        events["retention_interval_eligible"]
    ].copy()

    print_section(
        "IRRIGATION INTERVALS"
    )

    print(
        f"Strip-event rows: {len(events):,}\n"
        f"Eligible same-year intervals <= {MAX_INTERVAL_DAYS:g} days: "
        f"{len(eligible_events):,}\n"
        f"Excluded intervals: {len(excluded_intervals):,}"
    )

    print(
        eligible_events.groupby(
            "strip"
        )
        .size()
        .rename(
            "n_events"
        )
        .to_string()
    )

    all_long: list[
        pd.DataFrame
    ] = []

    all_checkpoints: list[
        pd.DataFrame
    ] = []

    event_summary_rows: list[
        dict[str, object]
    ] = []

    for year in YEARS:
        year_events = eligible_events[
            eligible_events[
                "year"
            ]
            .eq(
                year
            )
        ].copy()

        if year_events.empty:
            continue

        print_section(
            f"YEAR {year}"
        )

        logger_df = load_logger_year(
            year
        )
        weather_df = load_weather_year(year)
        print(f"CoAgMet: {len(weather_df):,} processed 15-minute rows.")

        strip_profiles: dict[
            str,
            pd.DataFrame
        ] = {}

        for strip in STRIPS:
            profile = (
                build_strip_profile_water(
                    logger_df,
                    strip=strip,
                )
            )

            strip_profiles[
                strip
            ] = profile

            if profile.empty:
                print(
                    f"{strip}: no profile "
                    "water series."
                )
            else:
                valid_count = int(
                    profile[
                        "strip_profile_water_in_0_18in"
                    ]
                    .notna()
                    .sum()
                )

                print(
                    f"{strip}: "
                    f"{valid_count:,} valid "
                    "strip-profile timestamps."
                )

        for _, event in (
            year_events.iterrows()
        ):
            strip = str(
                event[
                    "strip"
                ]
            )

            profile_df = (
                strip_profiles.get(
                    strip,
                    pd.DataFrame(),
                )
            )

            if profile_df.empty:
                continue

            event_long = (
                extract_event_retention_series(
                    profile_df,
                    event,
                )
            )

            if event_long.empty:
                continue

            checkpoints = (
                build_event_checkpoints(
                    event_long,
                    weather_df,
                )
            )

            summary = (
                build_event_summary(
                    event_long,
                    checkpoints,
                    weather_df,
                )
            )

            all_long.append(
                event_long
            )

            if not checkpoints.empty:
                all_checkpoints.append(
                    checkpoints
                )

            if summary is not None:
                event_summary_rows.append(
                    summary
                )

    retention_long = (
        pd.concat(
            all_long,
            ignore_index=True,
        )
        if all_long
        else pd.DataFrame()
    )

    checkpoints = (
        pd.concat(
            all_checkpoints,
            ignore_index=True,
        )
        if all_checkpoints
        else pd.DataFrame()
    )

    event_summary = (
        pd.DataFrame(
            event_summary_rows
        )
    )

    pair_summary = (
        build_pair_summary(
            event_summary
        )
    )

    print_section(
        "RETENTION EVENT SUMMARY"
    )

    if event_summary.empty:
        print(
            "No usable retention intervals."
        )
    else:
        print(
            event_summary.groupby(["year", "strip"]).agg(
                intervals=("event_id", "size"),
                median_interval_days=("interval_days", "median"),
                mean_reference_water_in=("reference_water_in", "mean"),
                mean_water_72h_in=("water_72h_in", "mean"),
                mean_water_7d_in=("water_7d_in", "mean"),
                mean_water_pre_next_in=("water_pre_next_in", "mean"),
                mean_interval_precip_in=("precip_0_next_irrigation_in", "mean"),
            ).round(3).to_string()
        )

    print_section(
        "MATCHED BIOCHAR / CONTROL SUMMARY"
    )

    if pair_summary.empty:
        print(
            "No matched comparisons "
            "available."
        )
    else:
        print(
            pair_summary[[
                "pair", "regime", "metric_label", "unit", "n_matched",
                "biochar_mean", "control_mean", "biochar_minus_control",
                "biochar_higher_pct",
            ]]
            .round(
                3
            )
            .to_string(
                index=False
            )
        )

    print_section(
        "WRITING OUTPUTS"
    )

    write_csv(
        retention_long,
        "post_irrigation_retention_long.csv",
    )

    write_csv(
        checkpoints,
        "post_irrigation_retention_checkpoints.csv",
    )

    write_csv(
        event_summary,
        "post_irrigation_retention_event_summary.csv",
    )

    write_csv(
        pair_summary,
        "post_irrigation_retention_pair_summary.csv",
    )

    write_csv(
        excluded_intervals,
        "post_irrigation_retention_excluded_intervals.csv",
    )

    print_section(
        "POST-IRRIGATION RETENTION DIAGNOSTIC COMPLETE"
    )

    print(
        f"Long rows: "
        f"{len(retention_long):,}"
    )

    print(
        f"Checkpoint rows: "
        f"{len(checkpoints):,}"
    )

    print(
        f"Event summaries: "
        f"{len(event_summary):,}"
    )

    print(
        f"Pair-summary rows: "
        f"{len(pair_summary):,}"
    )

    print(
        f"\nOutput directory:\n"
        f"{OUTPUT_DIR}"
    )


if __name__ == "__main__":
    main()
