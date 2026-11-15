#!/usr/bin/env python3
"""Area-weighted retention analysis for the shallow CS650 sensor band.

This analysis answers a narrower question than the existing 18-inch-profile
retention report: how much more (or less) water is present in the shallow
sensor-represented band in a biochar strip than in its paired control strip?

The nominal 6-inch sensor is centered six inches below the surface.  With the
configured three-inch vertical sensitivity radius it represents approximately
3--9 inches, not a literal 0--6-inch sample.  The represented band overlaps the
biochar-injected layer but cannot isolate only the injected soil.

For every eligible interval between irrigations, the script:

* converts shallow VWC to equivalent water depth in a six-inch band;
* requires simultaneous valid Top, Middle, and Bottom measurements;
* weights those positions by their mapped logger influence-zone areas;
* measures water 24 hours, 72 hours, seven days, halfway through the interval,
  and immediately before the next irrigation;
* uses the median from 24--36 hours after irrigation as the redistributed
  reference state;
* pairs S1 with S2 and S3 with S4 for the same irrigation event; and
* reports all matched events and a rain-free sensitivity subset.

Repeated events are not independent field replicates.  The experiment has one
strip per treatment within each irrigation regime, so the results describe
these field pairs and do not by themselves identify a population-level causal
biochar effect.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

from biochar_app.config.field_management_metadata import (
    INCHES_WATER_TO_GALLONS_PER_SQFT,
    ZONE_AREAS_SQFT_BY_STRIP,
)
from biochar_app.config.paths import IRRIGATION_DIAGNOSTICS_DIR

BIOCHAR_APP_DIR: Final[Path] = Path(__file__).resolve().parents[3]
DATA_PROCESSED_DIR: Final[Path] = BIOCHAR_APP_DIR / "data-processed"
PARQUET_DIR: Final[Path] = DATA_PROCESSED_DIR / "parquet"
IRRIGATION_CSV: Final[Path] = (
    DATA_PROCESSED_DIR / "management" / "irrigation" / "irrigation_clean.csv"
)
YEARS: Final[tuple[int, ...]] = (2023, 2024, 2025, 2026)
STRIPS: Final[tuple[str, ...]] = ("S1", "S2", "S3", "S4")
LOGGER_POSITIONS: Final[tuple[str, ...]] = ("T", "M", "B")
CHECKPOINT_HOURS: Final[dict[str, float]] = {
    "24h": 24.0,
    "72h": 72.0,
    "7d": 24.0 * 7.0,
}
MAX_INTERVAL_DAYS: Final[float] = 45.0
REFERENCE_WINDOW_START_HOURS: Final[float] = 24.0
REFERENCE_WINDOW_END_HOURS: Final[float] = 36.0
MIN_REFERENCE_OBSERVATIONS: Final[int] = 8
CHECKPOINT_TOLERANCE_MINUTES: Final[float] = 45.0
PRE_NEXT_OFFSET_MINUTES: Final[float] = 15.0
IRRIGATION_SEASON_START_MONTH_DAY: Final[tuple[int, int]] = (4, 1)
IRRIGATION_SEASON_END_MONTH_DAY: Final[tuple[int, int]] = (10, 31)
EXPECTED_OBSERVATIONS_PER_DAY: Final[int] = 96
MIN_DAILY_PAIRED_OBSERVATIONS: Final[int] = 72
MIN_INTERVAL_COVERAGE_FRACTION: Final[float] = 0.75

SHALLOW_SENSOR_DEPTH_IN: Final[float] = 6.0
SHALLOW_REPRESENTED_THICKNESS_IN: Final[float] = 6.0
SHALLOW_REPRESENTED_LOWER_BOUND_IN: Final[float] = 3.0
SHALLOW_REPRESENTED_UPPER_BOUND_IN: Final[float] = 9.0
RAIN_FREE_THRESHOLD_IN: Final[float] = 0.01
BOOTSTRAP_REPLICATES: Final[int] = 20_000
RANDOM_SEED: Final[int] = 20261021
ACRE_SQFT: Final[float] = 43_560.0
GALLONS_PER_ACRE_INCH: Final[float] = (
    ACRE_SQFT * INCHES_WATER_TO_GALLONS_PER_SQFT
)

OUTPUT_DIR: Final[Path] = (
    IRRIGATION_DIAGNOSTICS_DIR / "shallow_layer_retention"
)
REPORT_DIR: Final[Path] = BIOCHAR_APP_DIR / "docs" / "research" / "water_balance"
SUPPORTING_TABLE_DIR: Final[Path] = REPORT_DIR / "supporting_tables"
REPORT_PATH: Final[Path] = REPORT_DIR / "shallow_layer_water_retention_2026-10-21.md"

PAIR_DEFINITIONS: Final[dict[str, tuple[str, str, str]]] = {
    "S1_S2": ("S1", "S2", "reduced irrigation (monthly)"),
    "S3_S4": ("S3", "S4", "full irrigation (biweekly)"),
}
PAIR_LABELS: Final[dict[str, str]] = {
    "S1_S2": "BC50/CON50 (S1/S2)",
    "S3_S4": "BC100/CON100 (S3/S4)",
}

OUTCOMES: Final[dict[str, tuple[str, str]]] = {
    "reference_water_in": (
        "Redistributed water, 24--36 h median",
        "precip_0_reference_in",
    ),
    "water_24h_in": ("Water at 24 h", "precip_0_24h_in"),
    "water_72h_in": ("Water at 72 h", "precip_0_72h_in"),
    "water_7d_in": ("Water at 7 d", "precip_0_7d_in"),
    "water_half_interval_in": (
        "Water halfway to next irrigation",
        "precip_0_half_interval_in",
    ),
    "water_pre_next_in": (
        "Water before next irrigation",
        "precip_0_next_irrigation_in",
    ),
    "mean_interval_water_in": (
        "Mean water over irrigation interval",
        "precip_0_next_irrigation_in",
    ),
}


def first_existing_column(
    frame: pd.DataFrame,
    candidates: tuple[str, ...],
) -> str | None:
    """Return the first candidate column present in a data frame."""
    return next((column for column in candidates if column in frame.columns), None)


def load_irrigation_events() -> pd.DataFrame:
    """Load the canonical irrigation table and define drying intervals."""
    if not IRRIGATION_CSV.exists():
        raise FileNotFoundError(f"Irrigation file not found: {IRRIGATION_CSV}")
    source = pd.read_csv(IRRIGATION_CSV, low_memory=False)
    if source.empty:
        raise ValueError(f"Irrigation file is empty: {IRRIGATION_CSV}")

    start_column = first_existing_column(
        source,
        ("irrigation_start", "start_timestamp", "start_datetime", "start_time"),
    )
    end_column = first_existing_column(
        source,
        ("irrigation_end", "end_timestamp", "end_datetime", "end_time"),
    )
    group_column = first_existing_column(
        source,
        ("strip_group", "group", "irrigation_group"),
    )
    strip_column = first_existing_column(source, ("strip", "Strip"))
    if start_column is None or end_column is None or group_column is None:
        raise KeyError(
            "Could not identify irrigation start, end, and strip-group columns."
        )

    source["irrigation_start"] = pd.to_datetime(
        source[start_column], errors="coerce"
    )
    source["irrigation_end"] = pd.to_datetime(source[end_column], errors="coerce")
    source["strip_group"] = source[group_column].astype(str).str.strip()
    source = source.loc[
        source["irrigation_start"].notna()
        & source["irrigation_end"].notna()
        & source["irrigation_start"].dt.year.isin(YEARS)
    ].copy()

    rows: list[dict[str, object]] = []
    for _, row in source.iterrows():
        group = str(row["strip_group"]).strip()
        explicit_strip = ""
        if strip_column is not None and pd.notna(row[strip_column]):
            explicit_strip = str(row[strip_column]).strip()
        if explicit_strip in STRIPS:
            event_strips = (explicit_strip,)
        elif group == "S1_S2":
            event_strips = ("S1", "S2")
        elif group == "S3_S4":
            event_strips = ("S3", "S4")
        else:
            continue

        start = row["irrigation_start"]
        event_id = row.get("event_id", pd.NA)
        if pd.isna(event_id):
            event_id = f"{start:%Y-%m-%d}_{group}"
        for strip in event_strips:
            rows.append({
                "year": int(start.year),
                "strip_group": group,
                "strip": strip,
                "event_id": str(event_id),
                "irrigation_start": start,
                "irrigation_end": row["irrigation_end"],
            })

    events = pd.DataFrame(rows)
    if events.empty:
        raise ValueError("No usable irrigation events were found.")
    events = events.sort_values(["strip", "irrigation_start"]).reset_index(drop=True)
    events["next_irrigation_start"] = events.groupby("strip")[
        "irrigation_start"
    ].shift(-1)
    events["hours_to_next_irrigation"] = (
        events["next_irrigation_start"] - events["irrigation_end"]
    ).dt.total_seconds() / 3600.0
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
    return events


def find_logger_parquet(year: int) -> Path:
    """Locate the canonical raw logger parquet for one year."""
    preferred = PARQUET_DIR / str(year) / f"{year}_raw_logger.parquet"
    if preferred.exists():
        return preferred
    candidates = sorted(
        path
        for path in PARQUET_DIR.rglob(f"*{year}*raw*logger*.parquet")
        if "ratio" not in path.name.lower() and "sorted" not in path.name.lower()
    )
    if not candidates:
        raise FileNotFoundError(
            f"Could not locate raw logger parquet for {year} under {PARQUET_DIR}"
        )
    return candidates[0]


def load_logger_year(year: int) -> pd.DataFrame:
    """Load and normalize one year of 15-minute logger observations."""
    path = find_logger_parquet(year)
    print(f"Loading logger data for {year}: {path}")
    source = pd.read_parquet(path)
    if source.empty:
        raise ValueError(f"Logger parquet is empty: {path}")
    out = source.copy()
    if isinstance(out.index, pd.DatetimeIndex):
        out["timestamp"] = out.index
    elif "timestamp" not in out.columns:
        timestamp_column = first_existing_column(
            out, ("TIMESTAMP", "datetime", "DateTime")
        )
        if timestamp_column is None:
            raise KeyError(f"Could not identify logger timestamp in {path}")
        out["timestamp"] = out[timestamp_column]
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    return (
        out.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .drop_duplicates("timestamp", keep="last")
        .reset_index(drop=True)
    )


def find_weather_parquet(year: int) -> Path:
    """Locate processed 15-minute CoAgMet data for one year."""
    preferred = (
        PARQUET_DIR / "summary" / "weather" / "15min" / f"{year}_15min.parquet"
    )
    if preferred.exists():
        return preferred
    candidates = sorted(PARQUET_DIR.rglob(f"*weather*/*{year}*15min*.parquet"))
    if not candidates:
        raise FileNotFoundError(
            f"Could not locate 15-minute weather parquet for {year} under {PARQUET_DIR}"
        )
    return candidates[0]


def load_weather_year(year: int) -> pd.DataFrame:
    """Load precipitation increments without fetching external data."""
    path = find_weather_parquet(year)
    source = pd.read_parquet(path)
    if "timestamp" not in source.columns:
        source = source.reset_index()
    if "timestamp" not in source.columns or "precip_in" not in source.columns:
        raise KeyError(f"Weather parquet lacks timestamp or precip_in: {path}")
    out = source[["timestamp", "precip_in"]].copy()
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
    weather: pd.DataFrame,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> float:
    """Sum precipitation increments in the half-open interval [start, end)."""
    if weather.empty or pd.isna(start) or pd.isna(end) or end <= start:
        return np.nan
    values = weather.loc[
        weather["timestamp"].ge(start) & weather["timestamp"].lt(end),
        "precip_in",
    ]
    return float(values.sum(min_count=1)) if not values.empty else np.nan


def build_shallow_strip_water(
    logger_df: pd.DataFrame,
    *,
    strip: str,
) -> pd.DataFrame:
    """Build a complete, area-weighted shallow-band time series for one strip."""
    if strip not in STRIPS:
        raise ValueError(f"Unknown strip: {strip!r}")

    source_columns = {
        position: f"VWC_1_raw_{strip}_{position}"
        for position in LOGGER_POSITIONS
    }
    missing = [
        column for column in source_columns.values()
        if column not in logger_df.columns
    ]
    if missing or "timestamp" not in logger_df.columns:
        return pd.DataFrame()

    out = logger_df[["timestamp", *source_columns.values()]].copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    for column in source_columns.values():
        out[column] = pd.to_numeric(out[column], errors="coerce")
        out.loc[~out[column].between(0.0, 100.0), column] = np.nan

    vwc_columns: list[str] = []
    water_columns: list[str] = []
    for position, source_column in source_columns.items():
        label = position.lower()
        vwc_column = f"{label}_shallow_vwc_pct"
        water_column = f"{label}_shallow_water_in"
        out[vwc_column] = out[source_column]
        out[water_column] = (
            out[vwc_column] / 100.0 * SHALLOW_REPRESENTED_THICKNESS_IN
        )
        vwc_columns.append(vwc_column)
        water_columns.append(water_column)

    out["n_valid_logger_positions"] = out[vwc_columns].notna().sum(axis=1)
    out["complete_spatial_coverage"] = out["n_valid_logger_positions"].eq(3)

    zone_areas = ZONE_AREAS_SQFT_BY_STRIP[strip]
    total_area = float(sum(zone_areas.values()))
    out["represented_strip_area_sqft"] = total_area
    out["strip_shallow_vwc_pct"] = sum(
        out[f"{position.lower()}_shallow_vwc_pct"] * zone_areas[position]
        for position in LOGGER_POSITIONS
    ) / total_area
    out["strip_shallow_water_in"] = (
        out["strip_shallow_vwc_pct"]
        / 100.0
        * SHALLOW_REPRESENTED_THICKNESS_IN
    )
    out.loc[
        ~out["complete_spatial_coverage"],
        ["strip_shallow_vwc_pct", "strip_shallow_water_in"],
    ] = np.nan
    out["strip"] = strip

    return out[[
        "timestamp",
        "strip",
        *vwc_columns,
        *water_columns,
        "n_valid_logger_positions",
        "complete_spatial_coverage",
        "represented_strip_area_sqft",
        "strip_shallow_vwc_pct",
        "strip_shallow_water_in",
    ]]


def last_complete_source_day(
    source_end: pd.Timestamp,
    *,
    year: int,
) -> pd.Timestamp:
    """Return the last complete irrigation-season day available in a source."""
    source_end = pd.Timestamp(source_end)
    source_day = source_end.normalize()
    if source_end < source_day + pd.Timedelta(hours=23, minutes=45):
        source_day -= pd.Timedelta(days=1)
    season_end = pd.Timestamp(
        year,
        *IRRIGATION_SEASON_END_MONTH_DAY,
    )
    return min(source_day, season_end)


def build_daily_pair_differences(
    shallow_by_strip: dict[str, pd.DataFrame],
    *,
    year: int,
    source_end: pd.Timestamp,
    minimum_observations: int = MIN_DAILY_PAIRED_OBSERVATIONS,
) -> pd.DataFrame:
    """Build simultaneous daily shallow-water differences for each strip pair."""
    season_start = pd.Timestamp(year, *IRRIGATION_SEASON_START_MONTH_DAY)
    last_complete_day = last_complete_source_day(source_end, year=year)
    if last_complete_day < season_start:
        return pd.DataFrame()
    calendar = pd.DataFrame({
        "date": pd.date_range(season_start, last_complete_day, freq="D")
    })
    available_days = len(calendar)
    frames: list[pd.DataFrame] = []

    for pair, (biochar_strip, control_strip, regime) in PAIR_DEFINITIONS.items():
        biochar = shallow_by_strip[biochar_strip][
            ["timestamp", "strip_shallow_water_in"]
        ].rename(columns={"strip_shallow_water_in": "biochar_water_in"})
        control = shallow_by_strip[control_strip][
            ["timestamp", "strip_shallow_water_in"]
        ].rename(columns={"strip_shallow_water_in": "control_water_in"})
        simultaneous = biochar.merge(control, on="timestamp", how="inner")
        simultaneous = simultaneous.loc[
            simultaneous["timestamp"].ge(season_start)
            & simultaneous["timestamp"].lt(last_complete_day + pd.Timedelta(days=1))
        ].dropna(subset=["biochar_water_in", "control_water_in"])
        simultaneous["date"] = simultaneous["timestamp"].dt.normalize()
        simultaneous["difference_in"] = (
            simultaneous["biochar_water_in"] - simultaneous["control_water_in"]
        )

        daily = simultaneous.groupby("date", as_index=False).agg(
            simultaneous_observations=("difference_in", "size"),
            biochar_daily_mean_water_in=("biochar_water_in", "mean"),
            control_daily_mean_water_in=("control_water_in", "mean"),
            biochar_minus_control_daily_mean_in=("difference_in", "mean"),
        )
        daily = calendar.merge(daily, on="date", how="left")
        daily["simultaneous_observations"] = (
            daily["simultaneous_observations"].fillna(0).astype(int)
        )
        daily["year"] = year
        daily["pair"] = pair
        daily["pair_label"] = PAIR_LABELS[pair]
        daily["regime"] = regime
        daily["expected_observations"] = EXPECTED_OBSERVATIONS_PER_DAY
        daily["minimum_required_observations"] = minimum_observations
        daily["daily_coverage_fraction"] = (
            daily["simultaneous_observations"] / EXPECTED_OBSERVATIONS_PER_DAY
        )
        daily["daily_eligible"] = daily["simultaneous_observations"].ge(
            minimum_observations
        )
        daily["irrigation_season_days_available_in_source"] = available_days
        daily["biochar_minus_control_daily_mean_vwc_percentage_points"] = (
            daily["biochar_minus_control_daily_mean_in"]
            / SHALLOW_REPRESENTED_THICKNESS_IN
            * 100.0
        )
        daily["biochar_minus_control_daily_mean_gal_per_acre"] = (
            daily["biochar_minus_control_daily_mean_in"]
            * GALLONS_PER_ACRE_INCH
        )
        frames.append(daily)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def summarize_daily_irrigation_seasons(
    daily: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize daily means and signed water-inch days by pair and year."""
    rows: list[dict[str, object]] = []
    for (pair, year), group in daily.groupby(["pair", "year"], sort=True):
        eligible = group.loc[group["daily_eligible"]].copy()
        available_days = int(group["irrigation_season_days_available_in_source"].max())
        if eligible.empty:
            rows.append({
                "pair": pair,
                "pair_label": PAIR_LABELS[pair],
                "regime": PAIR_DEFINITIONS[pair][2],
                "year": int(year),
                "irrigation_season_days_available_in_source": available_days,
                "eligible_daily_pairs": 0,
                "eligible_day_fraction": 0.0,
                "first_eligible_date": pd.NaT,
                "last_eligible_date": pd.NaT,
            })
            continue

        differences = eligible["biochar_minus_control_daily_mean_in"]
        mean_difference = float(differences.mean())
        cumulative = float(differences.sum())
        rows.append({
            "pair": pair,
            "pair_label": PAIR_LABELS[pair],
            "regime": PAIR_DEFINITIONS[pair][2],
            "year": int(year),
            "irrigation_season_days_available_in_source": available_days,
            "eligible_daily_pairs": len(eligible),
            "eligible_day_fraction": len(eligible) / available_days,
            "first_eligible_date": eligible["date"].min(),
            "last_eligible_date": eligible["date"].max(),
            "mean_daily_difference_in": mean_difference,
            "median_daily_difference_in": float(differences.median()),
            "mean_daily_difference_vwc_percentage_points": (
                mean_difference / SHALLOW_REPRESENTED_THICKNESS_IN * 100.0
            ),
            "mean_daily_difference_gal_per_acre": (
                mean_difference * GALLONS_PER_ACRE_INCH
            ),
            "cumulative_water_inch_days": cumulative,
            "cumulative_gallon_days_per_acre": (
                cumulative * GALLONS_PER_ACRE_INCH
            ),
            "biochar_higher_day_fraction": float(differences.gt(0).mean()),
        })
    return pd.DataFrame(rows)


def extract_interval_series(
    shallow_df: pd.DataFrame,
    event: pd.Series,
) -> pd.DataFrame:
    """Extract one drying interval after an irrigation event."""
    if shallow_df.empty or pd.isna(event["next_irrigation_start"]):
        return pd.DataFrame()
    start = event["irrigation_end"]
    end = event["next_irrigation_start"]
    if end <= start:
        return pd.DataFrame()
    out = shallow_df.loc[
        shallow_df["timestamp"].ge(start) & shallow_df["timestamp"].lt(end)
    ].copy()
    if out.empty:
        return out
    interval_hours = (end - start).total_seconds() / 3600.0
    out["year"] = int(event["year"])
    out["strip_group"] = event["strip_group"]
    out["event_id"] = event["event_id"]
    out["irrigation_start"] = event["irrigation_start"]
    out["irrigation_end"] = start
    out["next_irrigation_start"] = end
    out["hours_since_irrigation_end"] = (
        out["timestamp"] - start
    ).dt.total_seconds() / 3600.0
    out["irrigation_interval_hours"] = interval_hours
    out["interval_fraction"] = out["hours_since_irrigation_end"] / interval_hours
    return out


def nearest_water_value(
    interval: pd.DataFrame,
    target: pd.Timestamp,
) -> tuple[float, pd.Timestamp | pd.NaT]:
    """Return the closest complete spatial value within the configured tolerance."""
    valid = interval.loc[interval["strip_shallow_water_in"].notna()].copy()
    if valid.empty:
        return np.nan, pd.NaT
    differences = (valid["timestamp"] - target).abs().dt.total_seconds() / 60.0
    index = differences.idxmin()
    if differences.loc[index] > CHECKPOINT_TOLERANCE_MINUTES:
        return np.nan, pd.NaT
    return float(valid.loc[index, "strip_shallow_water_in"]), valid.loc[index, "timestamp"]


def summarize_interval(
    interval: pd.DataFrame,
    weather: pd.DataFrame,
) -> dict[str, object] | None:
    """Summarize shallow water and precipitation for one strip interval."""
    if interval.empty:
        return None
    first = interval.iloc[0]
    valid = interval.loc[interval["strip_shallow_water_in"].notna()].copy()
    if valid.empty:
        return None

    irrigation_end = first["irrigation_end"]
    next_start = first["next_irrigation_start"]
    interval_hours = float(first["irrigation_interval_hours"])
    reference_start = irrigation_end + pd.Timedelta(
        hours=REFERENCE_WINDOW_START_HOURS
    )
    reference_end = irrigation_end + pd.Timedelta(
        hours=REFERENCE_WINDOW_END_HOURS
    )
    reference_values = valid.loc[
        valid["timestamp"].between(reference_start, reference_end),
        "strip_shallow_water_in",
    ].dropna()
    reference_water = (
        float(reference_values.median())
        if len(reference_values) >= MIN_REFERENCE_OBSERVATIONS
        else np.nan
    )

    targets: dict[str, pd.Timestamp] = {
        label: irrigation_end + pd.Timedelta(hours=hours)
        for label, hours in CHECKPOINT_HOURS.items()
        if irrigation_end + pd.Timedelta(hours=hours) < next_start
    }
    targets["half_interval"] = irrigation_end + (next_start - irrigation_end) / 2
    targets["pre_next"] = next_start - pd.Timedelta(minutes=PRE_NEXT_OFFSET_MINUTES)

    values: dict[str, float] = {}
    observed: dict[str, pd.Timestamp | pd.NaT] = {}
    for label, target in targets.items():
        values[label], observed[label] = nearest_water_value(interval, target)

    expected_observations = (
        interval_hours * EXPECTED_OBSERVATIONS_PER_DAY / 24.0
    )
    interval_coverage_fraction = min(
        len(valid) / expected_observations,
        1.0,
    )
    mean_interval_eligible = (
        interval_coverage_fraction >= MIN_INTERVAL_COVERAGE_FRACTION
    )
    mean_interval = (
        float(valid["strip_shallow_water_in"].mean())
        if mean_interval_eligible
        else np.nan
    )
    pre_next = values.get("pre_next", np.nan)

    def precip_to(target: pd.Timestamp) -> float:
        return accumulated_precipitation(
            weather, start=irrigation_end, end=target
        )

    return {
        "year": int(first["year"]),
        "strip_group": first["strip_group"],
        "strip": first["strip"],
        "event_id": first["event_id"],
        "irrigation_start": first["irrigation_start"],
        "irrigation_end": irrigation_end,
        "next_irrigation_start": next_start,
        "interval_days": interval_hours / 24.0,
        "n_complete_timestamps": len(valid),
        "expected_interval_observations": expected_observations,
        "interval_coverage_fraction": interval_coverage_fraction,
        "mean_interval_eligible": mean_interval_eligible,
        "reference_n_observations": len(reference_values),
        "reference_water_in": reference_water,
        "water_24h_in": values.get("24h", np.nan),
        "water_72h_in": values.get("72h", np.nan),
        "water_7d_in": values.get("7d", np.nan),
        "water_half_interval_in": values.get("half_interval", np.nan),
        "water_pre_next_in": pre_next,
        "fraction_remaining_72h": (
            values.get("72h", np.nan) / reference_water
            if np.isfinite(reference_water) and reference_water > 0 else np.nan
        ),
        "fraction_remaining_7d": (
            values.get("7d", np.nan) / reference_water
            if np.isfinite(reference_water) and reference_water > 0 else np.nan
        ),
        "fraction_remaining_pre_next": (
            pre_next / reference_water
            if np.isfinite(reference_water) and reference_water > 0 else np.nan
        ),
        "mean_interval_water_in": mean_interval,
        "precip_0_reference_in": precip_to(reference_end),
        "precip_0_24h_in": precip_to(targets["24h"]) if "24h" in targets else np.nan,
        "precip_0_72h_in": precip_to(targets["72h"]) if "72h" in targets else np.nan,
        "precip_0_7d_in": precip_to(targets["7d"]) if "7d" in targets else np.nan,
        "precip_0_half_interval_in": precip_to(targets["half_interval"]),
        "precip_0_next_irrigation_in": precip_to(next_start),
    }


def build_matched_events(event_summary: pd.DataFrame) -> pd.DataFrame:
    """Pair biochar and control intervals and add interpretable differences."""
    frames: list[pd.DataFrame] = []
    keys = ["year", "strip_group", "irrigation_start"]
    for pair, (biochar_strip, control_strip, regime) in PAIR_DEFINITIONS.items():
        pair_rows = event_summary.loc[event_summary["strip_group"].eq(pair)]
        biochar = pair_rows.loc[pair_rows["strip"].eq(biochar_strip)]
        control = pair_rows.loc[pair_rows["strip"].eq(control_strip)]
        matched = biochar.merge(
            control, on=keys, how="inner", suffixes=("_biochar", "_control")
        )
        if matched.empty:
            continue
        matched["pair"] = pair
        matched["regime"] = regime
        for outcome, (_, precip_column) in OUTCOMES.items():
            difference = (
                pd.to_numeric(matched[f"{outcome}_biochar"], errors="coerce")
                - pd.to_numeric(matched[f"{outcome}_control"], errors="coerce")
            )
            matched[f"difference__{outcome}"] = difference
            matched[f"gallons_per_acre_difference__{outcome}"] = (
                difference * GALLONS_PER_ACRE_INCH
            )
            matched[f"precip__{outcome}"] = matched[
                [f"{precip_column}_biochar", f"{precip_column}_control"]
            ].apply(pd.to_numeric, errors="coerce").mean(axis=1)
        frames.append(matched)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def stratified_bootstrap_ci(
    values: np.ndarray,
    years: np.ndarray,
    *,
    replicates: int = BOOTSTRAP_REPLICATES,
) -> tuple[float, float]:
    """Bootstrap a paired mean within years, preserving observed year weights."""
    rng = np.random.default_rng(RANDOM_SEED)
    groups = [values[years == year] for year in np.unique(years)]
    draws = np.empty(replicates, dtype=float)
    for index in range(replicates):
        draws[index] = np.concatenate([
            rng.choice(group, size=len(group), replace=True)
            for group in groups
        ]).mean()
    return tuple(float(value) for value in np.quantile(draws, [0.025, 0.975]))


def summarize_pairs(
    matched: pd.DataFrame,
    *,
    bootstrap_replicates: int = BOOTSTRAP_REPLICATES,
) -> pd.DataFrame:
    """Summarize all matched events and a rain-free sensitivity subset."""
    rows: list[dict[str, object]] = []
    scopes = (
        ("all_matched", None),
        ("rain_free_since_irrigation", RAIN_FREE_THRESHOLD_IN),
    )
    for pair, (_, _, regime) in PAIR_DEFINITIONS.items():
        pair_rows = matched.loc[matched["pair"].eq(pair)]
        for outcome, (label, _) in OUTCOMES.items():
            base = pd.DataFrame({
                "difference": pd.to_numeric(
                    pair_rows[f"difference__{outcome}"], errors="coerce"
                ),
                "year": pd.to_numeric(pair_rows["year"], errors="coerce"),
                "precip": pd.to_numeric(
                    pair_rows[f"precip__{outcome}"], errors="coerce"
                ),
            }).dropna(subset=["difference", "year"])
            for scope, rain_threshold in scopes:
                working = base.copy()
                if rain_threshold is not None:
                    working = working.loc[
                        working["precip"].notna()
                        & working["precip"].le(rain_threshold)
                    ]
                if working.empty:
                    continue
                differences = working["difference"].to_numpy(dtype=float)
                years = working["year"].to_numpy(dtype=float)
                ci_low, ci_high = stratified_bootstrap_ci(
                    differences, years, replicates=bootstrap_replicates
                )
                mean_difference = float(differences.mean())
                rows.append({
                    "pair": pair,
                    "regime": regime,
                    "scope": scope,
                    "outcome": outcome,
                    "outcome_label": label,
                    "n_matched_events": len(differences),
                    "first_year": int(years.min()),
                    "last_year": int(years.max()),
                    "biochar_minus_control_mean_in": mean_difference,
                    "biochar_minus_control_median_in": float(np.median(differences)),
                    "biochar_minus_control_mean_vwc_percentage_points": (
                        mean_difference / SHALLOW_REPRESENTED_THICKNESS_IN * 100.0
                    ),
                    "bootstrap_95ci_low_in": ci_low,
                    "bootstrap_95ci_high_in": ci_high,
                    "bootstrap_95ci_low_vwc_percentage_points": (
                        ci_low / SHALLOW_REPRESENTED_THICKNESS_IN * 100.0
                    ),
                    "bootstrap_95ci_high_vwc_percentage_points": (
                        ci_high / SHALLOW_REPRESENTED_THICKNESS_IN * 100.0
                    ),
                    "biochar_higher_fraction": float((differences > 0).mean()),
                    "biochar_minus_control_mean_gal_per_acre": (
                        mean_difference * GALLONS_PER_ACRE_INCH
                    ),
                    "bootstrap_95ci_low_gal_per_acre": (
                        ci_low * GALLONS_PER_ACRE_INCH
                    ),
                    "bootstrap_95ci_high_gal_per_acre": (
                        ci_high * GALLONS_PER_ACRE_INCH
                    ),
                    "rain_free_threshold_in": rain_threshold,
                })
    return pd.DataFrame(rows)


def summarize_by_year(matched: pd.DataFrame) -> pd.DataFrame:
    """Show whether the estimated paired difference is stable across years."""
    rows: list[dict[str, object]] = []
    for (pair, year), group in matched.groupby(["pair", "year"], dropna=False):
        regime = PAIR_DEFINITIONS[pair][2]
        for outcome, (label, _) in OUTCOMES.items():
            differences = pd.to_numeric(
                group[f"difference__{outcome}"], errors="coerce"
            ).dropna()
            if differences.empty:
                continue
            mean_difference = float(differences.mean())
            rows.append({
                "pair": pair,
                "regime": regime,
                "year": int(year),
                "outcome": outcome,
                "outcome_label": label,
                "n_matched_events": len(differences),
                "biochar_minus_control_mean_in": mean_difference,
                "biochar_minus_control_median_in": float(differences.median()),
                "biochar_higher_fraction": float(differences.gt(0).mean()),
                "biochar_minus_control_mean_gal_per_acre": (
                    mean_difference * GALLONS_PER_ACRE_INCH
                ),
            })
    return pd.DataFrame(rows)


def _format_signed(value: float, decimals: int = 3) -> str:
    return f"{value:+.{decimals}f}"


def write_markdown_report(
    pair_summary: pd.DataFrame,
    year_summary: pd.DataFrame,
    daily_season_summary: pd.DataFrame,
    matched: pd.DataFrame,
    *,
    source_end: pd.Timestamp,
) -> None:
    """Write the concise, maintained interpretation of the generated tables."""
    all_events = pair_summary.loc[pair_summary["scope"].eq("all_matched")]
    selected_outcomes = [
        "reference_water_in",
        "water_72h_in",
        "water_7d_in",
        "water_pre_next_in",
        "mean_interval_water_in",
    ]
    reference_rows = all_events.loc[all_events["outcome"].eq("reference_water_in")]
    source_year = int(source_end.year)
    last_complete_day = last_complete_source_day(source_end, year=source_year)
    lines = [
        "# Shallow-layer water retention in biochar and control strips",
        "",
        f"**Analysis date:** {date.today().isoformat()}",
        "",
        f"**Logger data included through:** {source_end}",
        "",
        "## Bottom line",
        "",
    ]
    for _, row in reference_rows.iterrows():
        pair = str(row["pair"])
        pair_label = PAIR_LABELS[pair]
        annual = year_summary.loc[
            year_summary["pair"].eq(pair)
            & year_summary["outcome"].eq("reference_water_in")
        ].sort_values("year")
        daily_annual = daily_season_summary.loc[
            daily_season_summary["pair"].eq(pair)
            & daily_season_summary["eligible_daily_pairs"].gt(0)
        ].sort_values("year")
        latest = annual.iloc[-1] if not annual.empty else None
        latest_daily = daily_annual.iloc[-1] if not daily_annual.empty else None
        latest_text = ""
        if latest is not None:
            latest_in = float(latest["biochar_minus_control_mean_in"])
            latest_vwc = latest_in / SHALLOW_REPRESENTED_THICKNESS_IN * 100.0
            latest_text = (
                f" The {int(latest['year'])} matched-event mean was "
                f"{_format_signed(latest_in)} inch "
                f"({_format_signed(latest_vwc, 1)} VWC percentage points)."
            )
        if latest_daily is not None:
            latest_text += (
                f" Across eligible days in the {int(latest_daily['year'])} irrigation "
                "season, the daily differences totaled "
                f"{_format_signed(float(latest_daily['cumulative_water_inch_days']), 1)} "
                "water-inch days."
            )
        lines.append(
            f"- **{pair_label}:** Across matched irrigation events, the biochar strip "
            f"averaged {_format_signed(float(row['biochar_minus_control_mean_in']))} inch "
            f"({_format_signed(float(row['biochar_minus_control_mean_vwc_percentage_points']), 1)} "
            "VWC percentage points) relative to its control."
            f"{latest_text}"
        )
    lines.extend([
        "",
        "A **water-inch day** is analogous to a degree-day: it combines magnitude and duration. For example, a 0.5-inch difference sustained for 10 eligible days equals 5 water-inch days. It does not mean that 5 inches of water were saved or applied, and it does not by itself establish that the additional water was plant-available or used by the crop.",
        "",
        "The pooled averages are descriptive, not stand-alone treatment estimates. The S1/S2 difference became positive after 2023 and remained positive through 2026. The S3/S4 difference declined from a large positive value in 2023 to approximately zero in 2025--2026. Thus the current record supports a persistent shallow-water difference for S1 versus S2, but not a stable shallow-water advantage for S3 versus S4.",
        "",
        "The complementary April--October calculation is reported separately for each year below. Daily differences and water-inch days are not summed across years.",
        "",
        "## Question and measurement boundary",
        "",
        "This report asks how much more or less water was present in the shallow sensor-represented soil band in each biochar strip than in its paired non-biochar strip. BC50 (S1) is compared with CON50 (S2) under the reduced-irrigation, approximately monthly regime. BC100 (S3) is compared with CON100 (S4) under the full-irrigation, approximately biweekly regime.",
        "",
        "The nominal 6-inch CS650 sensor represents an approximately six-inch-thick band centered at the sensor, or roughly 3--9 inches below the surface. It overlaps the biochar-injected layer but is not a direct measurement of only the 0--6-inch injected soil. The report therefore uses **shallow sensor band**, not **0--6-inch layer**.",
        "",
        "## Method",
        "",
        "For each strip and timestamp, Top, Middle, and Bottom shallow VWC readings were converted to equivalent inches of water and weighted by their mapped logger influence-zone areas. All three positions had to be valid. Irrigation intervals were paired within S1/S2 and S3/S4. Positive differences mean the biochar strip contained more water. One VWC percentage point across a six-inch band equals 0.06 inch of water.",
        "",
        "The 24--36-hour median is the primary redistributed post-irrigation reference. Later checkpoints describe persistence during drying. Descriptive bootstrap intervals resample repeated events within years. They describe these particular strip pairs; they do not create independent field replication.",
        "",
        "### Data sources and eligibility",
        "",
        "The analysis reads the maintained annual raw-logger parquet files, `biochar_app/data-processed/management/irrigation/irrigation_clean.csv`, and the processed 15-minute CoAgMet precipitation files. It performs no network retrieval. An interval must have a subsequent irrigation in the same calendar year, last no more than 45 days, and provide a matched biochar/control event. Each reported strip value also requires valid shallow readings at all three logger positions. The interval-mean metric additionally requires at least 75% complete temporal coverage in each strip.",
        "",
        "### Coverage",
        "",
    ])
    for pair in PAIR_DEFINITIONS:
        pair_rows = all_events.loc[all_events["pair"].eq(pair)]
        total_intervals = int(matched["pair"].eq(pair).sum())
        reference = pair_rows.loc[pair_rows["outcome"].eq("reference_water_in")]
        reference_intervals = (
            int(reference.iloc[0]["n_matched_events"]) if not reference.empty else 0
        )
        interval_mean = pair_rows.loc[
            pair_rows["outcome"].eq("mean_interval_water_in")
        ]
        interval_mean_count = (
            int(interval_mean.iloc[0]["n_matched_events"])
            if not interval_mean.empty
            else 0
        )
        lines.append(
            f"- **{PAIR_LABELS[pair]}:** {total_intervals} matched irrigation intervals; "
            f"{reference_intervals} had a complete 24--36-hour reference value, and "
            f"{interval_mean_count} met the paired 75% coverage requirement for the interval mean."
        )
    lines.extend([
        "",
        "Counts vary among later checkpoints because a checkpoint must occur before the next irrigation and must have complete Top, Middle, and Bottom shallow readings in both strips.",
        "",
        "## Irrigation-season daily results",
        "",
        "This complementary calculation uses every calendar day from April 1 through October 31 for which both strips have at least 72 simultaneous valid 15-minute observations—at least 18 of the day's 24 hours. It first averages the simultaneous biochar-minus-control observations within each day. The seasonal mean gives the average difference on eligible days.",
        "",
        "**Water-inch days** are the signed sum of those daily mean water-depth differences. For example, maintaining one extra inch for ten complete days equals ten water-inch days. This is a time-integrated storage difference, not ten inches of irrigation water saved or delivered. Missing days are omitted rather than extrapolated.",
        "",
        "### Results by irrigation season",
        "",
        "| Pair | Year | Eligible days / source-available days | Mean daily difference (VWC percentage points) | Mean daily difference (in) | Approx. gal/acre present on an average eligible day | Water-inch days | Biochar higher days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for _, row in daily_season_summary.iterrows():
        eligible_days = int(row["eligible_daily_pairs"])
        available_days = int(row["irrigation_season_days_available_in_source"])
        if eligible_days:
            vwc = _format_signed(
                float(row["mean_daily_difference_vwc_percentage_points"]), 1
            )
            mean = _format_signed(float(row["mean_daily_difference_in"]))
            gallons = f"{float(row['mean_daily_difference_gal_per_acre']):+,.0f}"
            cumulative = _format_signed(
                float(row["cumulative_water_inch_days"]), 1
            )
            higher = f"{float(row['biochar_higher_day_fraction']):.0%}"
        else:
            vwc = mean = gallons = cumulative = higher = "--"
        lines.append(
            f"| {row['pair_label']} | {int(row['year'])} | {eligible_days}/{available_days} | {vwc} | {mean} | {gallons} | {cumulative} | {higher} |"
        )
    lines.extend([
        "",
        f"The {source_year} source ends at {source_end}, so {last_complete_day.date().isoformat()} is the last complete day in the daily calculation. The 2023 BC100/CON100 record has no days meeting the 75% paired-coverage requirement and is not extrapolated.",
        "",
        "## Main matched-event results",
        "",
        "| Pair | Irrigation regime | Measurement | Events | Difference (VWC percentage points) | Biochar minus control (in) | Approx. gal/acre | Descriptive bootstrap 95% interval (in) | Biochar higher |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ])
    for pair in PAIR_DEFINITIONS:
        for outcome in selected_outcomes:
            row = all_events.loc[
                all_events["pair"].eq(pair) & all_events["outcome"].eq(outcome)
            ]
            if row.empty:
                continue
            item = row.iloc[0]
            lines.append(
                "| {pair} | {regime} | {label} | {n} | {vwc} | {mean} | {gallons:+,.0f} | {low} to {high} | {higher:.0%} |".format(
                    pair=PAIR_LABELS[pair],
                    regime=item["regime"],
                    label=item["outcome_label"],
                    n=int(item["n_matched_events"]),
                    vwc=_format_signed(
                        float(item["biochar_minus_control_mean_vwc_percentage_points"]),
                        1,
                    ),
                    mean=_format_signed(float(item["biochar_minus_control_mean_in"])),
                    gallons=float(item["biochar_minus_control_mean_gal_per_acre"]),
                    low=_format_signed(float(item["bootstrap_95ci_low_in"])),
                    high=_format_signed(float(item["bootstrap_95ci_high_in"])),
                    higher=float(item["biochar_higher_fraction"]),
                )
            )

    lines.extend([
        "",
        "## Interpretation",
        "",
    ])
    for _, row in reference_rows.iterrows():
        pair_label = PAIR_LABELS[str(row["pair"])]
        mean = float(row["biochar_minus_control_mean_in"])
        low = float(row["bootstrap_95ci_low_in"])
        high = float(row["bootstrap_95ci_high_in"])
        higher_fraction = float(row["biochar_higher_fraction"])
        if low > 0 and higher_fraction >= 0.8:
            conclusion = "Most repeated matched events favored the biochar strip."
        elif low > 0:
            conclusion = (
                "The pooled mean was positive, but individual event directions were mixed."
            )
        elif high < 0:
            conclusion = "The repeated matched events consistently favor the control strip."
        else:
            conclusion = "The uncertainty spans both less and more water, so there is no clear difference."
        lines.append(
            f"- **{pair_label}:** the pooled redistributed shallow-band difference was {_format_signed(mean)} inch "
            f"({float(row['biochar_minus_control_mean_gal_per_acre']):+,.0f} gal/acre). {conclusion}"
        )

    lines.extend([
        "",
        "## Rain-free sensitivity",
        "",
        "The sensitivity subset retains checkpoints with no more than 0.01 inch of accumulated rain after irrigation. It should be checked alongside the all-event estimates because rainfall can add water independently of the irrigation treatment.",
        "",
        "| Pair | Measurement | Rain-free events | Biochar minus control (in) | Approx. gal/acre |",
        "|---|---|---:|---:|---:|",
    ])
    rain_free = pair_summary.loc[
        pair_summary["scope"].eq("rain_free_since_irrigation")
        & pair_summary["outcome"].isin(["reference_water_in", "water_7d_in"])
    ]
    for _, row in rain_free.iterrows():
        lines.append(
            "| {pair} | {measurement} | {n} | {difference} | {gallons:+,.0f} |".format(
                pair=PAIR_LABELS[str(row["pair"])],
                measurement=row["outcome_label"],
                n=int(row["n_matched_events"]),
                difference=_format_signed(
                    float(row["biochar_minus_control_mean_in"])
                ),
                gallons=float(row["biochar_minus_control_mean_gal_per_acre"]),
            )
        )
    lines.extend([
        "",
        "## Year-to-year stability",
        "",
        "The year summary should be treated as a required diagnostic, not an optional detail. A treatment interpretation is weak when its direction changes among years, even if a pooled repeated-event interval excludes zero.",
        "",
        "| Pair | Year | Events | 24--36 h difference (in) | 72 h difference (in) | Pre-next difference (in) |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for (pair, year), group in year_summary.groupby(["pair", "year"]):
        by_outcome = group.set_index("outcome")
        def value(outcome: str) -> str:
            if outcome not in by_outcome.index:
                return "--"
            return _format_signed(float(by_outcome.loc[outcome, "biochar_minus_control_mean_in"]))
        reference = by_outcome.loc["reference_water_in"] if "reference_water_in" in by_outcome.index else None
        n = int(reference["n_matched_events"]) if reference is not None else 0
        lines.append(
            f"| {PAIR_LABELS[pair]} | {int(year)} | {n} | {value('reference_water_in')} | {value('water_72h_in')} | {value('water_pre_next_in')} |"
        )

    lines.extend([
        "",
        "## Limitations",
        "",
        "- There is one biochar strip and one control strip per irrigation regime. Logger positions and repeated irrigation events are subsamples, not independent treatment replicates.",
        "- Because treatment is confounded with strip identity, an absolute S1/S2 or S3/S4 water difference cannot be attributed entirely to biochar. Pre-existing soil, topographic, hydraulic, or sensor differences may contribute.",
        "- The shallow sensor band overlaps but does not exactly equal the biochar-injected layer.",
        "- The analysis estimates water present, not plant-available water and not water physically located inside biochar pores.",
        "- This analysis measures absolute water present after irrigation. It is not the separate event-storage-gain quantity (post-irrigation water minus the strip's own pre-irrigation baseline).",
        "- Spatial weighting assumes each logger represents its mapped Top, Middle, or Bottom influence zone.",
        "- Intervals ending after the last recorded irrigation cannot be evaluated because a next-irrigation boundary is required.",
        "",
        "## Reproducibility",
        "",
        "Run from the repository root:",
        "",
        "```bash",
        ".biochar_py313/bin/python -m biochar_app.scripts.management.irrigation_analysis.analyze_shallow_layer_retention",
        "```",
        "",
        "Supporting tables:",
        "",
        "Related analysis:",
        "",
        "- [`irrigation_cessation_july4_2026-10-21.md`](irrigation_cessation_july4_2026-10-21.md)",
        "",
        "- [`shallow_layer_pair_summary_2026-10-21.csv`](supporting_tables/shallow_layer_pair_summary_2026-10-21.csv)",
        "- [`shallow_layer_year_summary_2026-10-21.csv`](supporting_tables/shallow_layer_year_summary_2026-10-21.csv)",
        "- [`shallow_layer_matched_events_2026-10-21.csv`](supporting_tables/shallow_layer_matched_events_2026-10-21.csv)",
        "- [`shallow_layer_irrigation_season_summary_2026-10-21.csv`](supporting_tables/shallow_layer_irrigation_season_summary_2026-10-21.csv)",
        "- [`shallow_layer_daily_pair_differences_2026-10-21.csv`](supporting_tables/shallow_layer_daily_pair_differences_2026-10-21.csv)",
        "",
    ])
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, float_format="%.6f")
    print(f"Wrote {len(frame):,} rows: {path}")


def main() -> None:
    events = load_irrigation_events()
    eligible = events.loc[events["retention_interval_eligible"]].copy()
    event_rows: list[dict[str, object]] = []
    daily_frames: list[pd.DataFrame] = []
    source_end = pd.NaT

    for year in YEARS:
        year_events = eligible.loc[eligible["year"].eq(year)]
        if year_events.empty:
            continue
        logger = load_logger_year(year)
        weather = load_weather_year(year)
        year_source_end = logger["timestamp"].max()
        source_end = max(source_end, year_source_end) if pd.notna(source_end) else year_source_end
        shallow = {
            strip: build_shallow_strip_water(logger, strip=strip)
            for strip in STRIPS
        }
        daily_frames.append(
            build_daily_pair_differences(
                shallow,
                year=year,
                source_end=year_source_end,
            )
        )
        for _, event in year_events.iterrows():
            interval = extract_interval_series(shallow[str(event["strip"])], event)
            summary = summarize_interval(interval, weather)
            if summary is not None:
                event_rows.append(summary)

    event_summary = pd.DataFrame(event_rows)
    if event_summary.empty:
        raise ValueError("No eligible shallow-layer retention intervals were built.")
    matched = build_matched_events(event_summary)
    if matched.empty:
        raise ValueError("No matched shallow-layer biochar/control intervals were found.")
    pair_summary = summarize_pairs(matched)
    year_summary = summarize_by_year(matched)
    daily = pd.concat(daily_frames, ignore_index=True)
    daily_season_summary = summarize_daily_irrigation_seasons(daily)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(event_summary, OUTPUT_DIR / "shallow_layer_event_summary.csv")
    write_csv(matched, OUTPUT_DIR / "shallow_layer_matched_events.csv")
    write_csv(pair_summary, OUTPUT_DIR / "shallow_layer_pair_summary.csv")
    write_csv(year_summary, OUTPUT_DIR / "shallow_layer_year_summary.csv")
    write_csv(daily, OUTPUT_DIR / "shallow_layer_daily_pair_differences.csv")
    write_csv(
        daily_season_summary,
        OUTPUT_DIR / "shallow_layer_irrigation_season_summary.csv",
    )
    SUPPORTING_TABLE_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(
        matched,
        SUPPORTING_TABLE_DIR / "shallow_layer_matched_events_2026-10-21.csv",
    )
    write_csv(
        pair_summary,
        SUPPORTING_TABLE_DIR / "shallow_layer_pair_summary_2026-10-21.csv",
    )
    write_csv(
        year_summary,
        SUPPORTING_TABLE_DIR / "shallow_layer_year_summary_2026-10-21.csv",
    )
    write_csv(
        daily,
        SUPPORTING_TABLE_DIR
        / "shallow_layer_daily_pair_differences_2026-10-21.csv",
    )
    write_csv(
        daily_season_summary,
        SUPPORTING_TABLE_DIR
        / "shallow_layer_irrigation_season_summary_2026-10-21.csv",
    )
    write_markdown_report(
        pair_summary,
        year_summary,
        daily_season_summary,
        matched,
        source_end=source_end,
    )
    print(f"Wrote report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
