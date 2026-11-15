#!/usr/bin/env python3
"""Estimate pasture full-use days if irrigation stops on July 4.

This is a screening calculation, not a calibrated crop-growth model.  It uses
the complete 6-, 12-, and 18-inch CS650 profiles at the Top, Middle, and Bottom
positions, weights the positions by their mapped influence-zone areas, and
compares July 4 storage with a same-strip empirical dry-season reference.

The empirical reference is the fifth percentile of eligible daily median
profile-water values in the same April--October season.  It is not a measured
wilting point or a plant-stress threshold.  Estimated durations assume no
effective rain, no further irrigation, and constant pasture water use.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd

from biochar_app.config.experiment_config import (
    REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX,
    THREE_SENSOR_PROFILE_LOWER_BOUND_IN,
    THREE_SENSOR_PROFILE_UPPER_BOUND_IN,
)
from biochar_app.config.field_management_metadata import (
    ZONE_AREAS_SQFT_BY_STRIP,
)
from biochar_app.config.paths import IRRIGATION_DIAGNOSTICS_DIR
from biochar_app.scripts.management.irrigation_analysis.analyze_shallow_layer_retention import (
    BIOCHAR_APP_DIR,
    LOGGER_POSITIONS,
    PAIR_DEFINITIONS,
    PAIR_LABELS,
    STRIPS,
    YEARS,
    last_complete_source_day,
    load_logger_year,
)

STOP_MONTH_DAY: Final[tuple[int, int]] = (7, 4)
SEASON_START_MONTH_DAY: Final[tuple[int, int]] = (4, 1)
SEASON_END_MONTH_DAY: Final[tuple[int, int]] = (10, 31)
MIN_DAILY_OBSERVATIONS: Final[int] = 72
DRY_REFERENCE_QUANTILE: Final[float] = 0.05
PASTURE_USE_RATES_IN_PER_DAY: Final[tuple[float, ...]] = (0.20, 0.23, 0.30)
CENTRAL_PASTURE_USE_RATE_IN_PER_DAY: Final[float] = 0.23
REPORT_STAMP: Final[str] = "2026-10-21"

OUTPUT_DIR: Final[Path] = IRRIGATION_DIAGNOSTICS_DIR / "irrigation_cessation"
REPORT_DIR: Final[Path] = BIOCHAR_APP_DIR / "docs" / "research" / "water_balance"
SUPPORTING_TABLE_DIR: Final[Path] = REPORT_DIR / "supporting_tables"
REPORT_PATH: Final[Path] = (
    REPORT_DIR / f"irrigation_cessation_july4_{REPORT_STAMP}.md"
)

STRIP_LABELS: Final[dict[str, str]] = {
    "S1": "BC50 (S1)",
    "S2": "CON50 (S2)",
    "S3": "BC100 (S3)",
    "S4": "CON100 (S4)",
}


def build_strip_profile_water(
    logger_df: pd.DataFrame,
    *,
    strip: str,
) -> pd.DataFrame:
    """Build complete area-weighted three-depth profile water for one strip."""
    if strip not in STRIPS:
        raise ValueError(f"Unknown strip: {strip!r}")
    if "timestamp" not in logger_df.columns:
        raise KeyError("Logger data do not contain a timestamp column.")

    source_columns = {
        (position, depth): f"VWC_{depth}_raw_{strip}_{position}"
        for position in LOGGER_POSITIONS
        for depth in REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX
    }
    missing = [
        column for column in source_columns.values()
        if column not in logger_df.columns
    ]
    if missing:
        return pd.DataFrame()

    out = logger_df[["timestamp", *source_columns.values()]].copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    for column in source_columns.values():
        out[column] = pd.to_numeric(out[column], errors="coerce")
        out.loc[~out[column].between(0.0, 100.0), column] = np.nan

    position_columns: list[str] = []
    for position in LOGGER_POSITIONS:
        position_column = f"{position.lower()}_profile_water_in"
        profile_water = pd.Series(0.0, index=out.index, dtype=float)
        depth_columns: list[str] = []
        for depth, thickness in sorted(
            REPRESENTED_LAYER_THICKNESS_IN_BY_DEPTH_INDEX.items()
        ):
            source_column = source_columns[(position, depth)]
            depth_columns.append(source_column)
            profile_water += out[source_column] / 100.0 * thickness
        profile_water = profile_water.where(out[depth_columns].notna().all(axis=1))
        out[position_column] = profile_water
        position_columns.append(position_column)

    out["complete_profile"] = out[position_columns].notna().all(axis=1)
    zone_areas = ZONE_AREAS_SQFT_BY_STRIP[strip]
    total_area = float(sum(zone_areas.values()))
    out["strip_profile_water_in"] = sum(
        out[f"{position.lower()}_profile_water_in"] * zone_areas[position]
        for position in LOGGER_POSITIONS
    ) / total_area
    out.loc[~out["complete_profile"], "strip_profile_water_in"] = np.nan
    out["strip"] = strip
    out["represented_strip_area_sqft"] = total_area

    return out[[
        "timestamp",
        "strip",
        *position_columns,
        "complete_profile",
        "represented_strip_area_sqft",
        "strip_profile_water_in",
    ]]


def build_daily_profile(profile: pd.DataFrame) -> pd.DataFrame:
    """Summarize complete 15-minute profiles into robust daily medians."""
    if profile.empty:
        return pd.DataFrame()
    source = profile.copy()
    source["date"] = source["timestamp"].dt.normalize()
    daily = (
        source.groupby(["strip", "date"], as_index=False)
        .agg(
            complete_profile_observations=("strip_profile_water_in", "count"),
            daily_median_profile_water_in=("strip_profile_water_in", "median"),
        )
    )
    daily["daily_eligible"] = daily["complete_profile_observations"].ge(
        MIN_DAILY_OBSERVATIONS
    )
    daily.loc[
        ~daily["daily_eligible"], "daily_median_profile_water_in"
    ] = np.nan
    return daily


def build_year_scenario(
    logger_df: pd.DataFrame,
    *,
    year: int,
) -> pd.DataFrame:
    """Calculate July 4 reserves and full-use days for every strip in one year."""
    source_end = pd.Timestamp(logger_df["timestamp"].max())
    last_day = last_complete_source_day(source_end, year=year)
    season_start = pd.Timestamp(year, *SEASON_START_MONTH_DAY)
    season_end = min(pd.Timestamp(year, *SEASON_END_MONTH_DAY), last_day)
    stop_date = pd.Timestamp(year, *STOP_MONTH_DAY)
    rows: list[dict[str, object]] = []

    for strip in STRIPS:
        profile = build_strip_profile_water(logger_df, strip=strip)
        daily = build_daily_profile(profile)
        season = daily.loc[
            daily["date"].between(season_start, season_end)
            & daily["daily_eligible"]
        ].copy()
        stop = season.loc[season["date"].eq(stop_date)]
        start_available = not stop.empty
        start_water = (
            float(stop.iloc[0]["daily_median_profile_water_in"])
            if start_available
            else np.nan
        )
        stop_observations = (
            int(stop.iloc[0]["complete_profile_observations"])
            if start_available
            else int(
                daily.loc[
                    daily["date"].eq(stop_date),
                    "complete_profile_observations",
                ].max()
            )
            if daily["date"].eq(stop_date).any()
            else 0
        )
        dry_reference = (
            float(
                season["daily_median_profile_water_in"].quantile(
                    DRY_REFERENCE_QUANTILE
                )
            )
            if not season.empty
            else np.nan
        )
        reserve = (
            max(start_water - dry_reference, 0.0)
            if start_available and pd.notna(dry_reference)
            else np.nan
        )
        row: dict[str, object] = {
            "year": year,
            "stop_date": stop_date,
            "strip": strip,
            "strip_label": STRIP_LABELS[strip],
            "july4_profile_available": start_available,
            "july4_complete_observations": stop_observations,
            "july4_profile_water_in": start_water,
            "reference_eligible_days": len(season),
            "empirical_dry_reference_quantile": DRY_REFERENCE_QUANTILE,
            "empirical_dry_reference_in": dry_reference,
            "reserve_above_empirical_reference_in": reserve,
        }
        for rate in PASTURE_USE_RATES_IN_PER_DAY:
            rate_label = f"{rate:.2f}".replace(".", "p")
            row[f"full_use_days_at_{rate_label}_in_per_day"] = (
                reserve / rate if pd.notna(reserve) else np.nan
            )
        rows.append(row)
    return pd.DataFrame(rows)


def build_pair_summary(strip_summary: pd.DataFrame) -> pd.DataFrame:
    """Compare paired biochar and control July 4 storage and durations."""
    rows: list[dict[str, object]] = []
    central_column = "full_use_days_at_0p23_in_per_day"
    for year in sorted(strip_summary["year"].unique()):
        annual = strip_summary.loc[strip_summary["year"].eq(year)]
        for pair, (biochar, control, regime) in PAIR_DEFINITIONS.items():
            biochar_row = annual.loc[annual["strip"].eq(biochar)]
            control_row = annual.loc[annual["strip"].eq(control)]
            if biochar_row.empty or control_row.empty:
                continue
            biochar_item = biochar_row.iloc[0]
            control_item = control_row.iloc[0]
            complete = bool(
                biochar_item["july4_profile_available"]
                and control_item["july4_profile_available"]
            )
            start_difference = (
                float(biochar_item["july4_profile_water_in"])
                - float(control_item["july4_profile_water_in"])
                if complete
                else np.nan
            )
            biochar_days = (
                float(biochar_item[central_column]) if complete else np.nan
            )
            control_days = (
                float(control_item[central_column]) if complete else np.nan
            )
            rows.append({
                "year": int(year),
                "stop_date": pd.Timestamp(year, *STOP_MONTH_DAY),
                "pair": pair,
                "pair_label": PAIR_LABELS[pair],
                "regime": regime,
                "pair_complete": complete,
                "biochar_july4_profile_water_in": (
                    float(biochar_item["july4_profile_water_in"])
                    if complete
                    else np.nan
                ),
                "control_july4_profile_water_in": (
                    float(control_item["july4_profile_water_in"])
                    if complete
                    else np.nan
                ),
                "biochar_minus_control_july4_water_in": start_difference,
                "difference_equivalent_days_at_0p23_in_per_day": (
                    start_difference / CENTRAL_PASTURE_USE_RATE_IN_PER_DAY
                    if complete
                    else np.nan
                ),
                "biochar_days_to_own_empirical_reference_at_0p23": (
                    biochar_days
                ),
                "control_days_to_own_empirical_reference_at_0p23": (
                    control_days
                ),
                "biochar_minus_control_days_to_own_reference_at_0p23": (
                    biochar_days - control_days if complete else np.nan
                ),
            })
    return pd.DataFrame(rows)


def signed(value: float, decimals: int = 2) -> str:
    """Format one signed numeric result."""
    return f"{value:+.{decimals}f}"


def write_report(
    strip_summary: pd.DataFrame,
    pair_summary: pd.DataFrame,
) -> None:
    """Write the maintained interpretation of the July 4 scenario."""
    latest_year = int(strip_summary["year"].max())
    latest_pairs = pair_summary.loc[pair_summary["year"].eq(latest_year)]
    lines = [
        "# July 4 irrigation-cessation screening analysis",
        "",
        f"**Analysis date:** {date.today().isoformat()}",
        "",
        "## Bottom line",
        "",
    ]
    for _, row in latest_pairs.iterrows():
        if not row["pair_complete"]:
            lines.append(
                f"- **{row['pair_label']}:** no result; July 4 did not have a "
                "complete nine-sensor profile in both strips."
            )
            continue
        lines.append(
            f"- **{row['pair_label']}:** On July 4, {latest_year}, the biochar "
            f"strip contained {signed(float(row['biochar_minus_control_july4_water_in']), 3)} "
            "inch relative to its control in the represented profile. If that "
            "difference were fully plant-available, it equals "
            f"{signed(float(row['difference_equivalent_days_at_0p23_in_per_day']), 1)} "
            "days of pasture use at 0.23 inch/day. Using each strip's own "
            "same-year empirical dry reference, the modeled durations were "
            f"{float(row['biochar_days_to_own_empirical_reference_at_0p23']):.1f} "
            "days for biochar and "
            f"{float(row['control_days_to_own_empirical_reference_at_0p23']):.1f} "
            "days for control."
        )
    lines.extend([
        "",
        "These are **full-water-use days to an empirical dry reference**, not days until the soil is empty, crop death, or a measured wilting point. The calculation assumes no effective rainfall or further irrigation. Actual crop use would decline as water stress develops.",
        "",
        "## Question",
        "",
        "If irrigation stopped on July 4, how many days of midsummer pasture water use could the monitored soil-water reserve support, and did the biochar strip begin with or retain more water than its paired control?",
        "",
        "## Method and assumptions",
        "",
        f"The three horizontal CS650 sensors represent adjacent six-inch bands spanning approximately {THREE_SENSOR_PROFILE_LOWER_BOUND_IN:.0f}--{THREE_SENSOR_PROFILE_UPPER_BOUND_IN:.0f} inches below the surface. At every 15-minute timestamp, the calculation requires valid 6-, 12-, and 18-inch readings at the Top, Middle, and Bottom positions. Position profiles are weighted by their mapped logger influence-zone areas. A daily result requires at least {MIN_DAILY_OBSERVATIONS} complete observations, or 18 hours of coverage, and uses their median.",
        "",
        f"For each strip and year, the empirical dry reference is the {DRY_REFERENCE_QUANTILE * 100:.0f}th percentile of eligible daily median profile water from April 1 through the last complete day of the irrigation season. The modeled reserve is July 4 profile water minus that reference, with negative reserves set to zero.",
        "",
        "Pasture use is evaluated at 0.20, 0.23, and 0.30 inch/day. The central 0.23-inch/day value corresponds to approximately 7 inches during July. CSU reports this magnitude for pasture near Delta, Colorado; FAO midseason coefficients for managed pasture are broadly consistent with pasture water use near grass-reference evapotranspiration.",
        "",
        "Sources: [CSU pasture irrigation guidance](https://tra.extension.colostate.edu/wp-content/uploads/sites/9/2016/02/Pasture-Establishment-Renovation.pdf) and [FAO crop coefficients](https://www.fao.org/4/x0490e/x0490e0b.htm).",
        "",
        "## Results by strip and year",
        "",
        "| Year | Strip | July 4 observations | July 4 profile water (in) | Empirical dry reference (in) | Reserve above reference (in) | Days at 0.20 in/day | Days at 0.23 in/day | Days at 0.30 in/day |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for _, row in strip_summary.iterrows():
        if row["july4_profile_available"]:
            values = [
                f"{float(row['july4_profile_water_in']):.3f}",
                f"{float(row['empirical_dry_reference_in']):.3f}",
                f"{float(row['reserve_above_empirical_reference_in']):.3f}",
                f"{float(row['full_use_days_at_0p20_in_per_day']):.1f}",
                f"{float(row['full_use_days_at_0p23_in_per_day']):.1f}",
                f"{float(row['full_use_days_at_0p30_in_per_day']):.1f}",
            ]
        else:
            values = ["--"] * 6
        lines.append(
            f"| {int(row['year'])} | {row['strip_label']} | "
            f"{int(row['july4_complete_observations'])} | "
            + " | ".join(values)
            + " |"
        )
    lines.extend([
        "",
        "A missing result means July 4 had fewer than 72 complete observations across all nine sensors. No nearby date was substituted.",
        "",
        "## Paired interpretation at the central rate",
        "",
        "| Year | Pair | July 4 biochar minus control water (in) | Equivalent pasture-use days if fully available | Biochar days to own dry reference | Control days to own dry reference | Difference in modeled duration |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ])
    for _, row in pair_summary.iterrows():
        if row["pair_complete"]:
            values = [
                signed(float(row["biochar_minus_control_july4_water_in"]), 3),
                signed(
                    float(row["difference_equivalent_days_at_0p23_in_per_day"]),
                    1,
                ),
                f"{float(row['biochar_days_to_own_empirical_reference_at_0p23']):.1f}",
                f"{float(row['control_days_to_own_empirical_reference_at_0p23']):.1f}",
                signed(
                    float(
                        row[
                            "biochar_minus_control_days_to_own_reference_at_0p23"
                        ]
                    ),
                    1,
                ),
            ]
        else:
            values = ["--"] * 5
        lines.append(
            f"| {int(row['year'])} | {row['pair_label']} | "
            + " | ".join(values)
            + " |"
        )
    lines.extend([
        "",
        "The two paired measures answer different questions. The July 4 water difference asks whether one strip started with more monitored water. The duration comparison asks how long each strip would take to reach its own observed low-storage reference. They can point in different directions because the empirical reference also differs among strips.",
        "",
        "## Limitations",
        "",
        "- The represented profile spans approximately 3--21 inches. It is not a complete pasture root zone, and it omits water above and below that interval while extrapolating local sensor readings across mapped field zones.",
        "- The empirical dry reference is an observed lower-envelope statistic, not a laboratory-derived permanent wilting point or a crop-specific stress threshold.",
        "- Absolute VWC differences among sensors can reflect calibration, installation contact, soil texture, and local field conditions as well as real water storage.",
        "- The constant-use scenarios omit rainfall, irrigation, drainage, upward flow, and reduced evapotranspiration under stress.",
        "- Cutting or grazing changes canopy area and crop coefficients. July 4 does not represent the same regrowth stage in every year.",
        "- There is one strip per treatment within each irrigation regime, so the paired differences are descriptive rather than independently replicated treatment effects.",
        "",
        "## Reproducibility",
        "",
        "Run from the repository root:",
        "",
        "```bash",
        ".biochar_py313/bin/python -m biochar_app.scripts.management.irrigation_analysis.analyze_irrigation_cessation",
        "```",
        "",
        "Supporting tables:",
        "",
        f"- [`irrigation_cessation_july4_strip_summary_{REPORT_STAMP}.csv`](supporting_tables/irrigation_cessation_july4_strip_summary_{REPORT_STAMP}.csv)",
        f"- [`irrigation_cessation_july4_pair_summary_{REPORT_STAMP}.csv`](supporting_tables/irrigation_cessation_july4_pair_summary_{REPORT_STAMP}.csv)",
        "",
    ])
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    """Write one stable machine-readable result table."""
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, float_format="%.6f")
    print(f"Wrote {len(frame):,} rows: {path}")


def main() -> None:
    """Run the July 4 irrigation-cessation screening calculation."""
    annual = [build_year_scenario(load_logger_year(year), year=year) for year in YEARS]
    strip_summary = pd.concat(annual, ignore_index=True)
    pair_summary = build_pair_summary(strip_summary)

    write_csv(strip_summary, OUTPUT_DIR / "july4_strip_summary.csv")
    write_csv(pair_summary, OUTPUT_DIR / "july4_pair_summary.csv")
    write_csv(
        strip_summary,
        SUPPORTING_TABLE_DIR
        / f"irrigation_cessation_july4_strip_summary_{REPORT_STAMP}.csv",
    )
    write_csv(
        pair_summary,
        SUPPORTING_TABLE_DIR
        / f"irrigation_cessation_july4_pair_summary_{REPORT_STAMP}.csv",
    )
    write_report(strip_summary, pair_summary)
    print(f"Wrote report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
