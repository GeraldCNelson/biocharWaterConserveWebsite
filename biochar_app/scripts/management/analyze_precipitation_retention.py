#!/usr/bin/env python3
"""Build systematic independent-precipitation VWC retention diagnostics.

Run from the repository root:

    python biochar_app/scripts/management/analyze_precipitation_retention.py

Candidate precipitation comes from the weather station. Freeze eligibility is
based only on each VWC sensor's co-located CS650 logger temperature.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from biochar_app.config.paths import (
    HOLDING_CAPACITY_DIR,
    IRRIGATION_DIAGNOSTICS_DIR,
    IRRIGATION_PRODUCTION_CSV,
    PARQUET_SUMMARY_15MIN_DIR,
    PARQUET_SUMMARY_WEATHER_15MIN_DIR,
    PRECIPITATION_RETENTION_DIR,
)
from biochar_app.scripts.management.irrigation_analysis.precipitation_retention import (
    PrecipitationRetentionConfig,
    analyze_precipitation_sensor_responses,
    bootstrap_matched_profile_differences,
    bootstrap_unmatched_profile_differences,
    build_combined_empirical_capacity,
    build_irrigation_capacity_observations,
    build_irrigation_available_storage_comparison,
    build_irrigation_operational_diagnostics,
    build_matched_complete_profiles,
    build_paired_treatment_summary,
    build_profile_retention,
    build_profile_best_results,
    build_zone_upper_retained_water,
    compare_matched_and_unmatched_bootstraps,
    flag_irrigation_overlap,
    identify_precipitation_events,
    save_event_pair_plots,
    save_empirical_maxima_map,
    summarize_empirical_maxima,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", nargs="+", type=int, default=[2024, 2025, 2026])
    parser.add_argument(
        "--irrigation-years", nargs="+", type=int,
        default=[2023, 2024, 2025, 2026],
        help="Irrigation years included in the combined capacity table.",
    )
    parser.add_argument("--output-dir", type=Path, default=PRECIPITATION_RETENTION_DIR)
    parser.add_argument("--no-plots", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PrecipitationRetentionConfig()
    irrigation = pd.read_csv(IRRIGATION_PRODUCTION_CSV)
    event_tables: list[pd.DataFrame] = []
    result_tables: list[pd.DataFrame] = []
    logger_by_year: dict[int, pd.DataFrame] = {}

    for year in args.years:
        logger_path = PARQUET_SUMMARY_15MIN_DIR / f"{year}_15min.parquet"
        weather_path = PARQUET_SUMMARY_WEATHER_15MIN_DIR / f"{year}_15min.parquet"
        if not logger_path.exists() or not weather_path.exists():
            print(f"Skipping {year}: missing logger or weather parquet")
            continue
        logger = pd.read_parquet(logger_path)
        weather = pd.read_parquet(weather_path)
        events = flag_irrigation_overlap(
            identify_precipitation_events(weather, config=config),
            irrigation,
            config=config,
        )
        results = analyze_precipitation_sensor_responses(logger, events, config=config)
        event_tables.append(events)
        result_tables.append(results)
        logger_by_year[year] = logger

    output_dir: Path = args.output_dir
    figures_dir = output_dir / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)
    events = pd.concat(event_tables, ignore_index=True) if event_tables else pd.DataFrame()
    results = pd.concat(result_tables, ignore_index=True) if result_tables else pd.DataFrame()
    profiles = build_profile_retention(results) if not results.empty else pd.DataFrame()
    paired = build_paired_treatment_summary(results) if not results.empty else pd.DataFrame()
    maxima = summarize_empirical_maxima(results) if not results.empty else pd.DataFrame()

    irrigation_event_tables: list[pd.DataFrame] = []
    irrigation_qc_tables: list[pd.DataFrame] = []
    irrigation_arrival_tables: list[pd.DataFrame] = []
    for year in args.irrigation_years:
        event_path = (
            HOLDING_CAPACITY_DIR
            / f"debug_irrigation_events_{year}_all_loggers_all_depths.csv"
        )
        qc_path = (
            IRRIGATION_DIAGNOSTICS_DIR
            / f"trustworthy_irrigation_events_all_positions_{year}.csv"
        )
        if event_path.exists() and qc_path.exists():
            irrigation_event_tables.append(pd.read_csv(event_path))
            irrigation_qc_tables.append(pd.read_csv(qc_path))
            arrival_path = (
                IRRIGATION_DIAGNOSTICS_DIR
                / f"irrigation_arrival_times_{year}.csv"
            )
            if arrival_path.exists():
                irrigation_arrival_tables.append(pd.read_csv(arrival_path))
        else:
            print(f"Skipping combined irrigation evidence for {year}: missing event or QC table")
    irrigation_events = (
        pd.concat(irrigation_event_tables, ignore_index=True)
        if irrigation_event_tables else pd.DataFrame()
    )
    irrigation_qc = (
        pd.concat(irrigation_qc_tables, ignore_index=True)
        if irrigation_qc_tables else pd.DataFrame()
    )
    irrigation_arrivals = (
        pd.concat(irrigation_arrival_tables, ignore_index=True)
        if irrigation_arrival_tables else pd.DataFrame()
    )
    irrigation_capacity = build_irrigation_capacity_observations(
        irrigation_events, irrigation_qc
    )
    combined_observations, combined_capacity = build_combined_empirical_capacity(
        results, irrigation_capacity
    )
    complete_profiles, matched_profiles = build_matched_complete_profiles(
        combined_observations
    )
    bootstrap_summary = bootstrap_matched_profile_differences(matched_profiles)
    unmatched_bootstrap_summary = bootstrap_unmatched_profile_differences(
        complete_profiles
    )
    bootstrap_comparison = compare_matched_and_unmatched_bootstraps(
        bootstrap_summary, unmatched_bootstrap_summary
    )
    best_results = build_profile_best_results(bootstrap_comparison)
    zone_upper_retained_water = build_zone_upper_retained_water(combined_capacity)
    irrigation_available_storage = build_irrigation_available_storage_comparison(
        irrigation_events, irrigation_qc, combined_capacity
    )
    irrigation_operational_diagnostics = build_irrigation_operational_diagnostics(
        irrigation_available_storage, irrigation_arrivals, irrigation
    )

    events.to_csv(output_dir / "precipitation_event_catalog.csv", index=False)
    results.to_csv(output_dir / "precipitation_sensor_responses.csv", index=False)
    profiles.to_csv(output_dir / "precipitation_profile_retention.csv", index=False)
    paired.to_csv(output_dir / "precipitation_paired_treatment_events.csv", index=False)
    maxima.to_csv(output_dir / "precipitation_empirical_maxima.csv", index=False)
    save_empirical_maxima_map(
        maxima, output_dir / "precipitation_empirical_maxima_map.png"
    )
    combined_observations.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_capacity_observations.csv",
        index=False,
    )
    combined_capacity.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_capacity.csv", index=False
    )
    complete_profiles.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_complete_profiles.csv",
        index=False,
    )
    matched_profiles.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_matched_profiles.csv",
        index=False,
    )
    bootstrap_summary.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_profile_bootstrap.csv",
        index=False,
    )
    unmatched_bootstrap_summary.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_profile_unmatched_bootstrap.csv",
        index=False,
    )
    bootstrap_comparison.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_profile_bootstrap_comparison.csv",
        index=False,
    )
    best_results.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_profile_best_results.csv",
        index=False,
    )
    zone_upper_retained_water.to_csv(
        HOLDING_CAPACITY_DIR / "combined_empirical_zone_upper_retained_water.csv",
        index=False,
    )
    irrigation_available_storage.to_csv(
        HOLDING_CAPACITY_DIR / "irrigation_available_storage_comparison.csv",
        index=False,
    )
    irrigation_operational_diagnostics.to_csv(
        HOLDING_CAPACITY_DIR / "irrigation_operational_alert_diagnostics.csv",
        index=False,
    )
    save_empirical_maxima_map(
        combined_capacity,
        HOLDING_CAPACITY_DIR / "combined_empirical_capacity_map.png",
        title="Combined empirical upper retained VWC from precipitation and irrigation",
        event_label="eligible precipitation + irrigation events",
        include_profile=True,
    )

    plot_logs: list[pd.DataFrame] = []
    if not args.no_plots:
        for year, logger in logger_by_year.items():
            year_events = events.loc[events["year"].eq(year) & events["independent_event"]]
            if not year_events.empty:
                plot_logs.append(save_event_pair_plots(
                    logger, year_events, results, figures_dir / str(year)
                ))
    plot_log = pd.concat(plot_logs, ignore_index=True) if plot_logs else pd.DataFrame()
    plot_log.to_csv(output_dir / "precipitation_plot_log.csv", index=False)

    print("\n=== PRECIPITATION RETENTION ANALYSIS COMPLETE ===")
    print(f"Output: {output_dir}")
    print(f"Candidate events: {len(events)}")
    print(f"Independent events: {int(events.get('independent_event', pd.Series(dtype=bool)).sum())}")
    print(f"Sensor/event rows: {len(results)}")
    print(f"Retention-eligible rows: {int(results.get('retention_eligible', pd.Series(dtype=bool)).sum())}")
    print(f"Warm empirical-max eligible rows: {int(results.get('empirical_max_eligible', pd.Series(dtype=bool)).sum())}")
    print(f"Complete profile rows: {len(profiles)}")
    print(f"Matched treatment rows: {len(paired)}")
    print(f"Eligible irrigation-capacity rows: {len(irrigation_capacity)}")
    print(f"Combined empirical-capacity rows: {len(combined_capacity)}")
    print(f"Complete event-level profiles: {len(complete_profiles)}")
    print(f"Matched biochar-control profiles: {len(matched_profiles)}")
    print(f"Zone upper retained-water rows: {len(zone_upper_retained_water)}")
    print(f"Irrigation available-storage rows: {len(irrigation_available_storage)}")
    print(f"Irrigation operational-diagnostic rows: {len(irrigation_operational_diagnostics)}")
    if not irrigation_available_storage.empty:
        eligible = irrigation_available_storage["comparison_eligible"].fillna(False)
        exceeds = irrigation_available_storage["irrigation_exceeds_available_storage"].eq(True)
        print(f"Eligible irrigation comparisons: {int(eligible.sum())}")
        print(f"Applied volume exceeds available storage: {int((eligible & exceeds).sum())}")
        alert = irrigation_operational_diagnostics.get(
            "candidate_alert_before_irrigation_end", pd.Series(dtype=bool)
        ).eq(True)
        print(f"Candidate inspection alerts before irrigation end: {int(alert.sum())}")


if __name__ == "__main__":
    main()
