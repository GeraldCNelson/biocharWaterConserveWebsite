"""Precompute and verify standard seasonal summaries after publication."""

from __future__ import annotations

import argparse
import json
from time import perf_counter
from typing import Any, Iterable

from biochar_app.config.core import (
    DEFAULT_GSEASON_PERIODS,
    SENSOR_DEPTH_CODES,
    STRIPS,
    VARIABLES,
    YEARS,
)
from biochar_app.scripts.data_loading import clear_logger_data_cache, load_logger_data
from biochar_app.scripts.gseason_materialized_cache import (
    load_materialized_gseason_summary,
    save_materialized_gseason_summary,
)
from biochar_app.scripts.gseason_utils import compute_period_summary_rows, periods_to_list_of_dicts


def _dated_default_periods(year: int) -> list[dict[str, str]]:
    """Expand standard month-day periods exactly as the browser editor does."""
    periods = periods_to_list_of_dicts(DEFAULT_GSEASON_PERIODS, preserve_year=True)
    dated: list[dict[str, str]] = []
    for period in periods:
        start = str(period["start"])[-5:]
        end = str(period["end"])[-5:]
        start_month = int(start[:2])
        end_month = int(end[:2])
        start_year = int(year) - 1 if start_month > end_month else int(year)
        dated.append({
            **period,
            "start": f"{start_year}-{start}",
            "end": f"{int(year)}-{end}",
        })
    return dated


def warm_standard_gseason_cache(
    year: int,
    *,
    variables: Iterable[str] = VARIABLES,
    strips: Iterable[str] = STRIPS,
    depths: Iterable[str] = SENSOR_DEPTH_CODES,
    unit_systems: Iterable[str] = ("us", "metric"),
) -> dict[str, Any]:
    """Materialize default seasonal summaries for every dashboard filter.

    The source fingerprint stored with each cache entry makes current-year
    results safe to reuse until the next publication changes the source files.
    """
    started = perf_counter()
    periods = _dated_default_periods(int(year))
    variable_values = tuple(str(value) for value in variables)
    strip_values = tuple(str(value) for value in strips)
    depth_values = tuple(str(value) for value in depths)
    unit_values = tuple(str(value) for value in unit_systems)

    computed_configurations = 0
    reused_configurations = 0
    cache_entries = 0
    reused_cache_entries = 0
    total_rows = 0
    frame = None
    for variable in variable_values:
        for strip in strip_values:
            for depth in depth_values:
                cached_by_unit = {
                    unit_system: load_materialized_gseason_summary(
                        year=int(year),
                        variable=variable,
                        strip=strip,
                        depth=depth,
                        unit_system=unit_system,
                        periods=periods,
                    )
                    for unit_system in unit_values
                }
                missing_units = [
                    unit_system
                    for unit_system, cached_rows in cached_by_unit.items()
                    if cached_rows is None
                ]
                if not missing_units:
                    reused_configurations += 1
                    reused_cache_entries += len(unit_values)
                    if unit_values:
                        total_rows += len(cached_by_unit[unit_values[0]] or [])
                    continue

                if frame is None:
                    # Operational ETL runs in a subprocess, so discard any
                    # frame loaded before publication and read current files.
                    clear_logger_data_cache()
                    frame = load_logger_data(int(year), "15min")
                rows = compute_period_summary_rows(
                    frame,
                    year=int(year),
                    periods=periods,
                    variable=variable,
                    strip=strip,
                    depth=depth,
                )
                computed_configurations += 1
                total_rows += len(rows)
                for unit_system in missing_units:
                    save_materialized_gseason_summary(
                        year=int(year),
                        variable=variable,
                        strip=strip,
                        depth=depth,
                        unit_system=unit_system,
                        periods=periods,
                        rows=rows,
                    )
                    cache_entries += 1

    return {
        "status": "warmed",
        "year": int(year),
        "computed_configurations": computed_configurations,
        "reused_configurations": reused_configurations,
        "cache_entries": cache_entries,
        "reused_cache_entries": reused_cache_entries,
        "summary_rows": total_rows,
        "duration_seconds": round(perf_counter() - started, 3),
    }


def warm_standard_gseason_caches(
    years: Iterable[int] = YEARS,
) -> dict[str, Any]:
    """Verify all configured years, computing only missing or stale entries."""
    started = perf_counter()
    results = [warm_standard_gseason_cache(int(year)) for year in years]
    return {
        "status": "warmed",
        "years": [result["year"] for result in results],
        "computed_configurations": sum(
            int(result["computed_configurations"]) for result in results
        ),
        "reused_configurations": sum(
            int(result["reused_configurations"]) for result in results
        ),
        "cache_entries": sum(int(result["cache_entries"]) for result in results),
        "reused_cache_entries": sum(
            int(result["reused_cache_entries"]) for result in results
        ),
        "summary_rows": sum(int(result["summary_rows"]) for result in results),
        "duration_seconds": round(perf_counter() - started, 3),
        "year_results": results,
    }


def main(argv: list[str] | None = None) -> int:
    """Warm the standard cache on demand, primarily for deployment checks."""
    parser = argparse.ArgumentParser(description="Precompute standard seasonal summaries.")
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--year", type=int, help="Anchor year to process")
    selection.add_argument(
        "--all-years",
        action="store_true",
        help="Verify every configured year and rebuild only missing or stale entries",
    )
    args = parser.parse_args(argv)
    result = (
        warm_standard_gseason_caches()
        if args.all_years
        else warm_standard_gseason_cache(args.year)
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
