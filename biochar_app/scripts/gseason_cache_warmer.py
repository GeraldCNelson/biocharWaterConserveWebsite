"""Precompute standard current-year seasonal summaries after publication."""

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
)
from biochar_app.scripts.data_loading import clear_logger_data_cache, load_logger_data
from biochar_app.scripts.gseason_materialized_cache import save_materialized_gseason_summary
from biochar_app.scripts.gseason_utils import compute_period_summary_rows, periods_to_list_of_dicts


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
    periods = periods_to_list_of_dicts(DEFAULT_GSEASON_PERIODS, preserve_year=True)
    variable_values = tuple(str(value) for value in variables)
    strip_values = tuple(str(value) for value in strips)
    depth_values = tuple(str(value) for value in depths)
    unit_values = tuple(str(value) for value in unit_systems)

    # Operational ETL runs in a subprocess, so discard any frame loaded before
    # publication and read the newly written parquet files.
    clear_logger_data_cache()
    frame = load_logger_data(int(year), "15min")

    computed_configurations = 0
    cache_entries = 0
    total_rows = 0
    for variable in variable_values:
        for strip in strip_values:
            for depth in depth_values:
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
                for unit_system in unit_values:
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
        "cache_entries": cache_entries,
        "summary_rows": total_rows,
        "duration_seconds": round(perf_counter() - started, 3),
    }


def main(argv: list[str] | None = None) -> int:
    """Warm the standard cache on demand, primarily for deployment checks."""
    parser = argparse.ArgumentParser(
        description="Precompute standard seasonal-summary caches for one year."
    )
    parser.add_argument("--year", type=int, required=True, help="Anchor year to process")
    args = parser.parse_args(argv)
    print(json.dumps(warm_standard_gseason_cache(args.year), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
