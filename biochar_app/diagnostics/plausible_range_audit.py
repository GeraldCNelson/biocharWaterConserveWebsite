"""Audit proposed VWC and soil-temperature plausibility limits.

This module is intentionally read-only: it examines the processed 15-minute
logger data and reports observations that would be masked if the proposed
limits were adopted. It does not change the ETL thresholds or source data.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd

from biochar_app.config.core import YEARS
from biochar_app.scripts.data_loading import load_logger_data


DEFAULT_VWC_MAX_PERCENT = 50.0
DEFAULT_SOIL_TEMPERATURE_MIN_F = -10.0
DEFAULT_OUTPUT_DIR = Path("biochar_app/diagnostics/reports/plausible_ranges")

_SENSOR_COLUMN_RE = re.compile(
    r"^(?P<variable>VWC|T)_(?P<depth>\d+)_raw_(?P<strip>S\d+)_(?P<sensor>[A-Z]+)$"
)


def audit_frame(
    frame: pd.DataFrame,
    *,
    year: int,
    vwc_max_percent: float = DEFAULT_VWC_MAX_PERCENT,
    soil_temperature_min_f: float = DEFAULT_SOIL_TEMPERATURE_MIN_F,
) -> list[dict[str, Any]]:
    """Return one threshold-audit record for every VWC and temperature sensor."""
    timestamp = pd.to_datetime(frame.get("timestamp"), errors="coerce")
    records: list[dict[str, Any]] = []

    for column in sorted(frame.columns):
        match = _SENSOR_COLUMN_RE.match(str(column))
        if not match:
            continue

        variable = match.group("variable")
        values = pd.to_numeric(frame[column], errors="coerce")
        valid = values.notna()
        if variable == "VWC":
            threshold = float(vwc_max_percent)
            direction = "above"
            flagged = valid & (values > threshold)
            units = "%"
        else:
            threshold = float(soil_temperature_min_f)
            direction = "below"
            flagged = valid & (values < threshold)
            units = "degF"

        valid_n = int(valid.sum())
        flagged_n = int(flagged.sum())
        flagged_times = timestamp[flagged]
        records.append(
            {
                "year": int(year),
                "variable": variable,
                "strip": match.group("strip"),
                "depth_code": match.group("depth"),
                "sensor_location": match.group("sensor"),
                "column": str(column),
                "units": units,
                "threshold": threshold,
                "direction": direction,
                "valid_observations": valid_n,
                "flagged_observations": flagged_n,
                "flagged_percent": round(flagged_n / valid_n * 100.0, 4) if valid_n else 0.0,
                "observed_min": float(values[valid].min()) if valid_n else None,
                "observed_max": float(values[valid].max()) if valid_n else None,
                "first_flagged_timestamp": flagged_times.min().isoformat() if flagged_n else None,
                "last_flagged_timestamp": flagged_times.max().isoformat() if flagged_n else None,
            }
        )

    return records


def run_audit(
    years: Iterable[int],
    *,
    vwc_max_percent: float = DEFAULT_VWC_MAX_PERCENT,
    soil_temperature_min_f: float = DEFAULT_SOIL_TEMPERATURE_MIN_F,
    loader: Callable[[int, str], pd.DataFrame] = load_logger_data,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Audit each requested year and return detailed and summarized results."""
    requested_years = [int(year) for year in years]
    records: list[dict[str, Any]] = []
    for year in requested_years:
        records.extend(
            audit_frame(
                loader(year, "15min"),
                year=year,
                vwc_max_percent=vwc_max_percent,
                soil_temperature_min_f=soil_temperature_min_f,
            )
        )

    details = pd.DataFrame.from_records(records)
    variable_summary: dict[str, dict[str, Any]] = {}
    if not details.empty:
        for variable, group in details.groupby("variable", sort=True):
            valid_n = int(group["valid_observations"].sum())
            flagged_n = int(group["flagged_observations"].sum())
            variable_summary[str(variable)] = {
                "valid_observations": valid_n,
                "flagged_observations": flagged_n,
                "flagged_percent": round(flagged_n / valid_n * 100.0, 4) if valid_n else 0.0,
                "affected_sensors": int((group["flagged_observations"] > 0).sum()),
                "sensors_checked": int(len(group)),
            }

    summary = {
        "status": "audited",
        "years": requested_years,
        "thresholds": {
            "vwc_max_percent": float(vwc_max_percent),
            "soil_temperature_min_f": float(soil_temperature_min_f),
        },
        "variables": variable_summary,
    }
    return details, summary


def write_audit(
    details: pd.DataFrame,
    summary: dict[str, Any],
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write detailed CSV and summary JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "plausible_range_audit.csv"
    json_path = output_dir / "plausible_range_audit_summary.json"
    details.to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return csv_path, json_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Count observations affected by proposed VWC and soil-temperature limits."
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--year", type=int, help="Year to audit")
    selection.add_argument("--all-years", action="store_true", help="Audit all configured years")
    parser.add_argument("--vwc-max", type=float, default=DEFAULT_VWC_MAX_PERCENT)
    parser.add_argument(
        "--soil-temperature-min-f",
        type=float,
        default=DEFAULT_SOIL_TEMPERATURE_MIN_F,
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)

    years = YEARS if args.all_years else [args.year]
    details, summary = run_audit(
        years,
        vwc_max_percent=args.vwc_max,
        soil_temperature_min_f=args.soil_temperature_min_f,
    )
    csv_path, json_path = write_audit(details, summary, args.output_dir)
    summary["detail_csv"] = str(csv_path)
    summary["summary_json"] = str(json_path)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
