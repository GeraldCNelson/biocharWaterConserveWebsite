#!/usr/bin/env python3
"""
smoke_downloads.py — API smoke tests for Biochar dashboard downloads.

Purpose
-------
This script performs a lightweight, systematic check of the download API routes
without using the browser. It verifies that plot and summary downloads return
successful responses, usable ZIP files, README.txt files, and expected filenames.

It is intended as a pre-commit or pre-deploy smoke test after changes to:
- routes.py download endpoints
- downloads.js endpoint wiring
- README generation
- experiment metadata
- filename conventions

Prerequisites
-------------
Start the local FastAPI app first, for example:

    uvicorn biochar_app.scripts.app:app --reload

Then run this script from the project root:

    python biochar_app/scripts/dev-tools/smoke_downloads.py

Expected output
---------------
The script prints one section per test case. A successful run ends with:

    ✅ Smoke test completed

If an assertion fails, the script stops and prints the failing condition.
Common failures include:
- non-200 API response
- missing README.txt in ZIP
- missing CSV file in ZIP
- filename does not match expected grouping convention
"""

from __future__ import annotations

from io import BytesIO
import zipfile

import requests

from biochar_app.config.core import (
    GRANULARITIES,
    STRIPS,
    VARIABLES,
)

from biochar_app.config.experiment_config import (
    LOGGER_LOCATIONS,
    SENSOR_DEPTH_CODES,
)

BASE_URL = "http://127.0.0.1:8000"


# ---------------------------------------------------------------------
# Smoke-test metadata
# ---------------------------------------------------------------------
# These values should represent a small, stable set of known-good selections.
# They are intentionally not exhaustive; the goal is a fast regression check.

SMOKE_YEARS = [2025]
SMOKE_VARIABLES = ["VWC", "SWC", "EC", "T"]
SMOKE_GRANULARITIES = ["daily", "monthly"]
SMOKE_STRIPS = ["S1", "S3"]
SMOKE_DEPTHS = SENSOR_DEPTH_CODES
SMOKE_LOGGER_LOCATIONS = LOGGER_LOCATIONS

DEFAULT_YEAR = 2025
DEFAULT_VARIABLE = "VWC"
DEFAULT_STRIP = "S1"
DEFAULT_DEPTH = "1"
DEFAULT_LOGGER_LOCATION = "T"
DEFAULT_GRANULARITY = "daily"
DEFAULT_UNIT_SYSTEM = "us"
README_PREVIEW_LINES = 50

PLOT_TEST_CASES = [
    {
        "name": "Depth grouped raw VWC",
        "downloadType": "raw",
        "traceOption": "depth",
        "variable": "VWC",
    },
    {
        "name": "Depth grouped ratio VWC",
        "downloadType": "ratio",
        "traceOption": "depth",
        "variable": "VWC",
    },
    {
        "name": "Depth grouped all VWC",
        "downloadType": "all",
        "traceOption": "depth",
        "variable": "VWC",
    },
    {
        "name": "Location grouped raw VWC",
        "downloadType": "raw",
        "traceOption": "loggerLocation",
        "variable": "VWC",
    },
    {
        "name": "Location grouped ratio VWC",
        "downloadType": "ratio",
        "traceOption": "loggerLocation",
        "variable": "VWC",
    },
    {
        "name": "Location grouped all VWC",
        "downloadType": "all",
        "traceOption": "loggerLocation",
        "variable": "VWC",
    },
    {
        "name": "Temperature special case",
        "downloadType": "all",
        "traceOption": "depth",
        "variable": "T",
    },
]

SUMMARY_TEST_CASES = [
    {"name": "Summary raw CSV", "mode": "raw", "variable": "VWC"},
    {"name": "Summary ratio CSV", "mode": "ratio", "variable": "VWC"},
    {"name": "Summary ZIP", "mode": "zip", "variable": "VWC"},
    {"name": "Summary temperature ZIP", "mode": "zip", "variable": "T"},
]

# ---------------------------------------------------------------------
# Filename validation rules
# ---------------------------------------------------------------------
EXPECTED_FILENAME_RULES = {
    "depth": {
        "must_contain": ["logger"],
        "must_not_contain": ["depth1", "depth2", "depth3"],
    },
    "loggerLocation": {
        "must_contain": ["depth"],
        "must_not_contain": ["loggerT", "loggerM", "loggerB"],
    },
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def print_banner(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def content_disposition_filename(response: requests.Response) -> str:
    header = response.headers.get("Content-Disposition", "")
    marker = 'filename="'
    if marker in header:
        return header.split(marker, 1)[1].split('"', 1)[0]
    return header


def assert_filename_rules(filename: str, trace_option: str) -> None:
    if trace_option == "depth":
        assert "logger" in filename, (
            f"Depth-grouped download should include logger location in filename: {filename}"
        )
        assert "_depth" not in filename, (
            f"Depth-grouped download should not use selected depth in filename: {filename}"
        )

    elif trace_option == "loggerLocation":
        assert "depth" in filename, (
            f"Logger-location-grouped download should include depth in filename: {filename}"
        )
        assert "logger" not in filename, (
            f"Logger-location-grouped download should not use logger in filename: {filename}"
        )


def preview_text(text: str, max_lines: int = 35) -> str:
    lines = text.splitlines()
    shown = lines[:max_lines]
    if len(lines) > max_lines:
        shown.append("... [README preview truncated]")
    return "\n".join(shown)


def validate_filename_rules(
    filename: str,
    trace_option: str,
) -> None:
    rules = EXPECTED_FILENAME_RULES[trace_option]

    for token in rules["must_contain"]:
        assert token in filename, (
            f"Expected '{token}' in filename: {filename}"
        )

    for token in rules["must_not_contain"]:
        assert token not in filename, (
            f"Did not expect '{token}' in filename: {filename}"
        )


def validate_zip(
    response: requests.Response,
    *,
    trace_option: str | None = None,
) -> None:
    assert response.status_code == 200, (
        f"Expected HTTP 200, got {response.status_code}: "
        f"{response.text[:500]}"
    )

    filename = content_disposition_filename(response)
    print(f"Download filename: {filename}")

    if trace_option is not None:
        validate_filename_rules(filename, trace_option)

    zf = zipfile.ZipFile(BytesIO(response.content))
    names = zf.namelist()

    print("ZIP contents:")
    for name in names:
        print(f"  {name}")

    assert "README.txt" in names, "README.txt missing from ZIP"

    csv_files = [name for name in names if name.endswith(".csv")]
    assert csv_files, "No CSV files found in ZIP"

    readme = zf.read("README.txt").decode("utf-8")

    print("\nREADME preview:")
    print("-" * 40)
    print(preview_text(readme, README_PREVIEW_LINES))
    print("-" * 40)

def validate_csv_response(response: requests.Response) -> None:
    assert response.status_code == 200, (
        f"Expected HTTP 200, got {response.status_code}: {response.text[:500]}"
    )

    filename = content_disposition_filename(response)
    print(f"Download filename: {filename}")

    assert filename.endswith(".csv"), f"Expected CSV filename, got: {filename}"

    text = response.text
    assert text.strip(), "CSV response was empty"

    first_line = text.splitlines()[0] if text.splitlines() else ""
    print(f"CSV header: {first_line}")


# ---------------------------------------------------------------------
# Plot downloads
# ---------------------------------------------------------------------

def test_plot_download(case: dict[str, str]) -> None:
    payload = {
        "year": DEFAULT_YEAR,
        "variable": case.get("variable", DEFAULT_VARIABLE),
        "strip": DEFAULT_STRIP,
        "depth": DEFAULT_DEPTH,
        "loggerLocation": DEFAULT_LOGGER_LOCATION,
        "granularity": DEFAULT_GRANULARITY,
        "unitSystem": DEFAULT_UNIT_SYSTEM,
        "downloadType": case["downloadType"],
        "traceOption": case["traceOption"],
        "startDate": "2025-01-01",
        "endDate": "2025-12-31",
    }

    print_banner(f"PLOT DOWNLOAD: {case['name']}")

    response = requests.post(
        f"{BASE_URL}/api/download_plot_data",
        json=payload,
        timeout=60,
    )

    validate_zip(response, trace_option=case["traceOption"])


# ---------------------------------------------------------------------
# Summary downloads
# ---------------------------------------------------------------------

def get_summary_stats(variable: str = DEFAULT_VARIABLE) -> dict:
    payload = {
        "year": DEFAULT_YEAR,
        "variable": variable,
        "strip": DEFAULT_STRIP,
        "depth": DEFAULT_DEPTH,
        "granularity": DEFAULT_GRANULARITY,
        "unitSystem": DEFAULT_UNIT_SYSTEM,
    }

    response = requests.post(
        f"{BASE_URL}/api/get_summary_stats",
        json=payload,
        timeout=60,
    )

    response.raise_for_status()
    return response.json()


def test_summary_download(case: dict[str, str]) -> None:
    variable = case.get("variable", DEFAULT_VARIABLE)
    mode = case["mode"]
    summary_stats = get_summary_stats(variable)

    payload = {
        "year": DEFAULT_YEAR,
        "variable": variable,
        "strip": DEFAULT_STRIP,
        "depth": DEFAULT_DEPTH,
        "granularity": DEFAULT_GRANULARITY,
        "unitSystem": DEFAULT_UNIT_SYSTEM,
        "mode": mode,
        "summaryStats": summary_stats,
    }

    print_banner(f"SUMMARY DOWNLOAD: {case['name']}")

    response = requests.post(
        f"{BASE_URL}/api/download_summary_data",
        json=payload,
        timeout=60,
    )

    if mode == "zip":
        validate_zip(response)
    else:
        validate_csv_response(response)


# ---------------------------------------------------------------------
# Metadata sanity checks
# ---------------------------------------------------------------------

def validate_smoke_metadata() -> None:
    assert DEFAULT_YEAR in SMOKE_YEARS
    assert DEFAULT_VARIABLE in VARIABLES
    assert DEFAULT_STRIP in STRIPS
    assert DEFAULT_DEPTH in SENSOR_DEPTH_CODES
    assert DEFAULT_LOGGER_LOCATION in LOGGER_LOCATIONS

    granularity_values = [g[0] if isinstance(g, tuple) else g for g in GRANULARITIES]
    assert DEFAULT_GRANULARITY in granularity_values


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    validate_smoke_metadata()

    for case in PLOT_TEST_CASES:
        test_plot_download(case)

    for case in SUMMARY_TEST_CASES:
        test_summary_download(case)

    print("\n✅ Smoke test completed")


if __name__ == "__main__":
    main()