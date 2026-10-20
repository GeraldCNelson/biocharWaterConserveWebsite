#!/usr/bin/env python3
"""
biomass_field_tables.py

Builds the **Biomass Samples and Fertilizer Recommendations** payloads.

Expected input (wide CSV)
------------------------
- First column: field location (e.g., "S1M", "S1B", ...)
- Remaining columns: sampling dates (typically "YYYY-MM-DD")

Payload shape (STANDARD multi-set)
----------------------------------
{
  "title": "Biomass Samples and Fertilizer Recommendations",
  "sets": [
    {
      "key": "biomass_field_set1",
      "label": "Dry Biomass (g)",
      "note": "...",
      "periods":   [{"key": "...", "label": "..."}, ...],
      "variables": [{"key": "...", "label": "..."}, ...],
      "rows":      ["S1M", "S1B", ...],
      "rowLabels": {"S1M": "S1M", ...},
      "data": {
        "dry_biomass_g": {
          "S1M": {"2023-06-12": 12.3, ...},
          ...
        }
      }
    }
  ]
}

Why the explicit `variables` list matters:
------------------------------------------
Your JS renderer (tables.js -> renderOneSetFromPayload) loops over `set.variables`.
If it is missing/empty, the UI will show: "No variables available for this table set."
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from biochar_app.config.paths import BIOMASS_FIELD_CSV, FERTILIZER_CSV_OUT
from biochar_app.config.lab_specs import LabVarSpec
from biochar_app.scripts.tables.tables_common import build_variable_meta

import pandas as pd

# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------

def _input_csv_path() -> Path:
    """
    Default CSV location for the cleaned wide-form Biomass Field dataset.
    Adjust this path if you move the processed CSV.
    """
    return BIOMASS_FIELD_CSV

def _as_period_obj(p: str) -> dict[str, str]:
    p = str(p).strip()
    return {"key": p, "label": p}


def _year_from_period(period: str) -> int | None:
    """Return the four-digit year from a biomass sampling-period label."""
    text = str(period).strip()
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return int(parsed.year)


def _build_annual_strip_total_set(
    df: pd.DataFrame,
    period_keys: list[str],
    col_map: dict[str, Any],
) -> dict[str, Any] | None:
    """Sum dry field-sample weights by strip and calendar year."""
    years = sorted({year for period in period_keys if (year := _year_from_period(period)) is not None})
    if not years:
        return None

    rows = [f"S{strip}" for strip in range(1, 5)]
    totals: dict[str, dict[str, float | None]] = {
        strip: {str(year): None for year in years} for strip in rows
    }
    period_counts = {
        year: sum(_year_from_period(period) == year for period in period_keys)
        for year in years
    }

    for strip in rows:
        strip_rows = df.loc[
            df["location"].str.upper().str.match(rf"^{strip}[TMB]$", na=False)
        ]
        for year in years:
            year_columns = [
                col_map.get(period, period)
                for period in period_keys
                if _year_from_period(period) == year
            ]
            values = pd.to_numeric(
                strip_rows[year_columns].stack(future_stack=True), errors="coerce"
            ).dropna()
            if not values.empty:
                totals[strip][str(year)] = round(float(values.sum()), 3)

    coverage = ", ".join(
        f"{year}: {period_counts[year]} sampling event{'s' if period_counts[year] != 1 else ''}"
        for year in years
    )
    last_period = max(
        (
            pd.to_datetime(period, errors="coerce")
            for period in period_keys
        ),
        default=pd.NaT,
    )
    through_text = ""
    if pd.notna(last_period):
        through_text = f" Data extend through {last_period.date().isoformat()}."

    variable_key = "annual_dry_biomass_total_g"
    return {
        "key": "biomass_field_annual_totals",
        "label": "Annual strip sample totals",
        "periods": [str(year) for year in years],
        "variables": [
            {
                "key": variable_key,
                "value": variable_key,
                "label": "Dry biomass total (g)",
                "decimal_places": 1,
            }
        ],
        "rows": rows,
        "rowLabels": {strip: strip for strip in rows},
        "data": {variable_key: totals},
        "note": (
            "Sum of dry biomass from the top, middle, and bottom field samples "
            "across all sampling events in each calendar year. These are sample "
            "weight totals, not whole-strip hay yields in tons per acre. Comparisons "
            "among years also require a consistent sampling area and protocol. "
            f"Coverage: {coverage}.{through_text}"
        ),
    }


def _build_fertilizer_context_sets(
    fertilizer_csv_path: Path,
    *,
    min_year: int | None,
) -> list[dict[str, Any]]:
    """Build recommendation and activity-date tables for biomass context."""
    if not fertilizer_csv_path.exists():
        return []

    frame = pd.read_csv(fertilizer_csv_path)
    required = {"year", "record_type"}
    if frame.empty or not required.issubset(frame.columns):
        return []

    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    if min_year is not None:
        frame = frame.loc[frame["year"] >= int(min_year)]

    years = sorted(int(year) for year in frame["year"].dropna().unique())
    if not years:
        return []

    sets: list[dict[str, Any]] = []
    recommendations = frame.loc[frame["record_type"] == "recommendation"].copy()
    nutrient_columns = {
        "N": "estimated_n_lb_per_acre",
        "P": "estimated_p_lb_per_acre",
        "K": "estimated_k_lb_per_acre",
    }
    if not recommendations.empty and {
        "strip",
        *nutrient_columns.values(),
    }.issubset(recommendations.columns):
        row_keys = [
            f"S{strip}_{nutrient}"
            for strip in range(1, 5)
            for nutrient in nutrient_columns
        ]
        values: dict[str, dict[str, float | None]] = {
            row_key: {str(year): None for year in years} for row_key in row_keys
        }
        for strip in range(1, 5):
            strip_key = f"S{strip}"
            for nutrient, column in nutrient_columns.items():
                row_key = f"{strip_key}_{nutrient}"
                for year in years:
                    matches = recommendations.loc[
                        (recommendations["year"] == year)
                        & (recommendations["strip"].astype(str).str.upper() == strip_key),
                        column,
                    ]
                    numeric = pd.to_numeric(matches, errors="coerce").dropna()
                    if not numeric.empty:
                        values[row_key][str(year)] = round(float(numeric.sum()), 3)

        variable_key = "recommended_elemental_nutrient_lb_per_acre"
        sets.append(
            {
                "key": "fertilizer_recommendations",
                "label": "Fertilizer recommendations",
                "periods": [str(year) for year in years],
                "variables": [
                    {
                        "key": variable_key,
                        "value": variable_key,
                        "label": "Recommended elemental nutrient (lb/acre)",
                        "decimal_places": 1,
                    }
                ],
                "rows": row_keys,
                "rowLabels": {
                    row_key: row_key.replace("_", " — ") for row_key in row_keys
                },
                "data": {variable_key: values},
                "note": (
                    "Recommendation-derived rates, not confirmed fertilizer applications. "
                    "N is elemental nitrogen. P and K are elemental mass converted from "
                    "fertilizer-label P2O5 and K2O. Values sum the product-level nutrient "
                    "contributions for each strip and year."
                ),
            }
        )

    activities = frame.loc[frame["record_type"] == "fertilization_activity"].copy()
    if not activities.empty and "date" in activities.columns:
        activity_values = {
            "field": {
                str(year): None
                for year in years
            }
        }
        for year in years:
            dates = activities.loc[activities["year"] == year, "date"].dropna()
            if not dates.empty:
                activity_values["field"][str(year)] = ", ".join(
                    sorted({str(date).strip() for date in dates if str(date).strip()})
                )

        variable_key = "recorded_fertilization_activity_date"
        sets.append(
            {
                "key": "fertilizer_activity_dates",
                "label": "Recorded fertilization activity dates",
                "periods": [str(year) for year in years],
                "variables": [
                    {
                        "key": variable_key,
                        "value": variable_key,
                        "label": "Recorded field activity date",
                    }
                ],
                "rows": ["field"],
                "rowLabels": {"field": "Entire field"},
                "data": {variable_key: activity_values},
                "note": (
                    "These are recorded field-level fertilization dates. The source does "
                    "not identify the products, quantities, rates, or strip-specific scope "
                    "associated with these dates."
                ),
            }
        )

    return sets

# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def get_biomass_field_table_payload(
    csv_path: str | Path | None = None,
    min_year: int | None = None,
    fertilizer_csv_path: str | Path | None = None,
) -> dict[str, Any]:
    """
    Build the wide-form Biomass Field table payload.

    - csv_path: optional; falls back to _input_csv_path()
    - min_year: optional; if provided, keep only sampling columns whose year >= min_year
    - fertilizer_csv_path: optional cleaned fertilizer CSV for contextual tables

    Notes
    -----
    - Expects first column = location, remaining columns = sampling dates (e.g., "2023-06-12")
    - Returns JSON-friendly payload with NaN -> None
    """
    # Resolve CSV path
    if csv_path is None:
        csv_path = _input_csv_path()
    else:
        csv_path = Path(csv_path)

    if not csv_path.exists():
        return {
            "title": "Biomass Samples and Fertilizer Recommendations",
            "sets": [],
            "error": f"Biomass CSV not found at: {csv_path}",
        }

    df = pd.read_csv(csv_path)

    if df.empty or df.shape[1] < 2:
        return {
            "title": "Biomass Samples and Fertilizer Recommendations",
            "sets": [],
            "error": f"Biomass CSV is empty or not wide-form: {csv_path}",
        }

    # Normalize first column name and values
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "location"})
    df["location"] = df["location"].astype(str).str.strip()

    # Build a robust mapping from "stripped string column name" -> actual df column name
    # This prevents silent failures if the CSV has headers like "2023-06-12 " (trailing space).
    col_map: dict[str, Any] = {str(c).strip(): c for c in df.columns}

    # -----------------------------------------------------------------
    # Periods (date columns)
    # -----------------------------------------------------------------
    if min_year is not None:
        keep_cols: list[Any] = ["location"]
        kept_periods: list[str] = []

        for c in df.columns[1:]:
            s = str(c).strip()

            # Fast-path: "YYYY..." columns
            year: int | None = None
            if len(s) >= 4 and s[:4].isdigit():
                year = int(s[:4])
            else:
                # Try pandas datetime parsing
                try:
                    dt = pd.to_datetime(s, errors="coerce")
                    if pd.notna(dt):
                        year = int(dt.year)
                except Exception:
                    year = None

            if year is not None and year >= int(min_year):
                keep_cols.append(c)
                kept_periods.append(s)

        if len(keep_cols) < 2:
            return {
                "title": "Biomass Samples and Fertilizer Recommendations",
                "sets": [],
                "error": f"No biomass sampling columns found for year >= {min_year} in: {csv_path}",
            }

        df = df[keep_cols]
        period_keys: list[str] = kept_periods
    else:
        period_keys = [str(c).strip() for c in df.columns[1:]]

    # IMPORTANT: keep periods as STRINGS for max frontend compatibility.
    # (Some table renderers expect strings; others can handle objects.)
    periods = period_keys

    # -----------------------------------------------------------------
    # Rows (locations)
    # -----------------------------------------------------------------
    rows: list[str] = df["location"].tolist()
    row_labels: dict[str, str] = {r: r for r in rows}

    # -----------------------------------------------------------------
    # Variables (frontend expects an explicit list)
    #
    # Key point:
    # - Many of your UI renderers treat variables like dropdown options: {value,label}
    # - Some older code uses {key,label}
    # So we provide BOTH "value" and "key" with the same var_key.
    # -----------------------------------------------------------------
    var_key = "dry_biomass_g"

    var_spec = LabVarSpec(
        key=var_key,
        label="Dry Biomass (g)",
        candidates=(var_key,),
        reference_key=None,  # no reference yet
    )

    var_meta = build_variable_meta(var_spec)

    variables = [
        {
            "value": var_key,  # keep existing UI behavior
            "decimal_places": 1,
            **var_meta,  # inject reference-ready metadata
        }
    ]

    # -----------------------------------------------------------------
    # Data: data[varKey][rowKey][periodKey] = value
    # -----------------------------------------------------------------
    data_block: dict[str, dict[str, Any]] = {}

    for _, r in df.iterrows():
        loc = str(r["location"]).strip()
        per_map: dict[str, Any] = {}

        for p in period_keys:
            # Use the *actual* df column name if it differs due to whitespace/etc.
            actual_col = col_map.get(p, p)
            val = r.get(actual_col, None)

            # Convert NaN -> None for JSON cleanliness
            if val is None or pd.isna(val):
                per_map[p] = None
            else:
                try:
                    per_map[p] = float(val)
                except Exception:
                    per_map[p] = None

        data_block[loc] = per_map

    annual_totals = _build_annual_strip_total_set(df, period_keys, col_map)
    sample_set = {
        "key": "biomass_field_samples",
        "label": "Sample-level measurements",
        "variables": variables,
        "periods": periods,
        "rows": rows,
        "rowLabels": row_labels,
        "data": {var_key: data_block},
        "note": "Dry pasture biomass field samples collected about three times per year. Values are grams of dry biomass.",
    }

    sets: list[dict[str, Any]] = []
    if annual_totals is not None:
        sets.append(annual_totals)
    sets.append(sample_set)

    resolved_fertilizer_path = (
        FERTILIZER_CSV_OUT
        if fertilizer_csv_path is None
        else Path(fertilizer_csv_path)
    )
    sets.extend(
        _build_fertilizer_context_sets(
            resolved_fertilizer_path,
            min_year=min_year,
        )
    )

    payload: dict[str, Any] = {
        "title": "Biomass Samples and Fertilizer Recommendations",
        "sets": sets,
    }
    return payload
