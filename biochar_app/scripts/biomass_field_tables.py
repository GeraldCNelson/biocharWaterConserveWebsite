#!/usr/bin/env python3
"""
biomass_field_tables.py

Builds the **Biomass (Field Samples)** table payload(s) for the dashboard.

Expected input (wide CSV)
------------------------
- First column: field location (e.g., "S1M", "S1B", ...)
- Remaining columns: sampling dates (typically "YYYY-MM-DD")

Payload shape (STANDARD multi-set)
----------------------------------
{
  "title": "Biomass (Field Samples)",
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
from typing import Any, Dict, List

import pandas as pd
from biochar_app.scripts.csv_validation import normalize_dates

# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------

def _input_csv_path() -> Path:
    """
    Default CSV location for the cleaned wide-form Biomass Field dataset.
    Adjust this path if you move the processed CSV.
    """
    return Path("biochar_app/data-processed/field_biomass_dry_g_wide_clean.csv")


def _as_period_obj(p: str) -> Dict[str, str]:
    p = str(p).strip()
    return {"key": p, "label": p}


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def get_biomass_field_table_payload(
    csv_path: str | Path | None = None,
    min_year: int | None = None,
) -> Dict[str, Any]:
    """
    Build the wide-form Biomass Field table payload.

    - csv_path: optional; falls back to _input_csv_path()
    - min_year: optional; if provided, keep only sampling columns whose year >= min_year

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
            "title": "Biomass (Field Samples)",
            "sets": [],
            "error": f"Biomass CSV not found at: {csv_path}",
        }

    df = pd.read_csv(csv_path)

    if df.empty or df.shape[1] < 2:
        return {
            "title": "Biomass (Field Samples)",
            "sets": [],
            "error": f"Biomass CSV is empty or not wide-form: {csv_path}",
        }

    # Normalize first column name and values
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "location"})
    df["location"] = df["location"].astype(str).str.strip()

    # Build a robust mapping from "stripped string column name" -> actual df column name
    # This prevents silent failures if the CSV has headers like "2023-06-12 " (trailing space).
    col_map: Dict[str, Any] = {str(c).strip(): c for c in df.columns}

    # -----------------------------------------------------------------
    # Periods (date columns)
    # -----------------------------------------------------------------
    if min_year is not None:
        keep_cols: List[Any] = ["location"]
        kept_periods: List[str] = []

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
                "title": "Biomass (Field Samples)",
                "sets": [],
                "error": f"No biomass sampling columns found for year >= {min_year} in: {csv_path}",
            }

        df = df[keep_cols]
        period_keys: List[str] = kept_periods
    else:
        period_keys = [str(c).strip() for c in df.columns[1:]]

    # IMPORTANT: keep periods as STRINGS for max frontend compatibility.
    # (Some table renderers expect strings; others can handle objects.)
    periods = period_keys

    # -----------------------------------------------------------------
    # Rows (locations)
    # -----------------------------------------------------------------
    rows: List[str] = df["location"].tolist()
    row_labels: Dict[str, str] = {r: r for r in rows}

    # -----------------------------------------------------------------
    # Variables (frontend expects an explicit list)
    #
    # Key point:
    # - Many of your UI renderers treat variables like dropdown options: {value,label}
    # - Some older code uses {key,label}
    # So we provide BOTH "value" and "key" with the same var_key.
    # -----------------------------------------------------------------
    var_key = "dry_biomass_g"
    variables = [
        {
            "value": var_key,   # preferred by option-style renderers
            "key": var_key,     # backward compatibility
            "label": "Dry Biomass (g)",
        }
    ]

    # -----------------------------------------------------------------
    # Data: data[varKey][rowKey][periodKey] = value
    # -----------------------------------------------------------------
    data_block: Dict[str, Dict[str, Any]] = {}

    for _, r in df.iterrows():
        loc = str(r["location"]).strip()
        per_map: Dict[str, Any] = {}

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

    payload: Dict[str, Any] = {
        "title": "Biomass (Field Samples)",
        "sets": [
            {
                "key": "biomass_field_set1",
                "label": "Dry Biomass (g)",
                "variables": variables,
                "periods": periods,
                "rows": rows,
                "rowLabels": row_labels,
                "data": {var_key: data_block},
                "note": "Dry pasture biomass field samples collected ~3×/year. Values are grams (dry biomass).",
            }
        ],
    }
    return payload
