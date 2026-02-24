#!/usr/bin/env python3
"""
thresholds.py

Centralized value-thresholding + bounds enforcement for ETL.

Design goal:
- Put all human-editable thresholds near the top.
- Keep the implementation below.

Usage in etl.py (recommended order):
  1) scale_vwc_to_percent(df)
  2) convert_soil_t_to_fahrenheit(df)
  3) df, report = apply_value_bounds(df, year=year, collect_examples=5)

Notes:
- DEFAULT_BAD_VALUE_THRESHOLD uses Python numeric separators:
    10_000.0 == 10000.0
  The underscore is just a readability aid.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Pattern, Sequence, Tuple

import re
import pandas as pd

NAN = float("nan")

# ============================================================================
# HUMAN-EDITABLE THRESHOLDS (edit these)
# ============================================================================

# 1) Sentinel / placeholder masking
# Typical Campbell placeholders are around +/-9999, 6999, etc.
# 10_000.0 == 10000.0 (underscore is just readability sugar)
DEFAULT_BAD_VALUE_THRESHOLD: float = 10_000.0

# 2) Column-family bounds (applies to many columns by regex pattern)
#
# IMPORTANT:
# - VWC bounds here are for *percent*, because your ETL scales VWC x100.
# - T bounds here are for *Fahrenheit*, because your ETL converts C->F.
#
# The "pattern" is a regex matched against column names.
COLUMN_FAMILY_BOUNDS: List[Dict[str, Any]] = [
    {
        "name": "VWC raw (%)",
        "pattern": r"^VWC_\d+_raw_",
        "min": 1.0,      # you said 0 is too permissive; tune as needed
        "max": 80.0,     # tighten from 150; tune as needed
        "inclusive": True,
        "mask_to_nan": True,
    },
    {
        "name": "Soil temperature raw (°F)",
        "pattern": r"^T_\d+_raw_",
        "min": -30.0,
        "max": 110.0,
        "inclusive": True,
        "mask_to_nan": True,
    },
    {
        "name": "EC raw (dS/m)",
        "pattern": r"^EC_\d+_raw_",
        "min": -0.05,    # allow tiny negative sensor noise
        "max": 20.0,     # tune if you want tighter
        "inclusive": True,
        "mask_to_nan": True,
    },
]

# 3) Exact-column bounds (one-off columns)
# Key = exact column name, not regex.
EXPLICIT_COLUMN_BOUNDS: Dict[str, Dict[str, Any]] = {
    # Example:
    # "temp_air_degF": {"min": -40.0, "max": 120.0, "inclusive": True, "mask_to_nan": True},
}

# ============================================================================
# IMPLEMENTATION (mostly do not edit)
# ============================================================================


@dataclass(frozen=True)
class BoundRule:
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    inclusive: bool = True
    mask_to_nan: bool = True
    label: str = ""


@dataclass(frozen=True)
class ColumnFamilyRule:
    pattern: Pattern[str]
    rule: BoundRule


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _compile_family_rules() -> List[ColumnFamilyRule]:
    compiled: List[ColumnFamilyRule] = []
    for spec in COLUMN_FAMILY_BOUNDS:
        compiled.append(
            ColumnFamilyRule(
                pattern=re.compile(spec["pattern"]),
                rule=BoundRule(
                    min_value=spec.get("min"),
                    max_value=spec.get("max"),
                    inclusive=bool(spec.get("inclusive", True)),
                    mask_to_nan=bool(spec.get("mask_to_nan", True)),
                    label=str(spec.get("name", "")),
                ),
            )
        )
    return compiled


def _compile_explicit_rules() -> Dict[str, BoundRule]:
    out: Dict[str, BoundRule] = {}
    for explicit_col, spec in EXPLICIT_COLUMN_BOUNDS.items():
        out[explicit_col] = BoundRule(
            min_value=spec.get("min"),
            max_value=spec.get("max"),
            inclusive=bool(spec.get("inclusive", True)),
            mask_to_nan=bool(spec.get("mask_to_nan", True)),
            label=str(spec.get("name", explicit_col)),
        )
    return out


def _collect_examples(
    df: pd.DataFrame,
    is_violation: pd.Series,
    example_col: str,
    limit: int,
) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []

    idx = df.index[is_violation][:limit]
    has_ts = "timestamp" in df.columns

    out: List[Dict[str, Any]] = []
    for i in idx:
        out.append(
            {
                "timestamp": (df.loc[i, "timestamp"] if has_ts else None),
                "value": df.loc[i, example_col],
            }
        )
    return out


def apply_value_bounds(
    df: pd.DataFrame,
    *,
    year: int,
    # You can override these if you want custom behavior in a test
    bad_value_threshold: Optional[float] = DEFAULT_BAD_VALUE_THRESHOLD,
    bad_value_cols: Optional[Sequence[str]] = None,
    family_rules: Optional[Sequence[ColumnFamilyRule]] = None,
    explicit_rules: Optional[Dict[str, BoundRule]] = None,
    collect_examples: int = 0,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Apply sentinel masking + bounds enforcement.

    Returns:
      (df_out, report)
        report is a list of dicts: year, column, rule, violations, min/max, etc.
    """
    df_out = df.copy()
    report_list: List[Dict[str, Any]] = []

    # Use defaults from the human-readable policy section unless overridden
    compiled_family_rules = list(family_rules) if family_rules is not None else _compile_family_rules()
    compiled_explicit_rules = dict(explicit_rules) if explicit_rules is not None else _compile_explicit_rules()

    # -----------------------------
    # 0) Sentinel / placeholder masking
    # -----------------------------
    if bad_value_threshold is not None:
        if bad_value_cols is None:
            target_columns = [c for c in df_out.columns if c != "timestamp"]
        else:
            target_columns = [c for c in bad_value_cols if c in df_out.columns]

        threshold_value = float(bad_value_threshold)

        for target_col in target_columns:
            numeric_values = _coerce_numeric(df_out[target_col])
            bad_value_mask = numeric_values.abs() >= threshold_value
            bad_value_count = int(bad_value_mask.sum())

            if bad_value_count:
                threshold_report: Dict[str, Any] = {
                    "year": year,
                    "column": target_col,
                    "rule": "bad_value_threshold",
                    "label": f"|x| >= {threshold_value:g}",
                    "threshold": threshold_value,
                    "violations": bad_value_count,
                }

                if collect_examples > 0:
                    threshold_report["examples"] = _collect_examples(
                        df_out, bad_value_mask, target_col, collect_examples
                    )

                report_list.append(threshold_report)
                df_out.loc[bad_value_mask, target_col] = NAN

    # -----------------------------
    # Helper: apply a BoundRule to one column
    # NOTE: we intentionally use UNIQUE local var names here to avoid
    # PyCharm "shadows name from outer scope" warnings.
    # -----------------------------
    def _apply_bound_rule_to_column(col_to_check: str, rule_to_apply: BoundRule, rule_tag: str) -> None:
        nonlocal df_out, report_list

        series_numeric = _coerce_numeric(df_out[col_to_check])

        if rule_to_apply.min_value is None and rule_to_apply.max_value is None:
            return

        if rule_to_apply.inclusive:
            too_low = (
                (series_numeric < rule_to_apply.min_value)
                if rule_to_apply.min_value is not None
                else False
            )
            too_high = (
                (series_numeric > rule_to_apply.max_value)
                if rule_to_apply.max_value is not None
                else False
            )
        else:
            too_low = (
                (series_numeric <= rule_to_apply.min_value)
                if rule_to_apply.min_value is not None
                else False
            )
            too_high = (
                (series_numeric >= rule_to_apply.max_value)
                if rule_to_apply.max_value is not None
                else False
            )

        out_of_bounds_mask = pd.Series(False, index=df_out.index)
        if isinstance(too_low, pd.Series):
            out_of_bounds_mask |= too_low
        if isinstance(too_high, pd.Series):
            out_of_bounds_mask |= too_high

        out_of_bounds_count = int(out_of_bounds_mask.sum())
        if not out_of_bounds_count:
            return

        bounds_report: Dict[str, Any] = {
            "year": year,
            "column": col_to_check,
            "rule": rule_tag,
            "label": rule_to_apply.label or "",
            "min": rule_to_apply.min_value,
            "max": rule_to_apply.max_value,
            "inclusive": rule_to_apply.inclusive,
            "violations": out_of_bounds_count,
        }

        if collect_examples > 0:
            bounds_report["examples"] = _collect_examples(
                df_out, out_of_bounds_mask, col_to_check, collect_examples
            )

        report_list.append(bounds_report)

        if rule_to_apply.mask_to_nan:
            df_out.loc[out_of_bounds_mask, col_to_check] = NAN

    # -----------------------------
    # 1) Explicit column rules
    # -----------------------------
    for explicit_col_name, explicit_bound in compiled_explicit_rules.items():
        if explicit_col_name in df_out.columns:
            _apply_bound_rule_to_column(explicit_col_name, explicit_bound, rule_tag="explicit")

    # -----------------------------
    # 2) Column family rules
    # -----------------------------
    all_columns = list(df_out.columns)
    for family_rule in compiled_family_rules:
        for candidate_col in all_columns:
            if candidate_col == "timestamp":
                continue
            if family_rule.pattern.search(candidate_col):
                _apply_bound_rule_to_column(
                    candidate_col,
                    family_rule.rule,
                    rule_tag=f"family:{family_rule.pattern.pattern}",
                )

    return df_out, report_list