import os
import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict

from biochar_app.scripts.gseason_utils import compute_seasons, assign_gseason_periods

from biochar_app.scripts.config import (
    DATA_PROCESSED_DIR,
    VARIABLES,
    STRIPS,
    DEPTHS,
    DEFAULT_GSEASON_PERIODS,
    YEARS,
    PRECIP_COLS,
)

from biochar_app.scripts.routes_utils import load_logger_year

# Generate growing season summary statistics(min, mean, m
# ax, std) for each growing season period in a year
def generate_gseason_summary(
    year: int,
    periods: dict[str, dict[str, str]] | None = None,
    overwrite: bool = False,
) -> None:
    """
    Generate and persist growing-season summary for a given year.

    This function delegates the work to compute_seasons(), which:
      - loads the 15-minute logger data for `year`
      - applies each period in `periods` (defaulting to GSEASON_PERIODS)
      - computes raw and ratio statistics per variable/strip/depth
      - writes out a JSON file named gseason_summary_{year}.json
      - logs progress and respects the `overwrite` flag

    Args:
        year: calendar year to summarize.
        periods: mapping of period codes to { label, start, end }; if None,
                 uses the default GSEASON_PERIODS from config.
        overwrite: if True, regenerate even if the output JSON already exists.
    """
    compute_seasons(year=year, periods=periods or GSEASON_PERIODS, overwrite=overwrite)


def compute_summary_statistics(df, variable: str, strip: str, depth: str):
    """
    Compute summary statistics for raw and ratio values filtered by variable, strip, and depth.
    Returns two dictionaries: raw_stats and ratio_stats.
    """
    if not variable or not strip or not depth or df is None or df.empty:
        return {}, {}

    df = df.copy()
    raw_stats = {}
    ratio_stats = {}

    # Build expected prefixes
    raw_prefix = f"{variable}_{depth}_raw_{strip}_"
    ratio_prefixes = [
        f"{variable}_{depth}_ratio_S1_S2_",
        f"{variable}_{depth}_ratio_S3_S4_"
    ]

    # ✅ Compute RAW stats
    raw_cols = [col for col in df.columns if col.startswith(raw_prefix)]
    for col in raw_cols:
        series = df[col].dropna()
        if not series.empty:
            raw_stats[col] = {
                "min": round(series.min(), 4),
                "mean": round(series.mean(), 4),
                "max": round(series.max(), 4),
                "std": round(series.std(), 4),
            }

    # ✅ Compute RATIO stats
    for prefix in ratio_prefixes:
        for col in df.columns:
            if col.startswith(prefix):
                series = df[col].dropna()
                if not series.empty:
                    ratio_stats[col] = {
                        "min": round(series.min(), 4),
                        "mean": round(series.mean(), 4),
                        "max": round(series.max(), 4),
                        "std": round(series.std(), 4),
                    }

    return raw_stats, ratio_stats


def get_flat_gseason_summary(year: int) -> pd.DataFrame:
    """
    Flatten the nested dict into a DataFrame with columns:
      period_code, variable, strip, depth, location,
      plus raw_… statistics and ratio_… statistics all as separate columns.
    """
    nested = load_or_generate_gseason_summary(year)
    records = []

    for period_code, by_var in nested.items():
        # by_var is e.g. { "VWC": {"S1_D1": {...}, "S1_D2": {...}, …}, "EC": {…}, … }
        for var, by_strip_depth in by_var.items():
            # by_strip_depth is e.g. {"S1_D1": { "raw_statistics": {...}, "ratio_statistics": {...} }, …}
            for strip_depth, stats in by_strip_depth.items():
                strip, depth = strip_depth.split("_D", 1)
                # raw first
                raw_stats = stats.get("raw_statistics", {})
                for col_name, metrics in raw_stats.items():
                    # e.g. col_name = "VWC_1_raw_S1_T"
                    *_, loc = col_name.rsplit("_", 1)
                    rec = {
                        "period_code": period_code,
                        "variable":    var,
                        "strip":       strip,
                        "depth":       depth,
                        "location":    loc,
                    }
                    rec.update({ f"raw_{k}": v for k, v in metrics.items() })
                    records.append(rec)

                # then ratio
                ratio_stats = stats.get("ratio_statistics", {})
                for col_name, metrics in ratio_stats.items():
                    *_, loc = col_name.rsplit("_", 1)
                    rec = {
                        "period_code": period_code,
                        "variable":    var,
                        "strip":       strip,
                        "depth":       depth,
                        "location":    loc,
                    }
                    rec.update({ f"ratio_{k}": v for k, v in metrics.items() })
                    records.append(rec)
    return pd.DataFrame.from_records(records)


def load_or_generate_gseason_summary(year: int, overwrite: bool = False) -> dict:
    """
    Load the nested JSON summary (period → variable → strip_depth → stats).
    Returns the raw dict, not a DataFrame.
    """
    summary_path = Path(DATA_PROCESSED_DIR) / f"gseason_summary_{year}.json"
    if not summary_path.exists() or overwrite:
        # avoid circular imports at module‐load time
        generate_gseason_summary(year, overwrite=overwrite)
    with summary_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def format_gseason_label(code):
    period = GSEASON_PERIODS.get(code)
    if not period:
        return code.replace("_", " ")  # fallback

    month_abbr = lambda date_str: datetime.strptime(date_str, "%m-%d").strftime("%b")
    start_month = month_abbr(period["start"])
    end_month = month_abbr(period["end"])
    label = period["label"]
    return f"{label} Season Summary ({start_month}–{end_month})"


def calculate_gseason_precip(
    df: pd.DataFrame,
    year: int,
    unit_system: str,
) -> pd.DataFrame:
    """
    For each growing‐season period, sum up precipitation in the correct units
    and return a DataFrame with columns:
       period_code, start, end, precip
    """
    # pick the right precip column already in your gseason‐df
    precip_col = f"precip_{PRECIP_COLS[unit_system]}"
    if precip_col not in df.columns:
        return pd.DataFrame(columns=["period_code","start","end","precip"])

    # tag each row with its season code
    temp = df[["timestamp", precip_col]].dropna().copy()
    temp["period_code"] = temp["timestamp"].apply(lambda ts: assign_gseason_periods(ts, year))
    temp = temp[temp["period_code"].notna()]

    # sum by period
    out = (
        temp
        .groupby("period_code", as_index=False)[precip_col]
        .sum()
        .rename(columns={precip_col:"precip"})
    )

    # attach start/end timestamps
    starts, ends = {}, {}
    for code, period in GSEASON_PERIODS.items():
        sm, _ = map(int, period["start"].split("-"))
        em, _ = map(int, period["end"].split("-"))
        start_year = year-1 if sm>em else year
        end_year   = year
        starts[code] = pd.Timestamp(f"{start_year}-{period['start']}")
        ends[code]   = (pd.Timestamp(f"{end_year}-{period['end']}")
                        + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
    out["start"] = out["period_code"].map(starts)
    out["end"]   = out["period_code"].map(ends)

    return out
