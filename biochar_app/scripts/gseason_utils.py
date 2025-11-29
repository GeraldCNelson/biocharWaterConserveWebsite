"""
Growing-season utilities for:
  • generating/loading the nested JSON summary used by the UI
  • computing per-variable raw/ratio stats
  • flattening the nested structure for tables
  • weather seasonal precipitation (SUM of 5-min increments)

Relies on:
  - DEFAULT_GSEASON_PERIODS for period definitions (MM-DD windows)
  - assign_gseason_periods from gseason.py
"""

import json
from datetime import datetime
from pathlib import Path
import pandas as pd

from biochar_app.scripts.gseason import assign_gseason_periods  # core mapper
from biochar_app.scripts.config import (
    DATA_PROCESSED_DIR,
    DEFAULT_GSEASON_PERIODS,
    PRECIP_COLS,   # e.g., {"us": "in", "metric": "mm"}
    PARQUET_DIR,
    UNIT_CONVERSIONS,
)

from collections.abc import Mapping
from typing import Any


# ---------------------------------------------------------------------------
# Top‑level: generate & load JSON for logger seasonal summaries
# (Assumes you have a job that prepares this; we keep the interface unchanged.)
# ---------------------------------------------------------------------------

def generate_gseason_summary(
    year: int,
    periods: dict[str, dict[str, str]] | None = None,
    overwrite: bool = False,
) -> None:
    """
    Build and persist growing-season summary JSON for a given year
    directly from the 15-min logger parquet.

    Output schema (per your existing frontend):
    {
      "<period_code>": {
        "<VARIABLE>": {
          "<STRIP>_D<DEPTH>": {
            "raw_statistics":   { "<col>": {min, mean, max, std}, ... },
            "ratio_statistics": { "<col>": {min, mean, max, std}, ... }
          },
          ...
        },
        ...
      },
      ...
    }
    """
    import re
    from collections import defaultdict

    # 0) config + input/output paths
    summary_path = Path(DATA_PROCESSED_DIR) / f"gseason_summary_{year}.json"
    if summary_path.exists() and not overwrite:
        return

    # 1) load 15-min logger data for this year
    parquet_15 = Path(PARQUET_DIR) / "summary" / "15min" / f"{year}_15min.parquet"
    if not parquet_15.exists():
        raise FileNotFoundError(f"Missing 15-min parquet: {parquet_15}")

    df = pd.read_parquet(parquet_15)
    if "timestamp" not in df.columns:
        raise ValueError("15-min parquet must contain a 'timestamp' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # Keep only this calendar year (parquet may contain edge rows)
    df = df[(df["timestamp"].dt.year == year)]

    # 2) assign period_code to each row using wrap-aware mapper
    df["period_code"] = df["timestamp"].apply(lambda ts: assign_gseason_periods(ts, year))
    df = df[df["period_code"].notna()]

    if df.empty:
        # write empty scaffold to keep downstream happy
        summary_path.write_text(json.dumps({}, indent=2))
        return

    # 3) discover variable/strip/depth from column names
    #    RAW:   VAR_DEPTH_raw_S?_?   e.g., VWC_1_raw_S1_T
    #    RATIO: VAR_DEPTH_ratio_S1_S2_?  or _S3_S4_?
    raw_re   = re.compile(r"^(?P<var>[A-Z]+)_(?P<depth>\d)_raw_(?P<strip>S\d)_(?P<loc>[A-Z])$")
    ratio_re = re.compile(r"^(?P<var>[A-Z]+)_(?P<depth>\d)_ratio_(?P<pair>S\d_S\d)_(?P<loc>[A-Z])$")

    # 4) build nested accumulator
    nested: dict[str, dict] = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    def stats(series: pd.Series) -> dict:
        s = pd.to_numeric(series, errors="coerce").dropna()
        if s.empty:
            return {}
        return {
            "min":  round(float(s.min()),  4),
            "mean": round(float(s.mean()), 4),
            "max":  round(float(s.max()),  4),
            "std":  round(float(s.std()),  4),
        }

    # 5) per period, compute stats for RAW and RATIO groups
    for pcode, g in df.groupby("period_code", dropna=True):
        # RAW groups by (var, strip, depth, loc)
        for col in g.columns:
            m = raw_re.match(col)
            if m:
                var   = m.group("var")
                depth = m.group("depth")
                strip = m.group("strip")
                logger_loc  = m.group("logger_loc")
                key   = f"{strip}_D{depth}"
                rs = stats(g[col])
                if rs:
                    slot = nested[pcode][var].setdefault(key, {"raw_statistics": {}, "ratio_statistics": {}})
                    slot["raw_statistics"][col] = rs
                continue

            m = ratio_re.match(col)
            if m:
                var   = m.group("var")
                depth = m.group("depth")
                pair  = m.group("pair")   # S1_S2 or S3_S4
                logger_loc  = m.group("logger_loc")
                key   = f"{pair}_D{depth}"  # keep pairs separate from single-strip keys
                rs = stats(g[col])
                if rs:
                    slot = nested[pcode][var].setdefault(key, {"raw_statistics": {}, "ratio_statistics": {}})
                    slot["ratio_statistics"][col] = rs

    # 6) write JSON
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(nested, f, indent=2, default=str)


def load_or_generate_gseason_summary(year: int, overwrite: bool = False) -> dict:
    """
    Load the nested JSON summary (period → variable → strip_depth → stats).
    Returns the raw dict, not a DataFrame.
    """
    summary_path = Path(DATA_PROCESSED_DIR) / f"gseason_summary_{year}.json"
    if not summary_path.exists() or overwrite:
        generate_gseason_summary(year, overwrite=overwrite)
    with summary_path.open("r", encoding="utf-8") as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# Stats helpers for UI
# ---------------------------------------------------------------------------

def compute_summary_statistics(df: pd.DataFrame, variable: str, strip: str, depth: str):
    """
    Compute summary statistics for raw and ratio values filtered by variable, strip, and depth.
    Returns two dictionaries: raw_stats and ratio_stats.
    """
    if not variable or not strip or not depth or df is None or df.empty:
        return {}, {}

    df = df.copy()
    raw_stats: dict[str, dict] = {}
    ratio_stats: dict[str, dict] = {}

    raw_prefix = f"{variable}_{depth}_raw_{strip}_"
    ratio_prefixes = [
        f"{variable}_{depth}_ratio_S1_S2_",
        f"{variable}_{depth}_ratio_S3_S4_"
    ]

    # RAW stats
    raw_cols = [col for col in df.columns if col.startswith(raw_prefix)]
    for col in raw_cols:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if not s.empty:
            raw_stats[col] = {
                "min": round(float(s.min()), 4),
                "mean": round(float(s.mean()), 4),
                "max": round(float(s.max()), 4),
                "std": round(float(s.std()), 4),
            }

    # RATIO stats
    for prefix in ratio_prefixes:
        for col in df.columns:
            if col.startswith(prefix):
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                if not s.empty:
                    ratio_stats[col] = {
                        "min": round(float(s.min()), 4),
                        "mean": round(float(s.mean()), 4),
                        "max": round(float(s.max()), 4),
                        "std": round(float(s.std()), 4),
                    }

    return raw_stats, ratio_stats


def get_flat_gseason_summary(year: int) -> pd.DataFrame:
    """
    Flatten the nested gseason JSON into a wide, analysis‑friendly table.

    Output columns:
      period_code, variable, strip, depth, logger_location,
      raw_min, raw_mean, raw_max, raw_std,
      ratio_min, ratio_mean, ratio_max, ratio_std

    Notes:
    - `strip` will be "S1", "S2", etc., or "S1_S2" for ratio entries that
      were keyed by a pair. We preserve whatever key your JSON uses.
    - `depth` is the numeric depth from the key (e.g., "1", "2", "3").
    - `logger_location` is parsed from the column name suffix (e.g., "T"/"B").
    """
    nested = load_or_generate_gseason_summary(year)
    rows: list[dict] = []

    # Walk the nested structure
    for period_code, by_var in nested.items():
        for var, by_strip_depth in by_var.items():
            for strip_depth_key, stats in by_strip_depth.items():
                # strip_depth_key examples: "S1_D1" or "S1_S2_D1"
                if "_D" in strip_depth_key:
                    strip_key, depth = strip_depth_key.split("_D", 1)
                else:
                    strip_key, depth = strip_depth_key, ""

                # RAW stats (e.g., {"VWC_1_raw_S1_T": {"min":..., "mean":...}, ...})
                raw_stats = stats.get("raw_statistics", {}) or {}
                for col_name, metrics in raw_stats.items():
                    # logger_location is the last underscore token in the column name
                    # e.g., "VWC_1_raw_S1_T" -> "T"
                    logger_location = col_name.rsplit("_", 1)[-1]
                    rows.append({
                        "period_code":      period_code,
                        "variable":         var,
                        "strip":            strip_key,
                        "depth":            depth,
                        "logger_location":  logger_location,
                        "kind":             "raw",
                        "min":              metrics.get("min"),
                        "mean":             metrics.get("mean"),
                        "max":              metrics.get("max"),
                        "std":              metrics.get("std"),
                    })

                # RATIO stats (e.g., {"VWC_1_ratio_S1_S2_T": {...}})
                ratio_stats = stats.get("ratio_statistics", {}) or {}
                for col_name, metrics in ratio_stats.items():
                    logger_location = col_name.rsplit("_", 1)[-1]
                    rows.append({
                        "period_code":      period_code,
                        "variable":         var,
                        "strip":            strip_key,   # can be "S1_S2"
                        "depth":            depth,
                        "logger_location":  logger_location,
                        "kind":             "ratio",
                        "min":              metrics.get("min"),
                        "mean":             metrics.get("mean"),
                        "max":              metrics.get("max"),
                        "std":              metrics.get("std"),
                    })

    if not rows:
        # Return a correctly‑typed empty frame if there was no data
        return pd.DataFrame(columns=[
            "period_code", "variable", "strip", "depth", "logger_location",
            "raw_min", "raw_mean", "raw_max", "raw_std",
            "ratio_min", "ratio_mean", "ratio_max", "ratio_std",
        ])

    long_df = pd.DataFrame.from_records(rows)

    # Pivot so raw/ratio stats land in separate column groups, then flatten
    wide = (
        long_df
        .pivot_table(
            index=["period_code", "variable", "strip", "depth", "logger_location"],
            columns="kind",
            values=["min", "mean", "max", "std"],
            aggfunc="first"  # there should be at most one row per index/kind
        )
    )

    # Flatten MultiIndex columns to raw_min, ratio_mean, etc.
    wide.columns = [
        f"{kind}_{stat}"
        for stat, kind in wide.columns.to_flat_index()
    ]
    wide = wide.reset_index()

    # Ensure all expected columns exist (even if one of raw/ratio was absent)
    for col in ["raw_min", "raw_mean", "raw_max", "raw_std",
                "ratio_min", "ratio_mean", "ratio_max", "ratio_std"]:
        if col not in wide.columns:
            wide[col] = pd.NA

    # Sort for readability
    wide = wide.sort_values(
        ["period_code", "variable", "strip", "depth", "logger_location"],
        kind="stable"
    ).reset_index(drop=True)

    return wide

# ---------------------------------------------------------------------------
# Weather: seasonal precip SUMs for plotting right‑axis
# ---------------------------------------------------------------------------

def format_gseason_label(code: str) -> str:
    period = DEFAULT_GSEASON_PERIODS.get(code)
    if not period:
        return code.replace("_", " ")
    month_abbr = lambda md: datetime.strptime(md, "%m-%d").strftime("%b")
    start_month = month_abbr(period["start"])
    end_month = month_abbr(period["end"])
    label = period["label"]
    return f"{label} Season Summary ({start_month}–{end_month})"


def add_gseason_precip_from_daily(
    df_gs: pd.DataFrame,
    year: int,
    periods_raw,
) -> pd.DataFrame:
    """
    Attach seasonal precip sums to the growing-season dataframe.

    - Reads daily weather from:
        PARQUET_DIR/summary/weather/daily/{year}_daily.parquet
    - For each growing-season period, sums daily `precip_in`
      over the full window (handling wrap-around seasons).
    - Writes *new* columns on df_gs:
        - `precip_in`
        - `precip_mm` (derived from the same inches)
    """

    # 1) Normalize periods → list[dict] with keys: code,label,start,end
    periods = periods_to_list_of_dicts(periods_raw or [])

    # 2) Load daily weather
    daily_path = (
        Path(PARQUET_DIR)
        / "summary"
        / "weather"
        / "daily"
        / f"{year}_daily.parquet"
    )
    dfw = pd.read_parquet(daily_path)
    dfw["timestamp"] = pd.to_datetime(dfw["timestamp"], errors="coerce")
    dfw = dfw.set_index("timestamp").sort_index()

    conv_in_to_mm = UNIT_CONVERSIONS["us_to_metric"]["precip"]

    precip_in_sums = []
    precip_mm_sums = []

    for p in periods:
        start_mmdd = str(p["start"])  # e.g. "11-01"
        end_mmdd   = str(p["end"])    # e.g. "04-30"

        sm, sd = map(int, start_mmdd.split("-"))
        em, ed = map(int, end_mmdd.split("-"))

        # wrap-around if start month > end month (e.g. Nov→Apr)
        if sm > em:
            start_ts = pd.Timestamp(year - 1, sm, sd)
            end_ts   = pd.Timestamp(year, em, ed)
        else:
            start_ts = pd.Timestamp(year, sm, sd)
            end_ts   = pd.Timestamp(year, em, ed)

        # inclusive end-of-day (important)
        end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        mask = (dfw.index >= start_ts) & (dfw.index <= end_ts)
        total_in = float(dfw.loc[mask, "precip_in"].sum())

        precip_in_sums.append(total_in)
        precip_mm_sums.append(conv_in_to_mm(total_in))

    # 3) Attach to the g-season dataframe (assumes same row order)
    df_gs = df_gs.copy()
    df_gs["precip_in"] = precip_in_sums
    df_gs["precip_mm"] = precip_mm_sums

    return df_gs

# Normalize PeriodSpec / mappings → list of simple dicts for seasons
def periods_to_list_of_dicts(periods: Any) -> list[dict[str, str]]:
    """
    Normalize various PeriodSpec shapes to a list of dicts:
      [{"code":..., "label":..., "start":"MM-DD", "end":"MM-DD"}, ...]

    Accepts:
      - mapping: {code: {label,start,end}, ...}
      - list of dicts
      - list of Pydantic v2 models (model_dump), v1 models (dict),
        or plain objects with .code/.label/.start/.end

    Ensures start/end are 'MM-DD' strings (strips leading 'YYYY-').
    """
    if not periods:
        return []

    # Mapping → list of dicts
    if isinstance(periods, Mapping):
        return [
            {
                "code":  code,
                "label": spec.get("label", code.replace("_", " ")),
                "start": spec["start"][-5:] if isinstance(spec.get("start"), str) else spec["start"],
                "end":   spec["end"][-5:]   if isinstance(spec.get("end"), str)   else spec["end"],
            }
            for code, spec in periods.items()
        ]

    out: list[dict[str, str]] = []
    for p in periods:
        if isinstance(p, Mapping):
            code  = p["code"]
            label = p.get("label", code.replace("_", " "))
            start = p["start"]
            end   = p["end"]
        else:
            # Pydantic v2 / v1 / plain object
            if hasattr(p, "model_dump"):
                d = p.model_dump()
            elif hasattr(p, "dict"):
                d = p.dict()
            else:
                d = {
                    "code":  getattr(p, "code"),
                    "label": getattr(p, "label", None),
                    "start": getattr(p, "start"),
                    "end":   getattr(p, "end"),
                }
            code  = d["code"]
            label = d.get("label") or code.replace("_", " ")
            start = d["start"]
            end   = d["end"]

        # Normalize YYYY-MM-DD → MM-DD for strings
        if isinstance(start, str) and len(start) >= 5 and "-" in start:
            start = start[-5:]
        if isinstance(end, str) and len(end) >= 5 and "-" in end:
            end = end[-5:]

        out.append({"code": code, "label": label, "start": start, "end": end})
    return out