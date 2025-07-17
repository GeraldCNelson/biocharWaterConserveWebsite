import os
import json
import logging
import pandas as pd
from datetime import datetime

from biochar_app.scripts.config import (
    DATA_PROCESSED_DIR,
    VARIABLES,
    STRIPS,
    DEPTHS,
    GSEASON_PERIODS,
    YEARS,
    PRECIP_COLS,
)

from biochar_app.scripts.routes_utils import load_logger_year

# Generate growing season summary statistics(min, mean, m
# ax, std) for each growing season period in a year
def generate_gseason_summary(year, gseason_periods=None, overwrite=False):
    gseason_periods = gseason_periods or GSEASON_PERIODS
    logging.info(f"🌱 Generating growing season summary for {year}...")

    output_path = os.path.join(DATA_PROCESSED_DIR, f"gseason_summary_{year}.json")
    if os.path.exists(output_path) and not overwrite:
        logging.info(f"✅ Already exists: {output_path} — Skipping.")
        return

    df_15min = load_logger_year(year, "15min")
    if df_15min is None or df_15min.empty:
        raise RuntimeError("❌ No 15-minute logger data available.")

    df_15min["timestamp"] = pd.to_datetime(df_15min["timestamp"], errors="coerce")

    summary = {}

    for label, info in gseason_periods.items():
        start_str = info["start"]
        end_str = info["end"]
        start_year = year - 1 if start_str > end_str else year
        start = pd.Timestamp(f"{start_year}-{start_str}")
        end = pd.Timestamp(f"{year}-{end_str}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        mask = (df_15min["timestamp"] >= start) & (df_15min["timestamp"] <= end)
        df_season = df_15min[mask]
        logging.info(f"📅 {label}: {len(df_season)} records from {start.date()} to {end.date()}")

        season_stats = {}
        for variable in VARIABLES:
            season_stats[variable] = {}
            for strip in STRIPS:
                for depth in DEPTHS:
                    raw, ratio = compute_summary_statistics(df_season, variable, strip, depth)
                    is_temp = variable in ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"]
                    if is_temp:
                        ratio = {}
                    season_stats[variable][f"{strip}_D{depth}"] = {
                        "raw_statistics": raw,
                        "ratio_statistics": ratio
                    }

        summary[label] = season_stats

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    for y in YEARS:
        if y >= 2024:
            generate_gseason_summary(y, overwrite=True)


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
    Return a flattened DataFrame with columns:
      period_code, variable, strip, depth, location, **raw_statistics, **ratio_statistics
    """
    nested = load_or_generate_gseason_summary(year)
    records: list[dict] = []
    for period_code, vars_dict in nested.items():
        for var, strips in vars_dict.items():
            for strip_depth, stats in strips.items():
                strip, depth = strip_depth.split("_D")
                for loc_key, raw_stat in stats["raw_statistics"].items():
                    # raw_stat key naming: {var}_{depth}_raw_{strip}_{loc}
                    _, _, _, loc = loc_key.rsplit("_", 1)
                    record = {
                        "period_code": period_code,
                        "variable": var,
                        "strip": strip,
                        "depth": depth,
                        "location": loc,
                        **raw_stat,
                    }
                    records.append(record)

    return pd.DataFrame.from_records(records)


def assign_gseason_periods(ts: pd.Timestamp, year: int) -> str | None:
    for label, period in GSEASON_PERIODS.items():
        start_str = period["start"]
        end_str = period["end"]
        sm, sd = map(int, start_str.split("-"))
        em, ed = map(int, end_str.split("-"))

        start_year = year - 1 if sm > em else year
        end_year = year

        start = pd.Timestamp(f"{start_year}-{start_str}")
        end = pd.Timestamp(f"{end_year}-{end_str}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        if start <= ts <= end:
            return label
    return None


def load_or_generate_gseason_summary(year, overwrite=False):
    summary_path = os.path.join(DATA_PROCESSED_DIR, f"gseason_summary_{year}.json")
    if not os.path.exists(summary_path) or overwrite:
        # ✅ Local import to avoid circular dependency
        from biochar_app.scripts.gseason import generate_gseason_summary
        generate_gseason_summary(year, overwrite=overwrite)

    with open(summary_path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_gseason_summary(
    year: int,
    gseason_periods: dict | None = None,
    overwrite: bool = False,
) -> dict[str, dict]:
    """
    Load the per‐season summary for `year` if it exists (unless overwrite=True),
    otherwise compute it, save it, and return it.
    """
    gseason_periods = gseason_periods or GSEASON_PERIODS
    summary_path = os.path.join(DATA_PROCESSED_DIR, f"gseason_summary_{year}.json")

    # if file already there and not forcing a rebuild, just read & return it
    if os.path.exists(summary_path) and not overwrite:
        logging.info(f"✅ Loading existing summary from {summary_path}")
        with open(summary_path, "r", encoding="utf-8") as f:
            return json.load(f)

    # otherwise we need to build it
    logging.info(f"🌱 Building growing-season summary for {year}…")
    df = load_logger_year(year, granularity="15min")
    if df is None or df.empty:
        raise RuntimeError("No 15‐minute logger data found for year {year}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    summary: dict[str, dict] = {}

    for label, info in gseason_periods.items():
        # compute start/end timestamps (handles crossing calendar‐year boundary)
        start_year = year - 1 if info["start"] > info["end"] else year
        start = pd.Timestamp(f"{start_year}-{info['start']}")
        end   = pd.Timestamp(f"{year}-{info['end']}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        logging.info(f"  • {label}: {start.date()} → {end.date()}")
        season_df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]
        season_stats: dict[str, dict] = {}

        for var in ("VWC", "T", "EC", "SWC"):
            season_stats[var] = {}
            for strip in ("S1", "S2", "S3", "S4"):
                for depth in ("1", "2", "3"):
                    raw_stats, ratio_stats = compute_summary_statistics(season_df, var, strip, depth)
                    # zero‐out ratios on temperature if you’d like:
                    if var == "T":
                        ratio_stats = {}
                    season_stats[var][f"{strip}_D{depth}"] = {
                        "raw_statistics":   raw_stats,
                        "ratio_statistics": ratio_stats,
                    }

        summary[label] = season_stats

    # persist and return
    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logging.info(f"✅ Saved summary to {summary_path}")

    return summary


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
