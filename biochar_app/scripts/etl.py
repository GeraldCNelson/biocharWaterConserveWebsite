#!/usr/bin/env python3
"""
Full ETL:
  - Read all .dat logger files per year (in data-raw/datfiles_{year})
  - Merge into a single 15-min DataFrame (dropping duplicate timestamps)
  - Resample/aggregate to 15-min, hourly, daily, monthly
  - Pivot wide, write raw‐logger parquet + ratios parquet
  - Compute & write growing‐season summaries
"""

import os
from pathlib import Path

import pandas as pd

from utils import calculate_ratios
from config import (
    DATA_RAW_DIR,
    PARQUET_DIR,
    GRANULARITIES,
    GSEASON_PERIODS,
    YEARS,
)

# -----------------------------------------------------------------------------
# Logger setup
# -----------------------------------------------------------------------------
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Step 1: read one .dat file, parse header, drop duplicates
# -----------------------------------------------------------------------------
def read_logger_data(dat_path: Path) -> pd.DataFrame:
    """
    Read a single TOA5 .dat file:
      - skip metadata rows 0,2,3
      - header row is row 1
      - parse TIMESTAMP -> datetime index
      - drop any duplicate timestamps (keep first)
    """
    df = pd.read_csv(
        dat_path,
        skiprows=[0, 2, 3],
        header=1,
        parse_dates=["TIMESTAMP"],
        infer_datetime_format=True,
    )
    df = df.rename(columns={"TIMESTAMP": "timestamp"})
    df = df.set_index("timestamp").sort_index()
    # drop any duplicate timestamps
    df = df[~df.index.duplicated(keep="first")]
    return df


# -----------------------------------------------------------------------------
# Step 2: for a given year, merge *all* loggers into one 15-min DataFrame
# -----------------------------------------------------------------------------
def merge_all_loggers(year: int) -> pd.DataFrame:
    """
    Look in DATA_RAW_DIR/datfiles_{year} for all .dat files,
    read each, and concat horizontally into one DataFrame.
    """
    dat_dir = DATA_RAW_DIR / f"datfiles_{year}"
    frames = []
    for dat_path in sorted(dat_dir.glob("*.dat")):
        # e.g. dat_path.stem == "S1B_Table1"
        # we extract strip & logger code from the second and third fields in the very first metadata line:
        #   "TOA5","S1B","CR200X",...
        meta = pd.read_csv(dat_path, nrows=1, header=None).iloc[0].tolist()
        _, strip, logger_id, *_ = [m.strip('"') for m in meta]
        df = read_logger_data(dat_path)
        # rename each column to include strip & logger
        df = df.add_suffix(f"_{strip}_{logger_id}")
        frames.append(df)

    # horizontally concat on timestamp index
    df15 = pd.concat(frames, axis=1).sort_index()
    # restrict to that calendar year
    df15 = df15[df15.index.year == year]
    return df15


# -----------------------------------------------------------------------------
# Step 3: pivot/write fixed‐frequency & ratios
# -----------------------------------------------------------------------------
def _write_fixed(df_raw: pd.DataFrame, freq_name: str, resample_code: str, out_dir: Path, year: int):
    """
    From raw 15-min df:
      - resample to `resample_code`
      - melt/pivot wide by variable_strip_logger
      - write raw‐logger parquet + cross-strip ratios parquet
    """
    # 1) select only the numeric columns to agg
    agg_map = {col: "mean" for col in df_raw.columns}
    # 2) aggregate
    df_agg = df_raw.resample(resample_code).agg(agg_map).round(3)

    # 3) melt long
    df_long = (
        df_agg
        .reset_index()
        .melt(id_vars="timestamp", var_name="col", value_name="value")
    )
    # split our "col" back into variable, strip, logger_name
    df_long[["variable", "strip", "logger_name"]] = df_long["col"].str.rsplit("_", n=2, expand=True)
    df_long = df_long.drop(columns="col")

    # 4) pivot wide
    df_pivot = df_long.pivot_table(
        index="timestamp",
        columns=["variable", "strip", "logger_name"],
        values="value",
    )
    # flatten cols
    df_wide = df_pivot.copy()
    df_wide.columns = [
        f"{var}_{strip}_{logger_name}"
        for (var, strip, logger_name) in df_pivot.columns
    ]

    # ensure out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # 5) write raw‐logger parquet
    raw_path = out_dir / f"{year}_{freq_name}.parquet"
    df_wide.to_parquet(raw_path)
    logger.info(f"✅ Raw‐logger parquet written: {raw_path}")

    # 6) compute & write ratios
    df_ratio = calculate_ratios(df_wide)
    ratio_path = out_dir / f"{year}_{freq_name}_ratios.parquet"
    df_ratio.to_parquet(ratio_path)
    logger.info(f"✅ Ratio parquet written:      {ratio_path}")


# -----------------------------------------------------------------------------
# Step 4: compute growing‐season summary inline
# -----------------------------------------------------------------------------
def compute_gseason(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Using GSEASON_PERIODS, slice df_raw into each season,
    aggregate with mean, and stack into one DataFrame.
    """
    results = []
    # numeric agg map
    agg = {col: "mean" for col in df_raw.columns}

    for season, meta in GSEASON_PERIODS.items():
        sm, sd = map(int, meta["start"].split("-"))
        em, ed = map(int, meta["end"].split("-"))
        if sm <= em:
            months = list(range(sm, em + 1))
        else:
            months = list(range(sm, 13)) + list(range(1, em + 1))

        df_season = df_raw[df_raw.index.month.isin(months)]
        if df_season.empty:
            continue

        season_means = df_season.resample("D").agg(agg).mean()  # daily→season average
        season_means = season_means.to_frame().T
        season_means.insert(0, "period_code", season)
        results.append(season_means)

    if not results:
        return pd.DataFrame()

    df_g = pd.concat(results, ignore_index=True)
    return df_g


# -----------------------------------------------------------------------------
# Top‐level orchestration
# -----------------------------------------------------------------------------
def generate_summaries():
    summary_base = PARQUET_DIR / "summary"

    for year in YEARS:
        logger.info(f"🌦️  Generating summaries for {year}")

        # --- raw 15-min from all .dat files ---
        df15 = merge_all_loggers(year)

        # --- fixed-freq summaries ---
        for freq_name, code in GRANULARITIES:
            if freq_name == "gseason":
                # special handling
                df_g = compute_gseason(df15)
                out_dir = summary_base / "gseason"
                out_dir.mkdir(exist_ok=True)
                g_path = out_dir / f"{year}_gseason.parquet"
                df_g.to_parquet(g_path)
                logger.info(f"✅ Wrote gseason summary: {g_path} (rows={len(df_g)})")
            else:
                out_dir = summary_base / freq_name
                _write_fixed(df_raw=df15, freq_name=freq_name, resample_code=code, out_dir=out_dir, year=year)


def main():
    generate_summaries()


if __name__ == "__main__":
    main()