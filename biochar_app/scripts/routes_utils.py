import os
import logging
import pandas as pd
from pathlib import Path
from typing import Optional, cast, Any
from datetime import datetime

from biochar_app.scripts.config import PARQUET_DIR, DEFAULT_GSEASON_PERIODS, DATA_RAW_DIR
from biochar_app.scripts.gseason import compute_seasons
from dataclasses import dataclass
from biochar_app.scripts.gseason_utils import periods_to_list_of_dicts, add_gseason_precip_from_daily
@dataclass
class PeriodSpec:
    code:  str   # e.g. "Q1_Winter"
    label: str   # e.g. "Winter"
    start: str   # ISO date "YYYY-MM-DD"
    end:   str   # ISO date "YYYY-MM-DD"

GSEASON_SUMMARY_DIR = PARQUET_DIR / "summary" / "gseason"
logger = logging.getLogger(__name__)

def load_summary_df(year: int, granularity: str, variable: str, strip: str) -> pd.DataFrame:
    path = PARQUET_DIR / "summary" / granularity / f"{year}_{granularity}.parquet"
    df = pd.read_parquet(path)
    # filter to only the variable+strip you need:
    return df[(df.variable == variable) & (df.strip == strip)]


def load_logger_year(year: int, granularity: Optional[str] = None) -> pd.DataFrame:
    """
    1) Load sensor raw & ratio data from summary parquets at the given granularity.
    2) For standard granularities, ensure we have a datetime 'timestamp' column.
    3) For 'gseason', load the per‐season parquet, turn its 'period_code' into real
       datetimes (the season start), and then merge in the ratio file if present.
    4) Merge in weather as before for non‐gseason.
    """
    gran = (granularity or "15min").lower()
    base = Path(PARQUET_DIR) / "summary" / gran

    # ---- special branch for growing‐season ----
    if gran == "gseason":
        raw_file = base / f"{year}_gseason.parquet"
        if not raw_file.exists():
            raise FileNotFoundError(f"No gseason summary for {year} at {raw_file}")
        df = pd.read_parquet(raw_file)

        # make sure we have a 'period_code' column
        if "period_code" not in df.columns:
            df = df.reset_index().rename(columns={"index": "period_code"})

        # map each period_code → actual datetime (start of that season)
        def _code_to_dt(code: str) -> datetime:
            m_start, d_start = map(int, DEFAULT_GSEASON_PERIODS[code]["start"].split("-"))
            return datetime(year, m_start, d_start)
        df["timestamp"] = df["period_code"].map(_code_to_dt)

        # now bring in ratios (if any)
        ratio_file = base / f"{year}_gseason_ratios.parquet"
        if ratio_file.exists():
            df_ratio = pd.read_parquet(ratio_file)
            if "period_code" not in df_ratio.columns:
                df_ratio = df_ratio.reset_index().rename(columns={"index": "period_code"})
            df = df.merge(df_ratio, on="period_code", how="left")

        # final sort
        return df.sort_values("timestamp").reset_index(drop=True)

    # ---- all other (time‐series) granularities ----

    # 1) raw‐logger parquet
    raw_file = base / f"{year}_{gran}.parquet"
    if not raw_file.exists():
        raise FileNotFoundError(f"No summary raw file for granularity '{gran}' at {raw_file}")
    df = pd.read_parquet(raw_file)

    # if they nested the datetime in the index, reset it
    if "timestamp" not in df.columns:
        df = df.reset_index()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 2) rename raw columns to include '_raw_'
    new_cols = []
    for col in df.columns:
        if col == "timestamp":
            new_cols.append(col)
        else:
            var, strip, logger_name = col.split("_", maxsplit=2)
            new_cols.append(f"{var}_raw_{strip}_{logger_name}")
    df.columns = new_cols

    # 3) ratio parquet
    ratio_file = base / f"{year}_{gran}_ratios.parquet"
    if ratio_file.exists():
        df_ratio = pd.read_parquet(ratio_file)
        if "timestamp" not in df_ratio.columns:
            df_ratio = df_ratio.reset_index()
        df_ratio["timestamp"] = pd.to_datetime(df_ratio["timestamp"])
        df = df.merge(df_ratio, on="timestamp", how="left")

    # 4) weather parquet
    weather_file = Path(PARQUET_DIR) / "summary" / "weather" / gran / f"{year}_{gran}.parquet"
    if weather_file.exists():
        df_w = pd.read_parquet(weather_file)
        if "timestamp" not in df_w.columns:
            df_w = df_w.reset_index()
        ts = df_w["timestamp"]
        # detect epoch‐seconds vs strings
        if pd.api.types.is_integer_dtype(ts.dtype):
            df_w["timestamp"] = pd.to_datetime(ts, unit="s")
        else:
            df_w["timestamp"] = pd.to_datetime(ts)
        df = df.merge(df_w, on="timestamp", how="left")

    return df.sort_values("timestamp").reset_index(drop=True)

def merge_all_loggers(year: int) -> pd.DataFrame:
    dat_dir = Path(DATA_RAW_DIR) / f"datfiles_{year}"
    frames: list[pd.DataFrame] = []
    for dat_path in sorted(dat_dir.glob("*Table1.dat")):
        df = load_logger_year(year)
        if df.empty:
            logger.warning(f"⚠️  No data in {dat_path.name}")
            continue
        df = df[~df.index.duplicated(keep="first")]
        frames.append(df)
    if not frames:
        logger.warning(f"⚠️  No logger data found for year {year}")
        return pd.DataFrame()
    merged = pd.concat(frames, axis=1)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    merged = restrict_to_year(merged, year)
    return merged

def load_gseason_df(
    year: int,
    periods: Any,
    unit_system: str = "us",
    use_ratios: bool = False,
) -> pd.DataFrame:
    """
    Load growing-season aggregated data for `year`.

    If `periods` is empty:
      - loads summary/gseason/{year}_gseason.parquet        (use_ratios=False)
      - or summary/gseason/{year}_gseason_ratios.parquet    (use_ratios=True)
      - and uses DEFAULT_GSEASON_PERIODS for seasonal precip windows.

    Otherwise:
      - normalizes PeriodSpec / mappings -> list[dict]
      - builds the mapping shape compute_seasons expects
      - slices the raw 15-min data via compute_seasons() (unit-agnostic)
      - then attaches seasonal precip sums from *daily* weather parquet
        via add_gseason_precip_from_daily()

    In all cases:
      - ensures both precip_in and precip_mm exist
      - adds 'precip' + 'precip_unit' presentation columns per unit_system
    """
    # ------------------------------------------------------------------
    # 1) Load the base growing-season dataframe (raw or ratios)
    # ------------------------------------------------------------------
    if not periods:
        # Use precomputed seasonal parquet + the *default* period definitions
        fn_raw   = GSEASON_SUMMARY_DIR / f"{year}_gseason.parquet"
        fn_ratio = GSEASON_SUMMARY_DIR / f"{year}_gseason_ratios.parquet"
        fn = fn_ratio if use_ratios else fn_raw
        df = pd.read_parquet(fn)

        # For precip, we want to use the default period definitions
        period_source = DEFAULT_GSEASON_PERIODS

    else:
        # 2) Normalize PeriodSpec / dict / mapping → list[dict]
        periods_list = periods_to_list_of_dicts(periods)

        # 3) Convert to mapping shape for compute_seasons:
        #    { code: {"label": str, "start": "MM-DD", "end": "MM-DD"} }
        period_map = {
            p["code"]: {
                "label": p["label"],
                "start": p["start"],
                "end":   p["end"],
            }
            for p in periods_list
        }

        # 4) Load 15-min logger data and ensure a DatetimeIndex
        df_15min = load_logger_year(year, "15min")
        df_15min["timestamp"] = pd.to_datetime(df_15min["timestamp"], errors="coerce")
        df_15min = df_15min.set_index("timestamp", drop=False)

        # 5) Compute seasonal rows (means of sensors, but we'll override precip)
        df = compute_seasons(
            df=df_15min,
            year=year,
            periods=period_map,
            include_precip=True,   # safe to leave True; we'll reattach from daily
        )

        # For precip windows, we want the *custom* periods the user supplied
        period_source = periods

    # ------------------------------------------------------------------
    # 2) Attach seasonal precip sums from daily weather parquet
    #    - this ensures precip_in / precip_mm are based on daily weather,
    #      not whatever happened inside compute_seasons().
    # ------------------------------------------------------------------
    df = add_gseason_precip_from_daily(
        df_gs=df,
        year=year,
        periods_raw=period_source,
    )

    # ------------------------------------------------------------------
    # 3) Ensure both precip columns exist, then add presentation alias
    # ------------------------------------------------------------------
    has_in = "precip_in" in df.columns
    has_mm = "precip_mm" in df.columns

    if has_in and not has_mm:
        df["precip_mm"] = pd.to_numeric(df["precip_in"], errors="coerce") * 25.4
    elif has_mm and not has_in:
        df["precip_in"] = pd.to_numeric(df["precip_mm"], errors="coerce") / 25.4
    elif not has_in and not has_mm:
        df["precip_in"] = pd.NA
        df["precip_mm"] = pd.NA

    # Presentation columns used by the plotting code
    if unit_system == "metric":
        df["precip"] = df["precip_mm"]
        df["precip_unit"] = "mm"
    else:
        df["precip"] = df["precip_in"]
        df["precip_unit"] = "in"

    return df

def restrict_to_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    df = df.copy()

    # Make sure the index is a tz-naive DatetimeIndex
    idx = pd.DatetimeIndex(pd.to_datetime(df.index, errors="coerce"))
    df = df.loc[idx.notna()]
    idx = pd.DatetimeIndex(df.index)  # re-create after dropping NaT
    if idx.tz is not None:
        idx = idx.tz_localize(None)   # remove timezone
    df.index = idx

    # String slice by year → always a DataFrame
    out = df.loc[str(year)]
    return cast(pd.DataFrame, out)