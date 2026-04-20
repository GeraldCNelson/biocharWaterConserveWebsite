from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, cast, Any
from biochar_app.scripts.data_loading import load_logger_data

import pandas as pd

from biochar_app.config.paths import PARQUET_DIR, DATA_RAW_DIR
from biochar_app.scripts.config import DEFAULT_GSEASON_PERIODS
from biochar_app.scripts.gseason import compute_seasons
from biochar_app.scripts.gseason_utils import periods_to_list_of_dicts, add_gseason_precip_from_daily

logger = logging.getLogger(__name__)


@dataclass
class PeriodSpec:
    code: str
    label: str
    start: str
    end: str


GSEASON_SUMMARY_DIR = PARQUET_DIR / "summary" / "gseason"


def load_summary_df(year: int, granularity: str, variable: str, strip: str) -> pd.DataFrame:
    path = PARQUET_DIR / "summary" / granularity / f"{year}_{granularity}.parquet"
    df = pd.read_parquet(path)
    return df[(df.variable == variable) & (df.strip == strip)]


def merge_all_loggers(year: int) -> pd.DataFrame:
    dat_dir = Path(DATA_RAW_DIR) / f"datfiles_{year}"
    frames: list[pd.DataFrame] = []
    for dat_path in sorted(dat_dir.glob("*Table1.dat")):
        df = load_logger_data(year)
        if df.empty:
            logger.warning("⚠️ No data in %s", dat_path.name)
            continue
        df = df[~df.index.duplicated(keep="first")]
        frames.append(df)
    if not frames:
        logger.warning("⚠️ No logger data found for year %s", year)
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
    """
    if not periods:
        fn_raw = GSEASON_SUMMARY_DIR / f"{year}_gseason.parquet"
        fn_ratio = GSEASON_SUMMARY_DIR / f"{year}_gseason_ratios.parquet"
        fn = fn_ratio if use_ratios else fn_raw
        df = pd.read_parquet(fn)
        period_source = DEFAULT_GSEASON_PERIODS

    else:
        periods_list = periods_to_list_of_dicts(periods)

        period_map = {
            p["code"]: {
                "label": p["label"],
                "start": p["start"],
                "end": p["end"],
            }
            for p in periods_list
        }

        df_15min = load_logger_data(year, "15min")
        df_15min["timestamp"] = pd.to_datetime(df_15min["timestamp"], errors="coerce")
        df_15min = df_15min.set_index("timestamp", drop=False)

        df = compute_seasons(
            df=df_15min,
            year=year,
            periods=period_map,
            include_precip=True,
        )

        period_source = periods

    df = add_gseason_precip_from_daily(
        df_gs=df,
        year=year,
        periods_raw=period_source,
    )

    has_in = "precip_in" in df.columns
    has_mm = "precip_mm" in df.columns

    if has_in and not has_mm:
        df["precip_mm"] = pd.to_numeric(df["precip_in"], errors="coerce") * 25.4
    elif has_mm and not has_in:
        df["precip_in"] = pd.to_numeric(df["precip_mm"], errors="coerce") / 25.4
    elif not has_in and not has_mm:
        df["precip_in"] = pd.NA
        df["precip_mm"] = pd.NA

    if unit_system == "metric":
        df["precip"] = df["precip_mm"]
        df["precip_unit"] = "mm"
    else:
        df["precip"] = df["precip_in"]
        df["precip_unit"] = "in"

    return df


def restrict_to_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    df = df.copy()

    idx = pd.DatetimeIndex(pd.to_datetime(df.index, errors="coerce"))
    df = df.loc[idx.notna()]
    idx = pd.DatetimeIndex(df.index)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    df.index = idx

    out = df.loc[str(year)]
    return cast(pd.DataFrame, out)