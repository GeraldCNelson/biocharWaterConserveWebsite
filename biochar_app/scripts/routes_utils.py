from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, cast, Any

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


def load_logger_year(year: int, granularity: Optional[str] = None) -> pd.DataFrame:
    """
    1) Load sensor raw & ratio data from summary parquets at the given granularity.
    2) For standard granularities, ensure we have a datetime 'timestamp' column.
    3) For 'gseason', load the per-season parquet, turn its 'period_code' into real
       datetimes (the season start), and then merge in the ratio file if present.
    4) Merge in weather as before for non-gseason.
    """
    gran = (granularity or "15min").lower()
    base = Path(PARQUET_DIR) / "summary" / gran

    if gran == "gseason":
        raw_file = base / f"{year}_gseason.parquet"
        if not raw_file.exists():
            raise FileNotFoundError(f"No gseason summary for {year} at {raw_file}")
        df = pd.read_parquet(raw_file)

        if "period_code" not in df.columns:
            df = df.reset_index().rename(columns={"index": "period_code"})

        def _code_to_dt(code: str) -> datetime:
            m_start, d_start = map(int, DEFAULT_GSEASON_PERIODS[code]["start"].split("-"))
            return datetime(year, m_start, d_start)

        df["timestamp"] = df["period_code"].map(_code_to_dt)

        ratio_file = base / f"{year}_gseason_ratios.parquet"
        if ratio_file.exists():
            df_ratio = pd.read_parquet(ratio_file)
            if "period_code" not in df_ratio.columns:
                df_ratio = df_ratio.reset_index().rename(columns={"index": "period_code"})
            df = df.merge(df_ratio, on="period_code", how="left")

        return df.sort_values("timestamp").reset_index(drop=True)

    raw_file = base / f"{year}_{gran}.parquet"
    if not raw_file.exists():
        raise FileNotFoundError(f"No summary raw file for granularity '{gran}' at {raw_file}")
    df = pd.read_parquet(raw_file)

    if "timestamp" not in df.columns:
        df = df.reset_index()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    new_cols = []
    for col in df.columns:
        if col == "timestamp":
            new_cols.append(col)
        else:
            var, strip, logger_name = col.split("_", maxsplit=2)
            new_cols.append(f"{var}_raw_{strip}_{logger_name}")
    df.columns = new_cols

    ratio_file = base / f"{year}_{gran}_ratios.parquet"
    if ratio_file.exists():
        df_ratio = pd.read_parquet(ratio_file)
        if "timestamp" not in df_ratio.columns:
            df_ratio = df_ratio.reset_index()
        df_ratio["timestamp"] = pd.to_datetime(df_ratio["timestamp"])
        df = df.merge(df_ratio, on="timestamp", how="left")

    weather_file = Path(PARQUET_DIR) / "summary" / "weather" / gran / f"{year}_{gran}.parquet"
    if weather_file.exists():
        df_w = pd.read_parquet(weather_file)
        if "timestamp" not in df_w.columns:
            df_w = df_w.reset_index()
        ts = df_w["timestamp"]
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

        df_15min = load_logger_year(year, "15min")
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