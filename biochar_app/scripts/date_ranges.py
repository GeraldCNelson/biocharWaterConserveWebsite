from __future__ import annotations

from pathlib import Path
import pandas as pd
from typing import Dict, List, Optional


def parquet_timestamp_range(path: Path) -> Optional[Dict[str, str]]:
    try:
        df = pd.read_parquet(path, columns=["timestamp"])
    except Exception:
        return None

    if "timestamp" not in df.columns:
        return None

    s = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
    if s.empty:
        return None

    min_ts = pd.Timestamp(s.iloc[0] if len(s) == 1 else s.min())
    max_ts = pd.Timestamp(s.iloc[0] if len(s) == 1 else s.max())

    return {
        "min": min_ts.date().isoformat(),
        "max": max_ts.date().isoformat(),
    }


def parquet_gseason_range(path: Path) -> Optional[Dict[str, str]]:
    try:
        df = pd.read_parquet(path, columns=["period_start", "period_end"])
    except Exception:
        return None

    if "period_start" not in df.columns or "period_end" not in df.columns:
        return None

    start_s = pd.to_datetime(df["period_start"], errors="coerce").dropna()
    end_s = pd.to_datetime(df["period_end"], errors="coerce").dropna()

    if start_s.empty or end_s.empty:
        return None

    min_ts = pd.Timestamp(start_s.min())
    max_ts = pd.Timestamp(end_s.max())

    return {
        "min": min_ts.date().isoformat(),
        "max": max_ts.date().isoformat(),
    }


def build_date_ranges(
    base_dir: Path,
    years: List[int],
    granularities: List[str],
) -> Dict[int, Dict[str, Dict[str, str]]]:
    """
    Scan parquet files and return:

      DATE_RANGES[year][granularity] = {"min": ..., "max": ...}
    """
    out: Dict[int, Dict[str, Dict[str, str]]] = {}

    for year in years:
        year = int(year)
        out[year] = {}

        for granularity in granularities:
            if granularity == "raw":
                path = base_dir / str(year) / f"{year}_raw_logger.parquet"
                r = parquet_timestamp_range(path) if path.exists() else None

            elif granularity == "gseason":
                path = base_dir / "summary" / granularity / f"{year}_{granularity}.parquet"
                r = parquet_gseason_range(path) if path.exists() else None

            else:
                path = base_dir / "summary" / granularity / f"{year}_{granularity}.parquet"
                r = parquet_timestamp_range(path) if path.exists() else None

            if r:
                out[year][granularity] = r

    return out