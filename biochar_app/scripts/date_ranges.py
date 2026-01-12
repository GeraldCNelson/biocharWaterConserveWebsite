from __future__ import annotations

from pathlib import Path
import pandas as pd
from typing import Dict, List


def parquet_timestamp_range(path: Path):
    try:
        s = pd.read_parquet(path, columns=["timestamp"])["timestamp"]
    except Exception:
        return None

    s = pd.to_datetime(s, errors="coerce").dropna()
    if s.empty:
        return None

    return {
        "min": s.min().date().isoformat(),
        "max": s.max().date().isoformat(),
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
            else:
                path = (
                    base_dir
                    / "summary"
                    / granularity
                    / f"{year}_{granularity}.parquet"
                )

            if not path.exists():
                continue

            r = parquet_timestamp_range(path)
            if r:
                out[year][granularity] = r

    return out