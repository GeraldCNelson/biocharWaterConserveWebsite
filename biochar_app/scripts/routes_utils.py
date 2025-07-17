import os
import pandas as pd
from pathlib import Path
from typing import Optional

from biochar_app.scripts.config import PARQUET_DIR

def load_summary_df(year: int, granularity: str, variable: str, strip: str) -> pd.DataFrame:
    path = PARQUET_DIR / "summary" / granularity / f"{year}_{granularity}.parquet"
    df = pd.read_parquet(path)
    # filter to only the variable+strip you need:
    return df[(df.variable == variable) & (df.strip == strip)]

def load_logger_year(year: int, granularity: Optional[str] = None) -> pd.DataFrame:
    """
    Load a year's data at the given granularity.
      - If granularity is "15min" or None, dynamically import and return
        the wide, raw per-sensor/year DataFrame.
      - Otherwise read the pre-aggregated summary Parquet at
        PARQUET_DIR/summary/<granularity>/<year>_<granularity>.parquet.
    """
    gran = (granularity or "15min").lower()
    if gran == "15min":
        # lazy import to break the circular dependency
        from biochar_app.scripts.etl import load_raw_year_df
        return load_raw_year_df(year)

    # otherwise look for a summary parquet
    summary_path = Path(PARQUET_DIR) / "summary" / gran / f"{year}_{gran}.parquet"
    if not summary_path.exists():
        raise FileNotFoundError(f"No Parquet for {gran} in {summary_path}")
    return pd.read_parquet(summary_path)