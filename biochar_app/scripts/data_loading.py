from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional
import numpy as np

import pandas as pd

from biochar_app.config.core import DEFAULT_GSEASON_PERIODS
from biochar_app.config.paths import (
    IRRIGATION_CSV,
    PARQUET_DIR,
    PARQUET_SUMMARY_DIR,
    PARQUET_SUMMARY_WEATHER_15MIN_DIR,
    PARQUET_SUMMARY_WEATHER_HOURLY_DIR,
    PARQUET_SUMMARY_WEATHER_DAILY_DIR,
    PARQUET_SUMMARY_WEATHER_MONTHLY_DIR,
)


def load_logger_data(year: int, granularity: Optional[str] = None) -> pd.DataFrame:
    """
    Canonical loader for logger summary parquet data.

    For non-gseason granularities, returns raw logger data merged with
    ratio columns and weather columns, with a timezone-naive timestamp column.
    """
    gran = (granularity or "15min").lower()
    base = Path(PARQUET_SUMMARY_DIR) / gran

    def _normalize_timestamp_column(df_in: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
        """
        Force a clean, numpy-backed datetime64[ns] timestamp column.

        This avoids Arrow/extension-dtype edge cases during dropna/min/max/merge/sort.
        """
        df_out = df_in.copy()

        if col not in df_out.columns:
            df_out = df_out.reset_index()

        if col not in df_out.columns:
            raise KeyError(f"Required timestamp column '{col}' not found after reset_index().")

        raw = pd.Series(df_out[col], dtype="object")
        ts = pd.to_datetime(raw.astype(str), errors="coerce")

        valid_mask = ~pd.isna(ts)
        if not bool(valid_mask.any()):
            df_out = df_out.iloc[0:0].copy()
            df_out[col] = pd.Series([], dtype="datetime64[ns]")
            return df_out

        # Filter rows using iloc on numpy positions instead of dropna(subset=...)
        keep_idx = np.flatnonzero(valid_mask.to_numpy())
        df_out = df_out.iloc[keep_idx].copy()

        ts_valid = ts.iloc[keep_idx]
        df_out[col] = pd.Series(
            ts_valid.to_numpy(dtype="datetime64[ns]"),
            index=df_out.index,
        )

        return df_out

    if gran == "gseason":
        raw_file = base / f"{year}_gseason.parquet"
        if not raw_file.exists():
            raise FileNotFoundError(f"No gseason summary for {year} at {raw_file}")

        df = pd.read_parquet(raw_file)

        if "period_code" not in df.columns:
            df = df.reset_index().rename(columns={"index": "period_code"})

        def _code_to_dt(code: str) -> datetime:
            period_def = DEFAULT_GSEASON_PERIODS[code]
            if isinstance(period_def, dict):
                start_str = period_def["start"]
            else:
                start_str = period_def[0]
            m_start, d_start = map(int, start_str.split("-"))
            return datetime(year, m_start, d_start)

        df["timestamp"] = df["period_code"].map(_code_to_dt)
        df = _normalize_timestamp_column(df, "timestamp")

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
    df = _normalize_timestamp_column(df, "timestamp")

    ratio_file = base / f"{year}_{gran}_ratios.parquet"
    if ratio_file.exists():
        df_ratio = pd.read_parquet(ratio_file)
        df_ratio = _normalize_timestamp_column(df_ratio, "timestamp")
        df = df.merge(df_ratio, on="timestamp", how="left")

    weather_df = load_weather_data(year=year, granularity=gran)
    if not weather_df.empty:
        weather_df = _normalize_timestamp_column(weather_df, "timestamp")
        df = df.merge(weather_df, on="timestamp", how="left")

    return df.sort_values("timestamp").reset_index(drop=True)


def _weather_base_dir(granularity: str) -> Path:
    gran = granularity.lower()
    mapping: dict[str, Path] = {
        "15min": PARQUET_SUMMARY_WEATHER_15MIN_DIR,
        "hourly": PARQUET_SUMMARY_WEATHER_HOURLY_DIR,
        "daily": PARQUET_SUMMARY_WEATHER_DAILY_DIR,
        "monthly": PARQUET_SUMMARY_WEATHER_MONTHLY_DIR,
    }
    return mapping.get(gran, Path(PARQUET_SUMMARY_DIR) / "weather" / gran)


def _weather_parquet_candidates(year: int, granularity: str) -> list[Path]:
    base = _weather_base_dir(granularity)
    gran = granularity.lower()
    return [
        base / f"{year}_{gran}.parquet",
        base / f"{year}_{gran}_weather.parquet",
        base / f"weather_{year}_{gran}.parquet",
        Path(PARQUET_DIR) / str(year) / gran / "weather" / f"{year}_{gran}_weather.parquet",
        Path(PARQUET_DIR) / str(year) / gran / "weather" / f"{year}_{gran}.parquet",
        Path(PARQUET_DIR) / str(year) / gran / "weather" / f"weather_{year}_{gran}.parquet",
    ]


def load_weather_data(year: int, granularity: Optional[str] = None) -> pd.DataFrame:
    """
    Canonical loader for processed weather parquet data.

    This does NOT fetch from CoAgMet. It reads already-processed parquet
    written by ETL.

    Returns
    -------
    DataFrame with a naive `timestamp` column and weather variables
    at the requested granularity.

    If no weather parquet exists, returns an empty dataframe with
    a `timestamp` column.
    """
    gran = (granularity or "15min").lower()

    if gran == "gseason":
        return pd.DataFrame(columns=["timestamp"])

    candidates = _weather_parquet_candidates(year, gran)
    weather_file = next((p for p in candidates if p.exists()), None)

    if weather_file is None:
        return pd.DataFrame(columns=["timestamp"])

    df = pd.read_parquet(weather_file)

    if "timestamp" not in df.columns:
        df = df.reset_index()

    if "timestamp" not in df.columns:
        raise ValueError(f"Weather parquet missing timestamp column: {weather_file}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df


def load_irrigation_data() -> pd.DataFrame:
    """
    Canonical loader for cleaned irrigation management data.

    Source
    ------
    Reads the cleaned irrigation CSV defined by IRRIGATION_CSV.

    Required source columns
    -----------------------
    - strip_group
    - start_timestamp
    - end_timestamp
    - gallons

    Returned columns
    ----------------
    - strip_group
    - strip
    - start_timestamp
    - end_timestamp
    - duration_hours
    - gallons
    - year

    Notes
    -----
    The cleaned CSV stores irrigation by strip pair / side. This function
    expands each event to one row per strip:
    - WEST or S1_S2 -> S1 and S2
    - EAST or S3_S4 -> S3 and S4
    """
    path = Path(IRRIGATION_CSV)

    empty = pd.DataFrame(
        columns=[
            "strip_group",
            "strip",
            "start_timestamp",
            "end_timestamp",
            "duration_hours",
            "gallons",
            "year",
        ]
    )

    if not path.exists():
        return empty

    df = pd.read_csv(path)
    if df.empty:
        return empty

    required_cols = {"strip_group", "start_timestamp", "end_timestamp", "gallons"}
    missing = sorted(required_cols - set(df.columns))
    if missing:
        raise KeyError(f"Irrigation CSV is missing required columns: {missing}")

    df = df.copy()

    df["start_timestamp"] = pd.to_datetime(df["start_timestamp"], errors="coerce")
    df["end_timestamp"] = pd.to_datetime(df["end_timestamp"], errors="coerce")
    df["gallons"] = pd.to_numeric(df["gallons"], errors="coerce")

    df = df.dropna(subset=["start_timestamp", "end_timestamp", "gallons"]).copy()

    overnight = (
        (df["end_timestamp"] < df["start_timestamp"])
        & df["end_timestamp"].notna()
        & df["start_timestamp"].notna()
    )
    if bool(overnight.any()):
        df.loc[overnight, "end_timestamp"] = (
            df.loc[overnight, "end_timestamp"] + pd.Timedelta(days=1)
        )

    df = df.loc[df["gallons"] > 0].copy()

    df["duration_hours"] = (
        df["end_timestamp"] - df["start_timestamp"]
    ).dt.total_seconds() / 3600.0

    df["year"] = df["start_timestamp"].dt.year

    def expand_group(strip_group: object) -> list[str]:
        group = str(strip_group).strip().lower()

        if group in {"west", "s1_s2"}:
            return ["S1", "S2"]
        if group in {"east", "s3_s4"}:
            return ["S3", "S4"]
        return []

    rows: list[dict[str, object]] = []

    for _, row in df.iterrows():
        strips = expand_group(row["strip_group"])
        for strip in strips:
            rows.append(
                {
                    "strip_group": row["strip_group"],
                    "strip": strip,
                    "start_timestamp": row["start_timestamp"],
                    "end_timestamp": row["end_timestamp"],
                    "duration_hours": row["duration_hours"],
                    "gallons": row["gallons"],
                    "year": row["year"],
                }
            )

    if not rows:
        return empty

    out = pd.DataFrame(rows)
    out.sort_values(["strip", "start_timestamp"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def prepare_irrigation_input(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a dataframe for irrigation-analysis functions that require a
    DatetimeIndex with unique timestamps.

    Duplicate naive timestamps can exist because ETL exports civil local time
    after dropping timezone info, which collapses the repeated fall DST hour.
    """
    out = df.copy()

    if "timestamp" not in out.columns:
        raise KeyError("Expected 'timestamp' column.")

    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["timestamp"]).copy()
    out = out.sort_values("timestamp")
    out = out.drop_duplicates(subset=["timestamp"], keep="last")
    out = out.set_index("timestamp").sort_index()

    return out