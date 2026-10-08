"""
Growing-season core utilities.

Provides:
  • compute_seasons(...) – build per-period rows from a time-indexed DataFrame
      - MEANS for non-precip variables; matched ratios of means for VWC ratios
      - SUM for precip increments (precip_in / precip_mm)
  • assign_gseason_periods(...) – tag a timestamp with a season code
"""

from typing import Mapping, Any
import logging

import pandas as pd
from biochar_app.scripts.config import DEFAULT_GSEASON_PERIODS

logger = logging.getLogger(__name__)

def matched_ratio_of_means(df: pd.DataFrame, numerator: str, denominator: str) -> tuple[float | None, int]:
    """Seasonal ratio using the same finite observations for both strip means."""
    if numerator not in df or denominator not in df:
        return None, 0
    matched = df[[numerator, denominator]].apply(pd.to_numeric, errors="coerce").replace(
        [float("inf"), float("-inf")], float("nan")
    ).dropna()
    count = len(matched)
    denominator_mean = matched[denominator].mean()
    if not count or denominator_mean <= 0:
        return None, count
    return float(matched[numerator].mean() / denominator_mean), count

def _slice_and_mean(
    df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp
) -> pd.Series:
    """Column-wise mean of df[start:end] (inclusive). Assumes DatetimeIndex."""
    mask = (df.index >= start) & (df.index <= end)
    return df.loc[mask].mean(numeric_only=True)

def compute_seasons(
    df: pd.DataFrame,
    year: int,
    periods: Mapping[str, Mapping[str, Any]] = DEFAULT_GSEASON_PERIODS,
    *,
    include_precip: bool = True,
) -> pd.DataFrame:
    """
    Compute one row per custom period.

    Parameters
    ----------
    df : DataFrame
        Must have a DatetimeIndex in local (naive) America/Denver, and contain
        either 'precip_in' or 'precip_mm' if include_precip=True.
        Precip values must be **increments** (e.g., 5-min), already cleaned (-999→NaN→0, negatives→0).
        If a 'timestamp' column exists, it will be set as index.
    year : int
        Calendar year to which seasonal windows are anchored.
    periods : mapping
        DEFAULT_GSEASON_PERIODS-like dict: { code: {"label": str, "start": "MM-DD", "end": "MM-DD"} }
    include_precip : bool
        If True, sum the precip increments for each window.

    Returns
    -------
    DataFrame with one row per period (code, label, start, end, precip?,
    column means, and matched ratios of means for VWC ratio columns).
    """
    if "timestamp" in df.columns:
        df = df.set_index(
            pd.to_datetime(df["timestamp"], errors="coerce")
        ).drop(columns=["timestamp"])
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(
            "compute_seasons: df must be indexed by DatetimeIndex or contain a 'timestamp' column."
        )

    # Debug: what are we feeding into seasonal aggregation?
    logger.info(
        "🍂 compute_seasons(year=%s): %d rows, %d columns. Example columns: %s",
        year,
        len(df),
        len(df.columns),
        [c for c in df.columns if c.startswith("VWC") or c.startswith("SWC")][:10],
    )

    have_precip_in = "precip_in" in df.columns
    have_precip_mm = "precip_mm" in df.columns
    precip_col = "precip_in" if have_precip_in else ("precip_mm" if have_precip_mm else None)

    out_rows = []
    for code, spec in periods.items():
        sm, sd = map(int, spec["start"][-5:].split("-"))
        em, ed = map(int, spec["end"][-5:].split("-"))

        # Resolve window for this calendar year (wrap-aware, e.g., Nov–Feb)
        start_year = year - 1 if sm > em else year
        end_year = year
        start = pd.Timestamp(spec["start"] if len(spec["start"]) == 10 else f"{start_year}-{spec['start']}")
        end = (
            pd.Timestamp(spec["end"] if len(spec["end"]) == 10 else f"{end_year}-{spec['end']}")
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1)
        )

        # Warn if this window has no data at all
        window_mask = (df.index >= start) & (df.index <= end)
        if not window_mask.any():
            logger.warning(
                "🍂 compute_seasons: no data found for period %s (%s–%s) in year %s",
                code,
                start,
                end,
                year,
            )

        # MEAN of all non-precip columns
        means = _slice_and_mean(
            df.drop(columns=[precip_col], errors="ignore"), start, end
        ).to_dict()

        row = {
            "code": code,
            "label": spec.get("label", code.replace("_", " ")),
            "start": start,
            "end": end,
        }
        row.update(means)
        # Seasonal VWC bars and data downloads use matched ratios of means,
        # never means of the precomputed instantaneous ratio columns.
        window = df.loc[window_mask]
        for depth in ("1", "2", "3"):
            for location in ("T", "M", "B"):
                for numerator, denominator in (("S1", "S2"), ("S3", "S4")):
                    num_col = f"VWC_{depth}_raw_{numerator}_{location}"
                    den_col = f"VWC_{depth}_raw_{denominator}_{location}"
                    ratio_col = f"VWC_{depth}_ratio_{numerator}_{denominator}_{location}"
                    if ratio_col in df or (num_col in df and den_col in df):
                        row[ratio_col], _ = matched_ratio_of_means(window, num_col, den_col)
        expected = int((end + pd.Timedelta(seconds=1) - start) / pd.Timedelta(minutes=15))
        numeric_window = df.loc[window_mask].select_dtypes(include="number").drop(columns=[precip_col], errors="ignore")
        valid_index = numeric_window.index[numeric_window.notna().any(axis=1)]
        expected_index = pd.date_range(start, end, freq="15min")
        observed = int(expected_index.isin(valid_index.unique()).sum())
        now = pd.Timestamp.now()
        elapsed_index = expected_index[expected_index <= now.floor("15min")]
        missing = int((~elapsed_index.isin(valid_index.unique())).sum())
        row["period_observed_n"] = observed
        row["period_expected_n"] = expected
        row["period_incomplete"] = observed < expected
        row["period_missing_n"] = missing
        row["period_unfinished"] = end > now

        # SUM of precip increments over the window
        if include_precip and precip_col is not None:
            ser = (
                pd.to_numeric(df[precip_col], errors="coerce")
                .fillna(0.0)
                .clip(lower=0.0)
            )
            row[precip_col] = float(ser.loc[window_mask].sum())

        out_rows.append(row)

    out_df = pd.DataFrame(out_rows)

    # Debug: confirm that seasonal slice carries through key variables
    logger.info(
        "🍂 compute_seasons(year=%s): seasonal df shape=%s; SWC cols=%s; VWC cols=%s",
        year,
        out_df.shape,
        [c for c in out_df.columns if c.startswith("SWC")][:10],
        [c for c in out_df.columns if c.startswith("VWC")][:10],
    )

    return out_df

def assign_gseason_periods(ts: pd.Timestamp, year: int) -> str | None:
    """
    Return the period code in DEFAULT_GSEASON_PERIODS that contains timestamp `ts`.
    Handles wrap-around windows (e.g., Nov–Feb maps to the given `year`).
    """
    ts = pd.to_datetime(ts)
    for code, period in DEFAULT_GSEASON_PERIODS.items():
        sm, sd = map(int, period["start"].split("-"))
        em, ed = map(int, period["end"].split("-"))

        start_year = year - 1 if sm > em else year
        end_year = year

        start = pd.Timestamp(f"{start_year}-{period['start']}")
        end = (
            pd.Timestamp(f"{end_year}-{period['end']}")
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1)
        )

        if start <= ts <= end:
            return code
    return None
