#!/usr/bin/env python3
"""
fix_s3m_timestamps.py

Goal
----
Detect and correct historical timestamp offsets for the S3M logger by comparing
logger soil temperature (prefer 6-inch logger temp T_1_*) against a weather
reference series (prefer coagmet soil_temp_6in_degF, fallback soil_temp_2in_degF,
fallback temp_air_degF).

It:
  1) Loads S3M_Table1.dat from datfiles_2023..datfiles_2026 under --logger-root
  2) Loads weather parquet 2023..2026 under --weather-root
  3) Detects "clock jump" boundaries in logger timestamps (large gaps/backward)
  4) For each segment, finds best lag (minutes) on a 15-min grid by maximizing correlation
  5) Applies per-segment lag correction, writes corrected CSV + report CSV

Assumptions
-----------
- Logger timestamps are in local America/Denver style but may be offset by hours.
- Data are 15-minute cadence after aggregation in Table1 (or near it).
- We apply ONLY whole 15-minute shifts (snapped).

Usage
-----
python3 fix_s3m_timestamps.py \
  --logger-root "biochar_app/data-raw" \
  --weather-root "biochar_app/data-processed/parquet/summary/weather/15min" \
  --out-csv "/tmp/S3M_Table1_corrected.csv" \
  --report-csv "/tmp/S3M_timestamp_fix_report.csv"
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Config defaults
# -----------------------------
YEARS = (2023, 2024, 2025, 2026)

LOGGER_FILE_NAME = "S3M_Table1.dat"

# Prefer logger depth that moves most with ambient:
# Per your ETL naming: T_<depth>_raw_<strip>_<loc>
# In Table1.dat (raw logger files) we often see T_1_Avg, T_2_Avg, T_3_Avg etc.
# We’ll prefer T_1_Avg (6 inch) then T_2_Avg then T_3_Avg.
LOGGER_TEMP_PREFERRED = ["T_1_Avg", "T_2_Avg", "T_3_Avg", "T_1", "T_2", "T_3"]

# Weather parquet naming (your convention)
WEATHER_TS_CANDIDATES = ["timestamp", "TIMESTAMP", "Datetime", "DateTime", "datetime"]
WEATHER_TEMP_PREF = ["soil_temp_6in_degF", "soil_temp_2in_degF", "temp_air_degF"]

# Lag search (in minutes)
LAG_GRID_MINUTES = np.arange(-12 * 60, 12 * 60 + 1, 15)  # +/- 12 hours in 15-min steps

# Segment boundary detection
# If the logger timestamps jump backwards OR jump forward by a "too large" gap,
# we split segments. Battery swap + clock reset often creates such a jump.
BACKWARD_JUMP_MIN = -1        # anything negative indicates non-monotonic
FORWARD_GAP_MIN = 6 * 60      # split if gap > 6 hours (tune if needed)

# Minimum rows needed to compute a stable correlation within a segment
MIN_POINTS_SEGMENT = 500


# -----------------------------
# Helpers
# -----------------------------
def _pick_first_existing(columns: List[str], candidates: List[str]) -> Optional[str]:
    s = set(columns)
    for c in candidates:
        if c in s:
            return c
    # normalize
    norm = {c: " ".join(str(c).strip().lower().split()) for c in columns}
    for want in candidates:
        want_n = " ".join(str(want).strip().lower().split())
        for orig, n in norm.items():
            if n == want_n:
                return orig
    return None


def round_to_15min(ts: pd.Series) -> pd.Series:
    return ts.dt.floor("15min")


def corrcoef_safe(x: np.ndarray, y: np.ndarray) -> float:
    m = np.isfinite(x) & np.isfinite(y)
    x = x[m]
    y = y[m]
    if len(x) < 50:
        return np.nan
    sx = np.std(x)
    sy = np.std(y)
    if sx == 0 or sy == 0:
        return np.nan
    return float(np.corrcoef(x, y)[0, 1])


def read_toa5_table1(path: Path) -> pd.DataFrame:
    """
    Campbell TOA5 Table1 .dat has 4 header rows:
      row0: TOA5 metadata
      row1: column names
      row2: units
      row3: aggregation labels
      row4+: data
    We want row1 as header and skip units + agg rows.
    """
    df = pd.read_csv(
        path,
        header=1,
        skiprows=[2, 3],
        low_memory=False,
    )
    df.columns = [str(c).strip().strip('"') for c in df.columns]

    if "TIMESTAMP" not in df.columns:
        raise KeyError(f"TIMESTAMP not found in {path}. First cols: {list(df.columns)[:15]}")

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    df = df[df["TIMESTAMP"].notna()].sort_values("TIMESTAMP").reset_index(drop=True)

    # numeric conversion for all other columns
    for c in df.columns:
        if c != "TIMESTAMP":
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def load_all_s3m(logger_root: Path) -> pd.DataFrame:
    parts = []
    for y in YEARS:
        folder = logger_root / f"datfiles_{y}"
        f = folder / LOGGER_FILE_NAME
        if not f.exists():
            # silently skip missing years
            continue
        df = read_toa5_table1(f)
        df["source_year_folder"] = y
        df["source_file"] = str(f)
        parts.append(df)

    if not parts:
        raise FileNotFoundError(
            f"No {LOGGER_FILE_NAME} found under {logger_root}/datfiles_202x/"
        )

    out = pd.concat(parts, ignore_index=True).sort_values("TIMESTAMP").reset_index(drop=True)
    return out


def load_weather(weather_root: Path) -> Tuple[pd.DataFrame, str]:
    parts = []
    chosen_col = None

    for y in YEARS:
        f = weather_root / f"{y}_15min.parquet"
        if not f.exists():
            continue
        w = pd.read_parquet(f)
        w.columns = [str(c).strip() for c in w.columns]

        ts_col = _pick_first_existing(list(w.columns), WEATHER_TS_CANDIDATES)
        if ts_col is None:
            raise KeyError(f"{f}: could not find timestamp column. Columns: {list(w.columns)[:25]}")

        temp_col = None
        for cand in WEATHER_TEMP_PREF:
            if cand in w.columns:
                temp_col = cand
                break
        if temp_col is None:
            raise KeyError(f"{f}: could not find any of {WEATHER_TEMP_PREF}. Columns: {list(w.columns)[:40]}")

        if chosen_col is None:
            chosen_col = temp_col

        w = w[[ts_col, temp_col]].copy()
        w = w.rename(columns={ts_col: "timestamp", temp_col: "ref_temp"})
        w["timestamp"] = pd.to_datetime(w["timestamp"], errors="coerce")
        w = w[w["timestamp"].notna()].sort_values("timestamp").reset_index(drop=True)
        w["ref_temp"] = pd.to_numeric(w["ref_temp"], errors="coerce")
        w["source_year"] = y

        parts.append(w)

    if not parts:
        raise FileNotFoundError(f"No weather parquet files found in {weather_root} for years {YEARS}")

    weather = pd.concat(parts, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
    return weather, chosen_col or "ref_temp"


def choose_logger_temp_col(df: pd.DataFrame) -> str:
    cols = set(df.columns)
    for c in LOGGER_TEMP_PREFERRED:
        if c in cols:
            return c
    # fallback: first col that starts with T_ and has Avg
    tcols = [c for c in df.columns if str(c).startswith("T_") and ("Avg" in str(c) or str(c).endswith("_Avg"))]
    if tcols:
        return tcols[0]
    raise KeyError(f"Could not find logger temp column. Available cols: {list(df.columns)[:40]}")


@dataclass
class SegmentResult:
    seg_id: int
    start: pd.Timestamp
    end: pd.Timestamp
    n_rows: int
    best_lag_min: float
    best_corr: float
    logger_temp_col: str
    ref_temp_col: str


def split_into_segments(df: pd.DataFrame) -> List[Tuple[int, int]]:
    """
    Returns list of (start_idx, end_idx_exclusive) into df that define segments.
    """
    t = df["TIMESTAMP"].to_numpy()
    # compute diffs in minutes
    dt = (df["TIMESTAMP"].diff().dt.total_seconds() / 60.0).to_numpy()

    cut_idxs = [0]
    for i in range(1, len(df)):
        d = dt[i]
        if np.isnan(d):
            continue
        if d < BACKWARD_JUMP_MIN or d > FORWARD_GAP_MIN:
            cut_idxs.append(i)
    cut_idxs.append(len(df))

    # merge tiny segments if needed
    segs = []
    for a, b in zip(cut_idxs[:-1], cut_idxs[1:]):
        if b - a <= 0:
            continue
        segs.append((a, b))
    return segs


def best_lag_for_segment(
    seg: pd.DataFrame,
    logger_temp_col: str,
    weather: pd.DataFrame,
    min_points_segment: int
):
    """
    Find lag that maximizes correlation between logger temp and weather ref_temp.
    lag meaning: corrected_time = original_time + lag_minutes

    We compute on 15-min-rounded timestamps to avoid seconds noise.
    """
    s = seg.copy()
    s["t15"] = round_to_15min(s["TIMESTAMP"])
    s = s[["t15", logger_temp_col]].rename(columns={logger_temp_col: "log_temp"})

    w = weather.copy()
    w["t15"] = round_to_15min(w["timestamp"])
    w = w[["t15", "ref_temp"]].groupby("t15", as_index=False).mean(numeric_only=True)

    # reduce to overlap near segment (no point in joining entire multi-year)
    tmin = s["t15"].min()
    tmax = s["t15"].max()
    w = w[
        (w["t15"] >= tmin - pd.Timedelta(hours=24))
        & (w["t15"] <= tmax + pd.Timedelta(hours=24))
    ].copy()

    # NOTE: keep your existing behavior (MIN_POINTS_SEGMENT) even though
    # min_points_segment is passed in; your main() appears to use the global.
    if len(s) < min_points_segment:
        return np.nan, np.nan, 0

    best_corr = -np.inf
    best_lag = np.nan
    best_n = 0

    # Pre-build weather series for quick joins
    w_series = w.set_index("t15")["ref_temp"]

    # ✅ Guard against duplicate timestamps in weather index (rare, but possible)
    # Use mean for safety (works whether there are duplicates or not).
    if w_series.index.has_duplicates:
        w_series = w_series.groupby(level=0).mean()

    for lag in LAG_GRID_MINUTES:
        shifted_t = s["t15"] + pd.Timedelta(minutes=float(lag))
        log_series = pd.Series(s["log_temp"].to_numpy(), index=shifted_t)

        # ✅ Guard against duplicate timestamps in logger series (common)
        # Mean-of-duplicates is a sensible default for 15-min bins.
        if log_series.index.has_duplicates:
            log_series = log_series.groupby(level=0).mean()

        joined = pd.concat([w_series, log_series], axis=1, join="inner").dropna()
        n = len(joined)
        if n < 300:
            continue

        c = corrcoef_safe(joined.iloc[:, 0].to_numpy(), joined.iloc[:, 1].to_numpy())
        if np.isnan(c):
            continue

        if c > best_corr:
            best_corr = c
            best_lag = float(lag)
            best_n = int(n)

    if best_corr == -np.inf:
        return np.nan, np.nan, 0

    return best_lag, float(best_corr), best_n


def snap_to_15min(lag_min: float) -> float:
    if np.isnan(lag_min):
        return lag_min
    return float(15 * int(np.round(lag_min / 15.0)))


def apply_segment_lags(df: pd.DataFrame, segments: List[Tuple[int, int]], lags_min: List[float]) -> pd.DataFrame:
    out = df.copy()
    out["TIMESTAMP_orig"] = out["TIMESTAMP"]

    seg_id_col = np.full(len(out), -1, dtype=int)
    for seg_id, (a, b) in enumerate(segments):
        seg_id_col[a:b] = seg_id
    out["segment_id"] = seg_id_col

    # apply
    corr_ts = out["TIMESTAMP"].copy()
    for seg_id, lag in enumerate(lags_min):
        if np.isnan(lag):
            continue
        mask = out["segment_id"] == seg_id
        corr_ts.loc[mask] = corr_ts.loc[mask] + pd.Timedelta(minutes=float(lag))

    out["TIMESTAMP"] = corr_ts
    return out


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--logger-root", required=True, help="Path containing datfiles_2023, datfiles_2024, ...")
    ap.add_argument("--weather-root", required=True, help="Path containing 2023_15min.parquet, ...")
    ap.add_argument("--out-csv", required=True, help="Output corrected CSV path")
    ap.add_argument("--report-csv", required=True, help="Output report CSV path")
    ap.add_argument("--min-segment-rows", type=int, default=MIN_POINTS_SEGMENT, help="Minimum rows per segment")
    args = ap.parse_args()

    min_points_segment = int(args.min_segment_rows)

    logger_root = Path(args.logger_root).expanduser().resolve()
    weather_root = Path(args.weather_root).expanduser().resolve()
    out_csv = Path(args.out_csv).expanduser().resolve()
    report_csv = Path(args.report_csv).expanduser().resolve()

    print(f"[INFO] Loading S3M Table1 across years {YEARS}...")
    lg = load_all_s3m(logger_root)
    print(f"[OK] Loaded logger rows={len(lg)} range={lg['TIMESTAMP'].min()} -> {lg['TIMESTAMP'].max()}")

    logger_temp_col = choose_logger_temp_col(lg)
    print(f"[OK] Using logger temp column: {logger_temp_col}")

    print("[INFO] Loading weather parquet across years...")
    weather, ref_col_used = load_weather(weather_root)

    # Important: weather loader normalizes to ("timestamp","ref_temp")
    # but we want to report which physical ref variable was used
    # Choose best available overall (6in preferred, else 2in, else air)
    # load_weather picks first available in each file; but which one matters depends on your files.
    # We will just label it generically:
    # (If you always have soil_temp_6in_degF, it will choose that.)
    actual_ref_col = None
    # quick introspection from one file: just see preference exists in weather_root/2024_15min.parquet
    probe = None
    for y in YEARS:
        f = weather_root / f"{y}_15min.parquet"
        if f.exists():
            probe = pd.read_parquet(f, columns=None)
            probe.columns = [str(c).strip() for c in probe.columns]
            for cand in WEATHER_TEMP_PREF:
                if cand in probe.columns:
                    actual_ref_col = cand
                    break
            break
    if actual_ref_col is None:
        actual_ref_col = "ref_temp"

    print(f"[OK] Weather rows={len(weather)} range={weather['timestamp'].min()} -> {weather['timestamp'].max()}")
    print(f"[OK] Reference temperature column (preferred): {actual_ref_col}")

    # Restrict logger to weather coverage to avoid useless tail
    wmin, wmax = weather["timestamp"].min(), weather["timestamp"].max()
    lg = lg[(lg["TIMESTAMP"] >= wmin - pd.Timedelta(days=2)) & (lg["TIMESTAMP"] <= wmax + pd.Timedelta(days=2))].copy()
    lg = lg.sort_values("TIMESTAMP").reset_index(drop=True)
    print(f"[OK] Logger restricted to weather coverage: rows={len(lg)}")

    print("[INFO] Detecting timestamp segments (clock jumps)...")
    segments = split_into_segments(lg)
    print(f"[OK] Found {len(segments)} segment(s).")

    results: List[SegmentResult] = []
    lags: List[float] = []

    for seg_id, (a, b) in enumerate(segments):
        seg = lg.iloc[a:b].copy()
        start = seg["TIMESTAMP"].min()
        end = seg["TIMESTAMP"].max()
        n_rows = len(seg)

        print(f"\n[SEG {seg_id}] rows={n_rows} range={start} -> {end}")

        if n_rows < min_points_segment:
            print(f"  [WARN] Too few rows (<{min_points_segment}); skipping lag estimation for this segment.")
            best_lag = np.nan
            best_corr = np.nan
            best_n = 0
        else:
            best_lag, best_corr, best_n = best_lag_for_segment(seg, logger_temp_col, weather, min_points_segment)
            best_lag = snap_to_15min(best_lag)

        print(f"  best_lag_min={best_lag}  best_corr={best_corr}  n_join_used={best_n}")

        lags.append(best_lag)
        results.append(
            SegmentResult(
                seg_id=seg_id,
                start=start,
                end=end,
                n_rows=n_rows,
                best_lag_min=best_lag,
                best_corr=best_corr,
                logger_temp_col=logger_temp_col,
                ref_temp_col=actual_ref_col,
            )
        )

    print("\n[INFO] Applying per-segment lags...")
    corrected = apply_segment_lags(lg, segments, lags)

    # Write corrected CSV
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    corrected.to_csv(out_csv, index=False)
    print(f"[OK] Wrote corrected CSV: {out_csv}")

    # Write report CSV
    report_csv.parent.mkdir(parents=True, exist_ok=True)
    rep = pd.DataFrame([r.__dict__ for r in results])
    rep.to_csv(report_csv, index=False)
    print(f"[OK] Wrote report CSV: {report_csv}")

    # Quick sanity: show last few lines
    print("\n[INFO] Quick sanity check (last 5 rows):")
    show_cols = ["TIMESTAMP_orig", "TIMESTAMP", "segment_id", logger_temp_col]
    print(corrected[show_cols].tail(5).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())