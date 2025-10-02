#!/usr/bin/env python3
"""
compare_table1_magnitudes.py

Compare magnitudes between a TOA5 Table1 .dat file and BD-decoded table1_master.csv.
- Robust TOA5 parsing: skips units/type rows, coerces numerics safely.
- Generates stats and (if timestamps present) a time-aligned comparison.
- Writes a "quality dashboard" (rel_error_quality.csv) with per-field percentiles/flags.

Outputs (in --out-dir):
  dat_stats.csv, bd_stats.csv
  magnitude_comparison_common.csv
  time_aligned_15min.csv (if both have timestamps and overlap)
  time_aligned_error_summary.csv (if overlap)
  rel_error_quality.csv (if overlap)
"""

import argparse
import os
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

EXPECTED_FIELDS = [
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
]

# ---------- Helpers ----------

def clean_numeric_series(s: pd.Series) -> pd.Series:
    """
    Strip quotes/whitespace, map blanks to NaN, then coerce to numeric.
    Uses element-wise try/float to avoid deprecated to_numeric(...) options.
    """
    s = s.astype(str).str.strip().str.strip('"')
    s = s.replace({"": np.nan})
    def _to_num(x):
        try:
            return float(x)
        except Exception:
            return np.nan
    return s.map(_to_num)

def read_toa5_dat(path: str) -> pd.DataFrame:
    """
    Read a Campbell TOA5 .dat and return a dataframe with TIMESTAMP (datetime) and numeric fields.
    Handles the standard 4-line header:
      row0: "TOA5", row1: column names, row2: units, row3: types
    """
    with open(path, "r", errors="replace") as f:
        first = f.readline()
    is_toa5 = first.strip().startswith('"TOA5"') or first.strip().startswith("TOA5")
    if is_toa5:
        header_row = 1
        skiprows = [2, 3]
    else:
        header_row = 0
        skiprows = None

    df = pd.read_csv(
        path,
        header=header_row,
        skiprows=skiprows,
        low_memory=False,
        dtype=str,
        quoting=0,              # QUOTE_MINIMAL
        skip_blank_lines=True,
    )
    df.columns = [c.strip().strip('"') for c in df.columns]

    # TIMESTAMP
    ts_col = None
    for cand in ["TIMESTAMP", "timestamp", "TimeStamp", "DateTime"]:
        if cand in df.columns:
            ts_col = cand
            break
    if ts_col is not None:
        ts = df[ts_col].astype(str).str.strip().str.strip('"').replace({"": np.nan})
        dt = pd.to_datetime(ts, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        if dt.isna().all():
            dt = pd.to_datetime(ts, errors="coerce")
        df["TIMESTAMP"] = dt
    else:
        df["TIMESTAMP"] = pd.NaT

    # Coerce numeric for everything except TIMESTAMP
    for c in df.columns:
        if c == "TIMESTAMP":
            continue
        df[c] = clean_numeric_series(df[c])

    # RECORD (if present)
    if "RECORD" in df.columns:
        df["RECORD"] = clean_numeric_series(df["RECORD"])

    # Drop fully empty rows
    df = df.dropna(how="all")
    return df

def read_bd_csv(path: str) -> pd.DataFrame:
    """Read BD-decoded table1_master.csv, coerce numerics, parse TIMESTAMP (UTC) if present."""
    df = pd.read_csv(path, low_memory=False, dtype=str)
    df.columns = [c.strip().strip('"') for c in df.columns]

    # TIMESTAMP (BD side usually ISO with Z) -> force UTC-aware
    if "TIMESTAMP" in df.columns:
        ts = df["TIMESTAMP"].astype(str).str.strip().str.strip('"').replace({"": np.nan})
        dt = pd.to_datetime(ts, errors="coerce", utc=True)  # parses Z/offsets to UTC aware
        if dt.isna().all():
            dt = pd.to_datetime(ts, format="%Y-%m-%d %H:%M:%S", errors="coerce", utc=True)
        df["TIMESTAMP"] = dt
    else:
        df["TIMESTAMP"] = pd.NaT

    # Numeric coercion
    for c in df.columns:
        if c == "TIMESTAMP":
            continue
        df[c] = clean_numeric_series(df[c])

    df = df.dropna(how="all")
    return df

def choose_fields(df: pd.DataFrame) -> List[str]:
    """Pick fields to compare (prioritize EXPECTED_FIELDS, else first 10 numeric)."""
    num_cols = [c for c in df.columns if c != "TIMESTAMP" and pd.api.types.is_numeric_dtype(df[c])]
    chosen = [c for c in EXPECTED_FIELDS if c in num_cols]
    if chosen:
        return chosen
    return num_cols[:10]

def basic_stats(df: pd.DataFrame, fields: List[str], prefix: str) -> pd.DataFrame:
    sub = df[fields].copy()
    desc = pd.DataFrame({
        f"{prefix}_count": sub.count(),
        f"{prefix}_mean": sub.mean(numeric_only=True),
        f"{prefix}_std":  sub.std(numeric_only=True),
        f"{prefix}_min":  sub.min(numeric_only=True),
        f"{prefix}_p25":  sub.quantile(0.25, numeric_only=True),
        f"{prefix}_p50":  sub.quantile(0.50, numeric_only=True),
        f"{prefix}_p75":  sub.quantile(0.75, numeric_only=True),
        f"{prefix}_max":  sub.max(numeric_only=True),
    })
    desc.index.name = "field"
    return desc.reset_index()

def format_df_numeric(df: pd.DataFrame, precision: int = 6) -> pd.DataFrame:
    """Round numeric columns for pretty CSVs (no forced quoting)."""
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].round(precision)
    return out

def _normalize_df_ts(
    df: pd.DataFrame,
    is_dat: bool,
    tz: Optional[str]
) -> pd.DataFrame:
    """
    Make all timestamps tz-aware UTC.
    - DAT: naive -> localize to `tz` (e.g., America/Denver) with DST rules, then convert to UTC.
    - BD: already UTC-aware; if naive, localize to UTC.
    """
    if not df["TIMESTAMP"].notna().any():
        return df

    ser = df["TIMESTAMP"].copy()

    if ser.dt.tz is None:
        if is_dat:
            # local time -> localize with DST handling, then convert to UTC
            if tz is None:
                # fallback: treat as UTC if tz not given
                ser = ser.dt.tz_localize("UTC")
            else:
                # Resolve DST ambiguity; mark ambiguous as NaT, shift nonexistent forward
                ser = ser.dt.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward")
                ser = ser.dt.tz_convert("UTC")
        else:
            # BD naive -> assume UTC
            ser = ser.dt.tz_localize("UTC")
    else:
        # already tz-aware -> convert to UTC
        ser = ser.dt.tz_convert("UTC")

    df = df.copy()
    df["TIMESTAMP"] = ser
    # Drop any rows that became NaT from ambiguous DST
    df = df[df["TIMESTAMP"].notna()]
    return df

def _to_utc_bounds(since_str: Optional[str], until_str: Optional[str]) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Parse window bounds; if naive, localize to UTC for safe comparison."""
    def _one(x: Optional[str]) -> Optional[pd.Timestamp]:
        if not x:
            return None
        t = pd.to_datetime(x)
        if t.tzinfo is None:
            return t.tz_localize("UTC")
        return t.tz_convert("UTC")
    return _one(since_str), _one(until_str)

def resample_and_join(
    dat: pd.DataFrame,
    bd: pd.DataFrame,
    fields_common: List[str],
    bin_freq: str,
    dat_tz: Optional[str],
    precision: int,
    out_dir: str
):
    """
    Resample both sides on bin_freq and inner-join. Produces time_aligned_15min.csv and summaries.
    """
    # Prepare DAT timeline
    dat_t = dat[["TIMESTAMP"] + fields_common].dropna(subset=["TIMESTAMP"]).copy()
    dat_t = _normalize_df_ts(dat_t, is_dat=True, tz=dat_tz)
    dat_t = dat_t.set_index("TIMESTAMP").sort_index().resample(bin_freq).mean()

    # Prepare BD timeline
    bd_t = bd[["TIMESTAMP"] + fields_common].dropna(subset=["TIMESTAMP"]).copy()
    bd_t = _normalize_df_ts(bd_t, is_dat=False, tz=None)
    bd_t = bd_t.set_index("TIMESTAMP").sort_index().resample(bin_freq).mean()

    # Inner join
    joined = dat_t.join(bd_t, how="inner", lsuffix="_dat", rsuffix="_bd")
    if joined.empty:
        # Still write an empty CSV with header that includes TIMESTAMP
        out_time = os.path.join(out_dir, "time_aligned_15min.csv")
        joined.index.name = "TIMESTAMP"
        format_df_numeric(joined.reset_index(), precision).to_csv(out_time, index=False)
        return None, None, joined

    # Keep only paired columns and compute deltas/errors
    keep_cols = []
    for f in fields_common:
        d = f + "_dat"; b = f + "_bd"
        if d in joined.columns and b in joined.columns:
            keep_cols += [d, b]
    joined = joined[keep_cols].copy()

    for f in fields_common:
        d = f + "_dat"; b = f + "_bd"
        if d in joined.columns and b in joined.columns:
            joined[f + "_delta"]   = joined[b] - joined[d]
            joined[f + "_rel_err"] = joined[f + "_delta"] / joined[d].replace(0, np.nan)

    # ✅ Make TIMESTAMP a real column
    joined.index.name = "TIMESTAMP"
    joined = joined.reset_index()

    # Write time-aligned table
    out_time = os.path.join(out_dir, "time_aligned_15min.csv")
    format_df_numeric(joined, precision).to_csv(out_time, index=False)

    # Per-field error summary (absolute deltas)
    summary_rows = []
    for f in fields_common:
        dcol = f + "_delta"
        if dcol in joined.columns:
            s = joined[dcol].dropna()
            if not s.empty:
                summary_rows.append({
                    "field": f,
                    "count": int(s.count()),
                    "MAE": float(np.mean(np.abs(s))),
                    "RMSE": float(np.sqrt(np.mean(s**2))),
                    "mean_delta": float(np.mean(s)),
                    "std_delta": float(np.std(s, ddof=1)) if s.count() > 1 else np.nan,
                })
    out_err = None
    if summary_rows:
        out_err = os.path.join(out_dir, "time_aligned_error_summary.csv")
        format_df_numeric(pd.DataFrame(summary_rows), precision).to_csv(out_err, index=False)

    # Relative error quality dashboard
    quality_rows = []
    for f in fields_common:
        rcol = f + "_rel_err"
        if rcol not in joined.columns:
            continue
        r = joined[rcol].dropna()
        if r.empty:
            continue
        absr = r.abs()
        n    = int(absr.count())
        p50  = float(absr.quantile(0.50))
        p95  = float(absr.quantile(0.95))
        p99  = float(absr.quantile(0.99))
        maxa = float(absr.max())
        share_05 = float((absr <= 0.005).mean())  # within 0.5%
        share_10 = float((absr <= 0.010).mean())  # within 1%
        share_20 = float((absr <= 0.020).mean())  # within 2%
        mae  = float(np.mean(np.abs(joined[f + "_delta"].dropna())))
        rmse = float(np.sqrt(np.mean((joined[f + "_delta"].dropna())**2)))
        bias = float(np.mean(joined[f + "_delta"].dropna()))
        # Flags (tweak thresholds as desired)
        flag = ""
        if p95 > 0.02 or share_10 < 0.95:
            flag = "CHECK"
        if p95 <= 0.01 and share_05 >= 0.90:
            flag = "GOOD"
        quality_rows.append({
            "field": f,
            "n_overlap": n,
            "abs_rel_err_p50": p50,
            "abs_rel_err_p95": p95,
            "abs_rel_err_p99": p99,
            "abs_rel_err_max": maxa,
            "share_within_0p5pct": share_05,
            "share_within_1pct":   share_10,
            "share_within_2pct":   share_20,
            "MAE_abs_units": mae,
            "RMSE_abs_units": rmse,
            "bias_mean_delta": bias,
            "flag": flag,
        })
    if quality_rows:
        out_quality = os.path.join(out_dir, "rel_error_quality.csv")
        format_df_numeric(pd.DataFrame(quality_rows), precision).to_csv(out_quality, index=False)

    return out_time, out_err, joined

# ---------- Main comparison flow ----------

def compare_magnitudes(
    dat: pd.DataFrame,
    bd: pd.DataFrame,
    out_dir: str,
    precision: int,
    bin_freq: str,
    dat_tz: Optional[str]
):
    os.makedirs(out_dir, exist_ok=True)

    dat_fields = choose_fields(dat)
    bd_fields  = choose_fields(bd)

    # Stats per side
    dat_stats = basic_stats(dat, dat_fields, "dat")
    bd_stats  = basic_stats(bd,  bd_fields,  "bd")

    format_df_numeric(dat_stats, precision).to_csv(os.path.join(out_dir, "dat_stats.csv"), index=False)
    format_df_numeric(bd_stats,  precision).to_csv(os.path.join(out_dir, "bd_stats.csv"),  index=False)

    # Common fields: intersect
    common = sorted(set(dat_fields).intersection(bd_fields))
    if common:
        ds = dat_stats.set_index("field").loc[common]
        bs = bd_stats.set_index("field").loc[common]
        comp = ds.join(bs, how="inner")
        for m in ["mean", "std", "min", "p25", "p50", "p75", "max"]:
            a = f"bd_{m}"
            b = f"dat_{m}"
            comp[f"ratio_{m}"]   = comp[a] / comp[b].replace(0, np.nan)
            comp[f"absdiff_{m}"] = comp[a] - comp[b]
        out_path = os.path.join(out_dir, "magnitude_comparison_common.csv")
        format_df_numeric(comp.reset_index(), precision).to_csv(out_path, index=False)

    # Time-aligned analysis
    if dat["TIMESTAMP"].notna().any() and bd["TIMESTAMP"].notna().any():
        # Print spans
        dat_ts = dat["TIMESTAMP"].dropna()
        bd_ts  = bd["TIMESTAMP"].dropna()
        span_dat = f"{dat_ts.min()} .. {dat_ts.max()} (rows with TS={dat_ts.size})"
        span_bd  = f"{bd_ts.min()} .. {bd_ts.max()} (rows with TS={bd_ts.size})"
        print(f"[SPAN] DAT: {span_dat}")
        print(f"[SPAN] BD:  {span_bd}")

        # Resample & join
        resample_and_join(
            dat=dat,
            bd=bd,
            fields_common=common,
            bin_freq=bin_freq,
            dat_tz=dat_tz,
            precision=precision,
            out_dir=out_dir
        )

def main():
    ap = argparse.ArgumentParser(description="Compare magnitudes between TOA5 Table1 .dat and BD-decoded CSV.")
    ap.add_argument("--dat", required=True, help="Path to S1M Table1 .dat (TOA5)")
    ap.add_argument("--bd", required=True, help="Path to BD-decoded table1_master.csv")
    ap.add_argument("--out-dir", default="pakbus_runs/compare_table1", help="Output directory")
    ap.add_argument("--precision", type=int, default=6, help="Decimal places for CSV outputs")
    ap.add_argument("--bin", default="15min", help="Resample bin width (e.g., 15min, 5min, 1H)")
    ap.add_argument("--dat-tz", default=None, help="IANA TZ for DAT when timestamps are naive (e.g., America/Denver)")
    ap.add_argument("--since", default=None, help="Filter to timestamps >= this date (YYYY-MM-DD)")
    ap.add_argument("--until", default=None, help="Filter to timestamps < this date (YYYY-MM-DD)")
    args = ap.parse_args()

    # Load
    dat_df = read_toa5_dat(args.dat)
    bd_df  = read_bd_csv(args.bd)

    # Normalize timestamps to UTC BEFORE any windowing
    dat_df = _normalize_df_ts(dat_df, is_dat=True, tz=args.dat_tz)
    bd_df  = _normalize_df_ts(bd_df,  is_dat=False, tz=None)

    # Optional UTC window
    if args.since or args.until:
        since_utc, until_utc = _to_utc_bounds(args.since, args.until)

        def _apply_window_utc(df: pd.DataFrame) -> pd.DataFrame:
            if not df["TIMESTAMP"].notna().any():
                return df
            m = pd.Series(True, index=df.index)
            if since_utc is not None:
                m &= df["TIMESTAMP"] >= since_utc
            if until_utc is not None:
                m &= df["TIMESTAMP"] < until_utc
            return df.loc[m]

        dat_df = _apply_window_utc(dat_df)
        bd_df  = _apply_window_utc(bd_df)

    compare_magnitudes(
        dat=dat_df,
        bd=bd_df,
        out_dir=args.out_dir,
        precision=args.precision,
        bin_freq=args.bin,
        dat_tz=args.dat_tz
    )
    print(f"[OK] Wrote comparison outputs under: {args.out_dir}")

if __name__ == "__main__":
    main()