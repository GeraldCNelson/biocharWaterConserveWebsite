#!/usr/bin/env python3
"""
Resample *_decoded.csv files to fixed-width bins with several methods.

Examples:
  # Mean with >=10 raw samples per bin
  python tools/resample_decoded.py \
    --glob 'biochar_app/pakbus/bdFiles/out_fetch/*_decoded.csv' \
    --freq 15min --method mean --min-samples 10 \
    --suffix _decoded15mMean.csv

  # Last sample in each bin
  python tools/resample_decoded.py \
    --glob 'biochar_app/pakbus/bdFiles/out_fetch/*_decoded.csv' \
    --freq 15min --method last \
    --suffix _decoded15mLast.csv

  # Nearest to the bin anchor, drop matches farther than 8 minutes
  python tools/resample_decoded.py \
    --glob 'biochar_app/pakbus/bdFiles/out_fetch/*_decoded.csv' \
    --freq 15min --method nearest --max-gap 8min \
    --suffix _decoded15mNearest.csv
"""

import argparse
import glob
import pathlib
import sys
from typing import Optional

import pandas as pd


def _map_label(label: str) -> str:
    """Map user-friendly label 'start'/'end' to pandas 'left'/'right'."""
    label = (label or "").lower()
    if label in ("start", "left"):
        return "left"
    if label in ("end", "right"):
        return "right"
    # default to right
    return "right"


def fmt_ts_utc(idx: pd.DatetimeIndex) -> pd.Series:
    """Format timestamps as ISO8601 with explicit +00:00 offset."""
    return pd.to_datetime(idx, utc=True).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def resample_file(
    fpath: str,
    freq: str,
    method: str,
    min_samples: int,
    label: str,
    suffix: str,
    max_gap_str: Optional[str],
    timestamp_col: str = "TimestampUTC",
) -> Optional[str]:
    df = pd.read_csv(fpath)
    if timestamp_col not in df.columns:
        return None

    ts = pd.to_datetime(df[timestamp_col], utc=True, errors="coerce")
    df = df.loc[~ts.isna()].copy()
    if df.empty:
        return None

    df[timestamp_col] = ts
    df = df.sort_values(timestamp_col).set_index(timestamp_col)

    # Prepare numeric columns for aggregation
    num = df.select_dtypes(include=["number"]).copy()

    # Map label/closed to pandas' accepted values
    lr = _map_label(label)          # 'left' or 'right'
    closed = lr                     # keep symmetry: closed side = label side

    if method == "mean":
        counts = df.resample(freq, label=lr, closed=closed).size()
        out = num.resample(freq, label=lr, closed=closed).mean()
        out = out[counts >= min_samples]

    elif method == "last":
        counts = df.resample(freq, label=lr, closed=closed).size()
        out = num.resample(freq, label=lr, closed=closed).last()
        out = out[counts >= min_samples]

    elif method == "nearest":
        # Build anchor grid and pick nearest raw sample to each anchor,
        # optionally enforcing a maximum distance (tolerance).
        tol = pd.Timedelta(max_gap_str) if max_gap_str else None

        start = df.index.min().floor(freq)
        end = df.index.max().ceil(freq)
        anchors = pd.date_range(start=start, end=end, freq=freq)

        src = num.copy()
        src["__ts__"] = src.index
        src = src.sort_values("__ts__")

        anchor_df = pd.DataFrame({"__ts__": anchors})

        merged = pd.merge_asof(
            anchor_df,
            src,
            on="__ts__",
            direction="nearest",
            tolerance=tol,
        ).set_index("__ts__")

        # If tolerance removed a match, all numeric cols will be NaN — drop those rows.
        out = merged.dropna(how="all")

    else:
        raise ValueError(f"Unknown method: {method}")

    if out is None or out.empty:
        return None

    # Emit TimestampUTC first, formatted with +00:00
    out_df = out.copy()
    out_df.insert(0, "TimestampUTC", fmt_ts_utc(out_df.index))
    out_df = out_df.reset_index(drop=True)

    out_path = pathlib.Path(fpath).with_name(
        pathlib.Path(fpath).name.replace("_decoded.csv", suffix)
    )
    out_df.to_csv(out_path, index=False)
    print(f"wrote {out_path}")
    return str(out_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--glob", default="biochar_app/pakbus/bdFiles/out_fetch/*_decoded.csv",
                    help="Glob of input decoded CSVs")
    ap.add_argument("--freq", default="15min", help="Resample frequency (e.g., 15min)")
    ap.add_argument("--method", choices=["mean", "last", "nearest"], default="mean",
                    help="Aggregation method")
    ap.add_argument("--min-samples", type=int, default=1,
                    help="Minimum raw rows required in a bin (mean/last only)")
    ap.add_argument("--label", choices=["start", "end", "left", "right"], default="end",
                    help="Which side to label/close the bin (maps to pandas left/right)")
    ap.add_argument("--suffix", default="_decoded15mMean.csv",
                    help="Output filename suffix")
    ap.add_argument("--max-gap", default=None,
                    help="For --method nearest: max distance from anchor (e.g., 8min). "
                         "Rows with no match within tolerance are dropped.")
    ap.add_argument("--timestamp-col", default="TimestampUTC",
                    help="Timestamp column name")
    args = ap.parse_args()

    paths = sorted(glob.glob(args.glob))
    if not paths:
        print(f"No files matched: {args.glob}", file=sys.stderr)
        sys.exit(0)

    for f in paths:
        try:
            resample_file(
                f,
                freq=args.freq,
                method=args.method,
                min_samples=args.min_samples,
                label=args.label,
                suffix=args.suffix,
                max_gap_str=args.max_gap,
                timestamp_col=args.timestamp_col,
            )
        except Exception as e:
            print(f"[WARN] {f}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()