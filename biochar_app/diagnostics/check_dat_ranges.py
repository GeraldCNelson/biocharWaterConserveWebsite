#!/usr/bin/env python3
"""
check_dat_ranges.py

Scan Campbell TOA5 .dat files and report out-of-range raw values
directly from the .dat files (no ETL conversions).

Sentinel values are masked first (|x| >= bad_value_threshold).

Project-specific scan scope:
  biochar_app/data-raw/datfiles_2023..datfiles_2026

Outputs:
  1) detailed violations CSV (per-row)
  2) summary CSV grouped by logger_tag/column/rule

Usage:
  python -m biochar_app.diagnostics.check_dat_ranges \
    --root biochar_app/data-raw \
    --glob "*Table1*.dat" \
    --out biochar_app/diagnostics/dat_range_violations.csv
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from biochar_app.scripts.config import DEFAULT_BAD_VALUE_THRESHOLD


# ------------------------------------------------
# Range rule definition
# ------------------------------------------------

@dataclass(frozen=True)
class RangeRule:
    name: str
    min_value: Optional[float]
    max_value: Optional[float]
    inclusive: bool = True

    def violates(self, s: pd.Series) -> pd.Series:
        x = pd.to_numeric(s, errors="coerce")
        bad = pd.Series(False, index=x.index)

        if self.min_value is not None:
            bad |= (x < self.min_value)

        if self.max_value is not None:
            bad |= (x > self.max_value)

        return bad & x.notna()


# ------------------------------------------------
# Strict physical bounds (raw dat units)
# ------------------------------------------------

RULES: List[Tuple[str, RangeRule]] = [
    ("VWC_", RangeRule("VWC fraction (phys)", min_value=0.0, max_value=1.0)),
    ("T_", RangeRule("Soil temp raw (°C)", min_value=-50.0, max_value=80.0)),
    ("EC_", RangeRule("EC raw (dS/m)", min_value=0.0, max_value=20.0)),
    ("BattV_Min", RangeRule("Battery voltage (V)", min_value=0.0, max_value=20.0)),
]

TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%y %H:%M",
    "%m/%d/%Y %H:%M",
]


# ------------------------------------------------
# Utility helpers
# ------------------------------------------------

def extract_logger_tag(path: Path) -> str:
    """
    Extract logger tag from filename.
    Example: S3M_Table1.dat -> S3M
    """
    name = path.name
    if "_Table" in name:
        return name.split("_Table")[0]
    return name


def _clean_col_name(s: str) -> str:
    return s.lstrip("\ufeff").strip().strip('"').strip("'")


# ------------------------------------------------
# TOA5 reader
# ------------------------------------------------

def _read_toa5(datfile: Path) -> pd.DataFrame:
    with datfile.open("r", newline="") as f:
        r = csv.reader(f)
        _meta = next(r, None)
        colnames = next(r, None)
        _units = next(r, None)
        _aggs = next(r, None)

    if not colnames:
        raise ValueError(f"{datfile.name}: missing TOA5 column-name row")

    colnames_clean = [_clean_col_name(c) for c in colnames]

    return pd.read_csv(
        datfile,
        skiprows=4,
        header=None,
        names=colnames_clean,
        na_values=["", "NA", "NAN"],
        engine="python",
    )


def _parse_timestamp(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()

    for fmt in TS_FORMATS:
        ts = pd.to_datetime(s, format=fmt, errors="coerce")
        if ts.notna().any():
            return pd.to_datetime(s, format=fmt, errors="coerce")

    return pd.to_datetime(s, errors="coerce")


# ------------------------------------------------
# File iteration
# ------------------------------------------------

def iter_project_datfiles(root: Path, glob_pattern: str) -> Iterable[Path]:
    """
    If root is biochar_app/data-raw, scan ONLY datfiles_2023..datfiles_2026
    and do a NON-recursive glob inside each folder. This avoids scanning
    archived duplicates (old/, datsForPC/, etc.) unless you explicitly point
    root at those directories.
    """
    if root.is_file():
        yield root
        return

    if root.name == "data-raw":
        for year in (2023, 2024, 2025, 2026):
            d = root / f"datfiles_{year}"
            if d.exists():
                yield from sorted(d.glob(glob_pattern))
        return

    # Fallback: scan just this directory (non-recursive)
    yield from sorted(root.glob(glob_pattern))


# ------------------------------------------------
# Sentinel masking
# ------------------------------------------------

def mask_sentinels(df: pd.DataFrame, columns: List[str], threshold: float) -> None:
    thr = float(threshold)
    for col in columns:
        x = pd.to_numeric(df[col], errors="coerce")
        df[col] = x.mask(x.abs() >= thr)


# ------------------------------------------------
# Rule lookup
# ------------------------------------------------

def find_rule(col: str) -> Optional[RangeRule]:
    for key, rule in RULES:
        if key == col:
            return rule
        if key.endswith("_") and col.startswith(key):
            return rule
    return None


# ------------------------------------------------
# File scanner
# ------------------------------------------------

def scan_file(datfile: Path, bad_value_threshold: float) -> List[Dict[str, str]]:
    logger_tag = extract_logger_tag(datfile)
    df = _read_toa5(datfile)

    # Normalize timestamp column name
    ts_col = "timestamp" if "timestamp" in df.columns else ("TIMESTAMP" if "TIMESTAMP" in df.columns else None)
    if ts_col is None:
        return [{
            "logger_tag": logger_tag,
            "file": str(datfile),
            "timestamp": "",
            "column": "",
            "value": "",
            "rule": "missing_timestamp_column",
        }]

    df["timestamp_parsed"] = _parse_timestamp(df[ts_col])

    eval_cols: List[str] = []
    col_to_rule: Dict[str, RangeRule] = {}

    for col in df.columns:
        if col in (ts_col, "RECORD", "timestamp_parsed"):
            continue
        rule = find_rule(col)
        if rule is None:
            continue
        eval_cols.append(col)
        col_to_rule[col] = rule

    # Mask sentinels first (ETL-style)
    if eval_cols:
        mask_sentinels(df, eval_cols, bad_value_threshold)

    out: List[Dict[str, str]] = []

    for col in eval_cols:
        rule = col_to_rule[col]
        bad = rule.violates(df[col])
        if not bad.any():
            continue

        subset = df.loc[bad, ["timestamp_parsed", col]]

        for _, r in subset.iterrows():
            ts = r["timestamp_parsed"]
            out.append({
                "logger_tag": logger_tag,
                "file": str(datfile),
                "timestamp": "" if pd.isna(ts) else pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
                "column": col,
                "value": str(r[col]),
                "rule": rule.name,
            })

    return out


# ------------------------------------------------
# Summary builder
# ------------------------------------------------

def build_summary(df_viol: pd.DataFrame) -> pd.DataFrame:
    """
    Group by logger_tag/column/rule and count.
    Also report first/last timestamp where available.
    """
    if df_viol.empty:
        return pd.DataFrame(columns=[
            "logger_tag",
            "column",
            "rule",
            "violation_count",
            "first_timestamp",
            "last_timestamp",
        ])

    df = df_viol.copy()

    # parse timestamps for min/max; keep NaT where blank
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], errors="coerce")

    g = df.groupby(["logger_tag", "column", "rule"], dropna=False)

    summary = g.agg(
        violation_count=("value", "size"),
        first_timestamp=("timestamp_dt", "min"),
        last_timestamp=("timestamp_dt", "max"),
    ).reset_index()

    # format timestamps nicely
    summary["first_timestamp"] = summary["first_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    summary["last_timestamp"] = summary["last_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # sort: worst offenders first
    summary = summary.sort_values(["violation_count", "logger_tag", "column"], ascending=[False, True, True])

    return summary


# ------------------------------------------------
# Main
# ------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--root", required=True, help="Directory to scan OR a single .dat file")
    ap.add_argument("--glob", default="*Table1*.dat", help="Glob (non-recursive) within datfiles_202x")
    ap.add_argument("--out", required=True, help="Detailed violations CSV output path")

    ap.add_argument(
        "--summary-out",
        default="",
        help="Optional summary CSV output path. Default: <out>_summary.csv",
    )

    ap.add_argument("--max-per-file", type=int, default=0, help="0 = no limit; else cap rows per file")

    ap.add_argument(
        "--bad-value-threshold",
        type=float,
        default=float(DEFAULT_BAD_VALUE_THRESHOLD),
        help=(
            "Values with abs(x) >= this are treated as placeholders and ignored "
            f"(default: DEFAULT_BAD_VALUE_THRESHOLD={DEFAULT_BAD_VALUE_THRESHOLD})."
        ),
    )

    args = ap.parse_args()

    root = Path(args.root).expanduser()
    out_path = Path(args.out).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.summary_out:
        summary_path = Path(args.summary_out).expanduser()
    else:
        summary_path = out_path.with_name(out_path.stem + "_summary.csv")

    summary_path.parent.mkdir(parents=True, exist_ok=True)

    all_rows: List[Dict[str, str]] = []
    n_files = 0

    for f in iter_project_datfiles(root, args.glob):
        n_files += 1

        try:
            rows = scan_file(f, bad_value_threshold=float(args.bad_value_threshold))
        except Exception as e:
            rows = [{
                "logger_tag": extract_logger_tag(f),
                "file": str(f),
                "timestamp": "",
                "column": "",
                "value": "",
                "rule": f"read_error: {e}",
            }]

        if args.max_per_file and len(rows) > args.max_per_file:
            rows = rows[: args.max_per_file]

        all_rows.extend(rows)

    df_out = pd.DataFrame(all_rows, columns=["logger_tag", "file", "timestamp", "column", "value", "rule"])
    df_out.to_csv(out_path, index=False)

    df_summary = build_summary(df_out[df_out["rule"].ne("").fillna(True)])
    df_summary.to_csv(summary_path, index=False)

    print(f"Scanned files: {n_files}")
    print(f"Violations rows: {len(df_out)}")
    print(f"Wrote detailed: {out_path}")
    print(f"Wrote summary:  {summary_path}")


if __name__ == "__main__":
    main()