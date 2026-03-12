#!/usr/bin/env python3
"""
extract_pre2024_battv_from_2024.py

Find rows in datfiles_2024/*_Table1.dat that are actually from 2023
(e.g., Oct–Dec 2023 after BattV_Min was added), and write them into datfiles_2023/
as TOA5 .dat files with a name that makes it obvious they are "late 2023 with BattV".

Keeps original files unchanged.

Example:
  python biochar_app/diagnostics/extract_pre2024_battv_from_2024.py \
    --src-dir biochar_app/data-raw/datfiles_2024 \
    --dst-dir biochar_app/data-raw/datfiles_2023 \
    --cutoff "2024-01-01 00:00:00" \
    --require-col BattV_Min \
    --suffix "late2023_withBattV" \
    --write

Dry-run (no writes):
  python biochar_app/diagnostics/extract_pre2024_battv_from_2024.py \
    --src-dir biochar_app/data-raw/datfiles_2024 \
    --dst-dir biochar_app/data-raw/datfiles_2023 \
    --cutoff "2024-01-01 00:00:00" \
    --require-col BattV_Min
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd


def _clean_col_name(col: object) -> str:
    s = str(col)
    s = s.lstrip("\ufeff").strip()
    s = s.strip('"').strip("'").strip()
    return s


def _read_first_four_lines_raw(path: Path) -> List[str]:
    # Preserve exact TOA5 header rows as text (including quotes, etc.)
    lines: List[str] = []
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        for _ in range(4):
            line = f.readline()
            if not line:
                break
            lines.append(line.rstrip("\n"))
    if len(lines) < 2:
        raise ValueError(f"{path.name}: file too short to be TOA5.")
    return lines


def _read_toa5_data_frame(path: Path) -> Tuple[pd.DataFrame, List[str]]:
    """
    ETL-consistent TOA5:
      line 1: TOA5 metadata
      line 2: colnames
      line 3: units
      line 4: agg/processing
      data starts line 5
    Returns (df, cleaned_colnames)
    """
    with path.open("r", newline="") as f:
        r = csv.reader(f)
        _meta = next(r, None)
        colnames = next(r, None)
        _units = next(r, None)
        _aggs = next(r, None)

    if not colnames:
        raise ValueError(f"{path.name}: missing TOA5 column-name row (line 2).")

    cols = [_clean_col_name(c) for c in colnames]
    if "TIMESTAMP" not in cols and "timestamp" not in cols:
        raise ValueError(f"{path.name}: TOA5 header missing TIMESTAMP. Got: {cols}")

    df = pd.read_csv(
        path,
        skiprows=4,
        header=None,
        names=cols,
        na_values=["", "NA", "NAN"],
        engine="python",
    )
    df.columns = [_clean_col_name(c) for c in df.columns]
    return df, cols


def _coerce_timestamp(ts: pd.Series) -> pd.Series:
    s = ts.astype("string").str.strip().str.strip('"').str.strip("'")
    # Most of your TOA5 is exactly this:
    out = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if out.notna().mean() < 0.90:
        out = pd.to_datetime(s, errors="coerce")
    return out


def infer_logger_tag(path: Path) -> str:
    # S3M_Table1.dat -> S3M
    return path.name.split("_")[0]


def build_out_name(tag: str, suffix: str) -> str:
    # Example: S3M_Table1_late2023_withBattV.dat
    return f"{tag}_Table1_{suffix}.dat"


def write_toa5_subset(
    *,
    src_path: Path,
    dst_path: Path,
    header_lines: List[str],
    df_subset: pd.DataFrame,
) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    # Write the first 4 lines exactly as the source (raw text)
    with dst_path.open("w", encoding="utf-8", newline="") as f:
        for line in header_lines:
            f.write(line + "\n")

        # Then append data rows, quoted like TOA5
        df_subset.to_csv(
            f,
            index=False,
            header=False,
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-dir", required=True, help="e.g. biochar_app/data-raw/datfiles_2024")
    ap.add_argument("--dst-dir", required=True, help="e.g. biochar_app/data-raw/datfiles_2023")
    ap.add_argument("--cutoff", default="2024-01-01 00:00:00", help='Keep rows strictly before this timestamp')
    ap.add_argument("--require-col", default="BattV_Min", help="Only process files that contain this column")
    ap.add_argument("--suffix", default="late2023_withBattV", help="Suffix in output filename")
    ap.add_argument("--write", action="store_true", help="Actually write outputs; otherwise dry-run")
    args = ap.parse_args()

    src_dir = Path(args.src_dir).expanduser()
    dst_dir = Path(args.dst_dir).expanduser()
    cutoff = pd.Timestamp(args.cutoff)

    files = sorted(src_dir.glob("S*_Table1.dat"))
    if not files:
        raise FileNotFoundError(f"No S*_Table1.dat files found under: {src_dir}")

    wrote = 0
    considered = 0

    print(f"Scanning: {src_dir}")
    print(f"Cutoff:   {cutoff}")
    print(f"Dst dir:  {dst_dir}")
    print(f"Mode:     {'WRITE' if args.write else 'DRY-RUN'}")
    print()

    for fp in files:
        considered += 1
        tag = infer_logger_tag(fp)

        try:
            header_lines = _read_first_four_lines_raw(fp)
            df, cols = _read_toa5_data_frame(fp)
        except Exception as e:
            print(f"❌ {fp.name}: read failed: {e}")
            continue

        req = str(args.require_col).strip()
        if req and req not in df.columns:
            # Skip quietly: these are likely early 2024 or a program without BattV_Min
            continue

        ts_col = "TIMESTAMP" if "TIMESTAMP" in df.columns else "timestamp"
        df[ts_col] = _coerce_timestamp(df[ts_col])
        df = df.dropna(subset=[ts_col]).copy()

        pre = df[df[ts_col] < cutoff].copy()
        if pre.empty:
            continue

        # Sort by timestamp and keep original columns order
        pre = pre.sort_values(ts_col)
        pre = pre[cols] if all(c in pre.columns for c in cols) else pre

        out_name = build_out_name(tag, str(args.suffix))
        out_path = dst_dir / out_name

        pre_start = pre[ts_col].min()
        pre_end = pre[ts_col].max()
        print(f"✅ {fp.name}: found {len(pre):,} pre-cutoff rows ({pre_start} .. {pre_end}) -> {out_path.name}")

        if args.write:
            write_toa5_subset(src_path=fp, dst_path=out_path, header_lines=header_lines, df_subset=pre)
            wrote += 1

    print()
    print(f"Considered files: {considered}")
    print(f"Outputs {'written' if args.write else 'that would be written'}: {wrote}")


if __name__ == "__main__":
    main()