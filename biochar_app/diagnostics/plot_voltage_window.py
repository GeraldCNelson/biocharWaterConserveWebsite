#!/usr/bin/env python3
"""
plot_voltage_window.py

Plot battery/voltage around a suspected logger event (e.g., restart after voltage drop).

Handles Campbell Scientific TOA5 .dat format where:
- Row 1 is metadata (starts with "TOA5")
- Row 2 contains the column names
- Row 3 often contains units
- Row 4 often contains processing/type info
- Data begins after that

Example:
  python biochar_app/diagnostics/plot_voltage_window.py \
    --path biochar_app/data-raw/datfiles_2024/S3M_Table1.dat \
    --event "2024-07-07 06:30" \
    --months 2 \
    --voltage-col BattV_Min
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import matplotlib.pyplot as plt


TIMESTAMP_CANDIDATES = ["timestamp", "TIMESTAMP", "Timestamp", "DateTime", "Datetime", "datetime"]
VOLTAGE_REGEX = re.compile(r"(batt|battery|volt|vbat|battv)", re.IGNORECASE)


@dataclass(frozen=True)
class LoadedData:
    df: pd.DataFrame
    timestamp_col: str
    voltage_col: str


def _list_dat_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(path.glob("*.dat"))
    raise FileNotFoundError(f"Path not found: {path}")


def _clean_col_name(col: object) -> str:
    s = str(col)
    s = s.lstrip("\ufeff").strip()          # strip BOM + whitespace
    s = s.strip('"').strip("'").strip()     # strip surrounding quotes
    return s


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_clean_col_name(c) for c in df.columns]
    return df


def _find_toa5_header_offset(path: Path, *, max_scan_lines: int = 50) -> int:
    """
    Return the 0-based line index where the TOA5 metadata row occurs.
    Handles:
      - quoted "TOA5"
      - UTF-8 BOM
      - blank lines before TOA5
    Raises ValueError if not found quickly.
    """
    with path.open("r", newline="", encoding="utf-8", errors="replace") as f:
        r = csv.reader(f)
        for i in range(max_scan_lines):
            row = next(r, None)
            if row is None:
                break
            if not row:
                continue
            first = _clean_col_name(row[0]).upper()
            if first == "TOA5":
                return i
    raise ValueError(f"{path.name}: TOA5 header row not found in first {max_scan_lines} lines.")


def _read_toa5_dat(path: Path) -> pd.DataFrame:
    """
    Robust TOA5 reader:
      - locate the TOA5 row (even if quoted / after blank lines)
      - read the next row as column names
      - skip 4 header rows total (TOA5 + colnames + units + aggs)
      - read data with those names
    """
    toa5_line = _find_toa5_header_offset(path)

    # Re-open and read the 4 header rows starting at toa5_line
    with path.open("r", newline="", encoding="utf-8", errors="replace") as f:
        r = csv.reader(f)

        # advance to TOA5 row
        for _ in range(toa5_line):
            next(r, None)

        _meta = next(r, None)
        colnames = next(r, None)
        _units = next(r, None)
        _aggs = next(r, None)

    if not colnames:
        raise ValueError(f"{path.name}: missing TOA5 column-name row after TOA5 metadata row.")

    colnames_clean = [_clean_col_name(c) for c in colnames]

    if "TIMESTAMP" not in colnames_clean and "timestamp" not in colnames_clean:
        raise ValueError(
            f"{path.name}: TOA5 column-name row does not include TIMESTAMP. Got: {colnames_clean}"
        )

    # Number of lines to skip before first data row:
    # everything before TOA5 + 4 header lines (TOA5/colnames/units/aggs)
    skiprows = toa5_line + 4

    df = pd.read_csv(
        path,
        skiprows=skiprows,
        header=None,
        names=colnames_clean,
        na_values=["", "NA", "NAN"],
        engine="python",
    )
    return _normalize_columns(df)


def _read_generic_csvish(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=",", engine="python", header=0, skip_blank_lines=True)
    if df.shape[1] == 1:
        df = pd.read_csv(path, sep=r"\s+", engine="python", header=0)
    return _normalize_columns(df)


def _read_one_dat(path: Path) -> pd.DataFrame:
    # Always try TOA5 first; only fall back if it's truly not TOA5.
    try:
        return _read_toa5_dat(path)
    except Exception:
        return _read_generic_csvish(path)


def _detect_timestamp_col(columns: Sequence[str]) -> str:
    for c in TIMESTAMP_CANDIDATES:
        if c in columns:
            return c
    for col in columns:
        low = col.lower()
        if "time" in low and ("stamp" in low or "date" in low):
            return col
    return columns[0]


def _resolve_col_name(df: pd.DataFrame, wanted: str) -> Optional[str]:
    want = _clean_col_name(wanted)
    cols = list(df.columns)

    if want in cols:
        return want

    want_low = want.lower()
    for c in cols:
        if c.lower() == want_low:
            return c

    return None


def _detect_voltage_col(columns: Sequence[str]) -> Optional[str]:
    for preferred in ["BattV_Min", "BattV_Avg", "BattV", "Battery", "VBatt", "Vbat"]:
        if preferred in columns:
            return preferred

    matches = [c for c in columns if VOLTAGE_REGEX.search(c)]
    if not matches:
        return None

    priority = ["battv_min", "battv", "battery", "vbat", "vbatt", "volt", "batt"]

    def score(name: str) -> int:
        low = name.lower()
        for i, p in enumerate(priority):
            if p in low:
                return 100 - i
        return 0

    matches.sort(key=score, reverse=True)
    return matches[0]


def _coerce_timestamp(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip().str.strip('"').str.strip("'")

    # Prefer strict parse first
    ts = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if ts.notna().mean() > 0.95:
        return ts

    ts2 = pd.to_datetime(s, format="%Y-%m-%d %H:%M", errors="coerce")
    if ts2.notna().mean() > 0.95:
        return ts2

    return pd.to_datetime(s, errors="coerce")


def load_dat_files(path: Path, voltage_col: Optional[str] = None) -> LoadedData:
    files = _list_dat_files(path)
    if not files:
        raise FileNotFoundError(f"No .dat files found at: {path}")

    frames: list[pd.DataFrame] = []
    for fp in files:
        df = _read_one_dat(fp)
        df["__source_file__"] = fp.name
        frames.append(df)

    big = pd.concat(frames, ignore_index=True)
    big = _normalize_columns(big)

    # Normalize TIMESTAMP -> timestamp early
    if "TIMESTAMP" in big.columns and "timestamp" not in big.columns:
        big = big.rename(columns={"TIMESTAMP": "timestamp"})

    # --- Timestamp ---
    ts_col = _detect_timestamp_col(list(big.columns))
    big[ts_col] = _coerce_timestamp(big[ts_col])
    big = big.dropna(subset=[ts_col]).copy()

    # Canonicalize to "timestamp"
    if ts_col != "timestamp":
        big = big.rename(columns={ts_col: "timestamp"})
        ts_col = "timestamp"

    # --- Voltage column ---
    if voltage_col:
        resolved = _resolve_col_name(big, voltage_col)
        if not resolved:
            raise ValueError(
                f"Voltage column '{voltage_col}' not found after normalization.\n"
                f"Available columns: {list(big.columns)}"
            )
        vcol = resolved
    else:
        vcol = _detect_voltage_col(list(big.columns))
        if not vcol:
            raise ValueError(
                "Could not auto-detect a voltage/battery column. "
                "Pass --voltage-col with the exact column name.\n"
                f"Available columns: {list(big.columns)}"
            )

    big[vcol] = pd.to_numeric(big[vcol], errors="coerce")
    big = big.sort_values(ts_col).reset_index(drop=True)

    return LoadedData(df=big, timestamp_col=ts_col, voltage_col=vcol)


def plot_voltage_window(
    loaded: LoadedData,
    event_ts: pd.Timestamp,
    months: int = 2,
    out_png: Optional[Path] = None,
) -> None:
    df = loaded.df
    ts_col = loaded.timestamp_col
    vcol = loaded.voltage_col

    start = event_ts - pd.DateOffset(months=months)
    end = event_ts + pd.DateOffset(months=months)

    win = df[(df[ts_col] >= start) & (df[ts_col] <= end)].copy()
    if win.empty:
        raise ValueError(
            f"No rows found in window {start} to {end}. "
            f"Data range is {df[ts_col].min()} to {df[ts_col].max()}."
        )
    # Build time series indexed by timestamp
    series = pd.Series(win[vcol].to_numpy(), index=pd.DatetimeIndex(win[ts_col]))

    # Daily minimum battery voltage (best indicator of battery health)
    trend = series.resample("1D").min()

    plt.figure(figsize=(12, 7))

    # Raw data (faint)
    plt.plot(series.index, series.values, linewidth=0.6, alpha=0.25, label="Raw BattV_Min")

    # Trend
    plt.plot(trend.index, trend.values, linewidth=2, label="Daily minimum")

    # Event marker
    plt.axvline(event_ts, linestyle="--", label="Event")

    # Brownout threshold
    plt.axhline(9.6, linestyle=":", color="red", label="CR200 reset threshold")

    plt.title(f"Battery Voltage around event {event_ts:%Y-%m-%d %H:%M}")
    plt.xlabel("Timestamp (logger local time)")
    plt.ylabel(vcol)

    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.legend()
    plt.tight_layout()

    if out_png:
        out_png.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_png, dpi=200)
    else:
        plt.show()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True, help="Path to a .dat file OR a directory of .dat files")
    ap.add_argument("--event", required=True, help='Event timestamp, e.g. "2024-07-07 06:30"')
    ap.add_argument("--months", type=int, default=2, help="Months before/after event to plot (default 2)")
    ap.add_argument("--voltage-col", default=None, help="Exact voltage column name (e.g. BattV_Min)")
    ap.add_argument("--out", default=None, help="Optional output PNG path")
    args = ap.parse_args()

    path = Path(args.path).expanduser()
    event_ts = pd.to_datetime(args.event)

    loaded = load_dat_files(path, voltage_col=args.voltage_col)
    out_png = Path(args.out).expanduser() if args.out else None

    print(f"Loaded rows: {len(loaded.df)}")
    print(f"Timestamp column: {loaded.timestamp_col}")
    print(f"Voltage column: {loaded.voltage_col}")
    print(f"Columns (normalized): {list(loaded.df.columns)}")
    print(f"Data range: {loaded.df[loaded.timestamp_col].min()} .. {loaded.df[loaded.timestamp_col].max()}")

    plot_voltage_window(loaded, event_ts=event_ts, months=args.months, out_png=out_png)


if __name__ == "__main__":
    main()