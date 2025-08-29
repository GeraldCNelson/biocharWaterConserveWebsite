#!/usr/bin/env python3
"""
fetch_cr800_decoded.py

Fetch the last N records from one or more CR800 data loggers over HTTP and
emit normalized '*_decoded.csv' files that match the trust_check expectations.

It tries multiple strategies, in order:
  1) DataQuery CSV, mode=record_count       (fastest when supported)
  2) TableData CSV with "last N" parameters (common on many CRxxx builds)
  3) DataQuery CSV, mode=newest (1 record)  (last-ditch; won't get history)

Usage examples:
  ./tools/fetch_cr800_decoded.py \
      --site S1B --url 'http://[2605:59C0:30F3:2500:2D0:2CFF:FE02:1DDD]' \
      --num 200 --table Table1
"""
import argparse
import sys
from io import StringIO
from urllib.parse import quote
from pathlib import Path

import pandas as pd
import requests


# Columns your trust_check compares (adjust if your table layout differs)
CANONICAL_ORDER = [
    "TimestampUTC",
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
]

# Common renames from CRBasic-style outputs → expected names.
RENAME_MAP = {
    "TIMESTAMP": "TimestampUTC",
    "Timestamp": "TimestampUTC",
    "time": "TimestampUTC",
    # Example sensor names (adjust to your actual table column names)
    "BattV_Min": "BattV_Min",
    "VWC1_Avg": "VWC_1_Avg",
    "EC1_Avg":  "EC_1_Avg",
    "T1_Avg":   "T_1_Avg",
    "VWC2_Avg": "VWC_2_Avg",
    "EC2_Avg":  "EC_2_Avg",
    "T2_Avg":   "T_2_Avg",
    "VWC3_Avg": "VWC_3_Avg",
    "EC3_Avg":  "EC_3_Avg",
    "T3_Avg":   "T_3_Avg",
}


def _read_csv_text(text: str) -> pd.DataFrame:
    """Read a CSV payload into a DataFrame, even if empty."""
    buf = StringIO(text)
    try:
        df = pd.read_csv(buf)
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()
    return df


def fetch_dataquery_record_count(base_url: str, table: str, num: int) -> pd.DataFrame:
    """Strategy 1: DataQuery CSV, mode=record_count (fast when supported)."""
    params = {
        "command": "DataQuery",
        "uri": f"dl:{table}",
        "format": "csv",
        "mode": "record_count",
        "n": num,
    }
    qs = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    url = f"{base_url}/?{qs}"
    r = requests.get(url, timeout=30)
    if r.status_code == 404:
        raise RuntimeError("DataQuery record_count not supported on this logger.")
    r.raise_for_status()
    return _read_csv_text(r.text)


def fetch_tabledata_last_n(base_url: str, table: str, num: int) -> pd.DataFrame:
    """
    Strategy 2: TableData CSV for last N rows.
    Different CR800 firmwares have used slightly different parameter names.
    We’ll try a few common variants and stop on the first success with data.
    """
    attempts = [
        # format: (param dict, human hint)
        ({"command": "TableData", "table": table, "format": "csv", "records": f"-{num}"}, "records=-N"),
        ({"command": "TableData", "table": table, "format": "csv", "last": str(num)}, "last=N"),
        ({"command": "TableData", "table": table, "format": "csv", "n": str(num)}, "n=N"),
        # Some builds also accept uri-based addressing:
        ({"command": "TableData", "uri": f"dl:{table}", "format": "csv", "records": f"-{num}"}, "uri+records=-N"),
    ]
    last_exc = None
    for params, hint in attempts:
        qs = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
        url = f"{base_url}/?{qs}"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            df = _read_csv_text(r.text)
            if not df.empty:
                return df
        except Exception as e:
            last_exc = e
            continue
    raise RuntimeError(f"TableData CSV last-N not available (tried variants, last error: {last_exc})")


def fetch_dataquery_newest(base_url: str, table: str) -> pd.DataFrame:
    """
    Strategy 3 (last-ditch): DataQuery newest single record as CSV.
    This only yields one row — useful to verify connectivity or populate a seed file.
    """
    params = {
        "command": "DataQuery",
        "uri": f"dl:{table}",
        "format": "csv",
        "mode": "newest",
        "n": 1,
    }
    qs = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    url = f"{base_url}/?{qs}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return _read_csv_text(r.text)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns and format timestamp as ISO with +00:00."""
    if df.empty:
        return df

    # rename to our canonical field names (only where keys exist)
    present = {k: v for k, v in RENAME_MAP.items() if k in df.columns}
    df = df.rename(columns=present)

    # coerce timestamp
    if "TimestampUTC" not in df.columns:
        # try to detect a likely timestamp column
        ts_col = next((c for c in df.columns if c.lower() in ("timestamp", "time", "ts")), None)
        if ts_col is None:
            raise ValueError("No Timestamp column found to map to 'TimestampUTC'.")
        df = df.rename(columns={ts_col: "TimestampUTC"})

    ts = pd.to_datetime(df["TimestampUTC"], utc=True, errors="coerce")
    df = df.loc[~ts.isna()].copy()
    df["TimestampUTC"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    # Keep only columns we recognize (but don’t error if some are missing)
    keep = ["TimestampUTC"] + [c for c in CANONICAL_ORDER if c != "TimestampUTC" and c in df.columns]
    df = df[keep].copy()

    # Sort by time, drop exact dupes
    df = df.sort_values("TimestampUTC").drop_duplicates(subset=["TimestampUTC"], keep="last")
    return df


def fetch_last_records(base_url: str, table: str, num: int) -> pd.DataFrame:
    """
    Try each strategy in order until one returns data.
    """
    # 1) DataQuery record_count
    try:
        df = fetch_dataquery_record_count(base_url, table, num)
        if not df.empty:
            return df
    except Exception as e:
        print(f"[INFO] record_count failed: {e}", file=sys.stderr)

    # 2) TableData last N
    try:
        df = fetch_tabledata_last_n(base_url, table, num)
        if not df.empty:
            return df
    except Exception as e:
        print(f"[INFO] TableData last-N failed: {e}", file=sys.stderr)

    # 3) Newest (single)
    try:
        df = fetch_dataquery_newest(base_url, table)
        if not df.empty:
            print("[WARN] Only retrieved the newest single record; increase support on logger to get history.", file=sys.stderr)
            return df
    except Exception as e:
        print(f"[INFO] DataQuery newest failed: {e}", file=sys.stderr)

    raise RuntimeError("All fetch strategies failed (no data).")


def main():
    p = argparse.ArgumentParser(description="Fetch CR800 data and emit *_decoded.csv compatible with trust_check.")
    p.add_argument("--site", action="append", help="Site code (e.g., S1B). Repeatable.", required=True)
    p.add_argument("--url", action="append", help="Base URL for the site's CR800 (e.g., http://[IPv6])", required=True)
    p.add_argument("--num", "-n", type=int, default=200, help="Number of recent records to fetch (default: 200)")
    p.add_argument("--table", "-t", default="Table1", help="Table name (default: Table1)")
    p.add_argument("--out-dir", default="biochar_app/pakbus/bdFiles/out_fetch", help="Output directory")
    args = p.parse_args()

    if len(args.site) != len(args.url):
        print("ERROR: you must pass the same number of --site and --url arguments.", file=sys.stderr)
        sys.exit(2)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for site, base_url in zip(args.site, args.url):
        try:
            raw = fetch_last_records(base_url, args.table, args.num)
            norm = normalize(raw)
            out = out_dir / f"{site}_{args.table}_n{args.num}_decoded.csv"
            norm.to_csv(out, index=False)
            print(f"wrote {out}")
        except Exception as e:
            print(f"[WARN] {site}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()