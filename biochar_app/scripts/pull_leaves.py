#!/usr/bin/env python3
"""
pull_leaves.py

Production entry point (v1): incremental Table1 pulls from CR206 leaf loggers
via CR800 router using fetch_leaf_records.py.

- Pulls each leaf (2-13 by default)
- Filters to a rolling 1-hour window
- De-dups against last_seen_ts per leaf
- Appends "new" rows into a stable per-leaf CSV under data-raw/pakbus_ingest/
- Maintains JSON state so runs are incremental
- Uses a simple lock to prevent overlapping runs (cron/systemd safe)

Run:
  python -m biochar_app.pakbus.scripts.pull_leaves \
    --host <ipv6> --port 6785 --router 1 --src 4093
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd


# ---------------- Paths (repo-relative) ----------------

REPO_ROOT = Path(__file__).resolve().parents[3]

DEFAULT_STATE = REPO_ROOT / "biochar_app" / "data-processed" / "pakbus_pull_state.json"
DEFAULT_LOCK = REPO_ROOT / "biochar_app" / "data-processed" / "pakbus_pull.lock"

# Where we keep the “canonical” append-only ingested CSVs (easy + transparent)
INGEST_ROOT = REPO_ROOT / "biochar_app" / "data-raw" / "pakbus_ingest"

# Where we write per-run raw downloads (optional but useful for debugging)
RUN_ROOT = REPO_ROOT / "biochar_app" / "data-raw" / "pakbus_runs"


# ---------------- Time helpers ----------------

def floor_to_15min(ts: datetime) -> datetime:
    minute = (ts.minute // 15) * 15
    return ts.replace(minute=minute, second=0, microsecond=0)

def to_iso(ts: datetime) -> str:
    return ts.replace(microsecond=0).isoformat()

def parse_iso(ts: str) -> datetime:
    s = ts.strip().replace(" ", "T")
    return datetime.fromisoformat(s)

def compute_window(now: datetime, last_seen: Optional[datetime], hours: float) -> Tuple[datetime, datetime]:
    end = floor_to_15min(now)
    start = end - timedelta(hours=hours)
    if last_seen is not None:
        start = max(start, last_seen + timedelta(minutes=15))
    return start, end


# ---------------- Locking ----------------

class LockError(RuntimeError):
    pass

def acquire_lock(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        return fd
    except FileExistsError as e:
        raise LockError(f"Lock exists: {lock_path}") from e

def release_lock(fd: int, lock_path: Path) -> None:
    try:
        os.close(fd)
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass


# ---------------- State ----------------

def load_state(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8") or "{}")

def save_state(path: Path, state: Dict[str, Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


# ---------------- CSV schema handling ----------------

def pick_timestamp_column(df: pd.DataFrame) -> Optional[str]:
    """
    fetch_leaf_records.py output may vary; we try common possibilities.
    """
    for cand in ("timestamp", "TimestampUTC", "TimestampLocal", "Timestamp"):
        if cand in df.columns:
            return cand
    return None

def coerce_timestamp_series(s: pd.Series) -> pd.Series:
    # Be forgiving; you’ve been using naive timestamps in your pipeline.
    return pd.to_datetime(s, errors="coerce")


# ---------------- Ingestion ----------------

@dataclass
class IngestResult:
    new_rows: int
    last_ts: Optional[datetime]

def append_new_rows(
    *,
    leaf_id: int,
    table: str,
    pulled_csv: Path,
    ingest_csv: Path,
    last_seen: Optional[datetime],
    start: datetime,
    end: datetime,
) -> IngestResult:
    df = pd.read_csv(pulled_csv)

    ts_col = pick_timestamp_column(df)
    if not ts_col:
        print(f"[WARN] leaf={leaf_id}: no recognizable timestamp column in {pulled_csv.name}")
        return IngestResult(0, last_seen)

    df[ts_col] = coerce_timestamp_series(df[ts_col])
    df = df[df[ts_col].notna()].copy()

    # filter to requested window
    df = df[(df[ts_col] >= start) & (df[ts_col] <= end)].copy()

    # filter past last_seen
    if last_seen is not None:
        df = df[df[ts_col] > last_seen].copy()

    if df.empty:
        return IngestResult(0, last_seen)

    # add stable metadata columns
    df["leaf_id"] = leaf_id
    df["table_name"] = table

    ingest_csv.parent.mkdir(parents=True, exist_ok=True)

    # append-only write (create header if file doesn't exist)
    write_header = not ingest_csv.exists()
    df.to_csv(ingest_csv, mode="a", header=write_header, index=False)

    new_last = df[ts_col].max().to_pydatetime()
    return IngestResult(len(df), new_last)


# ---------------- Downloader call ----------------

def run_fetch_leaf_records(
    *,
    host: str,
    port: int,
    router: int,
    src: int,
    leaf: int,
    table: str,
    num: int,
    output_csv: Path,
) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "biochar_app.pakbus.scripts.fetch_leaf_records",
        "--host", host,
        "--port", str(port),
        "--router", str(router),
        "--src", str(src),
        "--leaf", str(leaf),
        "--table", table,
        "--collect-mode", "mostrecent",
        "--num", str(num),
        "--output", str(output_csv),
    ]
    subprocess.run(cmd, check=True)


# ---------------- Main ----------------

def parse_leaves(spec: str) -> list[int]:
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x.strip()) for x in spec.split(",") if x.strip()]

def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Incremental PakBus leaf pull (production entry point v1).")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=6785)
    ap.add_argument("--router", type=int, default=1)
    ap.add_argument("--src", type=int, default=4093)
    ap.add_argument("--table", default="Table1")
    ap.add_argument("--leaves", default="2-13")
    ap.add_argument("--window-hours", type=float, default=1.0)

    # how many rows to request from the leaf each run
    # (1 hour @ 15min = 4 rows; use a cushion for gaps/retries)
    ap.add_argument("--num", type=int, default=24)

    ap.add_argument("--state", type=Path, default=DEFAULT_STATE)
    ap.add_argument("--lock", type=Path, default=DEFAULT_LOCK)

    args = ap.parse_args(argv)

    leaves = parse_leaves(args.leaves)

    try:
        fd = acquire_lock(args.lock)
    except LockError as e:
        print(f"[SKIP] {e}")
        return 0

    try:
        state = load_state(args.state)
        now = datetime.now()
        end = floor_to_15min(now)

        any_new = False

        for leaf in leaves:
            key = f"leaf{leaf}"
            last_seen = None
            if key in state and state[key].get("last_seen_ts"):
                try:
                    last_seen = parse_iso(state[key]["last_seen_ts"])
                except Exception:
                    last_seen = None

            start, end_win = compute_window(end, last_seen, args.window_hours)
            run_tag = f"{start.strftime('%Y%m%dT%H%M')}_to_{end_win.strftime('%Y%m%dT%H%M')}"

            run_dir = RUN_ROOT / f"{end_win.year}" / f"leaf{leaf}"
            pulled_csv = run_dir / f"{args.table}_{run_tag}.csv"

            ingest_csv = INGEST_ROOT / f"{end_win.year}" / f"leaf{leaf}" / f"{args.table}_ingested.csv"

            print(f"[PULL] leaf={leaf} window={to_iso(start)} → {to_iso(end_win)}")
            try:
                run_fetch_leaf_records(
                    host=args.host, port=args.port, router=args.router, src=args.src,
                    leaf=leaf, table=args.table, num=args.num, output_csv=pulled_csv
                )
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] leaf={leaf} fetch failed: {e}")
                continue

            res = append_new_rows(
                leaf_id=leaf,
                table=args.table,
                pulled_csv=pulled_csv,
                ingest_csv=ingest_csv,
                last_seen=last_seen,
                start=start,
                end=end_win,
            )

            if res.new_rows > 0:
                any_new = True
                state.setdefault(key, {})
                state[key]["last_seen_ts"] = to_iso(res.last_ts) if res.last_ts else state[key].get("last_seen_ts", "")
                print(f"[OK] leaf={leaf} appended={res.new_rows} last_seen={state[key]['last_seen_ts']}")
            else:
                print(f"[OK] leaf={leaf} no new rows")

        save_state(args.state, state)

        if any_new:
            print("[INFO] New data ingested. Next: wire into ETL aggregation step (hourly/daily/monthly/gseason).")

        return 0
    finally:
        release_lock(fd, args.lock)


if __name__ == "__main__":
    raise SystemExit(main())