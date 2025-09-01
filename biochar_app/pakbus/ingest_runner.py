#!/usr/bin/env python3
import json, subprocess, sys, os
from pathlib import Path
import pandas as pd

# ---- config (edit to taste) ----
REPO_ROOT = Path(__file__).resolve().parent
FETCH = REPO_ROOT / "biochar_app/pakbus/send_catalog_request.py"

LEAVES = [
    # (leaf, table, host, port, tsv, dat)
    ("S2T", 1,
     "2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd", 6785,
     REPO_ROOT/"biochar_app/pakbus/bdFiles/S2T_Table1.tsv",
     REPO_ROOT/"biochar_app/data-raw/datfiles_2025/S2T_Table1.dat"),
    # add more loggers here
]

OUT_DIR = REPO_ROOT/"biochar_app/pakbus/bdFiles/out_fetch_hourly"
PARQUET_ROOT = REPO_ROOT/"data-lake/bronze"
STATE_PATH = REPO_ROOT/"ingest_state.json"

LAST_N = 12       # ask for ~3 hours at 15-min resolution
PRELUDE_COUNT = 6
COMPARE_LAST = 800
LOCAL_OFFSET_HOURS = -6
TIMEOUT = 10
IDLE_GAP = 0.10
MAX_PAGES = 8     # keep it small in hourly mode
GAP_TOL = 3
VERBOSE = True

def load_state():
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {}

def save_state(state):
    STATE_PATH.write_text(json.dumps(state, indent=2))

def ensure_parquet_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def run_fetch(leaf, table, host, port, tsv, dat) -> Path | None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, str(FETCH),
        "--catalog-dir", str(REPO_ROOT/"biochar_app/pakbus/bdFiles/out_catalog"),
        "--leaf", leaf, "--table", str(table),
        "--last-n", str(LAST_N),
        "--host", host, "--port", str(port),
        "--mine-prelude", str(tsv),
        "--prelude-count", str(PRELUDE_COUNT),
        "--dat-file", str(dat),
        "--compare-last", str(COMPARE_LAST),
        "--local-offset-hours", str(LOCAL_OFFSET_HOURS),
        "--gap-tolerance", str(GAP_TOL),
        "--timeout", str(TIMEOUT),
        "--idle-gap", str(IDLE_GAP),
        "--max-pages", str(MAX_PAGES),
        "--out-dir", str(OUT_DIR),
    ]
    if VERBOSE:
        cmd.append("-v")

    print("[ingest] running:", " ".join(map(str, cmd)))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    print(proc.stdout)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)

    # discover the combined CSV by naming pattern
    # We know the script writes: {leaf}_Table{table}_n{LAST_N}_{tag}.csv
    candidates = sorted(OUT_DIR.glob(f"{leaf}_Table{table}_n{LAST_N}_*_*.csv"))
    # prefer the combined (without _pN); head5 has suffix _head5.csv
    combined = [p for p in candidates if "_head5" not in p.name and "_p" not in p.name]
    if combined:
        return combined[-1]
    return None

def append_to_parquet(leaf, table, csv_path: Path, last_seen_ts: str | None):
    df = pd.read_csv(csv_path)
    # Expected columns: Row,TimestampUTC,TimestampLocal,BattV_Min,...
    if "TimestampUTC" not in df.columns:
        print(f"[ingest] WARNING: no TimestampUTC in {csv_path.name}; nothing appended.")
        return last_seen_ts

    # filter by last_seen_ts (lexicographic works on YYYY-mm-dd HH:MM:SS)
    if last_seen_ts:
        df = df[df["TimestampUTC"] > last_seen_ts]

    if df.empty:
        print("[ingest] no new rows.")
        return last_seen_ts

    # add partition columns
    df["leaf"] = leaf
    df["table"] = table
    # derive year from UTC (robust: fall back if blank)
    year = None
    if df["TimestampUTC"].notna().any():
        year = str(df["TimestampUTC"].dropna().iloc[-1])[:4]
    if not year or not year.isdigit():
        year = "unknown"

    year_dir = PARQUET_ROOT / f"year={year}" / f"leaf={leaf}" / f"table={table}"
    ensure_parquet_dir(year_dir)

    # write append-only parquet part
    part_path = year_dir / f"part-{int(pd.Timestamp.now().timestamp())}.parquet"
    try:
        df.to_parquet(part_path, index=False)
        print(f"[ingest] appended {len(df)} rows -> {part_path}")
    except Exception as e:
        print(f"[ingest] parquet write failed: {e}", file=sys.stderr)

    # update last_seen_ts from newest row
    newest = df["TimestampUTC"].dropna().max()
    return newest if isinstance(newest, str) else last_seen_ts

def main():
    state = load_state()
    for (leaf, table, host, port, tsv, dat) in LEAVES:
        key = f"{leaf}/table{table}"
        last_ts = state.get(key, {}).get("last_seen_ts")

        csv_path = run_fetch(leaf, table, host, port, tsv, dat)
        if not csv_path:
            print(f"[ingest] no CSV produced for {key}")
            continue

        new_last = append_to_parquet(leaf, table, csv_path, last_ts)
        state.setdefault(key, {})["last_seen_ts"] = new_last

    save_state(state)
    print("[ingest] done.")

if __name__ == "__main__":
    main()