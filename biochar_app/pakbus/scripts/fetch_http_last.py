#!/usr/bin/env python3
from __future__ import annotations
import argparse, logging, os
from io import StringIO
from pathlib import Path
from urllib.parse import quote
import pandas as pd
import requests

# Try config defaults
try:
    from biochar_app.scripts.config import PAKBUS as CFG
except Exception:
    CFG = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

TARGET_COLUMNS = [
    "TIMESTAMP","RECORD","BattV_Min",
    "VWC_1_Avg","EC_1_Avg","T_1_Avg",
    "VWC_2_Avg","EC_2_Avg","T_2_Avg",
    "VWC_3_Avg","EC_3_Avg","T_3_Avg",
]

def norm_host(h: str) -> str:
    h = (h or "").strip()
    if h.startswith("http://") or h.startswith("https://"):
        return h.rstrip("/")
    # default to http; keep IPv6 brackets for URLs
    return f"http://{h.strip('/')}".rstrip("/")

def check_root(base_url: str) -> None:
    r = requests.get(base_url, timeout=10)
    r.raise_for_status()
    logging.info("Root page OK (%s)", r.headers.get("Server","<unknown>"))

def dataquery_csv(base_url: str, table: str, n: int) -> pd.DataFrame | None:
    # record_count is the CRBasic Web API knob to get last-N rows
    params = {
        "command": "DataQuery",
        "uri": f"dl:{table}",
        "format": "csv",
        "mode": "record_count",
        "n": n,
    }
    qs = "&".join(f"{k}={quote(str(v))}" for k,v in params.items())
    url = f"{base_url}/?{qs}"
    r = requests.get(url, timeout=15)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return pd.read_csv(StringIO(r.text))

def newestrecord_html(base_url: str, table: str, n: int) -> pd.DataFrame:
    # Fallback: scrape N newest one-by-one, then reverse to oldest→newest
    rows = []
    for _ in range(n):
        url = f"{base_url}/?command=NewestRecord&table={quote(table)}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        df = pd.read_html(r.text, header=0)[0]
        rows.append(df)
    return pd.concat(rows[::-1], ignore_index=True)

def normalize_to_target(df: pd.DataFrame) -> pd.DataFrame:
    # Map common names coming back from CR800 to your target schema
    # Adjust these if your device uses slightly different labels.
    candidates = {
        "TIMESTAMP": ["Timestamp","TIMESTAMP","TimeStamp","Time"],
        "RECORD":    ["RECORD","Record","RecNbr","RecNo"],
        "BattV_Min": ["BattV_Min","BattV (Minimum)","BattV","BattV_Minimum"],
        "VWC_1_Avg": ["VWC_1_Avg","VWC_1","VWC1_Avg"],
        "EC_1_Avg":  ["EC_1_Avg","EC_1","EC1_Avg","BulkEC_1_Avg"],
        "T_1_Avg":   ["T_1_Avg","T_1","Temp_1_Avg"],
        "VWC_2_Avg": ["VWC_2_Avg","VWC_2","VWC2_Avg"],
        "EC_2_Avg":  ["EC_2_Avg","EC_2","EC2_Avg","BulkEC_2_Avg"],
        "T_2_Avg":   ["T_2_Avg","T_2","Temp_2_Avg"],
        "VWC_3_Avg": ["VWC_3_Avg","VWC_3","VWC3_Avg"],
        "EC_3_Avg":  ["EC_3_Avg","EC_3","EC3_Avg","BulkEC_3_Avg"],
        "T_3_Avg":   ["T_3_Avg","T_3","Temp_3_Avg"],
    }

    out = pd.DataFrame()
    for target, opts in candidates.items():
        for c in opts:
            if c in df.columns:
                out[target] = df[c]
                break
        if target not in out.columns:
            out[target] = pd.NA  # fill if missing

    # TIMESTAMP to UTC ISO8601
    out["TIMESTAMP"] = pd.to_datetime(out["TIMESTAMP"], utc=True, errors="coerce")

    # RECORD: if missing, create sequential placeholder (will be re-based upstream)
    if out["RECORD"].isna().all():
        out["RECORD"] = range(0, len(out))

    # Final column order
    out = out[TARGET_COLUMNS]
    return out

def write_outputs(df: pd.DataFrame, site: str, table: str, parquet: str | None):
    outdir = Path("biochar_app/pakbus/bdFiles/out_fetch")
    outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / f"{site}_{table}_http.csv"
    df_to_write = df.copy()
    # stringify timestamp for CSV
    df_to_write["TIMESTAMP"] = df_to_write["TIMESTAMP"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df_to_write.to_csv(csv_path, index=False)
    logging.info("Wrote %s (%d rows)", csv_path, len(df))

    if parquet:
        p = Path(parquet)
        p.parent.mkdir(parents=True, exist_ok=True)
        # Append/merge by TIMESTAMP
        try:
            old = pd.read_parquet(p)
            merged = pd.concat([old, df]).drop_duplicates(subset=["TIMESTAMP"]).sort_values("TIMESTAMP")
        except FileNotFoundError:
            merged = df
        merged.to_parquet(p, index=False)
        logging.info("Updated %s (%d total rows)", p, len(merged))

def main():
    ap = argparse.ArgumentParser(description="Fetch last-N via CR800 HTTP (DataQuery)")
    ap.add_argument("--host",
        default=(getattr(CFG,"host", None) or os.getenv("PAKBUS_HOST") or "[2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd]"),
        help="CR800 host; IPv6 may be bracketed (default from config or env)")
    ap.add_argument("--table", default="Table1")
    ap.add_argument("--num", "-n", type=int, default=8)
    ap.add_argument("--site", default="S1B")
    ap.add_argument("--parquet", help="optional Parquet path to upsert/merge by TIMESTAMP")
    args = ap.parse_args()

    base_url = norm_host(args.host)
    try:
        check_root(base_url)
    except Exception as e:
        logging.error("Root check failed: %s", e); return

    df = dataquery_csv(base_url, args.table, args.num)
    if df is None:
        logging.warning("record_count not supported; falling back to NewestRecord")
        df = newestrecord_html(base_url, args.table, args.num)

    if df.empty:
        logging.warning("No rows returned.")
        return

    df_norm = normalize_to_target(df)
    write_outputs(df_norm, args.site, args.table, args.parquet)

if __name__ == "__main__":
    main()