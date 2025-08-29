#!/usr/bin/env python3
"""
cr200_client.py
Fetch last-N records from a CR206X leaf (PakBus) via a CR800 gateway (PakBus/TCP),
using the local helpers in cr200_client_utils.py.

Outputs a CSV to:
  biochar_app/pakbus/bdFiles/out_fetch/<SITE>_<TABLE>_decoded.csv

Lightweight verifications done:
  - Table name & field names from GetTableDefs match returned data.
  - Timestamps decode and sort; reports gaps/dupes summary.
"""
from __future__ import annotations
import sys, pathlib

if __package__ in (None, ""):
    # Add the package parent (biochar_app/pakbus/..) to sys.path
    this_file = pathlib.Path(__file__).resolve()
    pkg_dir   = this_file.parent                 # .../biochar_app/pakbus
    parent    = pkg_dir.parent                   # .../biochar_app
    # (optional) also add project root, one level above biochar_app
    project_root = parent.parent
    for p in (str(parent), str(project_root)):
        if p not in sys.path:
            sys.path.insert(0, p)
    import cr200_client_utils as utils           # absolute (script mode)
else:
    from . import cr200_client_utils as utils    # relative (module mode)

from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timezone
import argparse, pathlib, sys, time
from datetime import datetime, timezone
from urllib.parse import urlparse

import pandas as pd

# Local helpers (no external pypakbus dependency)
from . import cr200_client_utils as utils


OUT_DIR = pathlib.Path("biochar_app/pakbus/bdFiles/out_fetch")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _flatten_records(
    recs: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Flatten utils.collect_data() result (list of fragments -> list of row dicts).
    Returns (rows, field_order).
    """
    rows: List[Dict[str, Any]] = []
    field_order: List[str] = []
    for frag in recs:
        for r in frag.get("RecFrag", []):
            ts_nsec = r.get("TimeOfRec")
            # convert (sec, nsec) to unix seconds (CR time base handled in utils)
            ts = utils.nsec_to_time(ts_nsec) if isinstance(ts_nsec, tuple) else None
            fields: Dict[str, Any] = r.get("Fields", {})
            # normalize bytes -> str for ASCII
            norm = {}
            for k, v in fields.items():
                if isinstance(k, (bytes, bytearray)):
                    key = k.decode("ascii", "ignore")
                else:
                    key = str(k)
                if isinstance(v, (bytes, bytearray)):
                    norm[key] = v.decode("ascii", "ignore")
                else:
                    norm[key] = v
            # remember field order (first occurrence defines the order)
            if not field_order:
                field_order = list(norm.keys())
            rows.append(
                {
                    "TimestampUTC": _iso_utc(ts) if ts is not None else None,
                    **norm,
                }
            )
    return rows, field_order


def _verify_schema(tabledef: List[Dict[str, Any]], table_name: str, field_names: List[str]) -> None:
    """Lightweight verification: requested table and returned fields make sense."""
    # Table present?
    tablenames = []
    for t in tabledef:
        name = t.get("Header", {}).get("TableName")
        if isinstance(name, (bytes, bytearray)):
            name = name.decode("ascii", "ignore")
        tablenames.append(name)
    if table_name not in tablenames:
        print(f"[WARN] Table {table_name!r} not found in GetTableDefs (have: {tablenames})", file=sys.stderr)

    # Field name sanity
    t_index = None
    try:
        t_index = tablenames.index(table_name)
    except ValueError:
        return
    defs = tabledef[t_index]["Fields"]
    def_names = []
    for f in defs:
        fn = f.get("FieldName")
        if isinstance(fn, (bytes, bytearray)):
            fn = fn.decode("ascii", "ignore")
        def_names.append(str(fn))
    # Note: logger may return subset/superset depending on Collect request
    missing = [f for f in field_names if f not in def_names]
    if missing:
        print(f"[WARN] Returned fields not in table def: {missing}", file=sys.stderr)


def _verify_time(df: pd.DataFrame) -> None:
    """Lightweight verification on time column: gaps/dupes summary."""
    if df.empty or "TimestampUTC" not in df.columns:
        print("[WARN] No timestamps in returned data.", file=sys.stderr)
        return
    ts = pd.to_datetime(df["TimestampUTC"], utc=True, errors="coerce")
    if ts.isna().any():
        bad = int(ts.isna().sum())
        print(f"[WARN] {bad} row(s) had unparseable timestamps.", file=sys.stderr)
    # Check monotonic & gaps
    ts_sorted = ts.sort_values()
    diffs = ts_sorted.diff().dropna()
    dupes = int((diffs == pd.Timedelta(0)).sum())
    if len(diffs) == 0:
        print("[INFO] Only one (or zero) timestamp returned; skip cadence check.")
        return
    min_gap = diffs.min()
    max_gap = diffs.max()
    print(f"[INFO] Cadence summary: count={len(ts)} dupes={dupes} min_gap={min_gap} max_gap={max_gap}")


def normalize_host(host: str) -> str:
    """
    Accepts:
      - hostnames (e.g., 'cr800.local')
      - literal IPv6 with or without brackets (e.g., '2605:...:1DDD' or '[2605:...:1DDD]')
      - full URLs (e.g., 'http://[2605:...:1DDD]' or 'https://cr800.local')
    Returns a clean host suitable for socket.getaddrinfo().
    """
    h = host.strip()

    # If it's a full URL, peel out the hostname part
    if "://" in h:
        parsed = urlparse(h)
        if parsed.hostname:
            h = parsed.hostname
        else:
            # fall back to raw string if parse fails
            h = h.split("://", 1)[-1]

    # Remove surrounding brackets (URL syntax for IPv6)
    if h.startswith("[") and h.endswith("]"):
        h = h[1:-1].strip()

    return h

def fetch_last_n(
    *,
    host: str,
    port: int,
    base_addr: int,
    leaf_addr: int,
    table: str,
    n: int,
    timeout_defs: float = 10.0,
    timeout_collect: float = 20.0,
) -> pd.DataFrame:
    """
    Connect TCP->CR800 (base_addr) and route to CR206X leaf_addr (RouterPhyAddr=base).
    Use BMP5 GetTableDefs then Collect Data (mode=0x05, last-N).
    """
    s = utils.open_socket(host, Port=port, Timeout=max(timeout_defs, timeout_collect))
    if not s:
        raise ConnectionError(f"Unable to connect to {host}:{port}")

    try:
        # 1) Get table defs (routed via base)
        blob = utils.get_tabledefs_bmp5(
            s,
            DstNodeId=leaf_addr,
            SrcNodeId=base_addr,     # our logical node id (we present as base for route pairing)
            RouterPhyAddr=base_addr, # route through CR800
            timeout=timeout_defs,
        )
        tabledef = utils.parse_tabledef(blob)

        # 2) Collect last-N records (mode 0x05, P1=n)
        #    Returns list of fragments; we flatten to rows.
        recs, more = utils.collect_data(
            s,
            DstNodeId=leaf_addr,
            SrcNodeId=base_addr,
            TableDef=tabledef,
            TableName=table,
            FieldNames=(),           # all fields
            CollectMode=0x05,        # "last N"
            P1=int(n),
            RouterPhyAddr=base_addr,
            timeout=timeout_collect,
        )

        rows, field_order = _flatten_records(recs)
        if not rows:
            # Build an empty DF with a predictable schema
            df = pd.DataFrame(columns=["TimestampUTC"] + field_order)
            return df

        df = pd.DataFrame(rows)
        # Drop exact duplicate timestamps, keep newest
        if "TimestampUTC" in df.columns:
            df = df.drop_duplicates(subset=["TimestampUTC"], keep="last")
            df = df.sort_values("TimestampUTC")
        df.reset_index(drop=True, inplace=True)

        # lightweight verifications
        _verify_schema(tabledef, table, [c for c in df.columns if c != "TimestampUTC"])
        _verify_time(df)

        if more:
            print("[INFO] Logger indicated 'more' data available, but last-N should be sufficient.", file=sys.stderr)

        return df

    finally:
        try:
            s.close()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="Fetch last-N rows from CR206X leaf via CR800 PakBus/TCP")
    ap.add_argument("--host", required=True, help="CR800 host/IP (IPv4/IPv6/hostname; brackets/URL OK)")
    ap.add_argument("--port", type=int, default=6785, help="CR800 PakBus/TCP port (default 6785)")
    ap.add_argument("--base", type=int, default=1, help="CR800 PakBus address (default 1)")
    ap.add_argument("--leaf", type=int, required=True, help="Leaf PakBus address (CR206X)")
    ap.add_argument("--site", required=True, help="Site label (S1B, S2T, etc.)")
    ap.add_argument("--table", default="Table1", help="Table name (default Table1)")
    ap.add_argument("--num", type=int, default=200, help="How many recent records to fetch")
    args = ap.parse_args()

    args.host = normalize_host(args.host)


    df = fetch_last_n(
        host=args.host,
        port=args.port,
        base_addr=args.base,
        leaf_addr=args.leaf,
        table=args.table,
        n=args.num
    )

    out = OUT_DIR / f"{args.site}_{args.table}_decoded.csv"
    df.to_csv(out, index=False)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()