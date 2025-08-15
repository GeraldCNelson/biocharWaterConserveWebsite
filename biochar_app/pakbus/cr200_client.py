#!/usr/bin/env python3
"""
cr200_client.py — Collect data from CR200-series loggers through a CR800 (PakBus/TCP over IPv6)
Uses the lightweight PyPak (pakbus.py) you have in your repo.

Requirements:
  - Your existing pakbus/pakbus.py on PYTHONPATH
  - pandas

Notes:
  - We connect to the CR800 via IPv6 TCP at port 6785 and let it route to leaf IDs.
  - We try a few common filenames to retrieve table definitions (CR200s expose them as a
    “file” over BMP5/File Upload): e.g., "TableDef", "TABLEDEF", "#TABLEDEF".
  - Pass the IPv6 address WITHOUT brackets (i.e. 2605:...:1ddd)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Tuple, Dict, Any, List

import pandas as pd

# import your PyPak
from biochar_app.pakbus import pakbus as pb

# import your config container
from biochar_app.scripts.config import (
    PAKBUS,
    DEFAULT_TABLE,
    DEFAULT_HOURS,
)

# -------- logging ----------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("cr200_client")


# ---------- helpers ----------
TABLEDEF_CANDIDATES = [
    "TableDef", "TABLEDEF", "#TABLEDEF", "TDF", "TABLEDEFS", "TABLEDEF.DAT"
]

def _open_router_socket(host: str, port: int, timeout: int = 15):
    """
    Open a TCP socket to the CR800 PakBus/TCP service.
    IMPORTANT: pass IPv6 literal WITHOUT [] brackets.
    """
    s = pb.open_socket(host, Port=port, Timeout=timeout)
    if not s:
        raise RuntimeError(f"Could not open PakBus TCP socket to [{host}]:{port}")
    return s

def _fetch_tabledef_raw(s, leaf_id: int, base_id: int) -> bytes:
    """
    Try multiple common file names to grab the table definitions blob.
    Returns raw bytes; raises if none work.
    """
    last_err = None
    for name in TABLEDEF_CANDIDATES:
        try:
            data, rc = pb.fileupload(s, DstNodeId=leaf_id, SrcNodeId=base_id, FileName=name)
            if rc == 0 and data:
                log.info(f"Leaf {leaf_id}: got table definitions via {name!r} ({len(data)} bytes).")
                return data
            else:
                log.debug(f"Leaf {leaf_id}: {name!r} returned rc={rc}, length={len(data) if data else 0}.")
        except Exception as e:
            last_err = e
            log.debug(f"Leaf {leaf_id}: {name!r} failed: {e!r}")
    raise RuntimeError(f"Leaf {leaf_id}: failed to retrieve table definitions. Last error: {last_err!r}")

def _collect_since(
    s,
    leaf_id: int,
    base_id: int,
    tabledef: List[Dict[str, Any]],
    table_name: str,
    hours: int,
) -> Iterable[Dict[str, Any]]:
    """
    Collect records from the given table for the past N hours.
    For CR200, CollectMode=0x05 (by last N records) is often simplest;
    but we'll prefer CollectMode=0x07 (by time range) if table has an interval.
    We’ll translate time window to PakBus NSec and request by time.
    """
    # Find table entry to see if it’s interval-based
    tbl = next((t for t in tabledef if t["Header"]["TableName"] == table_name), None)
    if not tbl:
        raise RuntimeError(f"Leaf {leaf_id}: table {table_name!r} not found in definitions.")

    interval = tbl["Header"]["TblInterval"]  # (sec, nsec)
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=hours)

    # CR200 expects NSec tuples wrt epoch (1990-01-01). Use helpers from pakbus.py
    start_nsec = pb.time_to_nsec(start_utc.timestamp(), epoch=pb.nsec_base, tick=pb.nsec_tick)
    stop_nsec  = pb.time_to_nsec(now_utc.timestamp(),   epoch=pb.nsec_base, tick=pb.nsec_tick)

    collect_mode = 0x07  # by time range
    P1, P2 = start_nsec, stop_nsec

    rec_frags, more = pb.collect_data(
        s,
        DstNodeId=leaf_id,
        SrcNodeId=base_id,
        TableDef=tabledef,
        TableName=table_name,
        FieldNames=[],                # all fields
        CollectMode=collect_mode,
        P1=P1, P2=P2,
    )

    # Flatten to dicts
    for frag in rec_frags:
        for rec in frag.get("RecFrag", []):
            # Convert logger NSec time to ISO8601
            t_nsec = rec.get("TimeOfRec")
            if t_nsec:
                ts = pb.nsec_to_time(t_nsec, epoch=pb.nsec_base, tick=pb.nsec_tick)
                ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            else:
                ts_iso = None
            row = {"logger_id": leaf_id, "table": table_name, "rec": rec["RecNbr"], "time": ts_iso}
            row.update(rec.get("Fields", {}))
            yield row

def fetch_cr200_batch(
    table: str = DEFAULT_TABLE,
    hours: int = DEFAULT_HOURS,
) -> Iterable[pd.DataFrame]:
    """
    Connect once to the CR800 and iterate all CR200 leaf IDs.
    Yields DataFrames with rows for each logger.
    """
    host = PAKBUS.host
    port = PAKBUS.port
    base_id = PAKBUS.base_id

    log.info(f"Connecting to CR800 at [{host}]:{port}")
    with _open_router_socket(host, port) as s:
        # Optional: tell the router we’re here (PakBus hello). Some networks don’t require it.
        try:
            pb.ping_node(s, DstNodeId=PAKBUS.router_id, SrcNodeId=base_id)
        except Exception:
            pass

        for leaf in PAKBUS.logger_ids:
            try:
                # Quick hello to the leaf (verifies route)
                hello = pb.ping_node(s, DstNodeId=leaf, SrcNodeId=base_id)
                if not hello:
                    log.warning(f"Leaf {leaf}: no hello response; skipping.")
                    continue

                raw = _fetch_tabledef_raw(s, leaf_id=leaf, base_id=base_id)
                tabledef = pb.parse_tabledef(raw)

                rows = list(_collect_since(s, leaf, base_id, tabledef, table, hours))
                if not rows:
                    log.info(f"Leaf {leaf}: no rows.")
                    continue

                df = pd.DataFrame(rows)
                yield df

            except Exception as e:
                log.exception(f"Leaf {leaf}: error during collection: {e}")
                continue


def main():
    import argparse
    p = argparse.ArgumentParser(description="Collect recent data from CR200 leaves via CR800 (IPv6 PakBus/TCP)")
    p.add_argument("--table", default=DEFAULT_TABLE, help="Table to collect (e.g., Table1)")
    p.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="How many hours back")
    args = p.parse_args()

    any_rows = False
    for df in fetch_cr200_batch(table=args.table, hours=args.hours):
        for rec in df.to_dict(orient="records"):
            print(rec)
            any_rows = True
    if not any_rows:
        log.warning("No data returned from any CR200 leaf.")

if __name__ == "__main__":
    main()