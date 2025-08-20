#!/usr/bin/env python3
r"""
cr200_client.py — Collect data from CR200-series loggers through a CR800 (PakBus/TCP over IPv6)

This version **does not** use GetTableDefs (0x16). Instead, it builds a *manual* table
definition for Table1 based on your CRBasic program and then uses BMP5 CollectData
(0x09) "by time range" with a routed header (DstPhy=router).

Requirements:
  - pandas
  - biochar_app.pakbus.cr200_client_utils (Python 3.13-safe helpers)

Examples
--------
# quick hello/diag to confirm routing works
python -m biochar_app.pakbus.cr200_client --station S2B --diag

# fetch last 6 hours from S2B Table1 via routed CollectData
python -m biochar_app.pakbus.cr200_client --station S2B --table Table1 --hours 6 --router 1

# same, but verbose parsing + try little-endian fallback if needed (automatic)
python -m biochar_app.pakbus.cr200_client --station S2B --table Table1 --hours 6 --router 1 --diag
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, Dict, Any, List, Optional, Tuple, Sequence

import pandas as pd

# PakBus helpers (modernized)
from biochar_app.pakbus.cr200_client_utils import (
    open_socket,
    ping_node,
    send,               # low-level send frame
    wait_pkt,           # wait for pkt (handles Please Wait, Hello auto-reply, etc.)
    pakbus_hdr,         # build a custom header (used for routed packets)
    pkt_collectdata_cmd,
    msg_collectdata_response,
    parse_collectdata,
    nsec_to_time,
    time_to_nsec,
    nsec_base,
    nsec_tick,
)

# Config (router host, ids, etc.)
from biochar_app.scripts.config import (
    PAKBUS,
    DEFAULT_TABLE,
    DEFAULT_HOURS,
    STATION_BY_ID,
    ID_BY_STATION,
)

# -------- logging ----------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("cr200_client")


# ------------------------------- Manual schema -------------------------------

def _manual_tabledef_table1() -> List[Dict[str, Any]]:
    """
    Build a synthetic TableDef for Table1 (based on the CRBasic you provided).

    Fields (11):
      BattV_Min,
      VWC_1_Avg, EC_1_Avg, T_1_Avg,
      VWC_2_Avg, EC_2_Avg, T_2_Avg,
      VWC_3_Avg, EC_3_Avg, T_3_Avg

    We start with IEEE4B (big-endian float) types; if parsing fails, we retry with IEEE4L.
    """
    # helper to construct one field entry
    def fld(name: bytes, ftype: str = "IEEE4B") -> Dict[str, Any]:
        return {
            "ReadOnly": 0,
            "FieldType": ftype,   # will be swapped to IEEE4L on fallback
            "FieldName": name,
            "AliasName": [],
            "Processing": b"",
            "Units": b"",
            "Description": b"",
            "BegIdx": 0,
            "Dimension": 1,
            "SubDim": [],
        }

    fields_be = [
        fld(b"BattV_Min"),
        fld(b"VWC_1_Avg"), fld(b"EC_1_Avg"),  fld(b"T_1_Avg"),
        fld(b"VWC_2_Avg"), fld(b"EC_2_Avg"),  fld(b"T_2_Avg"),
        fld(b"VWC_3_Avg"), fld(b"EC_3_Avg"),  fld(b"T_3_Avg"),
    ]

    table = {
        "Header": {
            "TableName": b"Table1",
            "TableSize": 0,                 # unknown; not needed for parsing
            "TimeType": 0,                  # not used here
            "TblTimeInto": (0, 0),          # not used here
            "TblInterval": (900, 0),        # 15 minutes * 60 = 900 seconds
        },
        "Fields": fields_be,
        "Signature": 0,                     # we don't have the real signature
    }
    return [table]


def _tabledef_swap_endian(tdefs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clone a tabledef list but swap IEEE4B <-> IEEE4L for all fields.
    """
    import copy
    td = copy.deepcopy(tdefs)
    for t in td:
        for f in t.get("Fields", []):
            if f.get("FieldType") == "IEEE4B":
                f["FieldType"] = "IEEE4L"
            elif f.get("FieldType") == "IEEE4L":
                f["FieldType"] = "IEEE4B"
    return td


# ------------------------------- Collect helpers -----------------------------

def _collect_since_routed(
    s,
    *,
    router: int,
    leaf_id: int,
    base_id: int,
    tabledef: List[Dict[str, Any]],
    table_name: str,
    start_utc: datetime,
    stop_utc: datetime,
    timeout: float = 20.0,
) -> List[Dict[str, Any]]:
    """
    Send a routed BMP5 CollectData (0x09) for [start_utc, stop_utc] using our synthetic tabledef.
    We set TableNbr=1 (Table1) and TableDefSig=0 (unknown). The device *may* accept 0.

    Returns a flat list of record dicts (with TimeOfRec converted to ISO).
    """
    # Only Table1 for now
    if table_name != "Table1":
        raise RuntimeError("This manual path currently supports only Table1")

    # Table1 is #1 in our synthetic def
    table_nbr = 1
    tabledef_sig = tabledef[table_nbr - 1]["Signature"]

    # PakBus NSec range
    start_nsec = time_to_nsec(start_utc.timestamp(), epoch=nsec_base, tick=nsec_tick)
    stop_nsec  = time_to_nsec(stop_utc.timestamp(),   epoch=nsec_base, tick=nsec_tick)

    # Build a *non-routed* pkt first
    body_pkt, tn = pkt_collectdata_cmd(
        DstNodeId=leaf_id, SrcNodeId=base_id,
        TableNbr=table_nbr, TableDefSig=tabledef_sig,
        FieldNbr=(), CollectMode=0x07,  # by time range
        P1=start_nsec, P2=stop_nsec,    # [(sec,nsec), (sec,nsec)]
        SecurityCode=0x0000,
    )

    # Replace header with a *routed* header (DstPhy=router)
    routed_hdr = pakbus_hdr(
        DstNodeId=leaf_id, SrcNodeId=base_id,
        HiProtoCode=0x01,             # BMP5 app
        ExpMoreCode=0, LinkState=0, Priority=0, HopCnt=0,
        DstPhyAddr=router, SrcPhyAddr=base_id,
    )
    pkt = routed_hdr + body_pkt[8:]   # reuse body (starts at byte 8)
    send(s, pkt)

    # Wait for response and parse minimally
    _hdr, msg = wait_pkt(
        s, DstNodeId=base_id, SrcNodeId=leaf_id, TranNbr=tn, timeout=timeout
    )
    if not msg:
        raise RuntimeError("No CollectData response (timeout).")

    # Decode response stubs (RespCode + payload)
    msg = msg_collectdata_response(msg)
    rc = msg.get("RespCode", 0xFF)
    if rc != 0:
        raise RuntimeError(f"CollectData returned RespCode={rc}")

    data = msg.get("RecData", b"")
    # We’ll decode with provided tabledef; if that fails, caller can retry with endian swap
    rec_frags, _more = parse_collectdata(data, tabledef, FieldNbr=())
    out_rows: List[Dict[str, Any]] = []
    for frag in rec_frags:
        for rec in frag.get("RecFrag", []):
            t_nsec = rec.get("TimeOfRec")
            ts_iso = None
            if t_nsec:
                ts = nsec_to_time(t_nsec, epoch=nsec_base, tick=nsec_tick)
                ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            row = {"rec": rec.get("RecNbr"), "time": ts_iso}
            row.update(rec.get("Fields", {}))
            out_rows.append(row)
    return out_rows


# ------------------------------ Public function ------------------------------

def fetch_one_leaf(
    leaf_id: int,
    *,
    table: str,
    hours: int,
    station_code: Optional[str] = None,
    list_files_only: bool = False,
    prefix: Optional[str] = None,
    datafile_override: Optional[str] = None,
    diag: bool = False,
    router: int = 1,
) -> Iterable[pd.DataFrame]:
    """
    Connect to the router and work with a single *leaf* (endpoint).

    Current strategy (no GetTableDefs):
      - Synthesize Table1 schema from CRBasic
      - Send BMP5 CollectData (time range) with routed header
      - Try IEEE4B, then fallback to IEEE4L

    list_files_only / prefix / datafile_override are accepted for CLI compatibility,
    but file listing / file upload aren’t effective on your CR200X setup (rc=14).
    """
    del list_files_only, prefix, datafile_override  # unused in this path

    host = PAKBUS.host
    port = PAKBUS.port
    base_id = PAKBUS.base_id

    if table != "Table1":
        raise SystemExit("This manual path currently supports only --table Table1")

    log.info("Connecting to CR800 at [%s]:%s", host, port)
    with open_socket(host, Port=port, Timeout=15) as s:
        # Warm-up routed hello (verifies that router->leaf route is active)
        hello = ping_node(s, DstNodeId=leaf_id, SrcNodeId=base_id, RouterPhyAddr=router)
        if diag:
            log.info("[diag] hello leaf %s via router %s: %s", leaf_id, router, hello)
        if not hello:
            log.warning("Leaf %s: no hello response via router %s.", leaf_id, router)
            return

        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(hours=hours)

        # Manual schema from CRBasic
        tdef_be = _manual_tabledef_table1()

        # Try big-endian first
        try:
            rows = _collect_since_routed(
                s,
                router=router, leaf_id=leaf_id, base_id=base_id,
                tabledef=tdef_be, table_name="Table1",
                start_utc=start_utc, stop_utc=now_utc,
                timeout=20.0,
            )
        except Exception as e_be:
            if diag:
                log.debug("CollectData with IEEE4B failed: %r (retry IEEE4L)", e_be)
            # Retry with little-endian floats
            tdef_le = _tabledef_swap_endian(tdef_be)
            try:
                rows = _collect_since_routed(
                    s,
                    router=router, leaf_id=leaf_id, base_id=base_id,
                    tabledef=tdef_le, table_name="Table1",
                    start_utc=start_utc, stop_utc=now_utc,
                    timeout=20.0,
                )
            except Exception as e_le:
                log.warning("CollectData failed with both endian assumptions: %r", e_le)
                return

        if not rows:
            log.warning("Leaf %s: no rows returned in range.", leaf_id)
            return

        # Normalize bytes->str for column names; build DataFrame
        df = pd.DataFrame(rows)
        yield df


# ----------------------------------- CLI -------------------------------------

def main():
    import argparse
    p = argparse.ArgumentParser(description="Collect recent data from a single CR200 leaf via CR800 (IPv6 PakBus/TCP)")
    p.add_argument("--table", default=DEFAULT_TABLE, help="Table to collect (currently only 'Table1' supported)")
    p.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="How many hours back (CollectData time range)")
    p.add_argument("--leaf", type=int, help="PakBus node ID of the leaf (e.g., 7 for S2B)")
    p.add_argument("--station", help="Station code (e.g., S2B). Overrides --leaf if both provided.")
    p.add_argument("--router", type=int, default=1, help="Router PakBus physical addr (default 1)")
    p.add_argument("--diag", action="store_true", help="Verbose diagnostics")
    args = p.parse_args()

    # Resolve target leaf/station
    station_code: Optional[str] = None
    leaf_id: Optional[int] = None
    if args.station:
        station_code = args.station.strip().upper()
        leaf_id = ID_BY_STATION.get(station_code)
        if not leaf_id:
            raise SystemExit(f"Unknown station code {station_code!r}. Valid: {sorted(ID_BY_STATION.keys())}")
    elif args.leaf:
        leaf_id = int(args.leaf)
        station_code = STATION_BY_ID.get(leaf_id)
    else:
        raise SystemExit("You must supply either --station or --leaf.")

    if args.table != "Table1":
        raise SystemExit("This manual path currently supports only --table Table1")

    any_rows = False
    for df in fetch_one_leaf(
        leaf_id,
        table=args.table,
        hours=args.hours,
        station_code=station_code,
        diag=args.diag,
        router=args.router,
    ):
        # Print a few human-friendly rows
        cols = ["time", "rec", "BattV_Min", "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
                "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
                "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"]
        have = [c for c in cols if c in df.columns]
        for rec in df[have].head(25).to_dict(orient="records"):
            print(rec)
        any_rows = True

    if not any_rows:
        log.warning("No data returned.")


__all__ = [
    "main",
    "fetch_one_leaf",
]