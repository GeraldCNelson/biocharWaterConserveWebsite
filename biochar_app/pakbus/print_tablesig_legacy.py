#!/usr/bin/env python3
"""
print_tablesig_legacy.py
Use legacy pakbus.py to fetch table definitions from a CR200 leaf (via CR800) and
print table names + signatures. This does NOT use BMP5 GetTableDefs; it uses FileUpload,
which the CR200 firmware consistently supports.
"""

from __future__ import annotations
import argparse
from datetime import datetime, timezone

from biochar_app.pakbus import pakbus as pb
from biochar_app.scripts.config import PAKBUS, STATION_BY_ID

# Common filenames that CR2xx expose for table defs
TABLEDEF_CANDIDATES = ["TableDef", "TABLEDEF", "#TABLEDEF", "TDF", "TABLEDEFS", "TABLEDEF.DAT"]

def fetch_tabledef_raw(sock, leaf_id: int, base_id: int) -> bytes:
    last_rc = None
    for name in TABLEDEF_CANDIDATES:
        data, rc = pb.fileupload(sock, DstNodeId=leaf_id, SrcNodeId=base_id, FileName=name)
        if rc == 0 and data:
            return data
        last_rc = rc
    raise RuntimeError(f"Could not read table defs via FileUpload (last rc={last_rc}).")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", help="CR800 IPv6 literal WITHOUT brackets; default from config.PAKBUS.host")
    ap.add_argument("--port", type=int, help="PakBus/TCP port; default from config")
    ap.add_argument("--base", type=int, help="CR800 PakBus ID; default from config")
    ap.add_argument("--leaf", type=int, help="Leaf PakBus ID (e.g., 4 for S1B); or provide --site")
    ap.add_argument("--site", help="Site label like S1B (looked up via config.STATION_BY_ID)")
    args = ap.parse_args()

    host = args.host or PAKBUS.host
    port = args.port or PAKBUS.port
    base = args.base or PAKBUS.base_id

    if args.leaf is not None:
        leaf = int(args.leaf)
    elif args.site:
        rev = {name.upper(): id_ for id_, name in STATION_BY_ID.items()}
        key = args.site.strip().upper()
        if key not in rev:
            raise SystemExit(f"--site {args.site!r} not found in config.STATION_BY_ID")
        leaf = int(rev[key])
    else:
        raise SystemExit("Provide either --leaf or --site")

    s = pb.open_socket(host, Port=port, Timeout=15)
    if not s:
        raise SystemExit(f"Could not open PakBus TCP to [{host}]:{port}")

    try:
        # Optional: say hello to open a route
        try:
            pb.ping_node(s, DstNodeId=leaf, SrcNodeId=base)
        except Exception:
            pass

        raw = fetch_tabledef_raw(s, leaf, base)
        tdefs = pb.parse_tabledef(raw)
        for i, t in enumerate(tdefs, start=1):
            name = t["Header"]["TableName"]
            sig = t["Signature"] & 0xFFFF
            print(f"Table #{i}: {name}, signature=0x{sig:04X}  ({sig})")
    finally:
        try: s.close()
        except Exception: pass

if __name__ == "__main__":
    main()