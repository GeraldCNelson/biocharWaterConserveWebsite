#!/usr/bin/env python3
"""
print_tablesig_fileupload.py
Fetch CR200 table definitions via FileUpload (through the CR800 router) using
the modern utils module, then print table names and signatures.

Usage examples:
  python -m biochar_app.pakbus.print_tablesig_fileupload --site S1B
  python -m biochar_app.pakbus.print_tablesig_fileupload --host 2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd --base 4094 --leaf 4
"""

from __future__ import annotations
import argparse
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse
import sys

from . import cr200_client_utils as utils

# Try to import config for defaults and site ↔︎ id mapping
PAKBUS_DEFAULTS = {"host": None, "port": 6785, "base_id": 1}
STATION_BY_ID: Dict[int, str] = {}
ID_BY_STATION: Dict[str, int] = {}

def _try_import_config() -> None:
    global PAKBUS_DEFAULTS, STATION_BY_ID, ID_BY_STATION
    for modname in ("biochar_app.scripts.config", "scripts.config", "config"):
        try:
            cfg = __import__(modname, fromlist=["*"])
        except Exception:
            continue
        if hasattr(cfg, "PAKBUS"):
            PAKBUS_DEFAULTS["host"] = getattr(cfg.PAKBUS, "host", PAKBUS_DEFAULTS["host"])
            PAKBUS_DEFAULTS["port"] = getattr(cfg.PAKBUS, "port", PAKBUS_DEFAULTS["port"])
            PAKBUS_DEFAULTS["base_id"] = getattr(cfg.PAKBUS, "base_id", PAKBUS_DEFAULTS["base_id"])
        if hasattr(cfg, "STATION_BY_ID") and isinstance(cfg.STATION_BY_ID, dict):
            STATION_BY_ID.clear()
            STATION_BY_ID.update(cfg.STATION_BY_ID)
            ID_BY_STATION.clear()
            ID_BY_STATION.update({name.upper(): id_ for id_, name in STATION_BY_ID.items()})
        return
_try_import_config()

TABLEDEF_CANDIDATES = [
    "TableDef", "TABLEDEF", "#TABLEDEF", "TDF", "TABLEDEFS", "TABLEDEF.DAT"
]

def _normalize_host(h: Optional[str]) -> Optional[str]:
    if not h: return None
    s = h.strip()
    if "://" in s:
        parsed = urlparse(s)
        s = parsed.hostname or s
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    return s

def fetch_tabledefs_via_fileupload(s, leaf: int, base: int) -> List[Dict[str, Any]]:
    """
    Try several known filenames via FileUpload (BMP5 0x0F) and parse the table defs blob.
    """
    # Be polite: say hello to open a route (ignore errors)
    try:
        utils.ping_node(s, DstNodeId=leaf, SrcNodeId=base, RouterPhyAddr=base, timeout=5.0)
    except Exception:
        pass

    last_rc = None
    for name in TABLEDEF_CANDIDATES:
        data, rc = utils.fileupload(
            s,
            DstNodeId=leaf,
            SrcNodeId=base,
            FileName=name,
            RouterPhyAddr=base,
            timeout=12.0,
        )
        if rc == 0 and data:
            try:
                return utils.parse_tabledef(data)
            except Exception as e:
                raise RuntimeError(f"Got bytes from {name!r} but failed to parse: {e}") from e
        last_rc = rc
    raise RuntimeError(f"All FileUpload candidates failed (last rc={last_rc}).")

def main():
    ap = argparse.ArgumentParser(description="Print table names and signatures via FileUpload (CR200).")
    ap.add_argument("--host", help="CR800 IPv6/hostname (no brackets needed). Defaults to config.PAKBUS.host")
    ap.add_argument("--port", type=int, help="PakBus/TCP port (default from config or 6785)")
    ap.add_argument("--base", type=int, help="CR800 PakBus ID (default from config)")
    ap.add_argument("--leaf", type=int, help="Leaf PakBus ID (e.g., 4 for S1B). You may use --site instead.")
    ap.add_argument("--site", help="Site label (S1B, S2T, ...), resolved via config.STATION_BY_ID")
    args = ap.parse_args()

    host = _normalize_host(args.host or PAKBUS_DEFAULTS.get("host"))
    port = args.port if args.port is not None else int(PAKBUS_DEFAULTS.get("port", 6785))
    base = args.base if args.base is not None else int(PAKBUS_DEFAULTS.get("base_id", 1))

    if not host:
        print("[ERROR] No --host and config.PAKBUS.host not available.", file=sys.stderr)
        sys.exit(2)

    leaf = args.leaf
    if leaf is None:
        if not args.site:
            print("[ERROR] Provide either --leaf or --site.", file=sys.stderr)
            sys.exit(2)
        key = args.site.strip().upper()
        if key not in ID_BY_STATION:
            print(f"[ERROR] site {args.site!r} not found in config.STATION_BY_ID.", file=sys.stderr)
            sys.exit(2)
        leaf = int(ID_BY_STATION[key])

    s = utils.open_socket(host, Port=port, Timeout=20.0)
    if not s:
        print(f"[ERROR] Unable to connect to {host}:{port}", file=sys.stderr)
        sys.exit(2)

    try:
        tdefs = fetch_tabledefs_via_fileupload(s, leaf=leaf, base=base)
        for i, t in enumerate(tdefs, start=1):
            name = t.get("Header", {}).get("TableName")
            if isinstance(name, (bytes, bytearray)):
                name = name.decode("ascii", "ignore")
            sig = int(t.get("Signature", 0)) & 0xFFFF
            print(f"Table #{i}: {name}, signature=0x{sig:04X}  ({sig})")
    finally:
        try: s.close()
        except Exception: pass

if __name__ == "__main__":
    main()