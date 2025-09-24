#!/usr/bin/env python3
"""
print_tablesig_fileupload.py
Fetch CR200 table definitions via FileUpload (through the CR800 router) and
print table names + signatures, with strict per-try timeouts & logging.

Usage:
  python -m biochar_app.pakbus.print_tablesig_fileupload \
    --host 2605:59C0:30F3:2500:2D0:2CFF:FE02:1DDD \
    --base 1 \
    --leaf 4
"""

from __future__ import annotations
import argparse, sys, time
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

from . import cr200_client_utils as utils

# Add near top
TABLEDEF_BASENAMES = [
    "TableDef", "TABLEDEF", "#TABLEDEF", "TDF",
    "TABLEDEFS", "TABLEDEF.DAT", "TABLEDEF.TDF"
]
DRIVE_PREFIXES = ["", "CPU:", "USR:", "Cpu:", "Usr:", "CPU:\\", "USR:\\"]

def fetch_tabledefs_via_fileupload(s, leaf: int, base: int) -> bytes:
    try:
        utils.ping_node(s, DstNodeId=leaf, SrcNodeId=base, RouterPhyAddr=base, timeout=4.0)
    except Exception:
        pass

    last_rc = None
    for name in _candidate_names():
        try:
            print(f"[INFO] Trying FileUpload({name!r}) via router={base} to leaf={leaf} …", flush=True)
            data, rc = utils.fileupload(
                s,
                DstNodeId=leaf,
                SrcNodeId=base,
                FileName=name,
                RouterPhyAddr=base,
                timeout=6.0,   # per-try timeout
            )
            print(f"[DEBUG] rc={rc}, bytes={len(data) if data else 0}")
            if rc == 0 and data:
                return data
            last_rc = rc
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[WARN] FileUpload({name!r}) failed: {e}")
            last_rc = -1
    raise RuntimeError(f"All FileUpload candidates failed (last rc={last_rc}).")

def fetch_tabledefs_via_fileupload(s, leaf: int, base: int) -> bytes:
    try:
        utils.ping_node(s, DstNodeId=leaf, SrcNodeId=base, RouterPhyAddr=base, timeout=4.0)
    except Exception:
        pass

    last_rc = None
    for name in _candidate_names():
        try:
            print(f"[INFO] Trying FileUpload('{name}') via router={base} to leaf={leaf} …", flush=True)
            data, rc = utils.fileupload(
                s,
                DstNodeId=leaf,
                SrcNodeId=base,
                FileName=name,
                RouterPhyAddr=base,   # CR800 hop
                timeout=6.0,          # per-try timeout
            )
            print(f"[DEBUG] rc={rc}, bytes={len(data) if data else 0}")
            if rc == 0 and data:
                return data
            last_rc = rc
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[WARN] FileUpload('{name}') failed: {e}")
            last_rc = -1
    raise RuntimeError(f"All FileUpload candidates failed (last rc={last_rc}).")

def _normalize_host(h: Optional[str]) -> Optional[str]:
    if not h: return None
    s = h.strip()
    if "://" in s:
        parsed = urlparse(s)
        s = parsed.hostname or s
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    return s

def fetch_tabledefs_via_fileupload(s, leaf: int, base: int) -> bytes:
    # Best-effort neighbor hello to open the RF route
    try:
        utils.ping_node(s, DstNodeId=leaf, SrcNodeId=base, RouterPhyAddr=base, timeout=4.0)
    except Exception:
        pass

    last_rc = None
    for name in TABLEDEF_BASENAMES:
        try:
            print(f"[INFO] Trying FileUpload('{name}') via router={base} to leaf={leaf} …", flush=True)
            # Short per-try timeout so we never hang on .recv(1)
            data, rc = utils.fileupload(
                s,
                DstNodeId=leaf,
                SrcNodeId=base,
                FileName=name,
                RouterPhyAddr=base,   # CR800 router hop (your real setup)
                timeout=6.0,
            )
            print(f"[DEBUG] FileUpload rc={rc}, bytes={len(data) if data else 0}")
            if rc == 0 and data:
                return data
            last_rc = rc
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[WARN] FileUpload('{name}') failed: {e}")
            last_rc = -1
    raise RuntimeError(f"All FileUpload candidates failed (last rc={last_rc}).")

def main():
    ap = argparse.ArgumentParser(description="List table names/signatures from a CR200 leaf via FileUpload.")
    ap.add_argument("--host", required=True, help="CR800 IPv6/hostname (no brackets).")
    ap.add_argument("--port", type=int, default=6785, help="PakBus/TCP port (default 6785)")
    ap.add_argument("--base", type=int, default=1, help="CR800 PakBus ID (router). Default 1")
    ap.add_argument("--leaf", type=int, required=True, help="Leaf PakBus ID (e.g., 4 for S1M)")
    ap.add_argument("--timeout", type=float, default=15.0, help="socket connect timeout")
    ap.add_argument("--save-tdf", help="Optional path to write raw TDF blob from device")
    args = ap.parse_args()

    host = _normalize_host(args.host)
    port = int(args.port)
    base = int(args.base)
    leaf = int(args.leaf)

    s = utils.open_socket(host, Port=port, Timeout=args.timeout)
    if not s:
        print(f"[ERROR] Unable to connect to {host}:{port}", file=sys.stderr)
        sys.exit(2)

    try:
        blob = fetch_tabledefs_via_fileupload(s, leaf=leaf, base=base)
        if args.save_tdf:
            import pathlib
            p = pathlib.Path(args.save_tdf)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(blob)
            print(f"[INFO] Saved device TDF blob → {p}")

        tdefs = utils.parse_tabledef(blob)
        print(f"[INFO] Retrieved {len(tdefs)} table definitions from leaf {leaf}")
        print("Idx  Signature  Name")
        print("---  ---------  ----------------")
        for i, t in enumerate(tdefs, start=1):
            name = t.get("Header", {}).get("TableName")
            if isinstance(name, (bytes, bytearray)):
                name = name.decode("ascii", "ignore")
            sig = int(t.get("Signature", 0)) & 0xFFFF
            print(f"{i:>3}  0x{sig:04X}   {name}")
    finally:
        try: s.close()
        except Exception: pass

if __name__ == "__main__":
    main()
