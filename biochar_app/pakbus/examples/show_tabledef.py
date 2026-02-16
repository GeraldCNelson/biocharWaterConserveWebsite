#!/usr/bin/env python3
"""
show_tabledef.py  —  Print table definitions from a CR200/CR800 using our local utils.

Usage:
  python -m biochar_app.pakbus.examples.show_tabledef \
      --host 2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd \
      --port 6785 --base 1 --leaf 12 --src 4094

It tries (in order):
  1) FileUpload of common def files (CPU:TDF / CPU:TABLEDEFS.TDF / CPU:DEF.TDF / CPU:DEF).
  2) If that fails, it reports and exits (your device currently does not allow 0x16 GetTableDefs
     over the routed path, so CollectData requires a matching cached signature/schema).
"""

from __future__ import annotations
import argparse
import sys
from typing import Any, Dict, List, Tuple

from biochar_app.pakbus import cr200_client_utils as u


def hexdump(b: bytes, n: int = 96) -> str:
    b = bytes(b or b"")
    return b[:n].hex() + ("..." if len(b) > n else "")


def try_fileupload_defs(
    s, *, leaf: int, src: int, router: int, timeout: float
) -> Tuple[bytes, int]:
    """Try a few common file names that sometimes hold table defs."""
    candidates = [
        "CPU:TDF",
        "CPU:.TDF",
        "CPU:TABLEDEFS.TDF",
        "CPU:TABLEDEFS",
        "CPU:DEF.TDF",
        "CPU:DEF",
        "CPU:prog.TDF",
        "CPU:prog",
    ]
    for name in candidates:
        data, rc = u.fileupload(
            s,
            DstNodeId=leaf,
            SrcNodeId=src,
            FileName=name,
            RouterPhyAddr=router,
            timeout=timeout,
        )
        print(f"  FileUpload {name!r}: resp={rc}  bytes={len(data)}  head={hexdump(data,64)}")
        if data and rc == 0:
            return data, rc
    return b"", 0x0E  # general error / not found


def print_defs(tabledef: List[Dict[str, Any]]) -> None:
    for i, t in enumerate(tabledef, start=1):
        hdr = t.get("Header", {})
        name = hdr.get("TableName")
        if isinstance(name, (bytes, bytearray)):
            name = name.decode("ascii", "ignore")
        sig = t.get("Signature")
        print(f"\nTable {i}: {name}  (signature=0x{int(sig):X})")
        print("  Header:", hdr)
        for j, f in enumerate(t.get("Fields", []), start=1):
            # normalize a few byte-ish strings for readability
            fn = f.get("FieldName")
            if isinstance(fn, (bytes, bytearray)):
                fn = fn.decode("ascii", "ignore")
            aliases = [a.decode("ascii", "ignore") if isinstance(a, (bytes, bytearray)) else a
                       for a in f.get("AliasName", [])]
            g = dict(f)
            g["FieldName"] = fn
            g["AliasName"] = aliases
            print(f"    Field {j}: {g}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Show table definitions (via FileUpload fallbacks).")
    ap.add_argument("--host", required=True, help="CR800 host/IP (IPv4/IPv6 ok; brackets allowed)")
    ap.add_argument("--port", type=int, default=6785)
    ap.add_argument("--base", type=int, default=1, help="CR800 PakBus address")
    ap.add_argument("--leaf", type=int, required=True, help="Leaf PakBus address (CR206X)")
    ap.add_argument("--src",  type=int, default=4094, help="Our client PakBus id")
    ap.add_argument("--timeout", type=float, default=10.0)
    args = ap.parse_args()

    s = u.open_socket(args.host, Port=args.port, Timeout=args.timeout)
    if not s:
        print(f"[ERROR] Unable to connect to {args.host}:{args.port}", file=sys.stderr)
        return 2

    try:
        # sanity hellos so we know routing works
        hb = u.ping_node(s, DstNodeId=args.base, SrcNodeId=args.src, RouterPhyAddr=None, timeout=5.0)
        hl = u.ping_node(s, DstNodeId=args.leaf, SrcNodeId=args.src, RouterPhyAddr=args.base, timeout=5.0)
        print("hello_base:", hb)
        print("hello_leaf_via_base:", hl)

        print("\nTrying FileUpload candidates for table defs...")
        blob, rc = try_fileupload_defs(
            s, leaf=args.leaf, src=args.src, router=args.base, timeout=args.timeout
        )
        if not blob:
            print("\n[INFO] No table-def file found via FileUpload.")
            print("Your logger currently doesn’t respond to routed GetTableDefs (0x16),")
            print("so remote CollectData requires a cached schema/signature.")
            print("If you can export the table def once (LoggerNet/Device Config),")
            print("we can cache it and proceed without 0x16.")
            return 1

        tdef = u.parse_tabledef(blob)
        print_defs(tdef)
        return 0

    finally:
        try:
            s.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())