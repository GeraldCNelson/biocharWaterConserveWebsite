#!/usr/bin/env python3
"""
fetch_leaf_records.py

Reconstructed December 2025 after the original script was lost.

CLI wrapper around PyCampbellCR1000 to fetch records from a single table
(e.g., Table1) on a Campbell Scientific logger and save them as CSV.

The CLI is designed to be compatible with existing callers like
biochar_app.scripts.collect_all_leaves, e.g.:

    python -m biochar_app.pakbus.scripts.fetch_leaf_records \
      --host 2605:59ca:2202:7700:2d0:2cff:fe02:1ddd \
      --port 6785 \
      --router 1 \
      --src 4093 \
      --leaf 2 \
      --tdf-file biochar_app/pakbus/pakbus_data/catalog/CSU_3depths.tdf \
      --table Table1 \
      --collect-mode mostrecent \
      --num 10 \
      --output leaf2_Table1_latest.csv

Notes / limitations of this reconstructed version:

  * Uses your IPv6-aware open_pakbus_link helper to build the TCP link.
  * Uses PyCampbellCR1000.CR1000 as the high-level client.
  * Currently normalizes all collect modes to "last N rows" logic:
      - mostrecent / lastn / auto → last N records from the table.
  * router/src/leaf arguments are parsed and logged; for now we talk directly
    to the router ID as the CR1000 “device” and ignore the leaf ID for routing.

Once this basic version is confirmed working, we can refine it to:

  * Respect the leaf ID / router ID for multi-hop routing if needed.
  * Use the TDF file to rename columns or perform validation.
"""

from __future__ import annotations

import argparse
import csv
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, List

from pycampbellcr1000.device import CR1000
from biochar_app.pakbus.core.link import open_pakbus_link

VALID_COLLECT_MODES = {"mostrecent", "lastn", "auto"}


# ---------------------------------------------------------------------------#
# CLI parsing
# ---------------------------------------------------------------------------#

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Fetch records from a Campbell logger table and write CSV."
    )

    ap.add_argument("--host", required=True, help="CR800 IPv4/IPv6 address")
    ap.add_argument("--port", required=True, type=int, help="PakBus TCP port")

    # These are kept for compatibility / future routing logic
    ap.add_argument("--router", required=True, type=int,
                    help="Router PakBus ID (CR800)")
    ap.add_argument("--src", required=True, type=int,
                    help="Source PakBus ID (PC / client)")
    ap.add_argument("--leaf", required=True, type=int,
                    help="Leaf PakBus ID (CR200-series logger)")

    ap.add_argument(
        "--tdf-file",
        help="Optional .tdf file describing table layout (currently unused).",
    )
    ap.add_argument(
        "--table",
        default="Table1",
        help="Table name to fetch (default: Table1).",
    )
    ap.add_argument(
        "--collect-mode",
        default="mostrecent",
        choices=sorted(VALID_COLLECT_MODES),
        help="Collection mode (currently all map to 'last N rows').",
    )
    ap.add_argument(
        "--num",
        type=int,
        default=10,
        help="Number of most recent records to fetch (0 ⇒ all records).",
    )
    ap.add_argument(
        "--output",
        required=True,
        help="Path to CSV file to create.",
    )

    return ap


# ---------------------------------------------------------------------------#
# Data helpers
# ---------------------------------------------------------------------------#

def normalize_collect_mode(mode: str) -> str:
    """Normalize collect mode string."""
    m = mode.lower().strip()
    if m not in VALID_COLLECT_MODES:
        raise ValueError(f"Unsupported collect-mode {mode!r}")
    # For now all modes are treated as "last N".
    return "lastn"


def slice_last_n(records: Iterable[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    """Return the last N records from any iterable of mapping-like rows."""
    if n <= 0:
        return list(records)
    buf: List[Dict[str, Any]] = []
    for rec in records:
        buf.append(rec)
        if len(buf) > n:
            buf.pop(0)
    return buf


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    """Write rows (List[dict]) to CSV at the given path."""
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        # Still create an empty, but valid, CSV file so callers see success.
        with path.open("w", newline="") as f:
            f.write("# No data returned from logger\n")
        print(f"⚠️  No rows returned; wrote empty CSV stub at {path}")
        return

    # Union of all keys so we don't silently drop columns
    fieldnames: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"✅ Wrote {len(rows)} records → {path}")


# ---------------------------------------------------------------------------#
# Core fetch logic
# ---------------------------------------------------------------------------#

def fetch_records(
    host: str,
    port: int,
    router_id: int,
    src_id: int,
    leaf_id: int,
    table: str,
    mode: str,
    num: int,
) -> List[Dict[str, Any]]:
    """
    Connect to the CR800 via IPv6 and return a list of dict records.

    For now we treat the CR800 router as the CR1000 “device” and ignore the
    leaf_id. Once this is talking reliably we can layer leaf routing on top.
    """
    mode_norm = normalize_collect_mode(mode)

    # --- Link + device -------------------------------------------------------
    print(
        f"🔌 Connecting with open_pakbus_link("
        f"host={host!r} ({type(host)}), "
        f"port={port!r} ({type(port)})"
        f") router_id={router_id}, src_id={src_id}, leaf_id={leaf_id}"
    )

    # Talk directly to the router PakBus ID over TCP.
    with open_pakbus_link(host, port) as link:
        # Talk to the leaf logger (CR200) via the router:
        #   - TCP socket terminates on the CR800
        #   - PakBus dest/src IDs are the leaf and PC IDs
        dev = CR1000(
            link,
            dest_addr=leaf_id,  # physical address of leaf
            dest=leaf_id,  # node ID of leaf
            src_addr=src_id,  # PC physical
            src=src_id,  # PC node
            security_code=0x0000,
        )
        print("📡 CR1000 device created; fetching table definitions…")

        # This will also verify connectivity.
        tables = dev.list_tables()
        print(f"📋 Available tables: {tables}")

        if table not in tables:
            raise RuntimeError(
                f"Requested table {table!r} not found on device. "
                f"Available tables: {tables}"
            )

        print(f"⬇️  Requesting data from table={table!r} (mode={mode_norm}, num={num})")

        # PyCampbellCR1000 returns a ListDict whose entries behave like dicts
        all_records = dev.get_data(table)

        # Convert to plain dicts for CSV writing
        plain: List[Dict[str, Any]] = []
        for rec in all_records:
            plain.append(dict(rec))

        print(f"ℹ️  Device returned {len(plain)} total records for {table!r}")

        if mode_norm == "lastn" and num > 0:
            plain = slice_last_n(plain, num)
            print(f"ℹ️  Keeping last {num} records (after slicing: {len(plain)})")

        return plain


# ---------------------------------------------------------------------------#
# Main
# ---------------------------------------------------------------------------#

def main(argv: list[str] | None = None) -> int:
    ap = build_arg_parser()
    args = ap.parse_args(argv)

    print(
        f"🌐 fetch_leaf_records: "
        f"host={args.host} port={args.port} "
        f"router={args.router} src={args.src} leaf={args.leaf} "
        f"table={args.table} collect-mode={args.collect_mode} num={args.num}"
    )

    try:
        rows = fetch_records(
            host=args.host,
            port=int(args.port),
            router_id=int(args.router),
            src_id=int(args.src),
            leaf_id=int(args.leaf),
            table=args.table,
            mode=args.collect_mode,
            num=int(args.num),
        )
        out_path = Path(args.output)
        write_csv(out_path, rows)
        return 0

    except Exception as exc:
        # Print a rich traceback so we can see exactly where the failure occurs.
        print(
            f"❌ Failed to fetch data from {args.host}:{args.port} "
            f"table={args.table!r}: {exc!r}"
        )
        print("\n----- Python traceback (for debugging) -----")
        traceback.print_exc()
        print("----- end traceback -----\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())