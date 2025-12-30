#!/usr/bin/env python3
"""
fetch_leaf_records_bd.py

Lightweight, BD-frame–based downloader for CR206 "Table1" records via a CR800 router.

This script:
  * Opens a plain TCP connection to the CR800 BD port.
  * Sends a fixed 27-byte BD "get data" request (same one used by the
    loop_27b_tx_pull tool that produced tx_9150_20251006T214256Z.hex).
  * Collects the raw reply bytes.
  * Scans the reply for candidate records using the known Campbell layout:

        >I10f

    where:
        - The first 4 bytes are a Campbell timestamp: seconds since 1990-01-01.
        - The 10 floats are:
              BattV,
              VWC_1, EC_1, T_1,
              VWC_2, EC_2, T_2,
              VWC_3, EC_3, T_3

  * Applies sanity checks (year window, BattV range, VWC range, etc.).
  * De-duplicates records and writes the most recent N to CSV.

NOTE
----
This script does *not* use pycampbellcr1000 or the CR1000/PakBus object model.
It works directly on the BD byte stream, using the same pattern that was
confirmed by decode_hex_candidates.py on the archived tx_9150_20251006... file.

Usage example
-------------
python -m biochar_app.pakbus.scripts.fetch_leaf_records_bd \\
  --host 2605:59ca:2202:7700:2d0:2cff:fe02:1ddd \\
  --port 6785 \\
  --leaf 2 \\
  --num 10 \\
  --output test_leaf2_Table1_bd.csv \\
  --include-raw
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import socket
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple


# --- Constants ---------------------------------------------------------------

# Fixed 27-byte BD "get data" request that we know produces valid Table1 rows
# for the CR206 leafs, as captured in tx_9150_20251006T214256Z.hex.
#
# If/when we reverse-engineer the path fully we can parameterize leaf/router,
# but for now this is the proven working payload.
TX_GETDATA = bytes.fromhex("bda0016ffd00010ffd09050102ffff2810bd")

# Record layout: 4-byte timestamp + 10 floats (BattV + 3 sensors × 3 fields)
RECORD_FLOATS = 10
RECORD_SIZE = 4 + 4 * RECORD_FLOATS  # 44 bytes

# Campbell epoch: seconds since 1990-01-01
CAMPBELL_EPOCH = _dt.datetime(1990, 1, 1, tzinfo=_dt.timezone.utc)


@dataclass
class CandidateRecord:
    offset: int
    ts_raw: int
    dt: _dt.datetime
    floats: Tuple[float, ...]
    raw_bytes: bytes


# --- Helpers -----------------------------------------------------------------


def _decode_campbell_ts(ts_raw: int) -> _dt.datetime:
    """Convert Campbell 'seconds since 1990-01-01' into a UTC datetime."""
    return CAMPBELL_EPOCH + _dt.timedelta(seconds=ts_raw)


def _looks_reasonable(
    dt: _dt.datetime,
    floats: Sequence[float],
    min_year: int,
    max_year: int,
) -> bool:
    """
    Basic sanity checks for a candidate record.

    We keep this deliberately simple – enough to filter junk but not so strict
    that we drop real data.
    """
    if not (min_year <= dt.year <= max_year):
        return False

    if len(floats) != RECORD_FLOATS:
        return False

    batt = floats[0]
    if not (8.0 <= batt <= 16.5):
        return False

    # VWC channels (index 1, 4, 7) should be in [0, ~1.6] (0–160%).
    for idx in (1, 4, 7):
        v = floats[idx]
        if not (-0.02 <= v <= 1.6):
            return False

    # Temperatures (indices 3, 6, 9) in a plausible soil range (°C-ish).
    for idx in (3, 6, 9):
        t = floats[idx]
        if not (-40.0 <= t <= 70.0):
            return False

    return True


def iter_candidate_records(
    raw: bytes,
    *,
    min_year: int,
    max_year: int,
) -> Iterable[CandidateRecord]:
    """
    Slide a 44-byte window over `raw` and yield records that match our layout.

    Each candidate is:
      - 4 bytes: big-endian uint32 Campbell timestamp
      - 10 floats: big-endian IEEE754
    """
    n = len(raw)
    for offset in range(0, n - RECORD_SIZE + 1):
        chunk = raw[offset : offset + RECORD_SIZE]
        ts_raw = struct.unpack_from(">I", chunk, 0)[0]
        dt = _decode_campbell_ts(ts_raw)
        floats = struct.unpack_from(">" + "f" * RECORD_FLOATS, chunk, 4)

        if _looks_reasonable(dt, floats, min_year=min_year, max_year=max_year):
            yield CandidateRecord(
                offset=offset,
                ts_raw=ts_raw,
                dt=dt,
                floats=floats,
                raw_bytes=chunk,
            )


def dedupe_and_sort(
    candidates: Iterable[CandidateRecord],
) -> List[CandidateRecord]:
    """
    De-duplicate candidate records based on (ts_raw, rounded floats) and
    return them sorted by ts_raw (ascending).
    """
    seen = set()
    deduped: List[CandidateRecord] = []

    for c in candidates:
        key = (c.ts_raw, tuple(round(f, 6) for f in c.floats))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(c)

    deduped.sort(key=lambda c: c.ts_raw)
    return deduped


def connect_and_fetch_bytes(host: str, port: int, timeout: float = 10.0) -> bytes:
    """
    Open a TCP connection to (host, port), send TX_GETDATA, and read reply bytes.

    We read until the peer closes the socket or until we see several consecutive
    timeouts, at which point we assume the logger is done talking.
    """
    addr = f"[{host}]:{port}"
    print(f"[CONNECT] {addr}")
    sock = socket.create_connection((host, port), timeout=timeout)
    try:
        sock.settimeout(timeout)
        print(f"[TX] Sending {len(TX_GETDATA)} bytes...")
        sock.sendall(TX_GETDATA)

        buf = bytearray()
        empty_reads = 0
        while True:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                empty_reads += 1
                if empty_reads >= 3:
                    # No more data after 3 consecutive timeouts – bail out.
                    break
                continue

            if not chunk:
                # Remote closed connection.
                break

            buf.extend(chunk)
            empty_reads = 0

        print(f"[RX] Received {len(buf)} bytes total.")
        return bytes(buf)
    finally:
        sock.close()
        print("[CLOSE] socket closed")


def write_csv(
    path: Path,
    records: Sequence[CandidateRecord],
    *,
    include_raw: bool,
) -> None:
    """
    Write decoded records to CSV.

    Columns:
      timestamp_iso, timestamp_campbell, BattV,
      VWC_1, EC_1, T_1,
      VWC_2, EC_2, T_2,
      VWC_3, EC_3, T_3,
      [raw_hex]
    """
    fieldnames = [
        "timestamp_iso",
        "timestamp_campbell",
        "BattV",
        "VWC_1",
        "EC_1",
        "T_1",
        "VWC_2",
        "EC_2",
        "T_2",
        "VWC_3",
        "EC_3",
        "T_3",
    ]
    if include_raw:
        fieldnames.append("raw_hex")

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for c in records:
            row = {
                "timestamp_iso": c.dt.replace(tzinfo=None).isoformat(sep=" "),
                "timestamp_campbell": c.ts_raw,
                "BattV": c.floats[0],
                "VWC_1": c.floats[1],
                "EC_1": c.floats[2],
                "T_1": c.floats[3],
                "VWC_2": c.floats[4],
                "EC_2": c.floats[5],
                "T_2": c.floats[6],
                "VWC_3": c.floats[7],
                "EC_3": c.floats[8],
                "T_3": c.floats[9],
            }
            if include_raw:
                row["raw_hex"] = c.raw_bytes.hex()
            writer.writerow(row)


# --- CLI ---------------------------------------------------------------------


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch CR206 Table1 records via BD frames and decode them "
            "using the known Campbell layout (>I10f)."
        )
    )

    parser.add_argument(
        "--host",
        required=True,
        help="IPv4/IPv6 address of the CR800 router.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6785,
        help="TCP port of the CR800 BD service (default: 6785).",
    )

    # These are accepted for CLI compatibility with fetch_leaf_records.py,
    # but not currently used by the BD-level approach.
    parser.add_argument("--router", type=int, default=1, help="Router PakBus ID (unused).")
    parser.add_argument("--src", type=int, default=4093, help="Source PakBus ID (unused).")
    parser.add_argument("--leaf", type=int, default=2, help="Leaf PakBus ID (unused).")
    parser.add_argument(
        "--tdf-file",
        help="TDF file for documentation only; not parsed by this script.",
    )
    parser.add_argument(
        "--table",
        default="Table1",
        help="Table name (for logging only; BD request is fixed).",
    )
    parser.add_argument(
        "--collect-mode",
        default="mostrecent",
        help="Collection mode (for logging only; BD request is fixed).",
    )
    parser.add_argument(
        "--num",
        type=int,
        default=10,
        help="Number of most-recent records to write to CSV (after de-dup & sort).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output CSV file.",
    )
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="Include the 44-byte raw hex for each record in the CSV.",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=2023,
        help="Minimum acceptable year for decoded timestamps (default: 2023).",
    )
    parser.add_argument(
        "--max-year",
        type=int,
        default=2030,
        help="Maximum acceptable year for decoded timestamps (default: 2030).",
    )
    parser.add_argument(
        "--debug-hex-out",
        help="Optional path to write the full RX dump as a hex string (for offline analysis).",
    )

    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)

    print(
        f"🌐 fetch_leaf_records_bd: host={args.host} port={args.port} "
        f"router={args.router} src={args.src} leaf={args.leaf} "
        f"table={args.table!r} collect-mode={args.collect_mode} num={args.num}"
    )

    raw = connect_and_fetch_bytes(args.host, args.port)

    if args.debug_hex_out:
        hex_path = Path(args.debug_hex_out)
        hex_path.parent.mkdir(parents=True, exist_ok=True)
        hex_text = raw.hex()
        hex_path.write_text(hex_text)
        print(f"[DEBUG] Wrote RX hex dump to {hex_path} ({len(hex_text)} hex chars).")

    if not raw:
        print("❌ No bytes received from logger; nothing to decode.")
        return

    print(
        f"[INFO] Scanning {len(raw)} bytes for >I10f records "
        f"(Campbell epoch, years {args.min_year}-{args.max_year}) ..."
    )
    candidates = list(
        iter_candidate_records(raw, min_year=args.min_year, max_year=args.max_year)
    )

    if not candidates:
        print(
            "[INFO] No candidate records found. "
            "Use --debug-hex-out and/or decode_hex_candidates.py for deeper analysis."
        )
        # Still write an empty CSV with header so the caller sees *something*.
        write_csv(Path(args.output), [], include_raw=args.include_raw)
        print(f"✅ Done: wrote 0 rows / 0 cols to {args.output}")
        return

    deduped = dedupe_and_sort(candidates)
    selected = deduped[-args.num :] if args.num > 0 else deduped

    print(
        f"[INFO] Found {len(candidates)} raw candidates → "
        f"{len(deduped)} unique records; writing {len(selected)} most-recent rows."
    )

    write_csv(Path(args.output), selected, include_raw=args.include_raw)

    print(
        f"✅ Done: wrote {len(selected)} rows to {args.output} "
        f"(host={args.host}, leaf={args.leaf})"
    )


if __name__ == "__main__":
    main()