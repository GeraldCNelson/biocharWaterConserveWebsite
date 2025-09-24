#!/usr/bin/env python3
"""
fetch_table1_live.py

Fetches Table1 data from a CR200X leaf over PakBus (TCP/UDP) and writes the result to a CSV file.

This script reads a template definition file (pc400_table1_templates.csv), constructs a Table1
request, sends it to the specified leaf, and parses the 0x89 response.  By default it only fetches
the most recent record.

Usage (with defaults):
  python fetch_table1_live.py --host <leaf-ip>
Optional overrides:
  --port       PakBus port (default 6785)
  --template   Path to template CSV (default biochar_app/pakbus/data/pc400_table1_templates.csv)
  --count      Number of records to fetch (default 1)
  --start-rec  Starting record index (default 0xFFFF)
  --output     Output CSV (default table1_live_fetch.csv in current directory)
"""

import argparse
import csv
import datetime
import os
import socket
import struct
from pathlib import Path

# ---------------------------------------------------------------------------
# Helper functions for CRC and float conversion
# ---------------------------------------------------------------------------

def crc_ibm(data: bytes) -> int:
    """Compute IBM CRC (used by PakBus 0x89 responses)."""
    crc = 0
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def float32_be(buf: bytes, offset: int) -> float | None:
    """Return a big‑endian IEEE754 float32 from buf[offset:offset+4], or None on error."""
    try:
        return struct.unpack(">f", buf[offset:offset+4])[0]
    except Exception:
        return None

def uint32_be(buf: bytes, offset: int) -> int | None:
    """Return an unsigned 32‑bit big‑endian integer from buf[offset:offset+4], or None on error."""
    if offset + 4 > len(buf):
        return None
    return int.from_bytes(buf[offset:offset+4], "big")

# ---------------------------------------------------------------------------
# Template handling
# ---------------------------------------------------------------------------

def read_template(template_path: Path) -> list[str]:
    """Read the template CSV and return a list of operation payload bytes."""
    payloads = []
    with template_path.open("r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            # Each row is expected to contain hex bytes like 'A0 01 6F ...'
            hexstr = row[0]
            if not hexstr:
                continue
            payload_bytes = bytes(int(x, 16) for x in hexstr.strip().split())
            # Ensure the payload contains op code 0x09 at index 8 (opIndex)
            op_index = payload_bytes.find(b"\x09")
            if op_index == -1:
                continue
            payloads.append(payload_bytes)
    return payloads

# ---------------------------------------------------------------------------
# Request and response handling
# ---------------------------------------------------------------------------

def build_request(template_payload: bytes, count: int, start_rec: int) -> bytes:
    """Insert the start_rec and count into the template request."""
    # The Table1 opcode is at index 8; transaction ID is at index 9 and must be nonzero.
    # We choose 0x80 as a fixed transaction ID so it doesn’t conflict with the template.
    op_index = template_payload.index(0x09)
    tran_index = op_index + 1
    request = bytearray(template_payload)

    # Set transaction ID (byte at tran_index) to 0x80 and record start & count.
    request[tran_index] = 0x80
    # The start record is 16‑bit BE at bytes [tran_index+2 : tran_index+4].
    request[tran_index+2:tran_index+4] = start_rec.to_bytes(2, "big")
    # Count (number of records) is 16‑bit BE at bytes [tran_index+4 : tran_index+6].
    request[tran_index+4:tran_index+6] = count.to_bytes(2, "big")
    return bytes(request)

def send_request(host: str, port: int, frame: bytes, timeout: float = 10.0) -> bytes | None:
    """Send the frame to the leaf over TCP and return the first complete BD‑framed reply."""
    # PakBus frames use 0xBD as start/stop markers.  We collect frames until we see
    # a matching 0x89 response (reply to 0x09 request).
    def deframe(buf: bytes) -> list[bytes]:
        """Return all BD‑framed packets from buf (no CRC check here)."""
        packets = []
        start = 0
        while True:
            try:
                s = buf.index(0xBD, start)
                e = buf.index(0xBD, s + 1)
                packets.append(buf[s:e+1])
                start = e + 1
            except ValueError:
                break
        return packets

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        sock.sendall(frame)
        # Collect data for a while
        buf = bytearray()
        start_time = datetime.datetime.now()
        while (datetime.datetime.now() - start_time).total_seconds() < timeout:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                buf.extend(data)
                for pkt in deframe(buf):
                    if 0x89 in pkt:
                        return pkt
            except socket.timeout:
                break
    finally:
        sock.close()
    return None

def parse_89_response(frame: bytes, col_names: list[str], count: int) -> list[dict]:
    """
    Parse a BD‑framed 0x89 response.  Returns a list of records with timestamp and float values.
    """
    # Strip BD markers and CRC
    if not (frame.startswith(b"\xBD") and frame.endswith(b"\xBD")):
        raise ValueError("Frame does not start and end with 0xBD")
    payload = frame[1:-1]
    # CRC is last two bytes
    if crc_ibm(payload[:-2]) != int.from_bytes(payload[-2:], "big"):
        raise ValueError("CRC mismatch in 0x89 response")

    # Look for the 0x89 opcode
    idx = payload.index(0x89)
    # After 0x89: [tran][rc][data...]
    rc = payload[idx+2]
    if rc != 0:
        raise ValueError(f"Nonzero RC in 0x89 response: {rc}")
    data = payload[idx+3:]
    records = []
    offset = 0
    for _ in range(count):
        if offset + 4 + 4*len(col_names) > len(data):
            break
        secs = uint32_be(data, offset)
        offset += 4
        if secs is None:
            break
        epoch1990 = datetime.datetime(1990, 1, 1)
        ts = epoch1990 + datetime.timedelta(seconds=secs)
        record = {"Timestamp": ts.strftime("%Y-%m-%d %H:%M:%S")}
        for name in col_names:
            v = float32_be(data, offset)
            offset += 4
            record[name] = round(float(v), 6) if v is not None else None
        records.append(record)
    return records

def write_csv(path: Path, records: list[dict]) -> None:
    """Write the list of record dictionaries to CSV."""
    if not records:
        return
    with path.open("w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=records[0].keys())
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)

# ---------------------------------------------------------------------------
# Main program
# ---------------------------------------------------------------------------

def main() -> None:
    # Determine default template path relative to this script
    script_dir = Path(__file__).resolve().parent
    default_template = script_dir.parent / "data" / "pc400_table1_templates.csv"
    parser = argparse.ArgumentParser(
        description="Fetch Table1 data from a CR200X leaf over PakBus."
    )
    parser.add_argument(
        "--host", required=True,
        help="Leaf IP address (required, as there’s no reasonable default)."
    )
    parser.add_argument(
        "--port", type=int, default=6785,
        help="PakBus TCP port (default: 6785)."
    )
    parser.add_argument(
        "--template", type=Path, default=default_template,
        help=f"CSV template file (default: {default_template})."
    )
    parser.add_argument(
        "--count", type=int, default=1,
        help="Number of records to fetch (default: 1)."
    )
    parser.add_argument(
        "--start-rec", type=lambda x: int(x, 0), default=0xFFFF,
        help="Start record index (hex or decimal, default: 0xFFFF)."
    )
    parser.add_argument(
        "--output", type=Path, default=Path("table1_live_fetch.csv"),
        help="Output CSV file (default: table1_live_fetch.csv)."
    )
    args = parser.parse_args()

    # Load template and choose first payload (there may be multiple identical rows)
    template_payloads = read_template(args.template)
    if not template_payloads:
        raise SystemExit("No valid 0x09 templates found in template file.")
    payload = template_payloads[0]

    # Determine column names for Table1 from the template header or define manually
    # In your case: BattV_Min, VWC_1_Avg, EC_1_Avg, T_1_Avg, VWC_2_Avg, EC_2_Avg, T_2_Avg, VWC_3_Avg, EC_3_Avg, T_3_Avg
    col_names = [
        "BattV_Min", "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
        "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
        "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"
    ]

    # Build request frame and send
    frame = build_request(payload, args.count, args.start_rec)
    # Prepend and append 0xBD markers
    frame = b"\xBD" + frame + b"\xBD"
    response = send_request(args.host, args.port, frame)
    if response is None:
        raise SystemExit("No 0x89 response received from leaf.")

    records = parse_89_response(response, col_names, args.count)
    if not records:
        raise SystemExit("No records parsed from 0x89 response.")

    # Write CSV
    write_csv(args.output, records)
    print(f"Wrote {len(records)} record(s) to {args.output}")

if __name__ == "__main__":
    main()