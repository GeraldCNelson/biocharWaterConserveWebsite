#!/usr/bin/env python3
"""
Fetch the last N records from a Table1 leaf via the base datalogger and print them.
Uses defaults from config.py in biochar_app/scripts.
"""

import socket
import struct
import csv
import importlib.util
from pathlib import Path
from datetime import datetime, timedelta, timezone

# -- Load config.py from biochar_app/scripts using importlib
CONFIG_PATH = Path(__file__).resolve().parents[2] / "scripts" / "config.py"
spec = importlib.util.spec_from_file_location("biochar_config", CONFIG_PATH)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)

# Use PAKBUS defaults from config
HOST = config.PAKBUS.host
PORT = config.PAKBUS.port
BASE_ID = config.PAKBUS.base_id
LOGGER_IDS = config.PAKBUS.logger_ids
DEFAULT_TABLE = config.DEFAULT_TABLE  # "Table1"
DEFAULT_COUNT = 10                    # fetch last 10 records
DEFAULT_START_REC = 0xFFFF            # start at newest (logger will clamp)
TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "data" / "pc400_table1_templates.csv"

def read_templates(path: Path) -> list[bytes]:
    """
    Read the pc400_table1_templates.csv and return list of payloads (as bytes).
    Assumes 'PayloadHex' column with space-separated hex bytes.
    """
    payloads = []
    with path.open(newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # row['PayloadHex'] should contain hex bytes separated by spaces
            hex_vals = row['PayloadHex'].split()
            payload = bytes(int(h, 16) for h in hex_vals)
            payloads.append(payload)
    return payloads

def compute_crc(data: bytes) -> int:
    """
    Compute the 16‑bit CRC for the PAKBUS frame (IBM CRC).
    """
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = (crc << 1) ^ 0x1021 if (crc & 0x8000) else crc << 1
            crc &= 0xFFFF
    return crc

def build_request(payload: bytes, tran_byte: int, start_rec: int, count: int) -> bytes:
    """
    Update the template payload with the new transaction ID, start record and count.
    Recompute CRC and wrap with frame markers (0xBD + <length> + payload + CRC).
    """
    # Copy the payload into mutable bytearray
    payload_mut = bytearray(payload)
    op_index = None
    tran_index = None
    # Find the 0x09 op code and transaction index (should be known from template)
    for i, b in enumerate(payload_mut):
        if b == 0x09:
            op_index = i
            tran_index = i + 1
            break
    if op_index is None:
        raise ValueError("Operation code 0x09 not found in template.")
    # Set new transaction id
    payload_mut[tran_index] = tran_byte
    # Set new start record and count (big endian)
    # Expect start_rec and count fields after table id; here offsets may vary by template
    # This code assumes start_rec at payload offset op_index + 6 and count at +8.
    # Adjust as necessary for your template.
    start_off = op_index + 6
    count_off = op_index + 8
    payload_mut[start_off:start_off+2] = start_rec.to_bytes(2, "big")
    payload_mut[count_off:count_off+2] = count.to_bytes(2, "big")
    # Compute CRC over 0x10 and following
    crc = compute_crc(payload_mut[1:])  # skip 0xA0 (start frame)
    # Wrap frame markers
    frame = b"\xBD" + payload_mut + crc.to_bytes(2, "big") + b"\x0D"
    return frame

def parse_0x89_data(frame: bytes) -> tuple[datetime, list[float]]:
    """
    Parse 0x89 response data: epoch seconds (uint32 big‑endian) + 10 floats (BE).
    Returns timestamp (UTC) and list of 10 floats.
    """
    idx = frame.find(b"\x89")
    if idx < 0 or idx + 3 >= len(frame):
        raise ValueError("0x89 header not found or incomplete.")
    # Data starts after 0x89, tran, rc
    data_start = idx + 3
    ts_raw = struct.unpack_from(">I", frame, data_start)[0]
    dt = datetime(1990, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=ts_raw)
    values = list(struct.unpack_from(">10f", frame, data_start+4))
    return dt, values

def fetch_latest_records():
    # Load template payload (use first row)
    payloads = read_templates(TEMPLATE_PATH)
    template = payloads[0]
    # Choose a transaction id (0x80..0xFF)
    tran_byte = 0x80
    request = build_request(template, tran_byte, DEFAULT_START_REC, DEFAULT_COUNT)
    # Connect to base logger
    with socket.create_connection((HOST, PORT), timeout=10) as sock:
        # Send the frame
        sock.sendall(request)
        # Collect response bytes
        sock.settimeout(5)
        data = sock.recv(4096)
    # Deframe and parse (only first frame expected)
    dt, values = parse_0x89_data(data)
    # Print results
    print(f"Timestamp: {dt.isoformat()}")
    # Map values to column names from config
    colnames = config.VALUE_COLS_STANDARD if config.DEFAULT_YEAR < 2024 else config.VALUE_COLS_2024_PLUS
    for name, val in zip(colnames, values):
        print(f"{name}: {val:.5f}")

if __name__ == "__main__":
    fetch_latest_records()