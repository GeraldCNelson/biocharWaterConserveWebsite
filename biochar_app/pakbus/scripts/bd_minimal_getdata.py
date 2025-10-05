#!/usr/bin/env python3
import socket
import argparse
import struct
import binascii

def crc_ibm(data: bytes) -> int:
    """IBM CRC-16, poly 0x8005, initial 0x0000"""
    crc = 0x0000
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x8005
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc

def crc_ccitt(data: bytes) -> int:
    """CCITT CRC-16, poly 0x1021, initial 0xFFFF"""
    crc = 0xFFFF
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc

def make_getdata_frame(table_id: int, count: int, crc_mode: str) -> bytes:
    """
    Construct a minimal PakBus frame with GetData request for table_id.
    For now we hardcode src/dest addresses (0x01), transaction, etc.
    """
    # PakBus frame skeleton (simplified for CR800)
    # bd <payload> crc bd

    # Simple header: dest=1, src=1, control byte 0x9f (transaction, prio)
    header = bytes([0xAF, 0xFD, 0x01, 0x01])

    # Service: GetData = 0x09
    # Format: FD 09 <tableID> 00 00 <start-key-secs 4B> <start-key-rec 2B> <count 2B>
    start_secs = 0  # request from beginning
    start_rec  = 0
    payload = bytes([
        0xFD, 0x09, table_id, 0x00, 0x00
    ])
    payload += struct.pack("<I", start_secs)
    payload += struct.pack("<H", start_rec)
    payload += struct.pack("<H", count)

    core = header + payload

    if crc_mode == "ibm":
        crc_val = crc_ibm(core)
    else:
        crc_val = crc_ccitt(core)
    crc_bytes = struct.pack("<H", crc_val)

    frame = b"\xbd" + core + crc_bytes + b"\xbd"
    return frame

def main():
    ap = argparse.ArgumentParser(description="Minimal PakBus GetData test")
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--table", type=lambda x: int(x,0), default=0x11)
    ap.add_argument("--count", type=int, default=5)
    ap.add_argument("--crc", choices=["ibm","ccitt"], default="ibm")
    args = ap.parse_args()

    frame = make_getdata_frame(args.table, args.count, args.crc)
    print(f"[TX] {len(frame)} bytes: {binascii.hexlify(frame).decode()}")

    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(5)
        s.connect((args.addr, args.port, 0, 0))
        s.sendall(frame)
        try:
            rx = s.recv(4096)
            print(f"[RX] {len(rx)} bytes: {binascii.hexlify(rx).decode()}")
        except socket.timeout:
            print("[RX] timeout, no data")

if __name__ == "__main__":
    main()