#!/usr/bin/env python3
"""
bd_minimal_getdata_v5.py

Minimal focused probe that tries multiple transaction IDs (txid) when issuing
a small network shim (FD 20 03 <txid>) followed by a GetData (FD 09 ...) frame.

Usage example:
  python bd_minimal_getdata_v5.py \
    --addr 2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd \
    --port 6785 \
    --table 0x11 \
    --count 5 \
    --txid-max 5 \
    --crc ibm \
    --hello

Notes:
 - This is a probing tool: it prints the TX bytes and any RX bytes returned.
 - Framing and CRC choices were chosen to match the observed captures:
   leading 0xBD, core payload, 2-byte CRC (little-endian), trailing 0xBD.
 - If no response, try increasing --txid-max, toggling --crc, or enabling/disabling --hello.
"""

import argparse
import socket
import time
import struct
from typing import Tuple

# ---- CRC helpers -----------------------------------------------------------

def crc16_ibm(data: bytes) -> int:
    """CRC-16-IBM / CRC-16-ARC (poly=0x8005, init=0x0000), returns 16-bit int."""
    crc = 0x0000
    for b in data:
        crc ^= b
        for _ in range(8):
            if (crc & 0x0001):
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def crc16_ccitt(data: bytes, init: int = 0xFFFF) -> int:
    """CRC-16-CCITT (poly=0x1021), returns 16-bit int."""
    crc = init & 0xFFFF
    for b in data:
        crc ^= (b << 8)
        for _ in range(8):
            if (crc & 0x8000):
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc & 0xFFFF

# ---- Framing helpers -------------------------------------------------------

def wrap_frame(core: bytes, crc_kind: str) -> bytes:
    """
    Wrap core payload with leading 0xBD, append CRC (little-endian), append trailing 0xBD.
    This matches the pattern seen in your captures (bd ... <2 byte crc> bd).
    """
    if crc_kind == "ibm":
        c = crc16_ibm(core)
    else:
        c = crc16_ccitt(core)
    # CRC little-endian bytes (as observed in captured frames)
    crc_bytes = struct.pack("<H", c)
    return bytes([0xBD]) + core + crc_bytes + bytes([0xBD])

def hexify(b: bytes) -> str:
    return b.hex()

# ---- Small helpers for FD shim / GetData payload construction ---------------

def build_shim_core(txid: int) -> bytes:
    """
    Build a small shim payload whose core will contain the 'FD 20 03 <txid>' service
    and a minimal prefix that resembles observed frames.

    Observed: 'af fd 00 01 1f fd 20 03 89 ...' etc.
    We use a compact / plausible prefix: AF FD 00 01 1F FD 20 03 <txid>
    (the AF/FD prefix appears frequently in your captures).
    """
    # AF FD 00 01 1F FD 20 03 <txid>
    return bytes([0xAF, 0xFD, 0x00, 0x01, 0x1F, 0xFD, 0x20, 0x03, txid & 0xFF])

def build_getdata_core(table_byte: int, start_key: int = 0, count: int = 1) -> bytes:
    """
    Build a GetData core payload with FD 09 and the table id and a small
    start-key / count area. Observed layout (approximate):
      AF FD 00 01 1F FD 09 <table> <8-byte start key> <2-byte count> ...
    We'll follow that pattern.

    start_key is 64-bit seconds-since-1990-like field; we will pack as big-endian.
    count will be 2 bytes big-endian (observed '0005' in previous captures).
    """
    prefix = bytes([0xAF, 0xFD, 0x00, 0x01, 0x1F, 0xFD, 0x09])
    t = bytes([table_byte & 0xFF])
    # pack start_key as 8-byte big-endian (most of your seed frames had zeros here)
    sk = struct.pack(">Q", start_key)
    # pack count as 2-byte big-endian
    cnt = struct.pack(">H", count)
    # some earlier frames had two bytes (maybe flags) prior to count; keep compact
    return prefix + t + sk + cnt

# ---- Networking / send-receive ---------------------------------------------

def send_and_recv(addr: str, port: int, payload: bytes, connect_timeout: float, idle_timeout: float) -> Tuple[bytes, int]:
    """
    Send UDP payload to IPv6 address and wait for replies for up to idle_timeout.
    Returns (accumulated_rx_bytes, total_bytes_received).
    """
    rx_accum = bytearray()
    total = 0
    sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    sock.settimeout(connect_timeout)
    try:
        # Connect sets default destination so send() can be used
        sock.connect((addr, port, 0, 0))
    except Exception as e:
        sock.close()
        raise

    # Send
    sock.send(payload)

    # Then poll for responses for idle_timeout seconds (short bursts)
    deadline = time.time() + idle_timeout
    sock.settimeout(max(0.1, idle_timeout))
    while time.time() < deadline:
        try:
            data = sock.recv(4096)
            if not data:
                break
            rx_accum.extend(data)
            total += len(data)
            # keep listening until idle_timeout is reached
        except socket.timeout:
            break
        except Exception:
            break

    sock.close()
    return bytes(rx_accum), total

# ---- Main probe routine ---------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Minimal BD GetData probe v5 (shim txid sweep).")
    ap.add_argument("--addr", required=True, help="IPv6 address of base station")
    ap.add_argument("--port", required=True, type=int, help="UDP port (e.g. 6785)")
    ap.add_argument("--table", required=True, type=lambda x: int(x,0), help="Table id (e.g. 0x11)")
    ap.add_argument("--count", type=int, default=5, help="Record count to request")
    ap.add_argument("--start-key", type=int, default=0, help="Start key (seconds since 1990 or 0 to let logger pick)")
    ap.add_argument("--txid-max", type=int, default=5, help="Maximum txid to try (1..N)")
    ap.add_argument("--crc", choices=["ibm","ccitt"], default="ibm", help="CRC flavor")
    ap.add_argument("--hello", action="store_true", help="Send a short hello before probing (one shot)")
    ap.add_argument("--hello-gap-ms", type=int, default=200, help="ms gap after hello")
    ap.add_argument("--connect-timeout", type=float, default=3.0, help="Socket connect timeout (seconds)")
    ap.add_argument("--idle-timeout", type=float, default=0.8, help="How long to wait for replies after each TX (seconds)")
    ap.add_argument("--shim-only", action="store_true", help="Only send shim (no GetData) - debug")
    args = ap.parse_args()

    addr = args.addr
    port = args.port
    table = args.table & 0xFF
    count = args.count

    if args.hello:
        # a short hello we observed as 'ef ff 10 01 0f ff 00 01 0e 00 dd f0'
        hello_core = bytes.fromhex("ef ff 10 01 0f ff 00 01 0e 00 dd f0")
        hello_frame = wrap_frame(hello_core, args.crc)
        print(f"[HELLO] TX {len(hello_frame)}B: {hexify(hello_frame)}")
        try:
            rx, n = send_and_recv(addr, port, hello_frame, args.connect_timeout, args.idle_timeout)
            if n:
                print(f"[HELLO] RX {n}B: {hexify(rx)}")
            else:
                print(f"[HELLO] RX 0B")
        except Exception as e:
            print(f"[HELLO] socket/send error: {e}")
        time.sleep(args.hello_gap_ms / 1000.0)


    # Sweep txid values
    for txid in range(1, args.txid_max + 1):
        shim_core = build_shim_core(txid)
        shim_frame = wrap_frame(shim_core, args.crc)

        print(f"\n=== TXID {txid:02d} : SHIM ===")
        print(f"[TX shim] {len(shim_frame)}B: {hexify(shim_frame)}")
        try:
            rx_shim, n_shim = send_and_recv(addr, port, shim_frame, args.connect_timeout, args.idle_timeout)
            print(f"[RX shim] {n_shim}B: {hexify(rx_shim) if n_shim else '(none)'}")
        except Exception as e:
            print(f"[ERR] shim send/recv error: {e}")
            continue

        if args.shim_only:
            continue

        get_core = build_getdata_core(table, start_key=args.start_key, count=count)
        # In some captures the getdata core used the same AF/FD prefix as shim. We build similarly.
        get_frame = wrap_frame(get_core, args.crc)

        print(f"[TX getdata] {len(get_frame)}B: {hexify(get_frame)}")
        try:
            rx_get, n_get = send_and_recv(addr, port, get_frame, args.connect_timeout, args.idle_timeout)
            print(f"[RX getdata] {n_get}B: {hexify(rx_get) if n_get else '(none)'}")
        except Exception as e:
            print(f"[ERR] getdata send/recv error: {e}")

        # small gap before the next txid attempt so logger (or network) can recover
        time.sleep(0.12)

    print("\n[DONE] Probe finished. Increase --txid-max or toggle --crc/--hello if needed.")

if __name__ == "__main__":
    main()