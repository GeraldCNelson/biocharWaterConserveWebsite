#!/usr/bin/env python3
"""
pakbus_get_fields.py
Send a PakBus 'Table Data / GetValues' (opcode 0x2C) request using the
field-list style you captured from PC400, and print the reply.

It sends:
  1) hello: BD <pre-hex> CRC CRC BD
  2) read : BD <router-hex> <0x2C> <selector-hex> <pairs...> 00 00 CRC CRC BD

Where <pairs...> are little-endian (table_id, field_id) tuples:
   table_id: 2 bytes LE
   field_id: 2 bytes LE

Example:
  2C 79 00  01 00 FC 0D  01 00 FE 0D  00 00

That matches the short 31-byte request you found:
  bd a0 01 6f fd 10 03 0f fd 09 68 00 00 06 00 02
     2c 79 00 01 00 fc 0d 01 00 fe 0d 00 00
     91 ec bd
"""

import argparse
import binascii
import socket
import struct
import sys
import time
from typing import List, Tuple, Optional

# ---------- utils ----------

def hexdump(b: bytes) -> str:
    return " ".join(f"{x:02x}" for x in b)

def parse_hex_bytes(s: str) -> bytes:
    s = (s or "").strip().replace(" ", "").replace("_", "")
    if len(s) % 2:
        raise ValueError("hex string must have even length")
    return bytes.fromhex(s)

def crc16_modbus(data: bytes) -> int:
    """CRC-16/Modbus (poly 0xA001), init 0xFFFF, reflected, out XOR 0."""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if (crc & 1) != 0:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def bd_wrap(inner: bytes) -> bytes:
    crc = crc16_modbus(inner)
    return bytes([0xBD]) + inner + bytes([(crc >> 8) & 0xFF, crc & 0xFF, 0xBD])

def bd_unwrap(frame: bytes) -> Optional[bytes]:
    if len(frame) < 4 or frame[0] != 0xBD or frame[-1] != 0xBD:
        return None
    inner = frame[1:-3]
    hi, lo = frame[-3], frame[-2]
    calc = crc16_modbus(inner)
    if hi != ((calc >> 8) & 0xFF) or lo != (calc & 0xFF):
        return None
    return inner

def recv_some(sock: socket.socket, timeout: float) -> bytes:
    sock.settimeout(timeout)
    try:
        return sock.recv(65535)
    except socket.timeout:
        return b""

def recv_until_quiet(sock: socket.socket, first_timeout: float, grace_ms: int) -> bytes:
    buf = bytearray()
    chunk = recv_some(sock, first_timeout)
    if chunk:
        buf += chunk
        end = time.time() + grace_ms / 1000.0
        while time.time() < end:
            chunk = recv_some(sock, 0.05)
            if chunk:
                buf += chunk
                end = time.time() + grace_ms / 1000.0
    return bytes(buf)

def split_bd_frames(buf: bytes) -> List[bytes]:
    frames = []
    cur = bytearray()
    in_frame = False
    for b in buf:
        if not in_frame:
            if b == 0xBD:
                cur = bytearray([0xBD])
                in_frame = True
        else:
            cur.append(b)
            if b == 0xBD:
                frames.append(bytes(cur))
                in_frame = False
    return frames

# ---------- request builder ----------

def build_field_list_payload(selector: bytes,
                             pairs: List[Tuple[int, int]]) -> bytes:
    """
    Build: 2C <selector>  <tbl_le, fld_le>...  00 00
    selector is typically b'\x79\x00' in your capture.
    """
    buf = bytearray()
    buf.append(0x2C)               # opcode
    buf += selector
    for tbl, fld in pairs:
        buf += struct.pack("<H", tbl)
        buf += struct.pack("<H", fld)
    buf += b"\x00\x00"             # terminator
    return bytes(buf)

# ---------- main flow ----------

def main():
    ap = argparse.ArgumentParser(description="PakBus 0x2C field-list request (PC400-style).")
    ap.add_argument("--addr", required=True, help="IPv6 address of logger")
    ap.add_argument("--port", type=int, default=6785, help="TCP port (default 6785)")
    ap.add_argument("--pre-hex", default="90 01 0f fd 73 d3",
                    help="hello inner hex (default from your pcap)")
    ap.add_argument("--router-hex", required=True,
                    help="router header hex exactly as in your pcap (no BD/CRC)")
    ap.add_argument("--selector-hex", default="79 00",
                    help="two bytes after 0x2C (default '79 00' from your short request)")
    ap.add_argument("--pairs", required=True,
                    help="comma-separated list like '0001:0dfc,0001:0dfe' (hex or decimal ok)")
    ap.add_argument("--timeout", type=float, default=10.0, help="socket timeout (s)")
    ap.add_argument("--pre-wait-ms", type=int, default=400, help="sleep before hello (ms)")
    ap.add_argument("--gap-ms", type=int, default=150, help="gap between hello and read (ms)")
    ap.add_argument("--post-recv-grace-ms", type=int, default=1200,
                    help="keep reading for this long after bytes arrive (ms)")
    args = ap.parse_args()

    pre = parse_hex_bytes(args.pre_hex)
    router = parse_hex_bytes(args.router_hex)
    selector = parse_hex_bytes(args.selector_hex)
    if len(selector) != 2:
        print("[ERR] selector-hex must be exactly 2 bytes", file=sys.stderr)
        sys.exit(2)

    def parse_pair(tok: str) -> Tuple[int, int]:
        a, b = tok.split(":")
        def to_int(x: str) -> int:
            x = x.strip().lower()
            return int(x, 16) if x.startswith("0x") or any(c in x for c in "abcdef") else int(x)
        return (to_int(a), to_int(b))

    pairs = [parse_pair(t) for t in args.pairs.split(",") if t.strip()]
    if not pairs:
        print("[ERR] no pairs provided", file=sys.stderr)
        sys.exit(2)

    # connect
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(args.timeout)
    print(f"[INFO] Connecting to [{args.addr}]:{args.port} …")
    s.connect((args.addr, args.port))
    print("[OK] TCP connected.")

    # hello
    if args.pre_wait_ms > 0:
        time.sleep(args.pre_wait_ms / 1000.0)
    hello = bd_wrap(pre)
    print(f"[TX] hello {len(hello)}B: {hexdump(hello)}")
    s.sendall(hello)

    hraw = recv_until_quiet(s, args.timeout, args.post_recv_grace_ms)
    hframes = split_bd_frames(hraw)
    print(f"[RX] hello-reply bytes={len(hraw)}, frames={len(hframes)}")
    if hframes:
        print(f"[RX] hello[0] {len(hframes[0])}B: {hexdump(hframes[0])}")

    # small gap
    if args.gap_ms > 0:
        time.sleep(args.gap_ms / 1000.0)

    # build and send read
    payload = build_field_list_payload(selector, pairs)
    inner = router + payload
    frame = bd_wrap(inner)
    print(f"[TX] read {len(frame)}B: {hexdump(frame)}")
    s.sendall(frame)

    # receive reply
    rraw = recv_until_quiet(s, args.timeout, args.post_recv_grace_ms)
    print(f"[RX] read-reply raw-bytes={len(rraw)}")
    rframes = split_bd_frames(rraw)
    if not rframes:
        print("[WARN] No BD frames in reply.")
        sys.exit(3)

    for i, fr in enumerate(rframes):
        inner_fr = bd_unwrap(fr)
        print(f"[RX] frame[{i}] {len(fr)}B: {hexdump(fr)}")
        if inner_fr is None:
            print("     (CRC mismatch)")
            continue
        # try to strip router header if present
        if inner_fr.startswith(router):
            app = inner_fr[len(router):]
            print(f"     inner {len(inner_fr)}B, app {len(app)}B: {hexdump(app)}")
        else:
            print(f"     inner {len(inner_fr)}B: {hexdump(inner_fr)}")

    s.close()

if __name__ == "__main__":
    main()