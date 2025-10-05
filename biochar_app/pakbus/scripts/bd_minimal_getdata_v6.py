#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bd_minimal_getdata_v6.py

Goal: send a realistic Link-State (0x20, subtype 0x03) that mirrors the logger's
beacon you captured, then immediately issue a minimal GetData (0x09) for table 0x11.

This script:
- Optional short hello (EF FF ... DD F0).
- Link-State broadcast: AF FD <dest=00> <src=ours> 1F FD | 20 03 89 <payload> | <crc> BD
  The payload mirrors fields seen in the logger's own 0x20 03 89 bursts, but with
  the addresses flipped so we announce ourselves.
- Then GetData: FD 09 <table> 00 00 <start-key(8B LE)> <count(2B LE)>

CLI knobs:
  --crc {ibm,ccitt}    : CRC flavor
  --hello              : send short hello first
  --our-id             : our PakBus ID to advertise (default 0x0FFE)
  --txid-max           : try TXIDs 1..N
  --link-magic         : 2 bytes; leave default unless we need to tweak
  --addr/--port        : IPv6/port of CR800
  --count              : rows to request
"""

import argparse
import socket
import struct
from typing import Tuple

def crc_ibm(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def crc_ccitt(data: bytes) -> int:
    # X25/CCITT (0x8408) reflected, init 0xFFFF
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0x8408
            else:
                crc >>= 1
    return crc & 0xFFFF

def add_crc_and_wrap(payload: bytes, use_ccitt: bool) -> bytes:
    c = crc_ccitt(payload) if use_ccitt else crc_ibm(payload)
    return payload + struct.pack("<H", c) + b"\xbd"

def pkt_header(dest_id: int, src_id: int, control: int) -> bytes:
    # AF FD <dest><src> <control> FD  (per your captures)
    return bytes([0xBD, 0xAF, 0xFD, dest_id & 0xFF, src_id & 0xFF, control & 0xFF, 0xFD])

def short_hello() -> bytes:
    # EF FF 10 01 0F FF 00 01 0E 00 DD F0
    core = bytes([0xBD, 0xEF, 0xFF, 0x10, 0x01, 0x0F, 0xFF, 0x00, 0x01, 0x0E, 0x00, 0xDD, 0xF0])
    # This short hello in your traces did not include an extra CRC (already framed),
    # but we’ll still add a trailing BD if missing, to keep our TX lines clear.
    return core if core.endswith(b"\xbd") else core + b"\xbd"

def build_link_state_broadcast(our_id: int, txid: int, link_magic: int) -> bytes:
    """
    Build a Link State: 0x20 0x03 0x89 <payload...>
    We mirror your captured beacon payload structure enough to look valid.

    Wire header we send: BD AF FD <dest=00> <src=our_id> <control=0x1F> FD
    Payload:
      20 03 89
      <txid>
      00 00          ; hop or flags (kept from capture shape)
      02 00          ; max routes? (kept)
      01             ; one neighbor (us)
      0F 76          ; link metric-ish  (kept constant seen often)
      00 01 43       ; small trailer slice seen at end of many beacons
    If device is picky, only *shape* needs to be right to refresh neighbor table.
    """
    dest = 0x00                        # broadcast
    src  = our_id & 0xFF
    control = 0x1F

    hdr = pkt_header(dest, src, control)
    # Keep close to the observed field order you pasted:
    body = bytes([
        0x20, 0x03, 0x89,
        txid & 0xFF,
        0x00, 0x00,
        0x02, 0x00,
        0x01,
        0x0F, 0x76,
        (link_magic >> 8) & 0xFF, link_magic & 0xFF,  # 2B tweakable trailer
        0x43
    ])
    return hdr + body

def build_getdata(table: int, count: int, txid: int) -> bytes:
    """
    GetData payload (little-endian):
      FD 09 <table> 00 00 <start-key 8B LE> <count 2B LE>
    We use start-key = 0 (beg of ring); logger should jump to its earliest retained.
    We’ll send with dest=1 (logger), src=our_id, control=0x1F.
    """
    dest = 0x01
    control = 0x1F
    # NOTE: src on header is our_id; set by caller per txid sweep
    payload = bytes([
        0xFD, 0x09,
        table & 0xFF,
        0x00, 0x00,                     # flags
        0x00, 0x00, 0x00, 0x00,         # start-key: secs-since-1990 (LE)
        0x00, 0x00, 0x00, 0x00,         # start-key: record (LE)
    ]) + struct.pack("<H", count & 0xFFFF)
    return dest, control, payload

def hexdump(b: bytes) -> str:
    return b.hex()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--table", required=True, help="e.g. 0x11", type=lambda x: int(x, 0))
    ap.add_argument("--count", type=int, default=5)
    ap.add_argument("--crc", choices=["ibm","ccitt"], default="ibm")
    ap.add_argument("--hello", action="store_true")
    ap.add_argument("--hello-gap-ms", type=int, default=300)
    ap.add_argument("--txid-max", type=int, default=10)
    ap.add_argument("--our-id", type=lambda x: int(x,0), default=0x0FFE)
    ap.add_argument("--link-magic", type=lambda x: int(x,0), default=0x0143)  # tweakable 2B tail
    ap.add_argument("--connect-timeout", type=float, default=8.0)
    ap.add_argument("--idle-timeout", type=float, default=1.2)
    args = ap.parse_args()

    use_ccitt = (args.crc == "ccitt")

    def send_once(sock: socket.socket, blob: bytes, tag: str):
        print(f"[TX {tag}] {len(blob)}B: {hexdump(blob)}")
        sock.sendall(blob)
        sock.settimeout(args.idle_timeout)
        try:
            rx = sock.recv(4096)
        except socket.timeout:
            rx = b""
        print(f"[RX {tag}] {len(rx)}B: {hexdump(rx) if rx else '(none)'}")
        return rx

    for txid in range(1, args.txid_max+1):
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.settimeout(args.connect_timeout)
            s.connect((args.addr, args.port, 0, 0))

            if args.hello:
                h = short_hello()
                print(f"\n=== TXID {txid:02d} : HELLO ===")
                send_once(s, h, "hello")
                # small pause
                s.settimeout(args.idle_timeout)
                try:
                    _ = s.recv(4096)
                except socket.timeout:
                    pass

            # 1) Link-State broadcast (announce ourselves)
            print(f"\n=== TXID {txid:02d} : LINK-STATE ===")
            ls_core = build_link_state_broadcast(args.our_id, txid, args.link_magic)
            ls = add_crc_and_wrap(ls_core, use_ccitt)
            send_once(s, ls, "link")

            # 2) GetData (dest=1, src=our_id)
            print(f"\n=== TXID {txid:02d} : GETDATA ===")
            dest, control, gd_payload = build_getdata(args.table, args.count, txid)
            hdr = pkt_header(dest, args.our_id & 0xFF, control)
            gd = add_crc_and_wrap(hdr + gd_payload, use_ccitt)
            send_once(s, gd, "get")

if __name__ == "__main__":
    main()