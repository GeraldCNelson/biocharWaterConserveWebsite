#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bd_replay_raw_v2.py — send one or more PakBus GetData frames (either exact hex,
or built from human-readable fields) and read replies.

Two ways to build a frame:
  A) --hex "bd....bd"
  B) Builder args:
     - LEAF MODE (go through router to a leaf):
         --host-id --router-id --leaf-id --table-code --count --start-key
        Payload order (matches your pcap working case):
          09 [table_code] [count] [leaf_id] FF FF [key_hi] [key_lo]
     - ROUTER-ONLY MODE (no --leaf-id, table at router):
         --host-id --router-id --table-code --table-index --count --start-key
        Payload order:
          09 [table_code] [table_index] [count] FF FF [key_hi] [key_lo]

Notes
- host-id default 0x0FFD (PC400 global), router-id default 0x01 (CR800)
- --hello is optional; we saw CR800 echo hello back.
"""

import argparse
import binascii
import socket
import sys
import time

# ---------- Helpers -----------------------------------------------------------

def u8(x: int) -> bytes:
    if not (0 <= x <= 0xFF):
        raise ValueError(f"value {x} out of 0..255")
    return bytes([x])

def parse_hex_string(s: str) -> bytes:
    s = s.strip().lower().replace(" ", "")
    if s.startswith("0x"):
        s = s[2:]
    return binascii.unhexlify(s)

def parse_hex_or_int(s: str) -> int:
    s = str(s)
    return int(s, 16) if s.lower().startswith("0x") else int(s, 10)

def parse_key_ffffxxxx(s: str) -> int:
    ss = s.strip()
    if ss.lower().startswith("0x"):
        val = int(ss, 16)
    else:
        val = int(ss, 16)  # allow "FFFF2810"
    return val & 0xFFFF

def build_getdata_payload_leaf(table_code: int, count: int, leaf_id: int, key_lo16: int) -> bytes:
    # 09 [table] [count] [leaf] FF FF [key_hi] [key_lo]
    return (b"\x09" + u8(table_code) + u8(count) + u8(leaf_id) +
            b"\xFF\xFF" + u8((key_lo16 >> 8) & 0xFF) + u8(key_lo16 & 0xFF))

def build_getdata_payload_router(table_code: int, table_index: int, count: int, key_lo16: int) -> bytes:
    # 09 [table] [index] [count] FF FF [key_hi] [key_lo]
    return (b"\x09" + u8(table_code) + u8(table_index) + u8(count) +
            b"\xFF\xFF" + u8((key_lo16 >> 8) & 0xFF) + u8(key_lo16 & 0xFF))

def build_outer_frame(host_id: int, router_id: int, payload: bytes) -> bytes:
    # BD [HOST] [ROUTER] 6F FD 00 [ROUTER] 0F FD <payload> BD
    return (b"\xBD" + u8(host_id) + u8(router_id) +
            b"\x6F\xFD\x00" + u8(router_id) + b"\x0F\xFD" +
            payload + b"\xBD")

def classify(pkt: bytes) -> str:
    if len(pkt) == 14 and pkt.startswith(b"\xBD\xEF\xFF\x10"):
        return "hello"
    if b"\x89\x05" in pkt:
        return "reply-89"
    if b"\x70" in pkt[:48]:
        return "neighbor-70"
    if b"\xAF\xFD" in pkt and b"\x10\x01\x0F\xFF" not in pkt:
        return "data-ish"
    return "other"

# ---------- Main --------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", required=True, type=int)

    # Builder args
    ap.add_argument("--host-id", default="0x0FFD")
    ap.add_argument("--router-id", default="0x01")
    ap.add_argument("--leaf-id")          # optional; if omitted => router-only mode
    ap.add_argument("--table-code")
    ap.add_argument("--table-index")      # only used in router-only mode; default 1
    ap.add_argument("--count")
    ap.add_argument("--start-key")

    # Raw hex override
    ap.add_argument("--hex")

    # runtime knobs
    ap.add_argument("--hello", action="store_true")
    ap.add_argument("--hello-gap-ms", type=int, default=150)
    ap.add_argument("--fanout-ctl-shim", action="store_true",
                    help="try ctl {0x0F,0x1F} × shim {off,on} automatically")
    ap.add_argument("--connect-timeout", type=float, default=6.0)
    ap.add_argument("--idle-timeout", type=float, default=6.0)
    ap.add_argument("--reads-per-tx", type=int, default=12)
    ap.add_argument("--read-gap-ms", type=int, default=400)
    ap.add_argument("--tx-gap-ms", type=int, default=400)
    ap.add_argument("--rx-limit", type=int, default=2048)

    args = ap.parse_args()

    # Build frames list
    frames = []
    descriptions = []

    if args.hex:
        tx = parse_hex_string(args.hex)
        frames.append(tx)
        descriptions.append("raw-hex")
    else:
        # Common fields
        host_id   = parse_hex_or_int(args.host_id)   & 0xFF
        router_id = parse_hex_or_int(args.router_id) & 0xFF

        table = parse_hex_or_int(args.table_code) if args.table_code else None
        count = parse_hex_or_int(args.count) if args.count else None
        if args.start_key is None:
            print("ERROR: missing --start-key")
            sys.exit(2)
        key_lo16 = parse_key_ffffxxxx(args.start_key)

        # Multiple keys support (comma-separated)
        keys = [k.strip() for k in args.start_key.split(",")]

        # Decide mode by presence of leaf-id
        mode_leaf = args.leaf_id is not None

        if mode_leaf:
            leaf_id = parse_hex_or_int(args.leaf_id) & 0xFF
            if table is None or count is None:
                print("ERROR: leaf mode requires --table-code and --count")
                sys.exit(2)
            print(f"[PATH] host={host_id} → router={router_id} → leaf={leaf_id} ; table=0x{table:02X} count=0x{count:02X} keys={keys}")
            for k in keys:
                k16 = parse_key_ffffxxxx(k)
                payload = build_getdata_payload_leaf(table, count, leaf_id, k16)
                frames.append(build_outer_frame(host_id, router_id, payload))
                descriptions.append(f"key={k}")
        else:
            # router-only mode needs table, index, count
            if table is None or count is None:
                print("ERROR: router-only mode requires --table-code and --count")
                sys.exit(2)
            table_index = parse_hex_or_int(args.table_index) & 0xFF if args.table_index else 0x01
            print(f"[PATH] host={host_id} → router={router_id} ; table-code=0x{table:02X} index=0x{table_index:02X} count=0x{count:02X} keys={keys}")
            for k in keys:
                k16 = parse_key_ffffxxxx(k)
                payload = build_getdata_payload_router(table, table_index, count, k16)
                frames.append(build_outer_frame(host_id, router_id, payload))
                descriptions.append(f"idx=0x{table_index:02X},key={k}")

    # Connect
    print(f"[CONNECT] [{args.addr}]:{args.port}")
    sock = socket.socket(socket.AF_INET6 if ":" in args.addr else socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(args.connect_timeout)
    sock.connect((args.addr, args.port))

    # Optional hello
    if args.hello:
        hello = binascii.unhexlify("bdefff10010fff00010e00ddf0bd")
        print(f"[TX hello] 14B: {binascii.hexlify(hello).decode()}")
        sock.sendall(hello)
        try:
            rx = sock.recv(args.rx_limit)
            tag = classify(rx)
            print(f"[RX after hello] {len(rx):>2} B [{tag}]\n{binascii.hexlify(rx).decode()}")
        except socket.timeout:
            print("[RX after hello] timeout")
        time.sleep(args.hello_gap_ms / 1000.0)

    # Send each frame (optionally sweep control/shim)
    total_counts = {"reply-89":0,"neighbor-70":0,"data-ish":0,"hello":0,"other":0,"timeout":0}

    def do_read_loop(label: str):
        counts = {"reply-89":0,"neighbor-70":0,"data-ish":0,"hello":0,"other":0,"timeout":0}
        for i in range(1, args.reads_per_tx+1):
            sock.settimeout(args.idle_timeout)
            try:
                rx = sock.recv(args.rx_limit)
                if not rx:
                    print(f"[RX {i}] empty")
                    counts["other"] += 1
                else:
                    tag = classify(rx)
                    counts[tag] = counts.get(tag,0)+1
                    print(f"[RX {i}] {len(rx)}B [{tag}]\n{binascii.hexlify(rx).decode()}")
            except socket.timeout:
                print(f"[RX {i}] timeout")
                counts["timeout"] += 1
            time.sleep(args.read_gap_ms / 1000.0)
        # summary for this frame
        print(f"\n[SUMMARY {label}]")
        for k in ["reply-89","neighbor-70","data-ish","hello","other","timeout"]:
            print(f"  {k:<11}: {counts[k]}")
        # fold into totals
        for k,v in counts.items():
            total_counts[k] += v

    if args.fanout_ctl_shim:
        # Sweep ctl in {0x0F,0x1F} and shim in {off,on}
        for tx, desc in zip(frames, descriptions):
            for ctl in (0x0F, 0x1F):
                for shim in (0, 1):
                    # Rewrite the control/shim inside the frame:
                    # Frame layout: BD [host] [router] 6F FD 00 [router] 0F FD <payload> BD
                    # Replace the last "0F FD" before payload:
                    tx_list = bytearray(tx)
                    # Find the sequence "0F FD" near header tail (after router id)
                    # At fixed offset: BD(0) host(1) router(2) 6F(3) FD(4) 00(5) router(6) 0F(7) FD(8)
                    if len(tx_list) >= 9 and tx_list[7] in (0x0F, 0x1F) and tx_list[8] == 0xFD:
                        tx_list[7] = ctl
                        if shim:
                            # Insert an extra FD after that 0xFD (shim byte), shifting payload by +1
                            tx_list = tx_list[:9] + b"\xFD" + tx_list[9:]
                        tx_variant = bytes(tx_list)
                    else:
                        tx_variant = bytes(tx_list)

                    print(f"[TX] {len(tx_variant)}B ctl=0x{ctl:02X},shim={shim}, {desc}: {binascii.hexlify(tx_variant).decode()}")
                    sock.sendall(tx_variant)
                    do_read_loop(f"ctl=0x{ctl:02X},shim={shim},{desc}")
                    time.sleep(args.tx_gap_ms / 1000.0)
    else:
        # Simple: send each frame as-is
        for tx, desc in zip(frames, descriptions):
            print(f"[TX] {len(tx)}B {desc}: {binascii.hexlify(tx).decode()}")
            sock.sendall(tx)
            do_read_loop(desc)
            time.sleep(args.tx_gap_ms / 1000.0)

    # Grand total
    print("\n[SUMMARY TOTAL]")
    for k in ["reply-89","neighbor-70","data-ish","hello","other","timeout"]:
        print(f"  {k:<11}: {total_counts[k]}")
    print("[CLOSE] socket closed")
    sock.close()

if __name__ == "__main__":
    main()