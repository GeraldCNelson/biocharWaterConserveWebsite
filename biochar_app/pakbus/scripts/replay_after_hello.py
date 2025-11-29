#!/usr/bin/env python3
"""
Replay PC400 app frame after hello.

Usage:
  python -m biochar_app.pakbus.scripts.replay_after_hello \
    --addr <ipv6> --port <port> \
    --pre-hex '90 01 0f fd 73 d3' \
    --inner-hex '<hex>' \
    [--router-hex '<hex>'] \
    [--pre-wait-ms ms] [--inter-gap-ms ms] \
    [--hello-recv-grace-ms ms] [--reply-recv-grace-ms ms]
"""
import argparse
import socket
import time
import sys

from biochar_app.pakbus.utils.frame import (
    bd_frame,
    bd_strip,
    split_bd_frames,
    recv_until_quiet,
)
from biochar_app.pakbus.utils.hex import parse_hex_bytes, hexdump


def main():
    ap = argparse.ArgumentParser(description="Replay PC400 app frame after hello")
    ap.add_argument("--addr",       required=True, help="IPv6 address of the logger")
    ap.add_argument("--port",       type=int, default=6785, help="TCP port (default 6785)")
    ap.add_argument(
        "--pre-hex", default="90 01 0f fd 73 d3",
        help="HELLO inner hex payload (no BD/CRC)"
    )
    ap.add_argument(
        "--inner-hex", required=True,
        help="App-frame inner hex for PC→logger (no BD/CRC)"
    )
    ap.add_argument(
        "--router-hex", default="",
        help="Optional router header hex (if included in --inner-hex, leave blank)"
    )
    ap.add_argument(
        "--pre-wait-ms",        type=int, default=400,
        help="Sleep before sending HELLO (ms)"
    )
    ap.add_argument(
        "--inter-gap-ms",       type=int, default=180,
        help="Gap between HELLO reply and app frame send (ms)"
    )
    ap.add_argument(
        "--hello-recv-grace-ms", type=int, default=800,
        help="Grace period after HELLO to collect its reply (ms)"
    )
    ap.add_argument(
        "--reply-recv-grace-ms", type=int, default=1500,
        help="Grace period after app frame to collect its reply (ms)"
    )
    args = ap.parse_args()

    # Decode hex-strings into bytes
    pre    = parse_hex_bytes(args.pre_hex)
    inner  = parse_hex_bytes(args.inner_hex)
    router = parse_hex_bytes(args.router_hex) if args.router_hex else b""

    print(f"[INFO] HELLO inner: {hexdump(pre)}")
    if router:
        print(f"[INFO] router hdr: {hexdump(router)}")
    print(f"[INFO] app    inner: {hexdump(inner)}")
    print(f"[INFO] target [{args.addr}]:{args.port}")

    # Open an IPv6 TCP socket
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(8.0)
    s.connect((args.addr, args.port))

    # Optional pacing before HELLO
    if args.pre_wait_ms > 0:
        time.sleep(args.pre_wait_ms / 1000.0)

    # Send HELLO (BD-framed)
    hello_pkt = bd_frame(pre)
    print(f"[TX] HELLO: {hexdump(hello_pkt)}")
    s.sendall(hello_pkt)

    # Receive HELLO reply
    buf = recv_until_quiet(s, 4.0, args.hello_recv_grace_ms)
    frames = split_bd_frames(buf)
    print(f"[RX] HELLO bytes={len(buf)}, frames={len(frames)}")
    if frames:
        print(f"     HELLO[0]: {hexdump(frames[0])}")

    # Gap before sending the app frame
    if args.inter_gap_ms > 0:
        time.sleep(args.inter_gap_ms / 1000.0)

    # Build & send the app frame
    app_inner = router + inner
    app_pkt   = bd_frame(app_inner)
    print(f"[TX] APP  : {hexdump(app_pkt)}")
    s.sendall(app_pkt)

    # Receive app reply
    buf = recv_until_quiet(s, 5.0, args.reply_recv_grace_ms)
    s.close()
    print(f"[RX] APP-reply bytes={len(buf)}")
    if not buf:
        print("[WARN] no reply bytes")
        sys.exit(2)

    # Split into BD frames and strip each
    r_frames = split_bd_frames(buf)
    print(f"[RX] APP-reply frames={len(r_frames)}")
    for i, fr in enumerate(r_frames[:6]):
        inner_r = bd_strip(fr)
        print(f"   frame[{i}] {len(fr)}B: {hexdump(fr)}")
        if inner_r is None:
            print("      (CRC mismatch)")
        else:
            print(f"      inner {len(inner_r)}B: {hexdump(inner_r)}")


if __name__ == "__main__":
    main()