#!/usr/bin/env python3
"""
Fetch Campbell PakBus Table 1 (last record or range) from a logger.

Adds durable logging & artifact dumps:
  - Each run writes to pakbus_runs/YYYYmmdd_HHMMSS/
  - session.log captures all console output
  - hello.raw, hello_reply.raw: raw TCP bytes around hello
  - read_reply.raw: raw TCP bytes for the table read reply
  - (optional) BD frame dumps + inner payloads

It defaults to the simple, PC400-like style seen in your pcap:
  - client hello inner bytes: 90 01 0f fd 73 d3
  - no router header mirroring in first app frame
  - TRAN fixed at 0x90 (not explicitly carried in our simple frame)

Example:
  python -m biochar_app.pakbus.scripts.fetch_table1_live \\
    --addr 2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd --port 6785 \\
    --timeout 20 --debug \\
    --pre-hex '90 01 0f fd 73 d3' --pre-wait-ms 600 \\
    --leaf 3 --table-id 0x0001 --start-rec 0xFFFF --count 0x0001 \\
    --post-recv-grace-ms 1500 --auto --dump-frames
"""

import argparse
import binascii
import logging
import os
import socket
import struct
import sys
import time
from datetime import datetime
from typing import Optional, Tuple, List

# ------------------------
# Utilities
# ------------------------

def hexdump(b: bytes) -> str:
    return " ".join(f"{x:02x}" for x in b)

def parse_hex_bytes(s: str) -> bytes:
    s = s.strip().replace(" ", "").replace("_", "")
    if len(s) % 2:
        raise ValueError("hex string must have even length")
    return bytes.fromhex(s)

def crc16_modbus(data: bytes) -> int:
    """CRC-16/Modbus (poly 0xA001), returns 0..0xFFFF"""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc

def bd_frame(inner: bytes) -> bytes:
    """Wrap inner payload into BD ... CRC CRC BD (CRC big-endian)."""
    crc = crc16_modbus(inner)
    crc_hi = (crc >> 8) & 0xFF
    crc_lo = crc & 0xFF
    return bytes([0xBD]) + inner + bytes([crc_hi, crc_lo, 0xBD])

def is_bd_framed(b: bytes) -> bool:
    return len(b) >= 4 and b[0] == 0xBD and b[-1] == 0xBD

def bd_strip(frame: bytes) -> Optional[bytes]:
    if not is_bd_framed(frame):
        return None
    inner = frame[1:-3]
    crc_hi, crc_lo = frame[-3], frame[-2]
    crc_calc = crc16_modbus(inner)
    if crc_hi != ((crc_calc >> 8) & 0xFF) or crc_lo != (crc_calc & 0xFF):
        return None
    return inner

def recv_some(sock: socket.socket, timeout: float) -> bytes:
    sock.settimeout(timeout)
    try:
        return sock.recv(65535)
    except socket.timeout:
        return b""

def recv_until_quiet(sock: socket.socket, first_timeout: float, grace_ms: int) -> bytes:
    """Read until no more data comes for 'grace_ms' after first byte(s)."""
    buf = bytearray()
    chunk = recv_some(sock, first_timeout)
    buf += chunk
    if not chunk:
        return bytes(buf)
    end_by = time.time() + (grace_ms / 1000.0)
    while time.time() < end_by:
        chunk = recv_some(sock, 0.050)
        if chunk:
            buf += chunk
            end_by = time.time() + (grace_ms / 1000.0)
    return bytes(buf)

def split_bd_frames(stream: bytes) -> List[bytes]:
    frames: List[bytes] = []
    cur = bytearray()
    in_frame = False
    for b in stream:
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

# ------------------------
# PakBus helpers (minimal)
# ------------------------

def make_read_table1(leaf: int, table_id: int, start_rec: int, count: int) -> bytes:
    """
    Build the PC400-style 11-byte 'read table 1' inner payload observed in pcap:

      2C <leaf> 00 00 00 01 00 FF FF 01 00
    """
    return bytes([
        0x2C,
        leaf & 0xFF,
        0x00, 0x00,            # two zeros
        0x00, 0x01,            # table id = 0x0001 (big-endian)
        0x00,                  # single 0x00
        0xFF, 0xFF,            # start_rec = 0xFFFF
        0x01,                  # count = 1
        0x00,                  # trailing option byte
    ])

# ------------------------
# Logging setup
# ------------------------

def setup_run_dir(base_dir: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(base_dir, f"pakbus_run_{ts}")
    os.makedirs(run_dir, exist_ok=True)
    return run_dir

def setup_logging(run_dir: str, verbose: bool):
    log_path = os.path.join(run_dir, "session.log")
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(ch)

    logging.info("Run dir: %s", run_dir)
    logging.info("Logging to: %s", log_path)

# ------------------------
# Core flow
# ------------------------

def send_bd(sock: socket.socket, inner: bytes, label: str, run_dir: str, fname: str):
    frame = bd_frame(inner)
    logging.debug("sending %s (%d bytes): %s", label, len(frame), hexdump(frame))
    sock.sendall(frame)
    try:
        with open(os.path.join(run_dir, fname), "wb") as f:
            f.write(frame)
    except Exception as e:
        logging.warning("Could not save %s frame to %s: %s", label, fname, e)

def connect_ipv6(addr: str, port: int, timeout: float) -> socket.socket:
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((addr, port))
    return s

def run(args) -> int:
    run_dir = setup_run_dir(args.log_dir)
    setup_logging(run_dir, verbose=args.debug)

    # Prep hello inner bytes
    pre = parse_hex_bytes(args.pre_hex)
    logging.debug("hello inner payload (len=%d): %s", len(pre), hexdump(pre))

    # Connect
    logging.debug("connecting to [%s]:%d …", args.addr, args.port)
    sock = connect_ipv6(args.addr, args.port, args.timeout)

    # Small wait before hello (PC400-like pacing)
    if args.pre_wait_ms > 0:
        time.sleep(args.pre_wait_ms / 1000.0)

    # Send hello
    send_bd(sock, pre, "hello", run_dir, "hello.tx")

    # Read hello reply
    reply_raw = recv_until_quiet(sock, args.timeout, args.post_recv_grace_ms)
    with open(os.path.join(run_dir, "hello_reply.raw"), "wb") as f:
        f.write(reply_raw)

    frames = split_bd_frames(reply_raw)
    logging.debug("received %d bytes; parsed %d BD-framed chunk(s)", len(reply_raw), len(frames))
    if frames:
        logging.debug("hello-reply candidate frame 0: %s", hexdump(frames[0]))

    # Auto inference (simple vs router header). We stick to simple given your traces.
    if args.auto:
        if len(frames) == 0 or len(frames[0]) <= 18:
            logging.info("[auto] inferring simple style: no router header; TRAN=0x90")
        else:
            logging.info("[auto] (conservative) using router header; TRAN=0x90 (not implemented)")

    # Build first app frame (simple style)
    read_inner = make_read_table1(args.leaf, args.table_id, args.start_rec, args.count)
    logging.debug("read inner payload: %s", hexdump(read_inner))

    # Gap between hello and read
    if args.inter_gap_ms > 0:
        time.sleep(args.inter_gap_ms / 1000.0)

    # Send read
    send_bd(sock, read_inner, "table1/read", run_dir, "read.tx")

    # Receive reply
    data = recv_until_quiet(sock, args.timeout, args.post_recv_grace_ms)
    with open(os.path.join(run_dir, "read_reply.raw"), "wb") as f:
        f.write(data)

    if not data:
        logging.error("No bytes in reply to read request.")
        return 3

    r_frames = split_bd_frames(data)
    if not r_frames:
        logging.error("No BD frames found in reply buffer.")
        return 4

    logging.debug("got %d reply frame(s).", len(r_frames))

    # Dump frames (optional)
    if args.dump_frames:
        for i, fr in enumerate(r_frames):
            fp = os.path.join(run_dir, f"reply_{i:02d}.bd")
            with open(fp, "wb") as f:
                f.write(fr)
            inner = bd_strip(fr)
            if inner:
                with open(os.path.join(run_dir, f"reply_{i:02d}.inner"), "wb") as f:
                    f.write(inner)

    # Print up to 4 frames to console/log for visibility
    for i, fr in enumerate(r_frames[:4]):
        inner = bd_strip(fr)
        logging.debug("reply[%d] %dB: %s", i, len(fr), hexdump(fr))
        if inner is None:
            logging.debug("          (CRC mismatch)")
        else:
            logging.debug("          inner %dB: %s", len(inner), hexdump(inner))

    # Optionally write first valid inner to a file
    if args.out:
        for fr in r_frames:
            inner = bd_strip(fr)
            if inner:
                out_path = os.path.join(run_dir, args.out) if not os.path.isabs(args.out) else args.out
                with open(out_path, "wb") as f:
                    f.write(inner)
                logging.info("[OK] wrote first inner payload to: %s", out_path)
                break

    logging.info("Artifacts in: %s", run_dir)
    return 0

# ------------------------
# CLI
# ------------------------

def _build_argparser():
    p = argparse.ArgumentParser(
        description="Fetch Table 1 from a PakBus logger (PC400-style simple framing) with durable logging."
    )
    p.add_argument("--addr", required=True, help="IPv6 address of logger")
    p.add_argument("--port", type=int, default=6785, help="TCP port (default 6785)")
    p.add_argument("--timeout", type=float, default=20.0, help="socket timeout (s)")
    p.add_argument("--debug", action="store_true", help="verbose console output")
    p.add_argument("--pre-hex", required=True,
                   help="hello inner hex BYTES (no BD/CRC), e.g. '90 01 0f fd 73 d3'")
    p.add_argument("--pre-wait-ms", type=int, default=400, help="sleep before hello (ms)")
    p.add_argument("--inter-gap-ms", type=int, default=150, help="gap between hello and read (ms)")
    p.add_argument("--post-recv-grace-ms", type=int, default=1200,
                   help="keep receiving for this long after first bytes (ms)")
    p.add_argument("--auto", action="store_true",
                   help="infer simple style from hello reply (defaults to simple PC400-style)")
    # read parameters
    p.add_argument("--leaf", type=int, default=3, help="PakBus leaf address (default 3)")
    p.add_argument("--table-id", type=lambda x: int(x, 0), default=0x0001,
                   help="Table id (default 0x0001)")
    p.add_argument("--start-rec", type=lambda x: int(x, 0), default=0xFFFF,
                   help="Start record (default 0xFFFF = last)")
    p.add_argument("--count", type=lambda x: int(x, 0), default=0x0001,
                   help="Record count (default 0x0001)")
    # output / logging
    p.add_argument("--out", help="Write first good inner reply to this file (saved inside run dir unless absolute path)")
    p.add_argument("--log-dir", default="pakbus_runs", help="Base directory for run artifacts")
    p.add_argument("--dump-frames", action="store_true", help="Dump each BD reply and inner to files")
    return p

def main(argv=None):
    args = _build_argparser().parse_args(argv)
    rc = run(args)
    sys.exit(rc)

if __name__ == "__main__":
    main()