#!/usr/bin/env python3
"""
fetch_table1_live.py

Fetch Campbell PakBus Table 1 (last record or range) from a logger,
either by building a read‐command from leaf/table‐ID, or by replaying
a raw BD frame from a PCAP, or by sending a user-supplied INNER payload
(with optional router header).

HELLO is PC400‐style (BD+inner+BD, no CRC).  Reads use X.25‐CRC.
"""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
import time
from datetime import datetime

from biochar_app.pakbus.utils.frame import (
    bd_frame,        # wraps inner→BD…CRC16-X.25…BD
    bd_strip,
    split_bd_frames,
    recv_until_quiet,
)
from biochar_app.pakbus.utils.hex import parse_hex_bytes, hexdump


# ---------------------------------------------------------------------------
def bd_hello(inner: bytes) -> bytes:
    """
    Wrap a raw HELLO inner payload in BD markers, no CRC (PC400 style):
      BD <inner> BD
    """
    if not isinstance(inner, (bytes, bytearray)):
        raise TypeError("bd_hello(inner) expects bytes or bytearray")
    return b"\xBD" + bytes(inner) + b"\xBD"


# ---------------------------------------------------------------------------
def make_read_table1(leaf: int, table_id: int, start_rec: int, count: int) -> bytes:
    """
    Build the 11-byte PC400‐style read payload:
      2C <leaf> 00 00 <table_id_hi> <table_id_lo> 00 <start_hi> <start_lo> <count> 00
    """
    return bytes([
        0x2C,
        leaf & 0xFF,
        0x00, 0x00,
        (table_id >> 8) & 0xFF, table_id & 0xFF,
        0x00,
        (start_rec >> 8) & 0xFF, start_rec & 0xFF,
        count & 0xFF,
        0x00,
    ])


# ---------------------------------------------------------------------------
def setup_run_dir(base: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(base, f"pakbus_run_{ts}")
    os.makedirs(path, exist_ok=True)
    return path


def setup_logging(run_dir: str, verbose: bool):
    logf = os.path.join(run_dir, "session.log")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fh = logging.FileHandler(logf, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    root.handlers[:] = [fh, ch]
    logging.info("Run dir: %s", run_dir)
    logging.info("Logging to: %s", logf)


def connect_ipv6(addr: str, port: int, timeout: float) -> socket.socket:
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((addr, port))
    return s


def send_pkt(sock: socket.socket, pkt: bytes, label: str, run_dir: str, fname: str):
    logging.debug("→ [%s] %d bytes → %s", label, len(pkt), hexdump(pkt))
    sock.sendall(pkt)
    try:
        with open(os.path.join(run_dir, fname), "wb") as f:
            f.write(pkt)
    except Exception as e:
        logging.warning("Could not save %s to %s: %s", label, fname, e)


# ---------------------------------------------------------------------------
def run(args) -> int:
    run_dir = setup_run_dir(args.log_dir)
    setup_logging(run_dir, args.debug)

    # parse HELLO inner
    pre = parse_hex_bytes(args.pre_hex)
    logging.debug("HELLO inner  : %s", hexdump(pre))

    # parse optional router header & raw TX or inner payload
    router = parse_hex_bytes(args.router_hex) if args.router_hex else b""
    tx_raw = parse_hex_bytes(args.tx_hex) if args.tx_hex else None

    inner_src = args.inner_hex
    if not inner_src and args.inner_file:
        try:
            with open(args.inner_file, "r", encoding="utf-8") as f:
                inner_src = f.read()
        except FileNotFoundError:
            logging.error("--inner-file not found: %s", args.inner_file)
            return 2

    # connect
    logging.debug("connecting to [%s]:%d …", args.addr, args.port)
    sock = connect_ipv6(args.addr, args.port, args.timeout)

    # HELLO handshake
    if args.hello:
        if args.pre_wait_ms > 0:
            time.sleep(args.pre_wait_ms / 1000.0)
        pkt = bd_hello(pre)
        send_pkt(sock, pkt, "HELLO", run_dir, "hello.tx")
        hello_reply = recv_until_quiet(sock, args.timeout, args.post_recv_grace_ms)
        with open(os.path.join(run_dir, "hello_reply.raw"), "wb") as f:
            f.write(hello_reply)
        frames = split_bd_frames(hello_reply)
        logging.debug("received %d bytes → %d BD frames", len(hello_reply), len(frames))
        if frames:
            logging.debug("HELLO↩︎ frame[0]: %s", hexdump(frames[0]))
    else:
        logging.info("Skipping HELLO (--hello not set)")

    # build/send data‐read
    if tx_raw is not None:
        cmd_pkt = tx_raw
        logging.debug("Using raw TX-hex (skipping inner/build path)")
    else:
        if inner_src:
            inner_payload = router + parse_hex_bytes(inner_src)
            logging.debug("read inner (user) : %s", hexdump(inner_payload))
        else:
            inner_payload = router + make_read_table1(
                args.leaf, args.table_id, args.start_rec, args.count
            )
            logging.debug("read inner (built): %s", hexdump(inner_payload))

        cmd_pkt = bd_frame(inner_payload)

    send_pkt(sock, cmd_pkt, "READ", run_dir, "read.tx")

    # collect reply
    data = recv_until_quiet(sock, args.timeout, args.post_recv_grace_ms)
    with open(os.path.join(run_dir, "read_reply.raw"), "wb") as f:
        f.write(data)

    if not data:
        logging.error("No bytes in reply to read request.")
        logging.info("Artifacts in %s", run_dir)
        return 3

    frames = split_bd_frames(data)
    if not frames:
        logging.error("No BD frames found in reply.")
        logging.info("Artifacts in %s", run_dir)
        return 4

    logging.debug("got %d BD reply frame(s)", len(frames))
    if args.dump_frames:
        for i, fr in enumerate(frames):
            with open(os.path.join(run_dir, f"reply_{i:02d}.bd"), "wb") as f:
                f.write(fr)
            inner = bd_strip(fr)
            if inner is not None:
                with open(os.path.join(run_dir, f"reply_{i:02d}.inner"), "wb") as f:
                    f.write(inner)

    # log first few
    for i, fr in enumerate(frames[:4]):
        inner = bd_strip(fr)
        logging.debug("↩ frame[%d] %dB: %s", i, len(fr), hexdump(fr))
        if inner is None:
            logging.debug("    (CRC mismatch)")
        else:
            logging.debug("    inner %dB: %s", len(inner), hexdump(inner))

    # write first valid inner to out file
    if args.out:
        for fr in frames:
            inner = bd_strip(fr)
            if inner is not None:
                out_path = args.out if os.path.isabs(args.out) else os.path.join(run_dir, args.out)
                with open(out_path, "wb") as f:
                    f.write(inner)
                logging.info("[OK] wrote inner to %s", out_path)
                break

    logging.info("Artifacts in %s", run_dir)
    return 0


# ---------------------------------------------------------------------------
def _build_argparser():
    p = argparse.ArgumentParser(
        description="Fetch PakBus Table1 via HELLO + BD frame (built, user inner, or replayed)"
    )
    p.add_argument("--addr",      required=True)
    p.add_argument("--port",      type=int, default=6785)
    p.add_argument("--timeout",   type=float, default=20.0)
    p.add_argument("--debug",     action="store_true")
    p.add_argument("--log-dir",   default="pakbus_runs")

    # HELLO
    p.add_argument("--hello",       action="store_true")
    p.add_argument("--pre-hex",     required=True)
    p.add_argument("--pre-wait-ms", type=int, default=400)
    p.add_argument("--post-recv-grace-ms", type=int, default=1200)

    # routing/raw-TX
    p.add_argument("--router-hex", default="")
    p.add_argument("--tx-hex",     default="")

    # user-supplied inner
    p.add_argument("--inner-hex",  default="")
    p.add_argument("--inner-file", default="")

    # leaf/table
    p.add_argument("--leaf",     type=int, default=3)
    p.add_argument("--table-id", type=lambda x: int(x, 0), default=2)
    p.add_argument("--start-rec",type=lambda x: int(x, 0), default=0xFFFF)
    p.add_argument("--count",    type=lambda x: int(x, 0), default=1)

    # output
    p.add_argument("--out")
    p.add_argument("--dump-frames", action="store_true")

    return p


def main(argv=None):
    args = _build_argparser().parse_args(argv)
    sys.exit(run(args))


if __name__ == "__main__":
    main()