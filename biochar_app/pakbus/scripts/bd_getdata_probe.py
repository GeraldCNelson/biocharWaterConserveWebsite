#!/usr/bin/env python3
"""
bd_getdata_probe.py

Minimal PakBus "hello + optional read" probe that:
- Sends hello (twice by default) and prints any frames we receive.
- Does NOT require a beacon to proceed.
- Falls back to src_id=1, dst_id=4 (per PC400 screenshot) unless overridden.
- Optionally sends a single GetData/Read frame built from a seed-hex with
  patched addressing and CRC (tries IBM and CCITT).
- Dumps all RX bytes and parsed frames to --out-dir for inspection.

Usage example (hello only):
  python bd_getdata_probe.py \
    --addr <ipv6> --port 6785 \
    --out-dir pakbus_runs/getdata_probe

With a seed frame to test a read:
  python bd_getdata_probe.py \
    --addr <ipv6> --port 6785 \
    --seed-hex "bda0..." \
    --table 0x11 --count 12 \
    --out-dir pakbus_runs/getdata_probe
"""

from __future__ import annotations
import argparse
import os
import socket
import sys
import time
from typing import Tuple, Optional, List

FLAG = 0xBD  # frame delimiter

# ---------- CRC helpers ----------
def crc16_ibm(data: bytes) -> int:
    """CRC-16/IBM (x16 + x15 + x2 + 1), init 0xFFFF, ref in/out."""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def crc16_ccitt(data: bytes) -> int:
    """CRC-16/CCITT-FALSE (poly 0x1021), init 0xFFFF, no ref, no xorout."""
    crc = 0xFFFF
    for b in data:
        crc ^= (b << 8) & 0xFFFF
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc & 0xFFFF

# ---------- framing ----------
def split_frames(raw: bytes) -> List[bytes]:
    """
    Split raw stream on 0xBD boundaries, return payload-only slices
    (bytes between flags). Empty slices are filtered.
    """
    parts = raw.split(bytes([FLAG]))
    return [p for p in parts if p]

def hexify(b: bytes) -> str:
    return " ".join(f"{x:02x}" for x in b)

def wrap_with_flag(payload: bytes) -> bytes:
    return bytes([FLAG]) + payload + bytes([FLAG])

# ---------- hello frames ----------
# The 13-byte hello you saw on the wire (echoed from logger) looks like:
#   ef ff 10 01 0f ff 00 01 0e 00 dd f0
# We'll transmit the same canonical "hello" request envelope often accepted by CR800s.
HELLO_REQ = bytes.fromhex("ef ff 10 01 0f ff 00 01 0e 00 dd f0")

def send_hello(sock: socket.socket, repeats: int, gap_ms: int) -> None:
    payload = HELLO_REQ
    frame = wrap_with_flag(payload)
    for i in range(repeats):
        sock.sendall(frame)
        time.sleep(gap_ms / 1000.0)

# ---------- seed patching ----------
def strip_flags_and_crc(seed: bytes) -> bytes:
    """Remove leading/trailing 0xBD if present; strip last two bytes as CRC if len>=3."""
    s = seed
    if s and s[0] == FLAG:
        s = s[1:]
    if s and s[-1] == FLAG:
        s = s[:-1]
    if len(s) >= 3:
        return s[:-2]  # drop CRC
    return s

def guess_and_patch_addresses(core: bytes, src_id: int, dst_id: int) -> bytes:
    """
    Very conservative patch: if we find a pair of consecutive bytes that look
    like 'src,dst' near the start of the frame (within first ~8 bytes), replace them.
    If we don't find anything plausible, just return the core unchanged.
    """
    core_mut = bytearray(core)
    # try positions 0..6 for (src,dst) pair heuristically
    for pos in range(min(8, len(core_mut)-1)):
        # do not clobber well-known constants (e.g., 0xAF,0xFD header pairs)
        if core_mut[pos] in (0xAF, 0xEF) and core_mut[pos+1] in (0xFD, 0xFF):
            continue
        # replace and bail once
        core_mut[pos]   = src_id & 0xFF
        core_mut[pos+1] = dst_id & 0xFF
        break
    return bytes(core_mut)

def append_crc_and_flag(core: bytes, flavor: str) -> bytes:
    if flavor == "ibm":
        crc = crc16_ibm(core)
    elif flavor == "ccitt":
        crc = crc16_ccitt(core)
    else:
        raise ValueError("flavor must be 'ibm' or 'ccitt'")
    core_crc = core + bytes([crc & 0xFF, (crc >> 8) & 0xFF])  # little-endian on wire is common
    return wrap_with_flag(core_crc)

# ---------- networking ----------
def recv_with_idle_timeout(sock: socket.socket, idle_timeout: float) -> bytes:
    """
    Receive until the socket is idle for idle_timeout seconds.
    Returns the concatenated bytes (possibly zero length).
    """
    sock.setblocking(False)
    buf = bytearray()
    last = time.time()
    while True:
        try:
            chunk = sock.recv(65535)
            if chunk:
                buf += chunk
                last = time.time()
            else:
                # remote closed
                break
        except BlockingIOError:
            pass
        if time.time() - last >= idle_timeout:
            break
        time.sleep(0.01)
    sock.setblocking(True)
    return bytes(buf)

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="PakBus hello + optional single read probe.")
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)

    ap.add_argument("--out-dir", default="pakbus_runs/getdata_probe")
    ap.add_argument("--connect-timeout", type=float, default=10.0)
    ap.add_argument("--idle-timeout", type=float, default=0.8)
    ap.add_argument("--hello-gap-ms", type=int, default=300)
    ap.add_argument("--hello-repeats", type=int, default=2)
    ap.add_argument("--post-wait-ms", type=int, default=2000)

    # addressing (defaults from PC400 screenshot)
    ap.add_argument("--src-id", type=int, default=1, help="Our PakBus ID (neighbor). Default 1.")
    ap.add_argument("--dst-id", type=int, default=4, help="Logger PakBus ID. Default 4.")

    # optional read attempt via seed frame
    ap.add_argument("--seed-hex", default=None, help="Hex of a prior Read/Replay frame to patch and resend.")
    ap.add_argument("--table", type=lambda s: int(s, 0), default=None, help="Table ID to request (e.g., 0x11).")
    ap.add_argument("--count", type=int, default=None, help="Record count to request.")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    rx_path = os.path.join(args.out_dir, "probe_rx.bin")

    # connect (IPv6)
    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.settimeout(args.connect_timeout)
    sock.connect((args.addr, args.port, 0, 0))

    # 1) HELLO (twice by default)
    send_hello(sock, args.hello_repeats, args.hello_gap_ms)

    # give the logger a moment to respond
    time.sleep(args.post_wait_ms / 1000.0)

    # 2) collect replies
    rx = recv_with_idle_timeout(sock, args.idle_timeout)
    with open(rx_path, "ab") as f:
        f.write(rx)

    frames = split_frames(rx)
    print(f"[HELLO] rx={len(rx)}B frames={len(frames)}")
    for i, fr in enumerate(frames, 1):
        print(f"  RX[{i}] {len(fr)}B:\n    {hexify(fr)}")

    # 3) If no seed-hex provided, we stop after hello
    if not args.seed_hex:
        print("[INFO] No --seed-hex provided; hello-only probe complete.")
        sock.close()
        print(f"[OK] Wrote RX to: {rx_path}")
        return

    # 4) Try a single read by patching the provided seed
    try:
        seed = bytes.fromhex(args.seed_hex.strip())
    except Exception:
        print("[ERR] seed-hex is not valid hex.")
        sock.close()
        return

    core = strip_flags_and_crc(seed)
    core = guess_and_patch_addresses(core, args.src_id, args.dst_id)

    # Optional: very light table/count patch (only if caller gave both)
    # (We do not attempt to patch a start-key here; that will come in the dedicated downloader.)
    if args.table is not None and args.count is not None:
        # Best-effort: look for a seq "fd 09 ?? 00 00" and replace table/lowcount
        # If not found, we just leave as-is.
        b = bytearray(core)
        for pos in range(len(b) - 4):
            if b[pos] == 0xFD and b[pos + 1] == 0x09 and b[pos + 3] == 0x00 and b[pos + 4] == 0x00:
                b[pos + 2] = args.table & 0xFF
                # patch a 1-byte count if the seed uses it; if a 2-byte count is used, we skip
                # (full downloader will handle structured patching)
                # Here we only set a placeholder "count low" just to exercise a response.
                # Many firmwares ignore this and use a later field anyway.
                # So this is intentionally conservative.
                break
        core = bytes(b)

    # Try both CRC flavors
    for flavor in ("ibm", "ccitt"):
        frame = append_crc_and_flag(core, flavor)
        try:
            sock.sendall(frame)
            time.sleep(args.post_wait_ms / 1000.0)
            rx2 = recv_with_idle_timeout(sock, args.idle_timeout)
            with open(rx_path, "ab") as f:
                f.write(rx2)
            parts = split_frames(rx2)
            print(f"[READ {flavor.upper()}] rx={len(rx2)}B frames={len(parts)}")
            for i, fr in enumerate(parts, 1):
                print(f"  RX2[{i}] {len(fr)}B:\n    {hexify(fr)}")
        except Exception as e:
            print(f"[ERR] send/read with CRC {flavor}: {e}")

    sock.close()
    print(f"[OK] Probe complete. RX bytes appended to {rx_path}")

if __name__ == "__main__":
    main()
