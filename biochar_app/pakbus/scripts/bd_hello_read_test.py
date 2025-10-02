#!/usr/bin/env python3
# bd_hello_read_test.py - minimal PakBus "hello + read" probe
# - Sends hello
# - Sends a provided read/replay frame (or a patched seed)
# - Prints/records ACK/DATA frames

import argparse, socket, time, pathlib, sys
from typing import Tuple, List

# --- Constants (re-used from your earlier tools) ---

HELLO = bytes.fromhex("bd 90 01 0f fd 73 d3 c2 d6 bd")

ACK_PREFIX  = bytes.fromhex("bd af fd 70")              # 18B-ish ack
DATA_PREFIXES = [
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x03',  # Read
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x02',  # CRBasic variant
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x01',  # other variant
]

# --- CRC helpers ---

def crc16_ibm(data: bytes) -> int:
    """CRC-16/IBM (aka CRC-16/ARC), poly=0xA001, init=0x0000, refin/out=True, xorout=0x0000."""
    crc = 0x0000
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def crc16_ccitt(data: bytes) -> int:
    """CRC-16/CCITT-FALSE, poly=0x1021, init=0xFFFF, refin/out=False, xorout=0x0000."""
    crc = 0xFFFF
    for b in data:
        crc ^= (b << 8) & 0xFFFF
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc & 0xFFFF

def add_crc(frame_wo_crc: bytes, flavor: str) -> bytes:
    if flavor.lower() == "ibm":
        c = crc16_ibm(frame_wo_crc)
    elif flavor.lower() == "ccitt":
        c = crc16_ccitt(frame_wo_crc)
    else:
        raise ValueError("CRC flavor must be 'ibm' or 'ccitt'")
    # PakBus CRC bytes are big-endian at the end, followed by 0xBD terminator if not present
    crc_bytes = c.to_bytes(2, "big")
    out = frame_wo_crc + crc_bytes
    # Ensure trailing 0xBD terminator
    return out if out.endswith(b"\xBD") else out + b"\xBD"

# --- I/O helpers ---

def hex_to_bytes(s: str) -> bytes:
    s = s.strip().lower().replace(" ", "")
    if not s:
        return b""
    if len(s) % 2:
        s = "0" + s
    try:
        return bytes.fromhex(s)
    except Exception:
        return b""

def recv_all(sock: socket.socket, idle_timeout=0.25, max_wait=2.0) -> bytes:
    sock.settimeout(idle_timeout)
    chunks, start = [], time.time()
    while True:
        try:
            data = sock.recv(65536)
            if data:
                chunks.append(data); start = time.time()
            else:
                break
        except socket.timeout:
            if time.time() - start >= max_wait:
                break
            continue
    return b"".join(chunks)

def split_bd_frames(buf: bytes) -> List[bytes]:
    out, cur = [], bytearray()
    for b in buf:
        cur.append(b)
        if b == 0xBD:        # 0xBD terminates a BD frame
            out.append(bytes(cur))
            cur.clear()
    if cur:
        out.append(bytes(cur))
    return out

def frame_is_ack(b: bytes) -> bool:
    return b.startswith(ACK_PREFIX) and len(b) >= 14

def frame_is_data(b: bytes) -> bool:
    if len(b) < 40:  # sanity
        return False
    return any(b.startswith(p) for p in DATA_PREFIXES)

# --- Seed patching (optional) ---

def find_patch_offsets(core_wo_crc: bytes, table_id: int) -> Tuple[int, int]:
    """
    Look for: FD 09 <table> 00 00 ... and then two 4B big-endian fields:
      - start key (epoch secs or record#)
      - count (or vice versa, depends on dialect)

    We return (pos_count, pos_start). If not found, raise.
    """
    for i in range(len(core_wo_crc) - 6):
        if (core_wo_crc[i]   == 0xFD and
            core_wo_crc[i+1] == 0x09 and
            core_wo_crc[i+2] == (table_id & 0xFF) and
            core_wo_crc[i+3] == 0x00 and
            core_wo_crc[i+4] == 0x00):
            # Expect next chunk to hold 8 bytes of parameters
            # Try layout: COUNT (4B BE), START (4B BE)
            pos_count = i + 5
            pos_start = i + 9
            if pos_start + 4 <= len(core_wo_crc):
                return pos_count, pos_start
    raise ValueError("Could not locate FD 09 <table> 00 00 pattern in seed frame.")

def patch_seed(seed_hex: str, table_id: int, new_count: int, new_start_be_u32: int, crc_flavor: str) -> bytes:
    """
    Patch a captured seed frame:
      - strip trailing CRC+BD if present
      - locate params
      - write COUNT and START (both 4B BE)
      - recompute CRC (ibm/ccitt)
    """
    seed = hex_to_bytes(seed_hex)
    if not seed:
        raise ValueError("Empty or invalid seed hex.")

    # strip trailing CRC + optional BD; keep leading BD
    if seed.endswith(b"\xBD"):
        core = seed[:-1]
        core_wo_crc = core[:-2]  # drop CRC (2B)
    else:
        core_wo_crc = seed

    pos_count, pos_start = find_patch_offsets(core_wo_crc, table_id)

    core_list = bytearray(core_wo_crc)
    core_list[pos_count:pos_count+4] = new_count.to_bytes(4, "big", signed=False)
    core_list[pos_start:pos_start+4] = new_start_be_u32.to_bytes(4, "big", signed=False)

    # If original did not start with 0xBD, add it
    patched_core = bytes(core_list)
    if not patched_core.startswith(b"\xBD"):
        patched_core = b"\xBD" + patched_core

    return add_crc(patched_core[0:-0], crc_flavor)  # recompute and add terminator

# --- Main ---

def main():
    ap = argparse.ArgumentParser(description="PakBus hello + read test (ACK/DATA probe)")
    ap.add_argument("--addr", required=True, help="IPv6 address of logger")
    ap.add_argument("--port", type=int, required=True, help="TCP port (e.g., 6785)")
    ap.add_argument("--frame-hex", help="Read frame to send (hex). If omitted, use --seed-hex + patch args.")
    ap.add_argument("--seed-hex", help="Seed frame to patch (hex).")
    ap.add_argument("--table", type=lambda x: int(x, 0), default=0x11, help="Table id byte used in patch search (default 0x11)")
    ap.add_argument("--patch-start", type=lambda x: int(x, 0), help="Big-endian 32-bit START key to patch (e.g., epoch-1990 seconds or record id)")
    ap.add_argument("--patch-count", type=int, help="COUNT to patch")
    ap.add_argument("--crc", choices=["ibm","ccitt"], default="ibm", help="CRC flavor for patched frame")
    ap.add_argument("--hello-gap-ms", type=int, default=150)
    ap.add_argument("--idle-timeout", type=float, default=0.25)
    ap.add_argument("--max-wait", type=float, default=1.2)
    ap.add_argument("--out-dir", default="pakbus_runs/hello_read_test")
    args = ap.parse_args()

    outdir = pathlib.Path(args.out_dir); outdir.mkdir(parents=True, exist_ok=True)

    # Build the frame to send
    if args.frame_hex:
        tx = hex_to_bytes(args.frame_hex)
        if not tx:
            print("[ERR] --frame-hex is invalid/empty.", file=sys.stderr); sys.exit(2)
        print(f"[TX] using provided frame-hex ({len(tx)}B)")
    else:
        if not args.seed_hex or args.patch_start is None or args.patch_count is None:
            print("[ERR] If --frame-hex is not provided, you must supply --seed-hex, --patch-start, and --patch-count.", file=sys.stderr)
            sys.exit(2)
        try:
            tx = patch_seed(args.seed_hex, args.table, args.patch_count, args.patch_start, args.crc)
            print(f"[TX] patched from seed (len={len(tx)}B), CRC={args.crc}")
        except Exception as e:
            print(f"[ERR] patch_seed failed: {e}", file=sys.stderr); sys.exit(2)

    # Connect (IPv6)
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(10.0)
        print(f"[INFO] Connecting to [{args.addr}]:{args.port} ...")
        s.connect((args.addr, args.port, 0, 0))

        # Send hello
        s.sendall(HELLO)
        time.sleep(args.hello_gap_ms / 1000.0)
        rx_hello = recv_all(s, idle_timeout=args.idle_timeout, max_wait=args.max_wait)
        if rx_hello:
            print(f"[RX hello] {len(rx_hello)}B: {rx_hello.hex()}")
        else:
            print("[RX hello] 0B")

        # Send read/replay
        s.sendall(tx)
        rx = recv_all(s, idle_timeout=args.idle_timeout, max_wait=args.max_wait)
        print(f"[RX] total {len(rx)}B")

    # Save raw
    raw_path = outdir / "rx_raw.bin"
    raw_path.write_bytes(rx)
    print(f"[SAVE] raw -> {raw_path}")

    # Parse BD frames
    frames = split_bd_frames(rx)
    acks, datas = [], []
    for f in frames:
        if frame_is_ack(f):
            acks.append(f)
        elif frame_is_data(f):
            datas.append(f)

    print(f"[PARSE] frames={len(frames)} acks={len(acks)} datas={len(datas)}")
    if acks:
        print(f"  first ACK: {acks[0].hex()}")
    if datas:
        # Save the longest data frame
        datas.sort(key=len, reverse=True)
        best = datas[0]
        data_file = outdir / f"data_best_{len(best)}B.hex"
        data_file.write_bytes(best)
        print(f"[DATA] saved longest data -> {data_file}")
    else:
        print("[DATA] none")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)