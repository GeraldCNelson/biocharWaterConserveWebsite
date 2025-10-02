#!/usr/bin/env python3
import argparse, socket, time, sys

# ---------- utils ----------
def hexdump(b: bytes) -> str:
    return " ".join(f"{x:02x}" for x in b)

def parse_hex_bytes(s: str) -> bytes:
    s = s.strip().replace(" ", "").replace("_", "")
    if len(s) % 2: raise ValueError("hex string must have even length")
    return bytes.fromhex(s)

def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if (crc & 1) else (crc >> 1)
    return crc & 0xFFFF

def bd_frame(inner: bytes) -> bytes:
    crc = crc16_modbus(inner)
    return bytes([0xBD]) + inner + bytes([(crc>>8)&0xFF, crc & 0xFF, 0xBD])

def split_bd_frames(buf: bytes):
    frames = []
    cur = bytearray(); in_f = False
    for b in buf:
        if not in_f:
            if b == 0xBD:
                cur = bytearray([0xBD]); in_f = True
        else:
            cur.append(b)
            if b == 0xBD:
                frames.append(bytes(cur)); in_f = False
    return frames

def bd_strip(frame: bytes):
    if len(frame) < 4 or frame[0]!=0xBD or frame[-1]!=0xBD: return None
    inner = frame[1:-3]
    crc_hi, crc_lo = frame[-3], frame[-2]
    calc = crc16_modbus(inner)
    return inner if (crc_hi == (calc>>8)&0xFF and crc_lo == (calc & 0xFF)) else None

def recv_until_quiet(sock: socket.socket, first_timeout: float, grace_ms: int) -> bytes:
    sock.settimeout(first_timeout)
    buf = bytearray()
    try:
        chunk = sock.recv(65535)
    except Exception:
        return bytes()
    buf += chunk
    if not chunk:
        return bytes(buf)
    end_by = time.time() + (grace_ms / 1000.0)
    while time.time() < end_by:
        sock.settimeout(0.08)
        try:
            c = sock.recv(65535)
            if c:
                buf += c
                end_by = time.time() + (grace_ms / 1000.0)
        except Exception:
            pass
    return bytes(buf)

# ---------- read builders ----------
def rd_v1_simple(leaf, table_id, start_rec, count):
    # 2C LEAF 00 00 00 01 00 FF FF 01 00
    return bytes([
        0x2C, leaf,
        0x00,0x00,
        (table_id>>8)&0xFF, table_id&0xFF,
        0x00,0x01,
        (start_rec>>8)&0xFF, start_rec&0xFF,
        0x01, 0x00,
    ])

def rd_v2_altfield(leaf, table_id, start_rec, count):
    # 2C LEAF 00 00 00 01 00 01 FF FF 00 01
    return bytes([
        0x2C, leaf,
        0x00,0x00,
        (table_id>>8)&0xFF, table_id&0xFF,
        0x00,0x01,
        0xFF,0xFF,
        0x00,0x01,
    ])

READS = [("V1", rd_v1_simple), ("V2", rd_v2_altfield)]

# ---------- prefix modes ----------
def apply_prefix(mode: str, prefix: bytes, inner: bytes) -> bytes:
    if not prefix or mode == "none":
        return inner
    if mode == "pre_router":
        return prefix + inner
    if mode == "inner":
        return prefix + inner   # as first bytes of inner payload
    raise ValueError("bad prefix mode")

# ---------- IO helpers ----------
def hello_and_read(addr, port, hello_inner, read_inner, first_wait=3.0, grace_ms=800):
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect((addr, port))
    time.sleep(0.35)
    s.sendall(bd_frame(hello_inner))
    hello = recv_until_quiet(s, first_wait, grace_ms)
    frames = split_bd_frames(hello)
    if frames:
        print(f"    ├─ hello[0]: {hexdump(frames[0])}")
    time.sleep(0.18)
    pkt = bd_frame(read_inner)
    print(f"    ├─ send read: {hexdump(pkt)}")
    s.sendall(pkt)
    r = recv_until_quiet(s, first_wait, grace_ms)
    s.close()
    return r

def run_matrix(addr, port, leaf, table_id, start_rec, count, hello_inner, router_hdr):
    successes = 0
    for (rname, rbuild) in READS:
        base = rbuild(leaf, table_id, start_rec, count)
        for mode in ["none", "pre_router", "inner"]:
            variant_name = f"{rname}+{mode}"
            for tran in [0x90, 0x91, 0x92]:
                # try with/without a 1-byte TRAN immediately after opcode (some firmwares)
                for place in ["no_txn", "txn_after_op"]:
                    if place == "no_txn":
                        inner = base
                    else:
                        inner = bytes([0x2C, tran, leaf]) + base[2:]

                    inner2 = apply_prefix(mode, router_hdr, inner)
                    print(f"\n=== {variant_name} {place} TRAN=0x{tran:02x} ===")
                    print(f"    ├─ read inner: {hexdump(inner2)}")
                    try:
                        r = hello_and_read(addr, port, hello_inner, inner2)
                    except Exception as e:
                        print(f"    └─ exception: {e}")
                        continue
                    if not r:
                        print("    └─ no reply bytes")
                        continue
                    frames = split_bd_frames(r)
                    print(f"    ├─ got {len(frames)} BD frame(s); raw {len(r)} bytes")
                    for i, fr in enumerate(frames[:3]):
                        inner = bd_strip(fr)
                        print(f"    │   frame[{i}] {len(fr)}B: {hexdump(fr)}")
                        if inner:
                            print(f"    │   → inner {len(inner)}B: {hexdump(inner)}")
                    if frames:
                        successes += 1
                        print(f"[OK] Reply present on {variant_name} {place} TRAN=0x{tran:02x}")
                        return successes
    return successes

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, default=6785)
    ap.add_argument("--leaf", type=int, default=3)
    ap.add_argument("--table-id", type=lambda x:int(x,0), default=0x0001)
    ap.add_argument("--start-rec", type=lambda x:int(x,0), default=0xFFFF)
    ap.add_argument("--count", type=lambda x:int(x,0), default=0x0001)
    ap.add_argument("--pre-hex", default="90 01 0f fd 73 d3", help="hello inner bytes")
    ap.add_argument("--router-hex", default="A0 01 6F FD 10 00 30 FF D0 90",
                    help="10-byte router header to try in first app frame")
    args = ap.parse_args()

    hello_inner = parse_hex_bytes(args.pre_hex)
    router_hdr = parse_hex_bytes(args.router_hex)

    print(f"[INFO] Using hello inner: {hexdump(hello_inner)}")
    print(f"[INFO] Router header guess: {hexdump(router_hdr)}")
    print(f"[INFO] Target [{args.addr}]:{args.port} leaf={args.leaf} table=0x{args.table_id:04x}")

    try:
        ok = run_matrix(args.addr, args.port, args.leaf, args.table_id,
                        args.start_rec, args.count, hello_inner, router_hdr)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")
        ok = 0
    print(f"\n[SUMMARY] success variants: {ok}")

if __name__ == "__main__":
    main()