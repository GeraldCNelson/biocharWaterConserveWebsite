#!/usr/bin/env python3
import argparse, socket, struct, time, binascii

# --- CRCs ---
def crc_ibm(data: bytes) -> int:
    crc = 0x0000
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x8005) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc

def crc_ccitt(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc

def wrap(core: bytes, mode: str) -> bytes:
    c = crc_ibm(core) if mode == "ibm" else crc_ccitt(core)
    return b"\xbd" + core + struct.pack("<H", c) + b"\xbd"

# --- Hello (same short “neighbor” hello you always see) ---
HELLO_CORE = bytes.fromhex("ef ff 10 01 0f ff 00 01 0e 00 dd f0")
HELLO = b"\xbd" + HELLO_CORE + b"\xbd"

def say_hello(sock, gap_ms=300):
    sock.sendall(HELLO)
    time.sleep(gap_ms/1000)

def recv_all(sock, timeout=1.8, max_len=65536) -> bytes:
    sock.settimeout(timeout)
    out = b""
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            out += chunk
            if len(out) >= max_len:
                break
    except socket.timeout:
        pass
    return out

# --- Payload (common across variants): FD 09, table, 00 00, start_secs, start_rec, count ---
def payload_getdata(table: int, start_secs: int, start_rec: int, count: int) -> bytes:
    p = bytes([0xFD, 0x09, table & 0xFF, 0x00, 0x00])
    p += struct.pack("<I", start_secs)
    p += struct.pack("<H", start_rec & 0xFFFF)
    p += struct.pack("<H", count & 0xFFFF)
    return p

# --- Variant builders (different header byte orderings we’ve seen in captures) ---
def v1(dest, src, control, payload):
    # Current “clean” guess (what we tried in v2):
    # AF FD [dest_lo dest_hi src_lo src_hi control] + payload
    return bytes([0xAF,0xFD,
                  dest & 0xFF, (dest>>8)&0xFF,
                  src & 0xFF,  (src>>8)&0xFF,
                  control & 0xFF]) + payload

def v2(dest, src, control, payload, txid=0x00):
    # AF FD [dest_lo dest_hi src_lo src_hi control txid] + payload
    return bytes([0xAF,0xFD,
                  dest & 0xFF, (dest>>8)&0xFF,
                  src & 0xFF,  (src>>8)&0xFF,
                  control & 0xFF, txid & 0xFF]) + payload

def v3(dest, src, control, payload, txid=0x00):
    # AF FD [dest_lo dest_hi src_lo src_hi] FD [control txid] + payload
    # (matches the “... 0f fd 00 01 ...” byte pattern we saw around service frames)
    return bytes([0xAF,0xFD,
                  dest & 0xFF, (dest>>8)&0xFF,
                  src & 0xFF,  (src>>8)&0xFF,
                  0xFD, control & 0xFF, txid & 0xFF]) + payload

def try_send(addr, port, frame, hello_gap_ms, idle_timeout, connect_timeout):
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(connect_timeout)
        s.connect((addr, port, 0, 0))
        say_hello(s, hello_gap_ms)
        # swallow the beacon
        _ = recv_all(s, timeout=1.2)
        s.sendall(frame)
        rx = recv_all(s, timeout=idle_timeout)
    return rx

def main():
    ap = argparse.ArgumentParser(description="PakBus minimal GetData v3 (small header/layout sweep)")
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--dest-id", type=int, default=1)
    ap.add_argument("--src-id",  type=int, default=4094)
    ap.add_argument("--table",   type=lambda x:int(x,0), default=0x11)
    ap.add_argument("--start-secs", type=int, default=0)
    ap.add_argument("--start-rec",  type=int, default=0)
    ap.add_argument("--count",      type=int, default=5)
    ap.add_argument("--hello-gap-ms", type=int, default=300)
    ap.add_argument("--idle-timeout", type=float, default=2.0)
    ap.add_argument("--connect-timeout", type=float, default=5.0)
    ap.add_argument("--crc", choices=["ibm","ccitt"], default="ibm")
    args = ap.parse_args()

    common_payload = payload_getdata(args.table, args.start_secs, args.start_rec, args.count)

    # A tiny, surgical set of variants that map to what we saw in your captures
    variants = [
        ("v1 ctl=0x9F", v1(args.dest_id, args.src_id, 0x9F, common_payload)),
        ("v1 ctl=0x1F", v1(args.dest_id, args.src_id, 0x1F, common_payload)),
        ("v2 ctl=0xFD tx=0x00", v2(args.dest_id, args.src_id, 0xFD, common_payload, 0x00)),
        ("v2 ctl=0xFD tx=0x01", v2(args.dest_id, args.src_id, 0xFD, common_payload, 0x01)),
        ("v3 ctl=0xFD tx=0x00", v3(args.dest_id, args.src_id, 0xFD, common_payload, 0x00)),
        ("v3 ctl=0x7D tx=0x00", v3(args.dest_id, args.src_id, 0x7D, common_payload, 0x00)),
    ]

    for name, core in variants:
        frame = wrap(core, args.crc)
        print(f"\n=== TRY {name} :: {len(frame)}B ===")
        print(f"TX: {binascii.hexlify(frame).decode()}")
        try:
            rx = try_send(args.addr, args.port, frame, args.hello_gap_ms, args.idle_timeout, args.connect_timeout)
            print(f"RX len={len(rx)}")
            if rx:
                print(binascii.hexlify(rx).decode())
            # Success heuristic: we got *more* than the 14B beacon after sending the frame.
            if len(rx) > 14:
                print("[HIT] got a non-beacon response; stop here.")
                break
        except Exception as e:
            print(f"[ERR] {e}")

if __name__ == "__main__":
    main()