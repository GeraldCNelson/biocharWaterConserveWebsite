#!/usr/bin/env python3
import argparse, socket, struct, time, binascii

# --- CRCs ---
def crc_ibm(data: bytes) -> int:
    crc = 0x0000
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x8005) & 0xFFFF if (crc & 0x8000) else ((crc << 1) & 0xFFFF)
    return crc

def crc_ccitt(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if (crc & 0x8000) else ((crc << 1) & 0xFFFF)
    return crc

def wrap(core: bytes, mode: str) -> bytes:
    c = crc_ibm(core) if mode == "ibm" else crc_ccitt(core)
    return b"\xbd" + core + struct.pack("<H", c) + b"\xbd"

# short “hello”
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

# ---- payloads ----
def pd_getdata(table: int, start_secs: int, start_rec: int, count: int) -> bytes:
    # FD 09 <table> 00 00 <start_secs:le u32> <start_rec:le u16> <count:le u16>
    p = bytes([0xFD, 0x09, table & 0xFF, 0x00, 0x00])
    p += struct.pack("<I", start_secs)
    p += struct.pack("<H", start_rec & 0xFFFF)
    p += struct.pack("<H", count & 0xFFFF)
    return p

def shim_fd2003(txid: int) -> bytes:
    # Minimal “network shim” we keep seeing in captures: FD 20 03 <txid>
    # (0x20 looks like a small link/net header; 0x03 == length; next byte behaves like a transaction)
    return bytes([0xFD, 0x20, 0x03, txid & 0xFF])

# ---- header layouts (mirroring your RX patterns) ----
def hdr_le(dest_id: int, src_id: int, control: int) -> bytes:
    # Matches “af fd 00 01 1f …” in your dumps (dest LE then control)
    return bytes([0xAF, 0xFD,
                  dest_id & 0xFF, (dest_id >> 8) & 0xFF,
                  src_id  & 0xFF, (src_id  >> 8) & 0xFF,
                  control & 0xFF])

def hdr_le_with_fd(dest_id: int, src_id: int, control: int) -> bytes:
    # Sometimes we see an extra FD after control in RX cores; try it
    return bytes([0xAF, 0xFD,
                  dest_id & 0xFF, (dest_id >> 8) & 0xFF,
                  src_id  & 0xFF, (src_id  >> 8) & 0xFF,
                  control & 0xFF, 0xFD])

def try_send(addr, port, frame, hello_gap_ms, idle_timeout, connect_timeout):
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(connect_timeout)
        s.connect((addr, port, 0, 0))
        say_hello(s, hello_gap_ms)
        _ = recv_all(s, timeout=1.2)  # swallow beacon
        s.sendall(frame)
        rx = recv_all(s, timeout=idle_timeout)
    return rx

def main():
    ap = argparse.ArgumentParser(description="PakBus minimal GetData v4 (insert FD 20 03 shim; header as seen in RX)")
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--dest-id", type=int, default=1)
    ap.add_argument("--src-id",  type=int, default=4094)
    ap.add_argument("--control", type=lambda x:int(x,0), default=0x1F)  # 0x1F is common in your RX
    ap.add_argument("--table",   type=lambda x:int(x,0), default=0x11)
    ap.add_argument("--start-secs", type=int, default=0)
    ap.add_argument("--start-rec",  type=int, default=0)
    ap.add_argument("--count",      type=int, default=5)
    ap.add_argument("--txid",       type=lambda x:int(x,0), default=0x89)  # seen a lot in your RX
    ap.add_argument("--hello-gap-ms", type=int, default=300)
    ap.add_argument("--idle-timeout", type=float, default=2.0)
    ap.add_argument("--connect-timeout", type=float, default=5.0)
    ap.add_argument("--crc", choices=["ibm","ccitt"], default="ibm")
    args = ap.parse_args()

    payload = pd_getdata(args.table, args.start_secs, args.start_rec, args.count)
    shim = shim_fd2003(args.txid)

    variants = []
    # 1) hdr_le + shim + getdata  (closest to RX: … 1f fd 20 03 <tx> fd 09 …)
    variants.append(("le+shim+getdata", hdr_le(args.dest_id, args.src_id, args.control) + shim + payload))
    # 2) hdr_le_with_fd + shim + getdata  (some RX showed an extra FD after control)
    variants.append(("leFD+shim+getdata", hdr_le_with_fd(args.dest_id, args.src_id, args.control) + shim + payload))
    # 3) hdr_le + getdata (no shim)  (control stays same but no FD 20 03)
    variants.append(("le+getdata", hdr_le(args.dest_id, args.src_id, args.control) + payload))
    # 4) hdr_le_with_fd + getdata (no shim)
    variants.append(("leFD+getdata", hdr_le_with_fd(args.dest_id, args.src_id, args.control) + payload))

    for name, core in variants:
        frame = wrap(core, args.crc)
        print(f"\n=== TRY {name} :: {len(frame)}B ===")
        print("TX:", binascii.hexlify(frame).decode())
        try:
            rx = try_send(args.addr, args.port, frame, args.hello_gap_ms, args.idle_timeout, args.connect_timeout)
            print(f"RX len={len(rx)}")
            if rx:
                print(binascii.hexlify(rx).decode())
            if len(rx) > 14:
                print("[HIT] non-beacon reply; stop.")
                break
        except Exception as e:
            print(f"[ERR] {e}")

if __name__ == "__main__":
    main()