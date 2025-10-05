#!/usr/bin/env python3
import socket
import argparse
import struct
import binascii
import time

# -------- CRCs --------

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

def wrap_frame(core: bytes, crc_mode: str) -> bytes:
    if crc_mode == "ibm":
        c = crc_ibm(core)
    else:
        c = crc_ccitt(core)
    return b"\xbd" + core + struct.pack("<H", c) + b"\xbd"

# -------- Hello (Neighbor discovery) --------
# This is the same short hello you've been seeing (the 14-byte echo).
# We send it to open link state and give the logger a beat to beacon back.

HELLO_CORE = bytes.fromhex("ef ff 10 01 0f ff 00 01 0e 00 dd f0")

def send_hello(sock, gap_ms: int = 300):
    # Raw hello is already framed (bd .. bd) and includes CRC per logger convention.
    hello = b"\xbd" + HELLO_CORE + b"\xbd"
    sock.sendall(hello)
    time.sleep(gap_ms / 1000.0)
    # Try to read whatever the logger emits (non-fatal if none)
    sock.settimeout(1.5)
    chunks = []
    try:
        while True:
            rx = sock.recv(4096)
            if not rx:
                break
            chunks.append(rx)
            if len(b"".join(chunks)) > 16384:
                break
    except socket.timeout:
        pass
    return b"".join(chunks)

# -------- GetData builder --------

def make_getdata_core(dest_id: int, src_id: int, control: int,
                      table_id: int, start_secs: int, start_rec: int, count: int) -> bytes:
    """
    Very small PakBus request core:
      [AF FD]   -- address marker used in your captured frames
      [dest_lo dest_hi src_lo src_hi]
      [control] -- include a reasonable control/transaction byte (0x9F often works)
      [FD 09]   -- GetData service
      [table_id] [00] [00]
      [start_secs LE32] [start_rec LE16] [count LE16]
    """
    addr = bytes([0xAF, 0xFD,
                  dest_id & 0xFF, (dest_id >> 8) & 0xFF,
                  src_id & 0xFF,  (src_id  >> 8) & 0xFF,
                  control & 0xFF])
    payload = bytes([0xFD, 0x09, table_id & 0xFF, 0x00, 0x00])
    payload += struct.pack("<I", start_secs)
    payload += struct.pack("<H", start_rec & 0xFFFF)
    payload += struct.pack("<H", count & 0xFFFF)
    return addr + payload

# -------- Main --------

def main():
    ap = argparse.ArgumentParser(description="Minimal-but-real PakBus GetData probe (hello + addressed request)")
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--dest-id", type=int, default=1, help="Logger PakBus ID (usually 1)")
    ap.add_argument("--src-id",  type=int, default=4094, help="Our PakBus ID (neighbor; 4094 is common)")
    ap.add_argument("--control", type=lambda x: int(x,0), default=0x9F, help="Control/transaction byte (try 0x9F, 0x1F)")
    ap.add_argument("--table",   type=lambda x: int(x,0), default=0x11)
    ap.add_argument("--count",   type=int, default=5)
    ap.add_argument("--start-secs", type=int, default=0, help="1990-epoch seconds (0 for start)")
    ap.add_argument("--start-rec",  type=int, default=0, help="record within second (0)")
    ap.add_argument("--crc", choices=["ibm","ccitt"], default="ibm")
    ap.add_argument("--hello-gap-ms", type=int, default=300)
    ap.add_argument("--idle-timeout", type=float, default=2.0)
    ap.add_argument("--connect-timeout", type=float, default=5.0)
    args = ap.parse_args()

    # Build request
    core = make_getdata_core(args.dest_id, args.src_id, args.control,
                             args.table, args.start_secs, args.start_rec, args.count)
    frame = wrap_frame(core, args.crc)

    print(f"[HELLO] sending; then sleeping {args.hello_gap_ms} ms")
    print(f"[TX getdata] {len(frame)}B: {binascii.hexlify(frame).decode()}")

    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(args.connect_timeout)
        s.connect((args.addr, args.port, 0, 0))

        # 1) Hello
        rx_hello = send_hello(s, gap_ms=args.hello_gap_ms)
        if rx_hello:
            print(f"[RX hello] {len(rx_hello)}B: {binascii.hexlify(rx_hello).decode()}")

        # 2) GetData
        s.sendall(frame)

        # 3) Read response
        s.settimeout(args.idle_timeout)
        rx = b""
        try:
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                rx += chunk
                # A single GetData reply tends to be < 2 KB; stop if it goes long or idle timeout hits
                if len(rx) > 65536:
                    break
        except socket.timeout:
            pass

    print(f"[RX] {len(rx)}B")
    if rx:
        print(binascii.hexlify(rx).decode())

if __name__ == "__main__":
    main()