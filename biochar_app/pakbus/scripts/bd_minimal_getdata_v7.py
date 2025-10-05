#!/usr/bin/env python3
import argparse, socket, struct, time, sys

def crc_ibm(bs: bytes) -> int:
    crc = 0xFFFF
    for b in bs:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def crc_ccitt(bs: bytes) -> int:
    crc = 0xFFFF
    for b in bs:
        crc ^= (b << 8) & 0xFFFF
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc & 0xFFFF

def wrap(frame_wo_crc: bytes, crc_mode: str) -> bytes:
    if crc_mode == "ibm":
        c = crc_ibm(frame_wo_crc)
    else:
        c = crc_ccitt(frame_wo_crc)
    return b"\xbd" + frame_wo_crc + struct.pack(">H", c) + b"\xbd"

def send_recv(sock, data: bytes, idle_timeout: float):
    sock.sendall(data)
    t0 = time.time()
    buf = bytearray()
    sock.settimeout(idle_timeout)
    while True:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf.extend(chunk)
            # small idle tail
            if time.time() - t0 > idle_timeout:
                break
        except socket.timeout:
            break
    return bytes(buf)

def hexdump(bs: bytes, maxlen=64):
    if not bs:
        return "(none)"
    s = bs.hex()
    if len(s) > maxlen:
        s = s[:maxlen] + " …"
    # group in pairs
    return " ".join(s[i:i+2] for i in range(0, len(s), 2))

def build_header(dest_id: int, src_id: int, control: int) -> bytes:
    # BD | AF FD | dest | src | control | FD |
    return b"\xaf\xfd" + bytes([dest_id & 0xFF, src_id & 0xFF, control & 0xFF, 0xFD])

def pkt_neighbor(dest: int, src: int, control: int, txid: int) -> bytes:
    """
    Minimal 'neighbor/hello' service try: service 0x70 0x01.
    Body: 0x70 0x01 0x0F 0xFD 0x00 0x01 0x09 <txid> 0x01 0x01 0xFF 0xFD
    (We mirror the short 17B pattern seen in your logs, but as a request from us.)
    """
    # header AF FD dest src control FD
    hdr = build_header(dest, src, control)
    body = bytes([
        0x70, 0x01,  # service / subservice
        0x0F, 0xFD,  # control-ish nibble seen in captures
        0x00, 0x01,  # route-ish short (match your frames' 00 01)
        0x09, txid & 0xFF,  # piggyback txid token (seen)
        0x01, 0x01, 0xFF, 0xFD  # tail seen in your 17B frames
    ])
    return hdr + body

def pkt_getdata(dest: int, src: int, control: int, table: int, count: int, shim_fd=False) -> bytes:
    # Service 0x09 "GetData": [0x09][table][startKey(6B zeros)][count(2B LE? here 00 05)]
    body = bytearray()
    body.append(0x09)
    body.append(table & 0xFF)
    body.extend(b"\x00\x00\x00\x00\x00\x00")
    body.extend(struct.pack(">H", count))  # stick to big-endian count like we used earlier
    core = build_header(dest, src, control)
    if shim_fd:
        core += b"\xFD"
    return core + bytes(body)

def pkt_linkstate(dest: int, src: int, control: int, txid: int) -> bytes:
    """
    Verbatim-ish link-state ‘20 03 …’ modeled after your captures:
      20 03 89 <txid> 00 00 02 00 01 0F 76 01 43 43
    (we keep it short; the logger often replies with a beacon if it accepts)
    """
    hdr = build_header(dest, src, control)
    body = bytes([0x20, 0x03, 0x89, txid & 0xFF, 0x00, 0x00, 0x02, 0x00, 0x01, 0x0F, 0x76, 0x01, 0x43, 0x43])
    return hdr + body

def pkt_hello_short() -> bytes:
    # The 14B hello your logger always sends (we can send it too)
    core = bytes.fromhex("efff10010fff00010e00ddf0")
    return b"\xbd" + core + b"\xbd"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--table", type=lambda x:int(x,0), required=True)
    ap.add_argument("--count", type=int, default=5)
    ap.add_argument("--our-id", type=lambda x:int(x,0), default=0x01)
    ap.add_argument("--dest-id", type=lambda x:int(x,0), default=0x00)
    ap.add_argument("--hello", action="store_true")
    ap.add_argument("--txid-max", type=int, default=6)
    ap.add_argument("--crc", choices=["ibm","ccitt"], default="ibm")
    ap.add_argument("--connect-timeout", type=float, default=8.0)
    ap.add_argument("--idle-timeout", type=float, default=0.6)
    ap.add_argument("--sleep-ms", type=int, default=250)
    args = ap.parse_args()

    crc_mode = args.crc

    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(args.connect-timeout if False else args.connect_timeout)  # safe attr
        s.connect((args.addr, args.port, 0, 0))

        for txid in range(1, args.txid_max+1):
            print(f"\n=== TXID {txid:02d} ===")

            # A) optional short hello
            if args.hello:
                hello = pkt_hello_short()
                print(f"[TX hello] {len(hello):02d}B:", hello.hex())
                s.sendall(hello)
                time.sleep(args.sleep_ms/1000.0)
                try:
                    s.settimeout(args.idle_timeout)
                    rx = s.recv(4096)
                except socket.timeout:
                    rx = b""
                print("[RX hello]", f"{len(rx):02d}B:", hexdump(rx))
                time.sleep(args.sleep_ms/1000.0)

            # B) Neighbor ping with ctl=0x0F then 0x1F
            for ctl in (0x0F, 0x1F):
                nb = pkt_neighbor(args.dest_id, args.our_id, ctl, txid)
                nbw = wrap(nb, crc_mode)
                print(f"[TX neighbor ctl=0x{ctl:02X}] {len(nbw):02d}B:", hexdump(nbw))
                rx = send_recv(s, nbw, args.idle_timeout)
                print(f"[RX neighbor ctl=0x{ctl:02X}] {len(rx):02d}B:", hexdump(rx))
                time.sleep(args.sleep_ms/1000.0)

            # C) Link-state teaser (ctl=0x0F)
            ls = pkt_linkstate(args.dest_id, args.our_id, 0x0F, txid)
            lsw = wrap(ls, crc_mode)
            print(f"[TX linkstate] {len(lsw):02d}B:", hexdump(lsw))
            rx = send_recv(s, lsw, args.idle_timeout)
            print(f"[RX linkstate] {len(rx):02d}B:", hexdump(rx))
            time.sleep(args.sleep_ms/1000.0)

            # D) GetData attempts (ctl=0x0F and 0x1F, with/without FD shim)
            for ctl in (0x0F, 0x1F):
                for shim in (False, True):
                    gd = pkt_getdata(args.dest_id, args.our_id, ctl, args.table, args.count, shim_fd=shim)
                    gdw = wrap(gd, crc_mode)
                    print(f"[TX getdata ctl=0x{ctl:02X} shim={int(shim)}] {len(gdw):02d}B:", hexdump(gdw))
                    rx = send_recv(s, gdw, args.idle_timeout)
                    print(f"[RX getdata ctl=0x{ctl:02X} shim={int(shim)}] {len(rx):02d}B:", hexdump(rx))
                    time.sleep(args.sleep_ms/1000.0)

if __name__ == "__main__":
    main()