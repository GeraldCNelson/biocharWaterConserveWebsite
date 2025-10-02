#!/usr/bin/env python3
import argparse, socket, time, sys

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
            if (crc & 1): crc = (crc >> 1) ^ 0xA001
            else:         crc >>= 1
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
    hi, lo = frame[-3], frame[-2]
    calc = crc16_modbus(inner)
    return inner if (hi == (calc>>8)&0xFF and lo == (calc & 0xFF)) else None

def recv_until_quiet(sock: socket.socket, first_timeout: float, grace_ms: int) -> bytes:
    sock.settimeout(first_timeout)
    try:
        chunk = sock.recv(65535)
    except Exception:
        return b""
    buf = bytearray(chunk)
    if not chunk:
        return bytes(buf)
    end_by = time.time() + (grace_ms/1000.0)
    while time.time() < end_by:
        sock.settimeout(0.08)
        try:
            c = sock.recv(65535)
            if c:
                buf += c
                end_by = time.time() + (grace_ms/1000.0)
        except Exception:
            pass
    return bytes(buf)

def main():
    ap = argparse.ArgumentParser(description="Replay PC400 app frame after hello")
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, default=6785)
    ap.add_argument("--pre-hex", default="90 01 0f fd 73 d3",
                    help="hello inner hex (no BD/CRC)")
    ap.add_argument("--inner-hex", required=True,
                    help="captured inner hex for the first PC->logger app frame (no BD/CRC)")
    ap.add_argument("--router-hex", default="",
                    help="OPTIONAL: if your captured inner actually begins with a 10B router header, "
                         "you can put those bytes here and pass only the app payload in --inner-hex. "
                         "If your inner already includes them, leave this blank.")
    ap.add_argument("--pre-wait-ms", type=int, default=400)
    ap.add_argument("--inter-gap-ms", type=int, default=180)
    ap.add_argument("--hello-recv-grace-ms", type=int, default=800)
    ap.add_argument("--reply-recv-grace-ms", type=int, default=1500)
    args = ap.parse_args()

    pre = parse_hex_bytes(args.pre_hex)
    inner = parse_hex_bytes(args.inner_hex)
    router = parse_hex_bytes(args.router_hex) if args.router_hex else b""

    print(f"[INFO] hello inner: {hexdump(pre)}")
    if router:
        print(f"[INFO] router hdr: {hexdump(router)}")
    print(f"[INFO] app inner to send: {hexdump(inner)}")
    print(f"[INFO] target [{args.addr}]:{args.port}")

    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(8.0)
    s.connect((args.addr, args.port))

    if args.pre_wait_ms > 0:
        time.sleep(args.pre_wait_ms/1000.0)

    hello_pkt = bd_frame(pre)
    print(f"[TX] hello: {hexdump(hello_pkt)}")
    s.sendall(hello_pkt)
    h = recv_until_quiet(s, 4.0, args.hello_recv_grace_ms)
    frames = split_bd_frames(h)
    print(f"[RX] hello total bytes={len(h)}, frames={len(frames)}")
    if frames:
        print(f"     hello[0]: {hexdump(frames[0])}")

    if args.inter_gap_ms > 0:
        time.sleep(args.inter_gap_ms/1000.0)

    app_inner = router + inner
    app_pkt = bd_frame(app_inner)
    print(f"[TX] app : {hexdump(app_pkt)}")
    s.sendall(app_pkt)

    r = recv_until_quiet(s, 5.0, args.reply_recv_grace_ms)
    s.close()
    print(f"[RX] app-reply bytes={len(r)}")
    if not r:
        print("[WARN] no reply bytes")
        sys.exit(2)

    r_frames = split_bd_frames(r)
    print(f"[RX] app-reply frames={len(r_frames)}")
    for i, fr in enumerate(r_frames[:6]):
        inner_r = bd_strip(fr)
        print(f"   frame[{i}] {len(fr)}B: {hexdump(fr)}")
        if inner_r is None:
            print("      (CRC mismatch)")
        else:
            print(f"      inner {len(inner_r)}B: {hexdump(inner_r)}")

if __name__ == "__main__":
    main()