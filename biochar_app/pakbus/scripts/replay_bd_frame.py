#!/usr/bin/env python3
import argparse, socket, time, sys
from biochar_app.pakbus.utils.frame import bd_wrap
from biochar_app.pakbus.utils.hex import hexdump

def parse_hex_stream(s: str) -> bytes:
    s = s.strip().replace(" ", "").replace("\n","").replace("\t","")
    return bytes.fromhex(s)

def recv_all_quiet(sock: socket.socket, first_timeout=2.0, grace=1.5) -> bytes:
    sock.settimeout(first_timeout)
    buf = bytearray()
    try:
        chunk = sock.recv(65535)
    except socket.timeout:
        chunk = b""
    buf += chunk
    if not chunk:
        return bytes(buf)
    end = time.time() + grace
    while time.time() < end:
        sock.settimeout(0.05)
        try:
            chunk = sock.recv(65535)
        except socket.timeout:
            chunk = b""
        if chunk:
            buf += chunk
            end = time.time() + grace
    return bytes(buf)


def main():
    ap = argparse.ArgumentParser(description="Replay multiple BD-framed packets from capture.")
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, default=6785)
    ap.add_argument("--hello", default="90 01 0f fd 73 d3",
                    help="hello inner bytes (no BD/CRC)")
    ap.add_argument("--frames-file", required=True,
                    help="Text file with one full BD…BD hex string per line")
    ap.add_argument("--pre-wait-ms", type=int, default=300)
    ap.add_argument("--gap-ms", type=int, default=400)
    ap.add_argument("--post-grace-ms", type=int, default=2500)
    args = ap.parse_args()

    # Load candidate frames
    with open(args.frames_file) as f:
        candidates = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]

    if not candidates:
        print("[ERR] No frames found in file.")
        sys.exit(1)

    # connect
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(8.0)
    s.connect((args.addr, args.port))
    print("[OK] TCP connected.")

    # send hello (wrap with BD + CRC16)
    inner = parse_hex_stream(args.hello)
    frame = bd_wrap(inner)
    time.sleep(args.pre_wait_ms/1000.0)
    s.sendall(frame)
    print(f"[TX] hello {len(frame)}B: {hexdump(frame)}")
    rx = recv_all_quiet(s, first_timeout=4.0, grace=args.post_grace_ms/1000.0)
    print(f"[RX] after hello: {len(rx)}B")

    # loop over candidate frames
    for idx, hexline in enumerate(candidates, 1):
        try:
            req = parse_hex_stream(hexline)
        except Exception as e:
            print(f"[SKIP] Frame {idx}: invalid hex ({e})")
            continue
        if not (len(req) >= 4 and req[0] == 0xBD and req[-1] == 0xBD):
            print(f"[SKIP] Frame {idx}: not full BD…BD")
            continue

        print(f"\n=== Candidate {idx} ===")
        time.sleep(args.gap_ms/1000.0)
        s.sendall(req)
        print(f"[TX] replay {len(req)}B: {hexdump(req)}")

        rx2 = recv_all_quiet(s, first_timeout=6.0, grace=args.post_grace_ms/1000.0)
        print(f"[RX] len={len(rx2)}B")
        if rx2:
            print(f"[RX] hex: {hexdump(rx2)}")
        else:
            print("[WARN] no reply.")

if __name__ == "__main__":
    main()