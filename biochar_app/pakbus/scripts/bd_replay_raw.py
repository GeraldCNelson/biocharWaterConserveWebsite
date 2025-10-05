#!/usr/bin/env python3
import argparse, socket, time, sys, binascii, textwrap, pathlib

HELLO_FRAME = bytes.fromhex("bd ef ff 10 01 0f ff 00 01 0e 00 dd f0 bd")

def parse_hex(s: str) -> bytes:
    s = s.strip().lower().replace(" ", "").replace("0x", "")
    if any(c not in "0123456789abcdef" for c in s):
        raise argparse.ArgumentTypeError("hex string contains non-hex characters")
    if len(s) % 2:
        raise argparse.ArgumentTypeError("hex string must have even number of nibbles")
    return bytes.fromhex(s)

def hexify(b: bytes) -> str:
    return " ".join(textwrap.wrap(binascii.hexlify(b).decode("ascii"), 2))

def recv_all(sock: socket.socket, idle_timeout: float, max_bytes: int) -> bytes:
    sock.settimeout(idle_timeout)
    chunks = []
    total = 0
    while True:
        try:
            data = sock.recv(min(65535, max_bytes - total))
            if not data:
                break
            chunks.append(data)
            total += len(data)
            if total >= max_bytes:
                break
        except socket.timeout:
            break
    return b"".join(chunks)

def main():
    ap = argparse.ArgumentParser(description="Replay raw PakBus frame(s) over IPv6 TCP.")
    ap.add_argument("--addr", required=True, help="IPv6 address/hostname of CR800")
    ap.add_argument("--port", type=int, required=True, help="TCP port (e.g. 6785)")
    ap.add_argument("--hex", action="append", required=True,
                    help="Raw frame hex (include BD … BD). May be given multiple times.")
    ap.add_argument("--hello", action="store_true", help="Send PC400-style Hello first")
    ap.add_argument("--hello-gap-ms", type=int, default=300, help="Sleep after Hello (ms)")
    ap.add_argument("--sleep-ms", type=int, default=150, help="Sleep between frames (ms)")
    ap.add_argument("--connect-timeout", type=float, default=6.0, help="Connect timeout (s)")
    ap.add_argument("--idle-timeout", type=float, default=1.2, help="RX idle timeout (s)")
    ap.add_argument("--max-rx", type=int, default=65536, help="Max bytes to read before stopping")
    ap.add_argument("--out", default="", help="Optional file to save raw RX bytes")
    args = ap.parse_args()

    frames = [parse_hex(h) for h in args.hex]

    # Connect IPv6 TCP
    print(f"[CONNECT] [{args.addr}]:{args.port}")
    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.settimeout(args.connect_timeout)
    try:
        sock.connect((args.addr, args.port, 0, 0))
    except Exception as e:
        print(f"[ERROR] connect failed: {e}")
        sys.exit(2)

    try:
        if args.hello:
            print(f"[TX hello] {len(HELLO_FRAME)}B: {hexify(HELLO_FRAME)}")
            sock.sendall(HELLO_FRAME)
            rx = recv_all(sock, args.idle_timeout, args.max_rx)
            if rx:
                print(f"[RX after hello] {len(rx)}B")
                print(hexify(rx))

        for i, fr in enumerate(frames, 1):
            print(f"[TX {i}] {len(fr)}B: {hexify(fr)}")
            sock.sendall(fr)
            rx = recv_all(sock, args.idle_timeout, args.max_rx)
            if rx:
                print(f"[RX {i}] {len(rx)}B")
                print(hexify(rx))
            else:
                print(f"[RX {i}] 0B (none)")

        if args.out:
            # If multiple RX bursts happened, we already printed them; capture the last one to file
            # For completeness, re-read any stragglers then save everything we can get now.
            tail = recv_all(sock, args.idle_timeout_timeout, args.max_rx_rx)
            if tail:
                rx_bytes = tail
                print(f"[RX tail] {len(rx_bytes)}B")
                print(hexify(rx_bytes))
            else:
                rx_bytes = b""

            outpath = pathlib.Path(args.out)
            outpath.parent.mkdir(parents=True, exist_ok=True)
            outpath.write_bytes(rx_bytes)
            print(f"[SAVED] {len(rx_bytes)}B -> {outpath}")

    finally:
        sock.close()
        print("[CLOSE] socket closed")

if __name__ == "__main__":
    main()