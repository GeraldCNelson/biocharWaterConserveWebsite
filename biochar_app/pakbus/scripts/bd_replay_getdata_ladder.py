#!/usr/bin/env python3
import argparse, socket, binascii, time, sys

HELLO = bytes.fromhex("bdefff10010fff00010e00ddf0bd")

def send(sock, blob, label):
    sock.sendall(blob)
    print(f"[TX {label}] {len(blob)}B: {binascii.hexlify(blob).decode()}")
    sock.settimeout(2.5)
    try:
        rx = sock.recv(2048)
        if rx:
            print(f"[RX {label}] {len(rx)}B")
            # pretty print in 16-byte rows
            hx = binascii.hexlify(rx).decode()
            for i in range(0, len(hx), 32):
                print(hx[i:i+32])
        else:
            print(f"[RX {label}] 0B (none)")
    except socket.timeout:
        print(f"[RX {label}] timeout")
    print()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--hello", action="store_true")
    ap.add_argument("--sleep-ms", type=int, default=350)
    ap.add_argument("--base-key", type=lambda x: int(x,16), required=True,
                    help="hex like FFFF2810 (no 0x needed)")
    ap.add_argument("--delta", type=int, default=1, help="+/- step")
    ap.add_argument("--n-up", type=int, default=3, help="# steps up from base")
    ap.add_argument("--n-down", type=int, default=3, help="# steps down from base")
    ap.add_argument("--repeat", type=int, default=1)
    args = ap.parse_args()

    def key_bytes(u32):
        return u32.to_bytes(4, "big")  # as seen in your pcap (FF FF 28 10)

    # Compose GetData exactly like your pcap (length/payload bytes preserved)
    def frame_for(key_u32):
        kb = key_bytes(key_u32)
        core = b"\xbd\xa0\x01\x6f\xfd\x00\x01\x0f\xfd\x09\x05\x01\x02" + kb + b"\xbd"
        return core

    with socket.create_connection((args.addr, args.port), timeout=8) as sock:
        print(f"[CONNECT] [{args.addr}]:{args.port}")
        if args.hello:
            send(sock, HELLO, "hello")
            time.sleep(args.sleep_ms/1000.0)

        # order: base, then ups, then downs
        keys = [args.base_key]
        keys += [args.base_key + (i+1)*args.delta for i in range(args.n_up)]
        keys += [args.base_key - (i+1)*args.delta for i in range(args.n_down)]

        for k in keys:
            if k < 0: continue
            blob = frame_for(k)
            print(f"--- KEY 0x{k:08X} ---")
            for r in range(args.repeat):
                send(sock, blob, f"{k:08X}#{r+1}")
                time.sleep(args.sleep_ms/1000.0)

        print("[CLOSE] socket closed")

if __name__ == "__main__":
    main()