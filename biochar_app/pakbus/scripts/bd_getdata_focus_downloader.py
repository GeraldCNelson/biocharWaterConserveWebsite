#!/usr/bin/env python3
import argparse, socket, time, os, binascii

def load_seeds(path):
    seeds = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                seeds.append(bytes.fromhex(line))
            except ValueError:
                pass
    return seeds

def crc_ibm(data: bytes) -> bytes:
    # CRC-16-IBM, poly 0xA001
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc.to_bytes(2, "little")

def crc_ccitt(data: bytes) -> bytes:
    # CRC-16-CCITT, poly 0x1021
    crc = 0xFFFF
    for b in data:
        crc ^= (b << 8)
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc.to_bytes(2, "big")

def wrap(seed: bytes, mode: str) -> bytes:
    if mode == "as-is":
        return seed
    core = seed[1:-2] if seed.startswith(b"\xbd") and seed.endswith(b"\xbd") else seed
    if mode == "wrap-ibm":
        return b"\xbd" + core + crc_ibm(core) + b"\xbd"
    if mode == "wrap-ccitt":
        return b"\xbd" + core + crc_ccitt(core) + b"\xbd"
    raise ValueError(f"Unknown wrap mode {mode}")

def try_send(addr, port, frame, connect_timeout=5, idle_timeout=2, fresh=True):
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(connect_timeout)
    if fresh:
        # always new connection
        s.connect((addr, port, 0, 0))
    else:
        # still reconnects each call (safe default)
        s.connect((addr, port, 0, 0))
    s.settimeout(idle_timeout)
    s.sendall(frame)
    time.sleep(0.2)
    try:
        rx = s.recv(4096)
    except socket.timeout:
        rx = b""
    finally:
        s.close()
    return rx

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--seeds-file", required=True)
    ap.add_argument("--table", type=lambda x: int(x, 0), default=0x11)
    ap.add_argument("--hello-repeats", type=int, default=1)
    ap.add_argument("--hello-gap-ms", type=int, default=300)
    ap.add_argument("--beacon-wait-ms", type=int, default=3000)
    ap.add_argument("--post-wait-ms", type=int, default=2000)
    ap.add_argument("--per-seed-sleep-ms", type=int, default=500)
    ap.add_argument("--connect-timeout", type=int, default=5)
    ap.add_argument("--idle-timeout", type=float, default=2.0)
    ap.add_argument("--crc", choices=["ibm", "ccitt", "both"], default="both")
    ap.add_argument("--order", choices=["hello-first", "frame-first", "double-hello"], default="hello-first")
    ap.add_argument("--no_filter", action="store_true")
    ap.add_argument("--out-dir", required=True)

    # new options
    ap.add_argument("--fresh-connection", action="store_true", help="Reconnect before each seed attempt")
    ap.add_argument("--only-seeds", type=str, help="Comma-separated list of seed indices (1-based)")
    ap.add_argument("--send", choices=["as-is", "wrap-ibm", "wrap-ccitt", "all"], default="all")

    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    seeds = load_seeds(args.seeds_file)
    if args.only_seeds:
        idxs = [int(x.strip()) - 1 for x in args.only_seeds.split(",")]
        seeds = [seeds[i] for i in idxs if 0 <= i < len(seeds)]

    wrap_modes = []
    if args.send == "all":
        wrap_modes = ["as-is", "wrap-ibm", "wrap-ccitt"]
    else:
        wrap_modes = [args.send]

    for i, seed in enumerate(seeds, start=1):
        for mode in wrap_modes:
            try:
                frame = wrap(seed, mode)
            except Exception as e:
                print(f"[SEED {i} | {mode}] wrap error: {e}")
                continue
            try:
                rx = try_send(args.addr, args.port, frame,
                              connect_timeout=args.connect_timeout,
                              idle_timeout=args.idle_timeout,
                              fresh=args.fresh_connection)
                print(f"[SEED {i} | {mode}] sent={len(frame)}B rx={len(rx)}B")
                if rx:
                    fn = os.path.join(args.out_dir, f"seed{i}_{mode}.bin")
                    with open(fn, "wb") as f:
                        f.write(rx)
            except Exception as e:
                print(f"[SEED {i} | {mode}] ERROR: {e}")
            time.sleep(args.per_seed_sleep_ms / 1000.0)

    print(f"[DONE] wrote outputs to {args.out_dir}")

if __name__ == "__main__":
    main()