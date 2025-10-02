#!/usr/bin/env python3
# bd_hello_read_sweep.py — send hello/replay seeds with verbose RX hex dumps

import argparse, socket, time, pathlib, sys, textwrap

HELLO = bytes.fromhex("bd90010ffd73d3c2d6bd")  # same as before

def hex_to_bytes(s: str) -> bytes:
    s = s.strip().lower().replace(" ", "")
    if not s: return b""
    if len(s) % 2: s = "0" + s
    try:
        return bytes.fromhex(s)
    except Exception:
        return b""

def recv_all(sock: socket.socket, idle_timeout=0.25, max_wait=1.5) -> bytes:
    sock.settimeout(idle_timeout)
    chunks, start = [], time.time()
    while True:
        try:
            data = sock.recv(65536)
            if data:
                chunks.append(data); start = time.time()
            else:
                break
        except socket.timeout:
            if time.time() - start >= max_wait:
                break
            continue
    return b"".join(chunks)

def split_bd_frames(buf: bytes):
    out, cur = [], bytearray()
    for b in buf:
        cur.append(b)
        if b == 0xBD:
            out.append(bytes(cur)); cur.clear()
    if cur:
        out.append(bytes(cur))
    return out

def is_ack(b: bytes) -> bool:
    # heuristic ACK signature seen in older runs
    return b.startswith(bytes.fromhex("bdaf")) and len(b) >= 8 and b[3] in (0x70, 0x71, 0x72)

def looks_data(b: bytes) -> bool:
    if len(b) < 40: return False
    # broad DATA-ish prefixes observed in earlier logs
    prefixes = [
        bytes.fromhex("bdaf"),
        bytes.fromhex("bd8f"),
    ]
    return any(b.startswith(p) for p in prefixes) and b[-1] == 0xBD

def hexdump(b: bytes, bytes_per_line=32, max_bytes=96):
    s = b[:max_bytes].hex()
    # group in 2-char bytes
    groups = " ".join(textwrap.wrap(s, 2))
    # wrap lines
    return "\n      ".join(textwrap.wrap(groups, 3*bytes_per_line))

def read_seeds(path: pathlib.Path):
    seeds = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        b = hex_to_bytes(s)
        if b:
            seeds.append(b)
    return seeds

def main():
    ap = argparse.ArgumentParser(description="Hello + replay seeds (as-is) sweep, verbose")
    ap.add_argument("--addr", required=True, help="IPv6 address of logger")
    ap.add_argument("--port", type=int, required=True, help="TCP port (e.g., 6785)")
    ap.add_argument("--seeds-file", required=True, help="File with one hex frame per line")
    ap.add_argument("--hello-first", action="store_true", help="Send hello then frame (default)")
    ap.add_argument("--frame-first", action="store_true", help="Send frame then hello")
    ap.add_argument("--double-hello", action="store_true", help="Send hello twice before frame")
    ap.add_argument("--hello-gap-ms", type=int, default=250)
    ap.add_argument("--idle-timeout", type=float, default=0.4)
    ap.add_argument("--max-wait", type=float, default=2.0)
    ap.add_argument("--post-wait-ms", type=int, default=2200)
    ap.add_argument("--out-dir", default="pakbus_runs/hello_sweep_verbose")
    args = ap.parse_args()

    # Default to hello-first unless explicitly flipped
    order = "hello-first"
    if args.frame_first:
        order = "frame-first"

    seeds_path = pathlib.Path(args.seeds_file)
    seeds = read_seeds(seeds_path)
    if not seeds:
        print("[ERR] no seed frames found.", file=sys.stderr); sys.exit(2)

    outdir = pathlib.Path(args.out_dir); outdir.mkdir(parents=True, exist_ok=True)

    for idx, frame in enumerate(seeds, start=1):
        try:
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
                s.settimeout(10.0)
                s.connect((args.addr, args.port, 0, 0))

                rx_all = b""

                def do_hello():
                    nonlocal rx_all
                    s.sendall(HELLO)
                    time.sleep(args.hello_gap_ms / 1000.0)
                    rx = recv_all(s, idle_timeout=args.idle_timeout, max_wait=args.max_wait)
                    rx_all += rx

                if order == "hello-first":
                    do_hello()
                    if args.double_hello:
                        do_hello()
                    s.sendall(frame)
                    time.sleep(args.post_wait_ms / 1000.0)
                    rx_all += recv_all(s, idle_timeout=args.idle_timeout, max_wait=args.max_wait)
                else:  # frame-first
                    s.sendall(frame)
                    time.sleep(args.post_wait_ms / 1000.0)
                    rx_all += recv_all(s, idle_timeout=args.idle_timeout, max_wait=args.max_wait)
                    do_hello()
        except OSError as e:
            print(f"[SEED {idx}] socket error: {e}")
            continue

        frames = split_bd_frames(rx_all)
        acks = [f for f in frames if is_ack(f)]
        datas = [f for f in frames if looks_data(f)]

        print(f"[SEED {idx}] order={order} rx={len(rx_all)}B frames={len(frames)} acks={len(acks)} data={len(datas)}")
        for j, f in enumerate(frames, start=1):
            print(f"    RX[{j}] {len(f)}B:\n      {hexdump(f)}")

        # Save artifacts
        (outdir / f"seed_{idx:03d}_tx.hex").write_text(frame.hex())
        (outdir / f"seed_{idx:03d}_rx.bin").write_bytes(rx_all)
        if datas:
            for j, df in enumerate(datas, start=1):
                (outdir / f"seed_{idx:03d}_data_{j:02d}_{len(df)}B.hex").write_bytes(df)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)