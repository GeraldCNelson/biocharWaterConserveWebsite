#!/usr/bin/env python3
import argparse, socket, time, sys, pathlib

HELLO = bytes.fromhex("bd 90 01 0f fd 73 d3 c2 d6 bd")

def hexline_to_bytes(s: str) -> bytes:
    s = s.strip().lower().replace(" ", "")
    if not s or any(c not in "0123456789abcdef" for c in s):
        return b""
    if len(s) % 2:
        s = "0" + s
    return bytes.fromhex(s)

def recv_all(sock: socket.socket, idle_timeout=0.25, max_wait=2.0) -> bytes:
    sock.settimeout(idle_timeout)
    chunks = []
    start = time.time()
    while True:
        try:
            data = sock.recv(4096)
            if data:
                chunks.append(data)
                start = time.time()  # reset idle timer on data
            else:
                # peer closed
                break
        except socket.timeout:
            if time.time() - start >= max_wait:
                break
            # else loop again until max_wait
            continue
    return b"".join(chunks)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True, help="IPv6 address")
    ap.add_argument("--port", required=True, type=int)
    ap.add_argument("--frames-file", required=True)
    ap.add_argument("--hello-gap-ms", type=int, default=150)  # small pause after hello
    ap.add_argument("--post-replay-wait-ms", type=int, default=600)  # wait for reply
    args = ap.parse_args()

    frames_path = pathlib.Path(args.frames_file)
    lines = frames_path.read_text().splitlines()
    frames = [hexline_to_bytes(x) for x in lines if hexline_to_bytes(x)]
    print(f"[INFO] Loaded {len(frames)} BD frames from {frames_path}")

    logdir = pathlib.Path("pakbus_runs"); logdir.mkdir(exist_ok=True)
    logfile = logdir / "replay_no_crc.log"
    out = logfile.open("w", encoding="utf-8")
    def log(msg):
        print(msg); out.write(msg + "\n"); out.flush()

    for idx, frame in enumerate(frames, 1):
        log(f"\n=== Frame {idx}/{len(frames)} ===")
        log(f"[TX frame] {len(frame)}B: {frame.hex()}")

        # connect fresh each time (mimics your working pattern)
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.settimeout(2.0)
            s.connect((args.addr, args.port, 0, 0))

            # send hello
            s.sendall(HELLO)
            time.sleep(args.hello_gap_ms / 1000.0)
            rx1 = recv_all(s, idle_timeout=0.2, max_wait=0.8)
            if rx1:
                log(f"[RX after hello] {len(rx1)}B: {rx1.hex()}")
            else:
                log("[RX after hello] 0B")

            # replay the frame
            s.sendall(frame)
            rx2 = recv_all(s, idle_timeout=0.2, max_wait=args.post_replay_wait_ms/1000.0)
            if rx2:
                log(f"[RX after replay] {len(rx2)}B: {rx2.hex()}")
                # quick parse: split on 0xbd boundaries to show BD slices
                parts = rx2.split(b"\xbd")
                # reattach trailing 0xbd to any non-empty slice to present frames
                bd_frames = []
                carry = b""
                for p in parts:
                    if not p:  # multiple bd in a row
                        continue
                    candidate = (b"\xbd" if not carry else b"") + p + b"\xbd"
                    bd_frames.append(candidate)
                if bd_frames:
                    for j, bf in enumerate(bd_frames, 1):
                        log(f"    [BD reply #{j}] {len(bf)}B: {bf.hex()}")
            else:
                log("[RX after replay] 0B")

    log(f"[INFO] Log written to: {logfile}")
    out.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)