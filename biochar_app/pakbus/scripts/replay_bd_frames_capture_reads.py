#!/usr/bin/env python3
import argparse, socket, time, sys, pathlib, csv

HELLO = bytes.fromhex("bd 90 01 0f fd 73 d3 c2 d6 bd")
ACK_PREFIX  = bytes.fromhex("bd af fd 70")  # 18B ack-ish

# BD server→client "replay/data" frames begin with these prefixes
DATA_PREFIXES = [
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x03',  # Read
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x02',  # CRBasic variant
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x01',  # other variant
]

def is_data_frame(frame: bytes) -> bool:
    # Just check the start and a sane length
    return any(frame.startswith(p) for p in DATA_PREFIXES) and len(frame) >= 40

def hexline_to_bytes(s: str) -> bytes:
    s = s.strip().lower().replace(" ", "")
    if not s or any(c not in "0123456789abcdef" for c in s):
        return b""
    if len(s) % 2:
        s = "0" + s
    return bytes.fromhex(s)

def recv_all(sock: socket.socket, idle_timeout=0.25, max_wait=2.0) -> bytes:
    sock.settimeout(idle_timeout)
    chunks, start = [], time.time()
    while True:
        try:
            data = sock.recv(65536)
            if data:
                chunks.append(data)
                start = time.time()
            else:
                break
        except socket.timeout:
            if time.time() - start >= max_wait:
                break
            continue
    return b"".join(chunks)

def split_bd_frames(buf: bytes):
    """Simple terminator-based splitter (kept for ACKs)."""
    out, cur = [], bytearray()
    for b in buf:
        cur.append(b)
        if b == 0xBD:  # 0xBD terminates a BD frame
            out.append(bytes(cur))
            cur.clear()
    if cur:  # flush any residual bytes
        out.append(bytes(cur))
    return out

def find_embedded_data_frames(buf: bytes):
    """
    More robust extractor: scan for any known data prefix anywhere in buf,
    then cut up to and including the next 0xBD terminator.
    """
    results = []
    n = len(buf)
    i = 0
    while i < n:
        # see if any prefix matches at i
        hit = None
        for p in DATA_PREFIXES:
            if i + len(p) <= n and buf[i:i+len(p)] == p:
                hit = p
                break
        if hit is None:
            i += 1
            continue

        # find next terminator after the prefix
        j = buf.find(b'\xbd', i + len(hit))
        if j == -1:
            # no terminator; give up on this buffer
            break
        frame = buf[i:j+1]
        if is_data_frame(frame):
            results.append(frame)
        i = j + 1  # continue scanning after this frame
    return results

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--frames-file", required=True)
    ap.add_argument("--hello-gap-ms", type=int, default=150)
    ap.add_argument("--post-replay-wait-ms", type=int, default=1200)
    args = ap.parse_args()

    frames_path = pathlib.Path(args.frames_file)
    frames = [hexline_to_bytes(x) for x in frames_path.read_text().splitlines()]
    frames = [f for f in frames if f]
    print(f"[INFO] Loaded {len(frames)} frames from {frames_path}")

    outdir = pathlib.Path("pakbus_runs/replies")
    outdir.mkdir(parents=True, exist_ok=True)
    log = (outdir / "capture.log").open("w", encoding="utf-8")
    idxcsv = (outdir / "index.csv").open("w", newline="", encoding="utf-8")
    idxw = csv.writer(idxcsv)
    idxw.writerow(["i", "tx_len", "ack_hex", "data_len", "data_file", "raw_file"])

    for i, frame in enumerate(frames, 1):
        print(f"\n=== Frame {i}/{len(frames)} ===")
        log.write(f"\n=== Frame {i}/{len(frames)} ===\n")
        print(f"[TX {len(frame)}B] {frame.hex()}")
        log.write(f"[TX {len(frame)}B] {frame.hex()}\n")

        data_file = ""
        raw_file = ""
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.settimeout(2.0)
            s.connect((args.addr, args.port, 0, 0))

            # hello/handshake
            s.sendall(HELLO)
            time.sleep(args.hello_gap_ms / 1000.0)
            rx1 = recv_all(s, idle_timeout=0.2, max_wait=0.8)
            if rx1:
                print(f"[RX hello] {len(rx1)}B: {rx1.hex()}")
                log.write(f"[RX hello] {len(rx1)}B: {rx1.hex()}\n")

            # replay one captured TX frame
            s.sendall(frame)
            rx2 = recv_all(
                s,
                idle_timeout=0.25,
                max_wait=args.post_replay_wait_ms / 1000.0
            )
            if not rx2:
                print("[RX replay] 0B")
                log.write("[RX replay] 0B\n")
                idxw.writerow([i, len(frame), "", 0, "", ""])
                continue

            print(f"[RX replay] {len(rx2)}B")
            log.write(f"[RX replay] {len(rx2)}B: {rx2.hex()}\n")

            # always dump the raw buffer so we can diagnose
            raw_file = f"raw_{i:03d}_{len(rx2)}B.hex"
            (outdir / raw_file).write_bytes(rx2)

            # classify split frames (ACKs), then scan for embedded data frames
            acks = []
            for bf in split_bd_frames(rx2):
                if bf.startswith(ACK_PREFIX) and len(bf) == 18:
                    acks.append(bf)

            datas = find_embedded_data_frames(rx2)

            ack_hex = acks[0].hex() if acks else ""
            if datas:
                # save the longest data frame (sometimes more than one shows)
                datas.sort(key=len, reverse=True)
                best = datas[0]
                data_file = f"data_{i:03d}_{len(best)}B.hex"
                (outdir / data_file).write_bytes(best)
                print(f"    [DATA] {len(best)}B -> {data_file}")
                log.write(f"[DATA] {len(best)}B: {best.hex()}\n")
                data_len = len(best)
            else:
                print("    [DATA] none")
                log.write("[DATA] none\n")
                data_len = 0

            idxw.writerow([i, len(frame), ack_hex, data_len, data_file, raw_file])

    log.close()
    idxcsv.close()
    print(f"\n[INFO] Wrote: {outdir}/index.csv and per-frame data hex files")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)