#!/usr/bin/env python3
import argparse, socket, time, sys, pathlib

HELLO = bytes.fromhex("bd 90 01 0f fd 73 d3 c2 d6 bd")

def hexd(b: bytes) -> str: return " ".join(f"{x:02x}" for x in b)

def classify_inner(inner: bytes) -> str:
    if not inner: return "empty"
    if inner[0] == 0x2C:          # app/table op seen in traces
        return "APP_TABLE(0x2C)"
    if inner[0] in (0xA0, 0xAF):  # router/control style
        return "ROUTER_CTRL"
    return f"UNKNOWN(op=0x{inner[0]:02x})"

def load_frames(path: str):
    frames = []
    bad = 0
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.lower().startswith("frame"):
                continue
            # accept bare hex or CSV last-field hex
            if "," in line:
                line = line.split(",")[-1].strip()
            try:
                raw = bytes.fromhex(line)
                # trust file contains full BD frames; keep as-is
                if len(raw) >= 5 and raw[0] == 0xBD and raw[-1] == 0xBD:
                    frames.append(raw)
                else:
                    bad += 1
            except ValueError:
                bad += 1
    if bad:
        print(f"[WARN] skipped {bad} non-hex/short lines", file=sys.stderr)
    return frames

def run(addr: str, port: int, frames_file: str, gap_ms: int, timeout: float, out_log: str):
    frames = load_frames(frames_file)
    print(f"[INFO] Loaded {len(frames)} BD frames from {frames_file}")
    logp = pathlib.Path(out_log)
    logp.parent.mkdir(parents=True, exist_ok=True)
    with open(logp, "w") as lf:
        for idx, fr in enumerate(frames, 1):
            print(f"\n=== Frame {idx}/{len(frames)} ===")
            print(f"[TX frame] {len(fr)}B: {hexd(fr)}")
            lf.write(f"\n=== Frame {idx}/{len(frames)} ===\n")
            lf.write(f"TX: {hexd(fr)}\n")
            try:
                with socket.create_connection((addr, port), timeout=timeout) as s:
                    # hello
                    s.sendall(HELLO)
                    time.sleep(0.25)
                    try:
                        rx1 = s.recv(65535)
                    except socket.timeout:
                        rx1 = b""
                    if rx1:
                        print(f"[RX hello] {len(rx1)}B: {rx1.hex()}")
                        lf.write(f"RX_hello: {rx1.hex()}\n")
                    # replay frame
                    time.sleep(0.25)
                    s.sendall(fr)
                    time.sleep(0.4)
                    try:
                        rx2 = s.recv(65535)
                    except socket.timeout:
                        rx2 = b""
                    if not rx2:
                        print("[RX replay] 0B")
                        lf.write("RX_replay: (none)\n")
                    else:
                        print(f"[RX replay] {len(rx2)}B: {rx2.hex()}")
                        lf.write(f"RX_replay: {rx2.hex()}\n")
                        # split any BD frames in the rx2 buffer and decode
                        cur, in_bd, acc = bytearray(), False, 0
                        for b in rx2:
                            if not in_bd:
                                if b == 0xBD:
                                    cur = bytearray([0xBD]); in_bd = True
                            else:
                                cur.append(b)
                                if b == 0xBD:
                                    acc += 1
                                    inner = parse_bd_frame(bytes(cur))
                                    if inner is None:
                                        print("    -> [BAD CRC] discarding")
                                        lf.write("    BADCRC\n")
                                    else:
                                        print(f"    -> inner {len(inner)}B: {hexd(inner)} | {classify_inner(inner)}")
                                        lf.write(f"    inner: {hexd(inner)} | {classify_inner(inner)}\n")
                                    in_bd = False
                        if acc == 0:
                            print("    -> no BD frame found in RX buffer")
                            lf.write("    no_bd\n")
            except Exception as e:
                print(f"[ERROR] {e}")
                lf.write(f"ERROR: {e}\n")
            time.sleep(gap_ms/1000.0)
    print(f"[INFO] Log written to: {logp}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, default=6785)
    ap.add_argument("--frames-file", required=True)
    ap.add_argument("--gap-ms", type=int, default=350)
    ap.add_argument("--timeout", type=float, default=3.0)
    ap.add_argument("--out-log", default="pakbus_runs/replay_v2.log")
    args = ap.parse_args()
    run(args.addr, args.port, args.frames_file, args.gap_ms, args.timeout, args.out_log)

if __name__ == "__main__":
    main()