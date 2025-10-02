#!/usr/bin/env python3
"""
probe_bd_field_offsets.py

Goal:
  - Given a known-good TX frame (hex) that elicits "replay/data" from your CR200X,
    find the byte offsets (within that TX frame) for:
      * start_epoch_1990 (UInt32 BE)
      * count (UInt16 BE)

How it works:
  - You provide:
      --addr, --port         (logger)
      --tx-template          (path to a hex *request* frame known to return data)
      --target-ts            (an ISO timestamp you want the logger to return; we’ll probe offsets)
      --count                (# of records to return per request, default 1)
  - The script tries candidate offsets, patches the template, sends, and evaluates the reply:
      1) it looks for BD "data" frames (prefix b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x0?')
      2) it decodes Table1 epoch+10 floats BE blocks inside the reply
      3) if the top-of-block epoch matches the requested start_ts (±1 record step), we consider it a hit.

Result:
  - If a (epoch_offset, count_offset) pair works, we save to JSON for later forging:
      pakbus_runs/offsets/bd_table1_offsets.json
"""

import argparse, socket, time, json, pathlib, struct, datetime, math, sys
from typing import Tuple, Optional, List

HELLO = bytes.fromhex("bd90010ffd73d3c2d6bd")

# BD server→client "replay/data" frames begin with these prefixes
DATA_PREFIXES = [
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x03',
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x02',
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x01',
]

ACK_PREFIX = bytes.fromhex("bd affd70".replace(" ",""))  # 18B ack-ish (bd af fd 70 ... bd)

FIELDS = ["BattV_Min",
          "VWC_1_Avg","EC_1_Avg","T_1_Avg",
          "VWC_2_Avg","EC_2_Avg","T_2_Avg",
          "VWC_3_Avg","EC_3_Avg","T_3_Avg"]

def is_data_frame(frame: bytes) -> bool:
    return any(frame.startswith(p) for p in DATA_PREFIXES) and len(frame) >= 40

def split_bd_frames(buf: bytes) -> List[bytes]:
    out, cur = [], bytearray()
    for b in buf:
        cur.append(b)
        if b == 0xBD:       # 0xBD terminates a BD frame
            out.append(bytes(cur)); cur.clear()
    if cur: out.append(bytes(cur))
    return out

def recv_all(sock: socket.socket, idle_timeout=0.25, max_wait=1.2) -> bytes:
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

def read_hex_frame(path: pathlib.Path) -> bytes:
    # Accept either raw bytes file or ascii-hex
    raw = path.read_bytes()
    # Heuristic: if it contains only plausible ASCII hex and whitespace, parse as hex text
    try:
        txt = raw.decode("ascii", errors="strict").strip()
        if all(c in "0123456789abcdefABCDEF \r\n\t" for c in txt):
            return bytes.fromhex("".join(txt.split()))
        # else fall back to raw
        return raw
    except Exception:
        return raw

def to_epoch1990(ts_utc: datetime.datetime) -> int:
    base = datetime.datetime(1990,1,1,tzinfo=datetime.timezone.utc)
    return int((ts_utc - base).total_seconds())

def iter_table1_blocks(b: bytes):
    """
    Find blocks of: 4B epoch (BE) + 10 * float32 BE.
    Yield tuples: (offset, epoch_seconds, values_list)
    """
    n = len(b)
    for i in range(0, n - (4 + 4*10)):
        sec = struct.unpack_from(">I", b, i)[0]
        if not (900_000_000 <= sec <= 1_800_000_000):  # 2018..2047 extended
            continue
        vals = struct.unpack_from(">" + "f"*10, b, i+4)
        yield i, sec, list(vals)

def send_one(addr, port, tx: bytes, hello_gap_ms=120, wait_ms=1200) -> bytes:
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(2.0)
        s.connect((addr, port, 0, 0))
        s.sendall(HELLO)
        time.sleep(hello_gap_ms/1000.0)
        _ = recv_all(s, idle_timeout=0.2, max_wait=0.8)  # hello reply (optional)
        s.sendall(tx)
        rx = recv_all(s, idle_timeout=0.25, max_wait=wait_ms/1000.0)
        return rx

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--tx-template", required=True, help="Path to a known-good *request* frame (hex or raw)")
    ap.add_argument("--target-ts", required=True, help="UTC timestamp e.g. 2025-09-24T11:00:00Z")
    ap.add_argument("--count", type=int, default=1)
    ap.add_argument("--max-tries", type=int, default=200)
    args = ap.parse_args()

    tmpl = read_hex_frame(pathlib.Path(args.tx_template))
    # Normalize target time
    ts = pd_to_datetime(args.target_ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=datetime.timezone.utc)
    ts_utc = ts.astimezone(datetime.timezone.utc)
    want_epoch = to_epoch1990(ts_utc)
    want_count = max(1, min(args.count, 1000))
    want_epoch_be = want_epoch.to_bytes(4, "big", signed=False)
    want_count_be = want_count.to_bytes(2, "big", signed=False)

    # Candidates: we’ll try patching 4B at pos i for epoch, and 2B at pos j for count.
    # To limit traffic, first find an epoch position that “moves the time we see” in replies.
    import itertools
    tried = 0
    best_epoch_pos: Optional[int] = None
    best_count_pos: Optional[int] = None

    # Scan epoch positions (skip the first ~8 bytes which are header-ish; skip last 8 for safety)
    epoch_positions = range(8, len(tmpl) - 8)
    for i in epoch_positions:
        if tried >= args.max_tries: break
        tried += 1
        tx = bytearray(tmpl)
        tx[i:i+4] = want_epoch_be
        rx = send_one(args.addr, args.port, bytes(tx))
        frames = split_bd_frames(rx)
        datas = [f for f in frames if is_data_frame(f)]
        if not datas:
            continue
        # decode any table1 epoch blocks in data frame
        any_hit = False
        for df in datas:
            for off, sec, _vals in iter_table1_blocks(df):
                # Accept exact match or within +/- 1 record step (15 min ~= 900 sec)
                if abs(sec - want_epoch) <= 60 or abs(sec - (want_epoch + 900)) <= 60:
                    best_epoch_pos = i
                    any_hit = True
                    break
            if any_hit: break
        if any_hit:
            print(f"[FOUND] epoch_offset={best_epoch_pos}")
            break

    if best_epoch_pos is None:
        print("[FAIL] Could not find epoch offset in template; try a different template TX frame.\n"
              "Hint: pick the TX that produced a data_XXX with a known timestamp you can aim for.")
        sys.exit(2)

    # Now find count position using the same epoch position; ask for count=1 vs 2 and see length change.
    for j in range(8, len(tmpl) - 2):
        tx1 = bytearray(tmpl)
        tx1[best_epoch_pos:best_epoch_pos+4] = want_epoch_be
        tx1[j:j+2] = (1).to_bytes(2,"big")
        rx1 = send_one(args.addr, args.port, bytes(tx1))
        n1 = sum(1 for f in split_bd_frames(rx1) if is_data_frame(f))

        tx2 = bytearray(tmpl)
        tx2[best_epoch_pos:best_epoch_pos+4] = want_epoch_be
        tx2[j:j+2] = (2).to_bytes(2,"big")
        rx2 = send_one(args.addr, args.port, bytes(tx2))
        n2 = sum(1 for f in split_bd_frames(rx2) if is_data_frame(f))

        if n1 != n2 or (len(rx2) != len(rx1)):
            best_count_pos = j
            print(f"[FOUND] count_offset={best_count_pos}")
            break

    if best_count_pos is None:
        print("[WARN] Could not isolate count offset. You can still forge with epoch only (defaults to template's count).")

    outdir = pathlib.Path("pakbus_runs/offsets"); outdir.mkdir(parents=True, exist_ok=True)
    cfg = {
        "template_file": str(args.tx_template),
        "epoch_offset": best_epoch_pos,
        "count_offset": best_count_pos,
    }
    outpath = outdir / "bd_table1_offsets.json"
    outpath.write_text(json.dumps(cfg, indent=2))
    print(f"[OK] Wrote offsets: {outpath}")

def pd_to_datetime(s: str) -> datetime.datetime:
    # lightweight parser: accept ...Z, or without Z (assume UTC)
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1]
        return datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone.utc)
    # allow space between date/time
    if " " in s and "T" not in s:
        s = s.replace(" ", "T")
    return datetime.datetime.fromisoformat(s)

if __name__ == "__main__":
    try:
        import pandas as pd  # only used for user familiarity if you add extensions later
    except Exception:
        pass
    main()