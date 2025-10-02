#!/usr/bin/env python3
import struct, math, csv, pathlib, sys

# Same prefixes we used to detect data frames during capture
DATA_PREFIXES = [
    bytes.fromhex("bd af fd 00 01 1f fd 20 03"),
    bytes.fromhex("bd af fd 00 01 1f fd 20 02"),
    bytes.fromhex("bd af fd 00 01 1f fd 20 01"),
]

def load_hex_bytes(p: pathlib.Path) -> bytes:
    # file is raw bytes already, not ascii-hex; just read it
    b = p.read_bytes()
    return b

def find_payload(frame: bytes) -> bytes:
    """
    Heuristic payload finder:
      1) locate any data prefix inside the frame
      2) start payload a few bytes *after* the prefix to skip response header
      3) stop before CRC+terminator if present
    """
    start = None
    for pref in DATA_PREFIXES:
        k = frame.find(pref)
        if k != -1:
            start = k + len(pref)
            break
    if start is None:
        return b""

    # Skip a small reply header after the prefix (transaction ids, sizes, etc.)
    # We don't know the exact spec, so start 8 bytes after the prefix.
    start += 8
    if start >= len(frame):
        return b""

    # If the last byte is 0xBD, assume 2 CRC bytes before it.
    end = len(frame)
    if end >= 3 and frame[-1] == 0xBD:
        end -= 3

    # make sure we’re not negative / inverted
    if end <= start:
        return b""

    return frame[start:end]

def floats_from_bytes(buf: bytes, endian: str):
    """
    Convert a buffer to float32s with the given endianness ('<' or '>').
    Return a contiguous run of “sane” floats (finite, abs < 1e9), preferring the longest run.
    """
    vals = []
    for i in range(0, len(buf) - (len(buf) % 4), 4):
        (f,) = struct.unpack(endian + "f", buf[i:i+4])
        vals.append(f)

    # Find best contiguous run of “sane” floats
    best = (0, 0)  # (start, length)
    cur_start, cur_len = 0, 0
    def sane(x): return math.isfinite(x) and abs(x) < 1e9

    for i, x in enumerate(vals):
        if sane(x):
            if cur_len == 0:
                cur_start = i
            cur_len += 1
        else:
            if cur_len > best[1]:
                best = (cur_start, cur_len)
            cur_len = 0
    if cur_len > best[1]:
        best = (cur_start, cur_len)

    s, L = best
    return vals[s:s+L], s, L, len(vals)

def decode_frame(frame_bytes: bytes):
    pay = find_payload(frame_bytes)
    if not pay or len(pay) < 8:
        return {"endianness": None, "floats": [], "pay_len": len(pay)}

    be_vals, be_s, be_L, be_N = floats_from_bytes(pay, ">")
    le_vals, le_s, le_L, le_N = floats_from_bytes(pay, "<")

    # Choose the run with more sane floats; tie-breaker: bigger spread
    def spread(v):
        return (max(v) - min(v)) if v else -1.0

    if le_L > be_L or (le_L == be_L and spread(le_vals) > spread(be_vals)):
        return {"endianness": "little", "floats": le_vals, "pay_len": len(pay)}
    else:
        return {"endianness": "big", "floats": be_vals, "pay_len": len(pay)}

def main():
    base = pathlib.Path("pakbus_runs/replies")
    idx = base / "index.csv"
    if not idx.exists():
        print("index.csv not found under pakbus_runs/replies", file=sys.stderr)
        sys.exit(1)

    outdir = pathlib.Path("pakbus_runs/decoded")
    outdir.mkdir(parents=True, exist_ok=True)

    combined_rows = []
    with idx.open() as f:
        rdr = csv.DictReader(f)
        rows = list(rdr)

    for row in rows:
        data_file = row.get("data_file","").strip()
        if not data_file:
            continue
        data_path = base / data_file
        frame_bytes = load_hex_bytes(data_path)
        res = decode_frame(frame_bytes)

        floats = res["floats"]
        endianness = res["endianness"]
        pay_len = res["pay_len"]

        # Write per-frame CSV
        stem = pathlib.Path(data_file).stem  # e.g., data_004_246B
        out_csv = outdir / f"{stem}.csv"
        with out_csv.open("w", newline="") as g:
            w = csv.writer(g)
            w.writerow(["index","value"])
            for i, v in enumerate(floats):
                w.writerow([i, f"{v:.9g}"])

        # Add to combined
        combined_rows.append({
            "frame": row["i"],
            "data_file": data_file,
            "endianness": endianness or "",
            "payload_bytes": pay_len,
            "count": len(floats),
            "first5": "; ".join(f"{x:.6g}" for x in floats[:5])
        })

        print(f"[OK] {data_file}: payload {pay_len}B, {len(floats)} float32 ({endianness}-endian) -> {out_csv.name}")

    # Write combined summary + a stacked values file
    with (outdir / "summary.csv").open("w", newline="") as g:
        w = csv.DictWriter(g, fieldnames=["frame","data_file","endianness","payload_bytes","count","first5"])
        w.writeheader()
        for r in combined_rows:
            w.writerow(r)

    # Also write a stacked values CSV (frame,row,value)
    with (outdir / "combined_values.csv").open("w", newline="") as g:
        w = csv.writer(g)
        w.writerow(["frame","row","value"])
        for row in rows:
            data_file = row.get("data_file","").strip()
            if not data_file:
                continue
            data_path = base / data_file
            frame_bytes = load_hex_bytes(data_path)
            res = decode_frame(frame_bytes)
            for i, v in enumerate(res["floats"]):
                w.writerow([row["i"], i, f"{v:.9g}"])

    print(f"\n[INFO] wrote: {outdir}/summary.csv and per-frame CSVs")
    print(f"[INFO] stacked values: {outdir}/combined_values.csv")

if __name__ == "__main__":
    main()