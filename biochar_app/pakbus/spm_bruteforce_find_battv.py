#!/usr/bin/env python3
import argparse, struct, math
from pathlib import Path

def hexdump(b: bytes, start: int, count: int = 32) -> str:
    a = max(0, start - 8)
    z = min(len(b), start + count)
    return " ".join(f"{x:02x}" for x in b[a:z])

def scan_file(p: Path, targets, eps=0.005, show=32):
    b = p.read_bytes()
    hits = []

    # Pre-pack targets for exact byte-pattern search (float32 little & big endian)
    packed_le = {t: struct.pack("<f", float(t)) for t in targets}
    packed_be = {t: struct.pack(">f", float(t)) for t in targets}
    ascii_s  = {t: str(t).encode("ascii") for t in targets}

    # 1) Exact 4-byte pattern matches (float32 LE & BE)
    for t, pat in packed_le.items():
        idx = 0
        while True:
            idx = b.find(pat, idx)
            if idx < 0: break
            hits.append(("f32-le", float(t), idx, hexdump(b, idx, show)))
            idx += 1

    for t, pat in packed_be.items():
        idx = 0
        while True:
            idx = b.find(pat, idx)
            if idx < 0: break
            hits.append(("f32-be", float(t), idx, hexdump(b, idx, show)))
            idx += 1

    # 2) ASCII “13.22” (etc.) matches
    for t, s in ascii_s.items():
        idx = 0
        while True:
            idx = b.find(s, idx)
            if idx < 0: break
            hits.append(("ascii", float(t), idx, hexdump(b, idx, show)))
            idx += 1

    # 3) Sliding numeric scan (tolerant): decode every 4B as float32 (LE & BE)
    #    and report within ±eps of any target. (This is slow but thorough.)
    targets_set = set(float(x) for x in targets)
    # LE
    for i in range(0, len(b) - 3):
        f = struct.unpack_from("<f", b, i)[0]
        for t in targets_set:
            if math.isfinite(f) and abs(f - t) <= eps:
                hits.append(("scan-le", t, i, hexdump(b, i, show)))
                break
    # BE
    for i in range(0, len(b) - 3):
        f = struct.unpack_from(">f", b, i)[0]
        for t in targets_set:
            if math.isfinite(f) and abs(f - t) <= eps:
                hits.append(("scan-be", t, i, hexdump(b, i, show)))
                break

    # Sort by file offset
    hits.sort(key=lambda x: x[2])
    return hits

def main():
    ap = argparse.ArgumentParser(description="Brute-force search for BattV float values in binary/text files.")
    ap.add_argument("files", nargs="+", type=Path, help="Files to scan (.spm, .txt, .bin, etc.)")
    ap.add_argument("--values", nargs="+", required=True, help="Target values, e.g. 13.22 13.22116 13.22452")
    ap.add_argument("--eps", type=float, default=0.005, help="Tolerance for float compare (default 0.005)")
    ap.add_argument("--context", type=int, default=32, help="Hex bytes to show after hit (default 32)")
    args = ap.parse_args()

    for f in args.files:
        if not f.exists():
            print(f"[warn] missing {f}")
            continue
        hits = scan_file(f, args.values, eps=args.eps, show=args.context)
        print(f"\n== {f}  hits={len(hits)} (eps={args.eps}) ==")
        if not hits:
            continue
        print("kind     value       offset    hex-context")
        print("-"*78)
        for kind, val, off, ctx in hits:
            print(f"{kind:<8} {val:<10.5f} @+0x{off:08X}  {ctx}")

if __name__ == "__main__":
    main()