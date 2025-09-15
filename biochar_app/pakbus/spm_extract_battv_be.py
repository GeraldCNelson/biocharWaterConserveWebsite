#!/usr/bin/env python3
"""
Brute-force BattV extractor for SPM dumps (big-endian floats).

Handles both:
  - space-separated hex bytes: "41 53 7C 20 ..."
  - run-on hex strings:        "41537c20..."

Outputs CSV at decoded/battv_from_dump_scan.csv by default.
"""

import argparse
import re
import struct
from pathlib import Path
from typing import List, Tuple

SESS = Path("biochar_app/data-raw/spm_sessions")
DECODED = SESS / "decoded"
DECODED.mkdir(parents=True, exist_ok=True)

# Matches either long hex runs OR sequences of spaced hex pairs.
HEX_RUN_RE   = re.compile(r"[0-9A-Fa-f]{6,}")             # e.g., "bda00d6ffd10..."
HEX_PAIRS_RE = re.compile(r"(?:\b[0-9A-Fa-f]{2}\b(?:\s+|$))+")  # e.g., "41 53 7C 20 "

def latest_dump_txt() -> Path | None:
    # Prefer *.txt, then *.frames.txt
    cands = sorted(SESS.glob("*.txt")) + sorted(SESS.glob("*.frames.txt"))
    return cands[-1] if cands else None

def _pairs_from_run(run: str) -> List[int]:
    # run is an even-length hex string with no spaces
    if len(run) % 2 != 0:
        run = run[:-1]  # trim odd nibble, just in case
    return [int(run[i:i+2], 16) for i in range(0, len(run), 2)]

def read_hex_bytes_from_txt(p: Path) -> bytes:
    """
    Extract bytes from a dump text that may mix:
      - spaced pairs ("aa bb cc")
      - run-on hex strings ("aabbcc")
    We gather ALL matches in order of appearance.
    """
    text = p.read_text(errors="ignore")

    # Collect matches with positions so we can preserve original order.
    matches: List[Tuple[int, List[int]]] = []

    for m in HEX_PAIRS_RE.finditer(text):
        seg = m.group(0)
        toks = [t for t in seg.strip().split() if re.fullmatch(r"[0-9A-Fa-f]{2}", t)]
        if toks:
            matches.append((m.start(), [int(t, 16) for t in toks]))

    for m in HEX_RUN_RE.finditer(text):
        run = m.group(0)
        # Avoid double-counting: skip if this region was consumed by HEX_PAIRS_RE
        # (Pairs regex usually won’t cover long runs, but this keeps order clean.)
        matches.append((m.start(), _pairs_from_run(run)))

    if not matches:
        return b""

    # Sort by occurrence and flatten
    matches.sort(key=lambda x: x[0])
    out: List[int] = []
    for _, arr in matches:
        out.extend(arr)
    return bytes(out)

def scan_be_floats(b: bytes, vmin: float, vmax: float) -> List[Tuple[int, float]]:
    hits: List[Tuple[int, float]] = []
    for off in range(0, len(b) - 3):
        try:
            f = struct.unpack_from(">f", b, off)[0]  # BIG-ENDIAN float
        except Exception:
            continue
        if vmin <= f <= vmax:
            hits.append((off, f))
    return hits

def hex_context(b: bytes, off: int, before: int = 8, after: int = 16) -> str:
    s = max(0, off - before)
    e = min(len(b), off + 4 + after)
    return " ".join(f"{x:02x}" for x in b[s:e])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump_txt", nargs="?", type=Path,
                    help="SPM Dump .txt or .frames.txt (if omitted, use latest in spm_sessions)")
    ap.add_argument("--min", type=float, default=10.0, help="Minimum plausible BattV (default 10.0)")
    ap.add_argument("--max", type=float, default=16.0, help="Maximum plausible BattV (default 16.0)")
    ap.add_argument("--out", type=Path,
                    help="CSV output (default: decoded/battv_from_dump_scan.csv)")
    ap.add_argument("--include-trace", action="store_true",
                    help="Add hex context around each match")
    ap.add_argument("--dedupe", action="store_true",
                    help="Coalesce hits closer than 8 bytes (keeps the first)")
    args = ap.parse_args()

    dump = args.dump_txt or latest_dump_txt()
    if not dump or not dump.exists():
        print("[err] no dump .txt provided and none found in", SESS)
        return

    out = args.out or (DECODED / "battv_from_dump_scan.csv")

    blob = read_hex_bytes_from_txt(dump)
    if not blob:
        print(f"[warn] no hex tokens parsed from {dump}")
        return

    hits = scan_be_floats(blob, args.min, args.max)

    if args.dedupe and hits:
        deduped: List[Tuple[int, float]] = []
        last_off = -999
        for off, val in hits:
            if off - last_off < 8:
                continue
            deduped.append((off, val))
            last_off = off
        hits = deduped

    with open(out, "w") as f:
        if args.include_trace:
            f.write("offset,battv,hex\n")
            for off, val in hits:
                f.write(f"{off},{val:.5f},{hex_context(blob, off)}\n")
        else:
            f.write("offset,battv\n")
            for off, val in hits:
                f.write(f"{off},{val:.5f}\n")

    print(f"[ok] scanned {len(blob)} bytes; wrote {len(hits)} BattV hits → {out}")
    print(f"[info] source: {dump}")

if __name__ == "__main__":
    main()