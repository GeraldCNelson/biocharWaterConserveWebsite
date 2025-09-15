#!/usr/bin/env python3
"""
Find a target year in SPM dump files:
- ASCII "2025" in UI Dump text
- 16-bit year field in bytes (LE and BE, e.g., 0x07E9 for 2025)
Writes a CSV with hits from both paths, including context.
"""

import argparse
import csv
import re
from pathlib import Path

# ---------- loader (matches your other tools) ----------
HEX_TOKEN = re.compile(r'(?i)\b([0-9A-F]{2})\b')

def load_dump_text_and_bytes(path: str):
    p = Path(path)
    raw = p.read_bytes()
    # Try to decode as text (we want to keep any ASCII dates intact)
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        # If it’s a pure hex file saved as bytes, we still create a token list from bytes→text
        text = raw.decode("latin1", errors="replace")

    # Decide whether this looks like a tokenized hex dump (all tokens are 2-hex chars)
    tokens = HEX_TOKEN.findall(text)
    # Heuristic: if we see at least ~100 tokens and very few non-hex chars, treat as hex tokens.
    # Otherwise we consider it UI text that may include ASCII dates.
    hex_like = (len(tokens) > 100) and (len(tokens) * 3 > len(text))  # crude but effective

    if hex_like:
        # Build bytes from tokens (ignoring non-hex text)
        try:
            b = bytes(int(t, 16) for t in tokens)
        except Exception:
            b = b""
        print(f"[info] source parsed from text as hex tokens: {p}  (bytes={len(b)}, tokens={len(tokens)})")
        return p, text, b
    else:
        # Not primarily hex tokens; still extract any hex tokens to bytes so “bytes path” can run
        try:
            b = bytes(int(t, 16) for t in tokens) if tokens else b""
        except Exception:
            b = b""
        print(f"[info] source kept as UI text: {p}  (text chars={len(text)}, hex-bytes={len(b)})")
        return p, text, b

# ---------- search helpers ----------
def find_ascii_years(text: str, needle: str, window: int):
    hits = []
    i = 0
    while True:
        i = text.find(needle, i)
        if i == -1:
            break
        lo = max(0, i - window)
        hi = min(len(text), i + len(needle) + window)
        ctx = text[lo:hi].replace("\n", "\\n")
        hits.append({
            "kind": "ascii",
            "offset_text": i,
            "offset_byte": "",
            "year": needle,
            "context": ctx
        })
        i += 1
    return hits

def find_u16_years(b: bytes, year: int, window: int):
    if not b:
        return []
    y_le = year.to_bytes(2, "little")
    y_be = year.to_bytes(2, "big")
    hits = []
    for i in range(0, len(b) - 1):
        pair = b[i:i+2]
        if pair == y_le or pair == y_be:
            lo = max(0, i - window)
            hi = min(len(b), i + 2 + window)
            ctx = " ".join(f"{x:02x}" for x in b[lo:hi])
            hits.append({
                "kind": "year16-le" if pair == y_le else "year16-be",
                "offset_text": "",
                "offset_byte": i,
                "year": str(year),
                "context": ctx
            })
    return hits

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("source", help="SPM dump (.txt UI dump OR .frames.txt OR hexy .txt)")
    ap.add_argument("--year", type=int, default=2025, help="Target year (e.g., 2025)")
    ap.add_argument("--window", type=int, default=32, help="Context window (chars for ASCII, bytes for hex)")
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("biochar_app/data-raw/spm_sessions/decoded") / "year_hits.csv",
        help="Output CSV file"
    )
    args = ap.parse_args()

    src_path, dump_text, b = load_dump_text_and_bytes(args.source)

    # Search both paths
    ascii_hits = find_ascii_years(dump_text, str(args.year), args.window)
    u16_hits = find_u16_years(b, args.year, args.window)

    hits = ascii_hits + u16_hits
    # Stable, warning-free sort key
    hits.sort(key=lambda x: (
        (x["offset_text"] if isinstance(x["offset_text"], int) else float("inf")),
        (x["offset_byte"] if isinstance(x["offset_byte"], int) else float("inf")),
        x["kind"]
    ))

    out_path = args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["kind", "year", "offset_text", "offset_byte", "context"])
        w.writeheader()
        for h in hits:
            w.writerow(h)

    print(f"[ok] scanned {len(b)} bytes; wrote {len(hits)} hits → {out_path}")
    if not hits:
        print("[hint] If you expect ASCII dates like [11/09/2025 ...], run this on the UI Dump text file.")
        print("       If you only have hex, we also searched for u16 year (LE/BE). Try a bigger --window if needed.")

if __name__ == "__main__":
    main()