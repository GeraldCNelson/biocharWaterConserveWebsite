#!/usr/bin/env python3
"""
Decode request hints out of frames cataloged by tsv_to_request_catalog.py.

Input:
  biochar_app/pakbus/bdFiles/out_catalog/ALL_frames_catalog.csv
    (delimiter may be tab or comma; headers may vary slightly)

Output:
  biochar_app/pakbus/bdFiles/out_catalog/decoded_request_hints.csv
  biochar_app/pakbus/bdFiles/out_catalog/decoded_per_leaf/<LEAF>_hints.csv
"""

import csv
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[0]
CATALOG_DIR = ROOT / "bdFiles" / "out_catalog"
ALL_FRAMES = CATALOG_DIR / "ALL_frames_catalog.csv"
OUT_COMBINED = CATALOG_DIR / "decoded_request_hints.csv"
OUT_PER_LEAF_DIR = CATALOG_DIR / "decoded_per_leaf"

# ---------------- helpers ----------------

def hex_to_bytes(h: str) -> bytes:
    h = (h or "").strip().replace(" ", "").replace(":", "")
    if not h:
        return b""
    try:
        return bytes.fromhex(h)
    except Exception:
        return b""

def int16_be(b: bytes, off: int) -> int:
    if off + 2 > len(b):
        return 0
    v = (b[off] << 8) | b[off + 1]
    if v & 0x8000:
        v -= 0x10000
    return v

def normalize_header(name: str) -> str:
    # Lowercase, strip, replace spaces/dots/hyphens with underscores
    return (
        (name or "")
        .strip()
        .strip("\ufeff")  # BOM guard
        .lower()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
    )

def load_all_frames_smart(path: Path):
    """
    Auto-detect delimiter and normalize headers.
    Returns list of dicts with normalized keys.
    """
    text = path.read_text(encoding="utf-8-sig", errors="ignore")
    # Try to sniff delimiter
    try:
        dialect = csv.Sniffer().sniff(text.splitlines()[0])
        delim = dialect.delimiter
    except Exception:
        # Fall back: prefer tab if present anywhere, else comma
        delim = "\t" if "\t" in text.splitlines()[0] else ","

    rdr = csv.reader(text.splitlines(), delimiter=delim)
    try:
        raw_header = next(rdr)
    except StopIteration:
        return [], {}

    header = [normalize_header(h) for h in raw_header]
    rows = []
    for row in rdr:
        # pad/truncate to header length
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        elif len(row) > len(header):
            row = row[:len(header)]
        d = {header[i]: row[i] for i in range(len(header))}
        rows.append(d)
    return rows, {"delimiter": delim, "header": header}

def pick(d: dict, *names, default=""):
    """
    Pick first available key among plausible variants.
    Expect normalized keys (lowercase, underscores).
    """
    for n in names:
        if n in d and d[n] != "":
            return d[n]
    return default

# ---------------- decoding ----------------

def infer_request(row):
    """
    Return a dict of decoded/inferred fields from one catalog row.
    Works with normalized keys; tolerates missing fields.
    """
    frame_len = int(pick(row, "frame_len", default="0") or 0)
    leaf      = pick(row, "leaf")
    table     = int(pick(row, "table", default="0") or 0)
    frame_hex = pick(row, "frame_hex")

    b = hex_to_bytes(frame_hex)

    request_mode = ""
    n_hint = ""
    signed_20 = ""
    signed_22 = ""

    if frame_len == 31 and len(b) == 31 and b[:1] == b"\xbd" and b[-1:] == b"\xbd":
        s20 = int16_be(b, 20)
        s22 = int16_be(b, 22)
        signed_20 = s20
        signed_22 = s22
        if s20 < 0:
            request_mode = "last_n"
            n_hint = abs(s20)
        else:
            request_mode = "raw"

    # Anchor fields produced by the catalog script
    anchor_off    = pick(row, "anchor_off")
    anchor_sec_le = pick(row, "anchor_sec_le")
    anchor_iso    = pick(row, "anchor_iso")

    out = {
        "leaf": leaf,
        "table": table,
        "frame_len": frame_len,
        "request_mode": request_mode,
        "n_hint": n_hint,
        "signed16_be@20": signed_20,
        "signed16_be@22": signed_22,
        "anchor_off": anchor_off,
        "anchor_sec_le": anchor_sec_le,
        "anchor_iso": anchor_iso,
        "tcp_stream": pick(row, "tcp_stream", "stream", "tcp_stream_id"),
        "frame_number": pick(row, "frame_number", "frame_no", "no"),
        "src_file": pick(row, "src_file", "source", "file"),
        "frame_hex": frame_hex,
    }
    return out

def write_csv(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

# ---------------- main ----------------

def main():
    if not ALL_FRAMES.exists():
        print(f"ERROR: {ALL_FRAMES} not found. Run tsv_to_request_catalog.py first.")
        return

    rows, meta = load_all_frames_smart(ALL_FRAMES)
    print(f"[info] read {len(rows)} rows from {ALL_FRAMES}")
    print(f"[info] detected delimiter: {meta.get('delimiter')!r}")
    print(f"[info] headers: {', '.join(meta.get('header', [])[:12])} ...")

    decoded = [infer_request(r) for r in rows]

    # Quick sanity: count non-empty leaves and how many last_n we found
    leaves = {r["leaf"] for r in decoded if r["leaf"]}
    lastn  = sum(1 for r in decoded if r["request_mode"] == "last_n")
    print(f"[info] distinct leaves: {len(leaves)} → {sorted(list(leaves))[:10]}{' ...' if len(leaves)>10 else ''}")
    print(f"[info] frames with last_n hint: {lastn}")

    cols = [
        "leaf", "table", "frame_len",
        "request_mode", "n_hint", "signed16_be@20", "signed16_be@22",
        "anchor_off", "anchor_sec_le", "anchor_iso",
        "tcp_stream", "frame_number", "src_file", "frame_hex",
    ]
    write_csv(OUT_COMBINED, decoded, cols)
    print(f"[ok] wrote {OUT_COMBINED} ({len(decoded)} rows)")

    OUT_PER_LEAF_DIR.mkdir(parents=True, exist_ok=True)
    by_leaf = defaultdict(list)
    for r in decoded:
        by_leaf[r["leaf"]].append(r)

    for leaf, lst in by_leaf.items():
        name = leaf if leaf else "__unknown__"
        out = OUT_PER_LEAF_DIR / f"{name}_hints.csv"
        write_csv(out, lst, cols)
        print(f"[ok] wrote {out} ({len(lst)} rows)")

if __name__ == "__main__":
    main()