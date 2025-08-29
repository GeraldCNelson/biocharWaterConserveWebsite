#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

CAMPBELL_EPOCH = datetime(1990, 1, 1, tzinfo=timezone.utc)
U32_MAX = (1 << 32) - 1

# --------------------------
# Basic helpers
# --------------------------

def to_int(s, default=None):
    try:
        return int(s)
    except Exception:
        return default

def seconds_to_iso(sec_le):
    try:
        if sec_le is None:
            return ""
        if not (0 <= sec_le <= U32_MAX):
            return ""
        return (CAMPBELL_EPOCH + timedelta(seconds=int(sec_le))).isoformat()
    except Exception:
        return ""

def bytes_from_hex_maybe(h: str):
    if not h:
        return b""
    h = h.strip().replace(":", "").replace(" ", "")
    if len(h) % 2 == 1:
        h = "0" + h
    try:
        return bytes.fromhex(h)
    except Exception:
        return b""

def deframe_segments(data_bytes: bytes):
    """
    Collect BD ... BD sequences (inclusive).
    """
    frames = []
    start = None
    for i, b in enumerate(data_bytes):
        if b == 0xBD:
            if start is None:
                start = i
            else:
                frames.append(data_bytes[start:i+1])
                start = None
    return frames

def read_le_u32(b: bytes, off: int):
    if off + 4 > len(b):
        return None
    return int.from_bytes(b[off:off+4], "little")

def read_be_i16(b: bytes, off: int):
    if off + 2 > len(b):
        return None
    return int.from_bytes(b[off:off+2], "big", signed=True)

# --------------------------
# File / name parsing
# --------------------------

LEAF_TABLE_RE = re.compile(r"^([A-Za-z0-9]+)_Table(\d+)\.tsv$", re.IGNORECASE)

def parse_leaf_table_from_filename(p: Path):
    m = LEAF_TABLE_RE.match(p.name)
    if m:
        return m.group(1).upper(), to_int(m.group(2), 0)
    return "", 0

# --------------------------
# Robust TSV reading (fixes 0xff decode)
# --------------------------

def read_tsv_text(tsv_path: Path) -> str:
    """
    Open TSV as bytes, detect encoding:
      - UTF-16 BOM (ff fe / fe ff) -> 'utf-16'
      - UTF-8 BOM -> 'utf-8-sig'
      - Try utf-8, else latin-1
    """
    raw = tsv_path.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        enc = "utf-16"
    elif raw.startswith(b"\xef\xbb\xbf"):
        enc = "utf-8-sig"
    else:
        try:
            raw.decode("utf-8")
            enc = "utf-8"
        except UnicodeDecodeError:
            enc = "latin-1"
    return raw.decode(enc, errors="replace")

def parse_tsv_rows(tsv_path: Path):
    """
    Yield dict rows with at least:
      - tcp.payload
      - frame.number (may be blank)
      - tcp.stream (may be blank)
    Works with or without header.
    """
    text = read_tsv_text(tsv_path)
    lines = text.splitlines()
    if not lines:
        return

    header_line = lines[0]
    has_header = ("tcp.payload" in header_line) or ("frame.number" in header_line) or ("tcp.stream" in header_line)

    if has_header:
        header = header_line.split("\t")
        def idx(name, default_last=False):
            try:
                return header.index(name)
            except ValueError:
                return len(header) - 1 if default_last else None

        tcp_payload_idx = idx("tcp.payload", default_last=True)
        frame_idx = idx("frame.number")
        stream_idx = idx("tcp.stream")

        data_lines = lines[1:]
        for raw in data_lines:
            if not raw:
                continue
            cols = raw.split("\t")
            payload = cols[tcp_payload_idx].strip() if tcp_payload_idx is not None and tcp_payload_idx < len(cols) else ""
            frame_no = cols[frame_idx].strip() if frame_idx is not None and frame_idx < len(cols) else ""
            stream_no = cols[stream_idx].strip() if stream_idx is not None and stream_idx < len(cols) else ""
            yield {"tcp.payload": payload, "frame.number": frame_no, "tcp.stream": stream_no}
    else:
        # No header: last column is payload
        for raw in lines:
            if not raw:
                continue
            cols = raw.split("\t")
            payload = cols[-1].strip()
            yield {"tcp.payload": payload, "frame.number": "", "tcp.stream": ""}

# --------------------------
# Frame feature extraction
# --------------------------

ANCHOR_OFF_CANDIDATES = [8, 12, 16, 20, 24, 28, 32]

def choose_best_anchor(frame: bytes):
    """
    Pick the 'most time-like' 4-byte LE integer among the candidate offsets:
      - must be within 0..2^31-1
      - prefer values >= 1e8 (avoid early-1990 noise)
      - among remaining, choose the largest value
    Return (offset, seconds) or ("", "")
    """
    cands = []
    for off in ANCHOR_OFF_CANDIDATES:
        v = read_le_u32(frame, off)
        if v is None:
            continue
        if 0 <= v <= (1 << 31) - 1:
            cands.append((off, v))
    if not cands:
        return "", ""

    hi = [c for c in cands if c[1] >= 100_000_000]
    chosen = max(hi, key=lambda x: x[1]) if hi else max(cands, key=lambda x: x[1])
    return chosen

def extract_frame_features(frame: bytes):
    """
    Return a dict with features for cataloging.
    """
    out = {
        "frame_len": len(frame),
        "frame_hex": frame.hex(),
        "anchor_off": "",
        "anchor_sec_le": "",
        "anchor_iso": "",
        "signed16_be@20": "",
        "signed16_be@22": "",
    }

    # Better anchor selection (see choose_best_anchor)
    off, sec = choose_best_anchor(frame)
    if off != "":
        out["anchor_off"] = off
        out["anchor_sec_le"] = sec
        out["anchor_iso"] = seconds_to_iso(sec)

    # Only probe the short request shape for BE i16 values
    if len(frame) == 31:
        v20 = read_be_i16(frame, 20)
        v22 = read_be_i16(frame, 22)
        if v20 is not None:
            out["signed16_be@20"] = v20
        if v22 is not None:
            out["signed16_be@22"] = v22

    return out

def decode_request_hint(leaf, table, frame: bytes, feats: dict):
    """
    Classify short request frames.
    Returns dict with: leaf, table, frame_len, request_mode, n_hint
    """
    mode = ""
    n_hint = ""
    flen = len(frame)

    if flen == 31:
        v20 = feats.get("signed16_be@20", None)
        if isinstance(v20, int) and v20 < 0:
            mode = "last_n"
            n_hint = abs(v20)

    return {
        "leaf": leaf,
        "table": table,
        "frame_len": flen,
        "request_mode": mode,
        "n_hint": n_hint,
    }

# --------------------------
# CSV helpers
# --------------------------

def write_csv(path: Path, rows, header):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow([r.get(k, "") for k in header])

# --------------------------
# Main processing
# --------------------------

def process_tsv(tsv_path: Path, out_dir: Path, all_frames_accum, decoded_accum):
    leaf, table = parse_leaf_table_from_filename(tsv_path)
    per_leaf_rows = []

    for row in parse_tsv_rows(tsv_path):
        payload_hex = row.get("tcp.payload", "").strip()
        if not payload_hex:
            continue
        data = bytes_from_hex_maybe(payload_hex)
        if not data:
            continue

        frames = deframe_segments(data)
        if not frames:
            continue

        frame_no = to_int(row.get("frame.number", ""), "")
        stream_no = to_int(row.get("tcp.stream", ""), "")

        for frm in frames:
            feats = extract_frame_features(frm)

            cat_row = {
                "src_file": tsv_path.name,
                "leaf": leaf,
                "table": table,
                "frame_len": feats["frame_len"],
                "frame_hex": feats["frame_hex"],
                "anchor_off": feats["anchor_off"],
                "anchor_sec_le": feats["anchor_sec_le"],
                "anchor_iso": feats["anchor_iso"],
                "signed16_be@20": feats["signed16_be@20"],
                "signed16_be@22": feats["signed16_be@22"],
                "frame_number": frame_no,
                "tcp_stream": stream_no,
            }
            per_leaf_rows.append(cat_row)
            all_frames_accum.append(cat_row)

            hint = decode_request_hint(leaf, table, frm, feats)
            if hint["request_mode"]:
                hint["tcp_stream"] = stream_no
                hint["frame_number"] = frame_no
                hint["src_file"] = tsv_path.name
                hint["frame_hex"] = feats["frame_hex"]
                decoded_accum.append(hint)

    # Per-leaf catalog
    out_leaf = out_dir / (f"{leaf}_Table{table}_catalog.csv" if leaf and table else "__catalog.csv")
    header = [
        "src_file","leaf","table","frame_len","frame_hex",
        "anchor_off","anchor_sec_le","anchor_iso",
        "signed16_be@20","signed16_be@22","frame_number","tcp_stream",
    ]
    write_csv(out_leaf, per_leaf_rows, header)
    print(f"[ok] wrote {out_leaf} ({len(per_leaf_rows)} frames)")

def build_decoded_summary(decoded_rows, out_dir: Path):
    groups = defaultdict(list)
    for r in decoded_rows:
        key = (r.get("leaf",""), r.get("table",0), r.get("request_mode",""))
        n = r.get("n_hint", "")
        if isinstance(n, int):
            groups[key].append(n)

    summary_rows = []
    for (leaf, table, mode), values in sorted(groups.items()):
        if not values:
            continue
        summary_rows.append({
            "leaf": leaf, "table": table, "request_mode": mode,
            "min_n": min(values), "max_n": max(values), "count": len(values),
        })

    out_path = out_dir / "decoded_summary.csv"
    header = ["leaf","table","request_mode","min_n","max_n","count"]
    write_csv(out_path, summary_rows, header)
    print(f"[ok] wrote {out_path} ({len(summary_rows)} groups)")

def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv-dir", required=True, help="Directory with Wireshark/TShark TSVs")
    ap.add_argument("--out-dir", required=True, help="Output directory for catalogs")
    args = ap.parse_args()

    tsv_dir = Path(args.tsv_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    all_frames = []
    decoded_rows = []

    tsvs = sorted(tsv_dir.glob("*.tsv"))
    if not tsvs:
        print(f"No .tsv files found in {tsv_dir}")
        sys.exit(0)

    for t in tsvs:
        process_tsv(t, out_dir, all_frames, decoded_rows)

    # ALL frames catalog
    all_out = out_dir / "ALL_frames_catalog.csv"
    all_header = [
        "src_file","leaf","table","frame_len","frame_hex",
        "anchor_off","anchor_sec_le","anchor_iso",
        "signed16_be@20","signed16_be@22","frame_number","tcp_stream",
    ]
    write_csv(all_out, all_frames, all_header)
    print(f"[summary] aggregate: {all_out}")

    # Decoded request hints
    decoded_out = out_dir / "decoded_request_hints.csv"
    decoded_header = [
        "leaf","table","frame_len","request_mode","n_hint",
        "signed16_be@20","signed16_be@22","anchor_off","anchor_sec_le","anchor_iso",
        "tcp_stream","frame_number","src_file","frame_hex",
    ]
    write_csv(decoded_out, decoded_rows, decoded_header)
    print(f"[ok] wrote {decoded_out} ({len(decoded_rows)} frames)")

    # Rollup
    build_decoded_summary(decoded_rows, out_dir)

    print(f"[summary] total frames cataloged: {len(all_frames)}")

if __name__ == "__main__":
    main()