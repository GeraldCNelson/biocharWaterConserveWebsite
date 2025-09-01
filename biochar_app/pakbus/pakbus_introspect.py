#!/usr/bin/env python3
import csv
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# -----------------------------
# Config (adjust paths as needed)
# -----------------------------
PCAP_DIR_DEFAULT = Path("biochar_app/pakbus/bdFiles")
TSV_DIR_DEFAULT  = Path("biochar_app/pakbus/bdFiles")     # where *.tsv will live
OUT_DIR_DEFAULT  = Path("biochar_app/pakbus/decoded")     # where summary CSVs will go

TCP_PORT = 6785

# -----------------------------
# Utilities
# -----------------------------

def have_tshark() -> Optional[str]:
    exe = shutil.which("tshark")
    if exe:
        return exe
    # Homebrew default on Apple Silicon
    cand = "/opt/homebrew/bin/tshark"
    return cand if os.path.exists(cand) else None

def station_from_file(stem: str) -> Tuple[str, str]:
    """
    Accepts stems like:
      - 'Table1S3M' -> table='Table1', leaf='S3M'
      - 'S3M_Table1' -> table='Table1', leaf='S3M'
    Returns (table_str, leaf_str) or ('','') if not recognized.
    """
    m = re.match(r"^(Table\d+)([A-Za-z0-9_]+)$", stem)
    if m:
        return m.group(1), m.group(2).upper()
    m = re.match(r"^([A-Za-z0-9_]+)_Table(\d+)$", stem)
    if m:
        return f"Table{m.group(2)}", m.group(1).upper()
    return "", ""

def deframe_all(buf: bytes) -> List[bytes]:
    """Return inner payloads between 0xBD … 0xBD."""
    frames, cur = [], bytearray()
    in_frame = False
    for b in buf:
        if b == 0xBD:
            if in_frame and cur:
                frames.append(bytes(cur))
            cur = bytearray()
            in_frame = not in_frame
            continue
        if in_frame:
            cur.append(b)
    return frames

def run_tshark_to_tsv(pcap_dir: Path, tsv_dir: Path, port: int = TCP_PORT) -> None:
    """
    Create TSVs from all *.pcapng in pcap_dir.
    Output name: LEAF_Table#.tsv (e.g., S3M_Table1.tsv) to match your earlier convention.
    """
    tshark = have_tshark()
    if not tshark:
        print("WARNING: tshark not found; skipping TSV generation.")
        return

    tsv_dir.mkdir(parents=True, exist_ok=True)

    for pcap in sorted(pcap_dir.glob("*.pcapng")):
        base = pcap.stem  # e.g., Table1S3M
        table, leaf = station_from_file(base)
        if table and leaf:
            out_name = f"{leaf}_{table}.tsv"
        else:
            # Fallback
            out_name = f"{base}.tsv"

        out_path = tsv_dir / out_name
        print(f"→ Writing {out_path.name} from {pcap.name}")

        cmd = [
            tshark, "-r", str(pcap),
            "-Y", f"tcp.port=={port}",
            "-T", "fields",
            "-E", "header=y", "-E", "separator=\t",
            "-e", "frame.number", "-e", "tcp.stream", "-e", "ip.src", "-e", "ip.dst", "-e", "tcp.payload",
        ]
        with open(out_path, "w", encoding="utf-8") as f:
            subprocess.run(cmd, stdout=f, check=False)

def parse_tsv(tsv_path: Path) -> List[Dict[str, str]]:
    """
    Read a tshark TSV (headers expected), return list of rows with keys:
      frame.number, tcp.stream, ip.src, ip.dst, tcp.payload
    """
    rows = []
    with open(tsv_path, "r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        # find needed columns by name (robust to column order)
        cols = {name: i for i, name in enumerate(header)}
        need = ["frame.number", "tcp.stream", "ip.src", "ip.dst", "tcp.payload"]
        if not all(n in cols for n in need):
            # Try a no-header TSV (unlikely if we used header=y)
            f.seek(0)
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 5:
                    rows.append({
                        "frame.number": parts[0],
                        "tcp.stream":   parts[1],
                        "ip.src":       parts[2],
                        "ip.dst":       parts[3],
                        "tcp.payload":  parts[4],
                    })
            return rows

        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < len(header):  # skip short lines
                continue
            rows.append({
                "frame.number": parts[cols["frame.number"]],
                "tcp.stream":   parts[cols["tcp.stream"]],
                "ip.src":       parts[cols["ip.src"]],
                "ip.dst":       parts[cols["ip.dst"]],
                "tcp.payload":  parts[cols["tcp.payload"]],
            })
    return rows

def clean_hex(s: str) -> str:
    return s.replace(":", "").replace(" ", "").strip()

def extract_request_frames_from_payload_hex(payload_hex: str) -> List[bytes]:
    """
    Given a *TCP segment* hex string, deframe all BD…BD packets and
    return only those frames whose *full framed* length is 27 or 31 bytes
    (i.e., the request blobs we care about).
    """
    out = []
    if not payload_hex:
        return out
    try:
        b = bytes.fromhex(clean_hex(payload_hex))
    except Exception:
        return out
    for inner in deframe_all(b):
        framed = b"\xBD" + inner + b"\xBD"
        if len(framed) in (27, 31):
            out.append(framed)
    return out

# -----------------------------
# Best-effort request decoding
# -----------------------------
# We’ll keep this conservative (no false claims):
# - Fill table from filename (reliable).
# - Try to infer a small 16-bit "count" if a stable pattern emerges; otherwise leave blank.
# - Start-kind/time is left blank for now (we’ll populate once we lock the exact wire layout).

def maybe_guess_count(inner_payload: bytes) -> Optional[int]:
    """
    Very cautious heuristic: look for a small unsigned 16-bit near the tail
    that often matches the requested record count. If not found, return None.
    (We deliberately keep this conservative to avoid mislabeling.)
    """
    if len(inner_payload) < 6:
        return None
    # Try last two bytes as BE or LE
    tail2 = inner_payload[-2:]
    be = int.from_bytes(tail2, "big")
    le = int.from_bytes(tail2, "little")
    # Prefer small, non-zero values typical for "count"
    candidates = [x for x in (be, le) if 0 < x <= 512]
    if candidates:
        return min(candidates)
    return None

def decode_request_fields(framed: bytes) -> Dict[str, Optional[str]]:
    """
    Returns a dict with keys:
      frame_len, frame_hex, guessed_count (str or '')
    We keep it minimal and honest for now.
    """
    inner = framed[1:-1]
    # In many captures, the first ~8 bytes are the PakBus transport header.
    # The application piece comes after that; we don't assume a fixed command code here.
    app = inner[8:] if len(inner) > 8 else b""
    guessed = maybe_guess_count(app)
    return {
        "frame_len": str(len(framed)),
        "frame_hex": framed.hex(),
        "guessed_count": str(guessed) if guessed is not None else "",
        # Future: "start_kind" / "start_value" once we formalize.
    }

# -----------------------------
# Main “introspect” flow
# -----------------------------

def process_all(tsv_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    tsvs = sorted(tsv_dir.glob("*.tsv"))
    if not tsvs:
        print(f"No TSV files in {tsv_dir}")
        return

    # Group records by station (leaf+table) to produce one CSV per station
    grouped: Dict[str, List[Dict[str, str]]] = {}

    for tsv in tsvs:
        table, leaf = station_from_file(tsv.stem)
        if not (table and leaf):
            # Skip unknown naming
            continue
        station_key = f"{leaf}_{table}"
        rows = parse_tsv(tsv)
        # Track first/last stream/frame where we saw a request
        first_frame, last_frame = None, None
        first_stream, last_stream = None, None

        for r in rows:
            payload_hex = r.get("tcp.payload", "").strip()
            req_frames = extract_request_frames_from_payload_hex(payload_hex)
            if not req_frames:
                continue
            # Update seen frame/stream bounds
            try:
                fr = int(r.get("frame.number", "") or "0")
                st = int(r.get("tcp.stream", "") or "0")
            except ValueError:
                fr, st = None, None
            if fr is not None:
                if first_frame is None or fr < first_frame: first_frame = fr
                if last_frame is None or fr > last_frame:   last_frame  = fr
            if st is not None:
                if first_stream is None or st < first_stream: first_stream = st
                if last_stream is None or st > last_stream:   last_stream  = st

            for framed in req_frames:
                info = decode_request_fields(framed)
                info.update({
                    "leaf": leaf,
                    "table": table.replace("Table", ""),
                    "first_frame": str(first_frame) if first_frame is not None else "",
                    "last_frame":  str(last_frame)  if last_frame  is not None else "",
                    "first_stream": str(first_stream) if first_stream is not None else "",
                    "last_stream":  str(last_stream)  if last_stream  is not None else "",
                })
                grouped.setdefault(station_key, []).append(info)

    # Write one CSV per station
    for station_key, items in grouped.items():
        out_csv = out_dir / f"{station_key}_requests_summary.csv"
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "leaf","table","frame_len","frame_hex",
                "guessed_count",
                "first_frame","last_frame","first_stream","last_stream"
            ])
            for it in items:
                w.writerow([
                    it.get("leaf",""),
                    it.get("table",""),
                    it.get("frame_len",""),
                    it.get("frame_hex",""),
                    it.get("guessed_count",""),
                    it.get("first_frame",""),
                    it.get("last_frame",""),
                    it.get("first_stream",""),
                    it.get("last_stream",""),
                ])
        print(f"Wrote {out_csv}")

def main():
    import argparse
    p = argparse.ArgumentParser(description="PakBus request introspection")
    p.add_argument("--pcap-dir", default=str(PCAP_DIR_DEFAULT),
                   help="Directory with *.pcapng (optional; used to generate TSVs)")
    p.add_argument("--tsv-dir", default=str(TSV_DIR_DEFAULT),
                   help="Directory with *.tsv (either pre-made or generated here)")
    p.add_argument("--out-dir", default=str(OUT_DIR_DEFAULT),
                   help="Directory to write per-station request summary CSVs")
    p.add_argument("--make-tsv", action="store_true",
                   help="If set, run tshark on all *.pcapng to (re)build TSVs first")
    args = p.parse_args()

    pcap_dir = Path(args.pcap_dir)
    tsv_dir  = Path(args.tsv_dir)
    out_dir  = Path(args.out_dir)

    if args.make_tsv:
        run_tshark_to_tsv(pcap_dir, tsv_dir, port=TCP_PORT)

    process_all(tsv_dir, out_dir)

if __name__ == "__main__":
    main()