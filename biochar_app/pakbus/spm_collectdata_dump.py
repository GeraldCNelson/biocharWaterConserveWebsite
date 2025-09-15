# biochar_app/pakbus/spm_collectdata_dump.py
# Analyze / dump CollectData traffic from a Serial Port Monitor (SPM) "Dump view" text
# and (optionally) decode CollectData *responses* for the CR200X Public table.
#
# Examples:
#   # raw TX/RX frame heads (sanity check)
#   python -m biochar_app.pakbus.spm_collectdata_dump "…/Session 09_11_2025 10 am.txt" --raw
#
#   # try to decode Public rows found in RX (0x89) frames
#   python -m biochar_app.pakbus.spm_collectdata_dump "…/Session 09_11_2025 10 am.txt" --decode-public
#
# Notes:
# - Heuristic for CR200X PakBus frames seen in SPM Dump output:
#     b[8]  = opcode (0x09=request, 0x89=response)
#     b[9]  = transaction number (changes each request)
#     payload ≈ b[10:-2]   (skip 2-byte CRC)
# - Public table schema here matches your TOA5 Public .dat header.

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import struct

# ---------- existing CollectData-line analyzer (kept) ----------

LINE_RE = re.compile(
    r"""
    ^\s*
    \[(?P<idx>\d+)]\s+CollectData\s+
    (?P<kind>req|resp)\s+
    table=(?P<table>-?\d+)\s+
    mode=0x(?P<mode>[0-9A-Fa-f]{1,4})\s+
    sig=(?P<sig>(0x[0-9A-Fa-f]{1,4}|n/a))
    """,
    re.VERBOSE,
)

ASCIIISH = {0x43, 0x4F, 0x4D}  # 'C','O','M'

@dataclass
class CDEntry:
    idx: int
    kind: str
    table: int
    mode: int
    sig: Optional[int]
    raw_line: str

def _head(b: bytes, n: int = 24) -> str:
    return " ".join(f"{x:02x}" for x in b[:n])

# CR200X epoch (seconds since 1990-01-01 UTC)
CR200X_EPOCH = datetime(1990, 1, 1, tzinfo=timezone.utc)

def cr200x_secs_to_iso8601(raw_secs: float) -> str:
    try:
        # logger sends seconds since 1990 as IEEE-754 double (for TOA5 timestamp)
        dt = CR200X_EPOCH + timedelta(seconds=float(raw_secs))
        return dt.isoformat()
    except Exception:
        return f"raw={raw_secs}"

def parse_entries(text: str) -> List[CDEntry]:
    out: List[CDEntry] = []
    for line in text.splitlines():
        m = LINE_RE.search(line)
        if not m:
            continue
        idx = int(m.group("idx"))
        kind = m.group("kind")
        table = int(m.group("table"))
        try:
            mode = int(m.group("mode"), 16)
        except ValueError:
            continue
        sig_raw = m.group("sig")
        sig = None if sig_raw.lower() == "n/a" else int(sig_raw, 16)
        out.append(CDEntry(idx, kind, table, mode, sig, line))
    return out

def is_mode_suspicious(m: int) -> bool:
    return m > 0x10 or m in ASCIIISH

def is_table_suspicious(t: int) -> bool:
    return t < 0 or t > 70000 or ((t & 0xFF00) in {0x4300, 0x4F00, 0x4D00})

def is_sig_suspicious(sig: Optional[int]) -> bool:
    if sig is None:
        return True
    hi = (sig >> 8) & 0xFF
    lo = sig & 0xFF
    return hi in ASCIIISH or lo in ASCIIISH

def summarize(entries: List[CDEntry]) -> str:
    total = len(entries)
    req = sum(1 for e in entries if e.kind == "req")
    resp = total - req

    modes_susp = sum(1 for e in entries if is_mode_suspicious(e.mode))
    tables_susp = sum(1 for e in entries if is_table_suspicious(e.table))
    sigs_susp = sum(1 for e in entries if is_sig_suspicious(e.sig))

    reqs = [e for e in entries if e.kind == "req"]
    resps = [e for e in entries if e.kind == "resp"]
    resp_by_table: dict[int, List[CDEntry]] = {}
    for r in resps:
        resp_by_table.setdefault(r.table, []).append(r)

    used: set[Tuple[int, int]] = set()
    matched = 0
    for r in reqs:
        cand = resp_by_table.get(r.table, [])
        best: Optional[CDEntry] = None
        for c in cand:
            key = (c.table, c.idx)
            if key in used or c.idx < r.idx:
                continue
            if r.sig is not None and c.sig is not None and r.sig == c.sig:
                best = c
                break
            if best is None:
                best = c
        if best:
            used.add((best.table, best.idx))
            matched += 1

    lines = [
        f"[ok] parsed frames with CollectData msg: {total}",
        f"      req={req}  resp={resp}  paired={matched}  unpaired_reqs={max(0, req - matched)}",
    ]
    if total:
        lines.append(
            f"      suspicious ratios:  mode={modes_susp}/{total}  table={tables_susp}/{total}  sig={sigs_susp}/{total}"
        )
    return "\n".join(lines)

# ---------- NEW: parse SPM Dump text into raw TX/RX frames ----------

# SPM lines look like:
#   [MM/DD/YYYY HH:MM:SS] Written data (COM7)
#   bd a0 0d 6f fd 10 0d 0f fd 09 34 ...
# or
#   [..] Read data (COM7)
#   bd af fd 00 0d ...
RE_WHICH = re.compile(r"\]\s+(Written|Read)\s+data", re.I)
RE_HEXROW = re.compile(r"(?i)(?:^|\s)([0-9a-f]{2})(?=\s|$)")

@dataclass
class RawFrame:
    dir: str          # 'TX' or 'RX'
    line_no: int      # approximate source line in SPM text (for reference)
    bytes_: bytes

def _collect_hex_after(i: int, lines: List[str]) -> Tuple[bytes, int]:
    """Collect contiguous hex rows that follow a 'Written/Read data' line."""
    buf: List[int] = []
    j = i + 1
    while j < len(lines):
        m_all = RE_HEXROW.findall(lines[j])
        if not m_all:
            break
        for hh in m_all:
            buf.append(int(hh, 16))
        j += 1
    return bytes(buf), j

def extract_raw_frames(spm_text: str) -> List[RawFrame]:
    lines = spm_text.splitlines()
    frames: List[RawFrame] = []
    i = 0
    while i < len(lines):
        m = RE_WHICH.search(lines[i])
        if not m:
            i += 1
            continue
        kind = m.group(1).lower()
        b, nxt = _collect_hex_after(i, lines)
        if b:
            frames.append(RawFrame("TX" if kind == "written" else "RX", i + 1, b))
        i = nxt
    return frames

# ---------- NEW: decode CR200X 'Public' table row from 0x89 response payload ----------

PUBLIC_SCHEMA = [
    # TOA5 header order (Public)
    "TIMESTAMP", "RECORD",
    "BattV",
    "VWC_1","EC_1","T_1","P_1","PA_1","VR_1",
    "VWC_2","EC_2","T_2","P_2","PA_2","VR_2",
    "VWC_3","EC_3","T_3","P_3","PA_3","VR_3",
]
# Bytes per field
# TIMESTAMP: 8 (double), RECORD: 4 (uint32), the rest: 4 (float) each
PUBLIC_PAYLOAD_LEN = 8 + 4 + (len(PUBLIC_SCHEMA) - 2) * 4  # 88 bytes

def decode_public_payload(payload: bytes) -> Optional[dict]:
    """Return a dict for one Public row if payload length matches, else None."""
    if len(payload) != PUBLIC_PAYLOAD_LEN:
        return None
    off = 0
    # timestamp (double, little-endian)
    ts, = struct.unpack_from("<d", payload, off); off += 8
    # record (uint32)
    rn, = struct.unpack_from("<I", payload, off); off += 4
    row = {
        "TIMESTAMP": cr200x_secs_to_iso8601(ts),
        "RECORD": rn,
    }
    for name in PUBLIC_SCHEMA[2:]:
        val, = struct.unpack_from("<f", payload, off); off += 4
        row[name] = float(val)
    return row

def try_decode_public_from_frames(frames: List[RawFrame]) -> List[dict]:
    """Find RX CollectData response frames (opcode 0x89) and decode Public rows."""
    out: List[dict] = []
    for fr in frames:
        if fr.dir != "RX":
            continue
        b = fr.bytes_
        # heuristic: opcode at byte 8 (0x89 means CollectData response)
        if len(b) < 12 or b[8] != 0x89:
            continue
        # assume payload = b[10:-2] (skip CRC)
        payload = b[10:-2] if len(b) > 12 else b[10:]
        row = decode_public_payload(payload)
        if row is not None:
            out.append(row)
    return out

# ---------- CLI ----------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Parse SPM session text for CollectData and (optionally) decode CR200X Public rows."
    )
    ap.add_argument("session_file", help="Path to SPM Dump-view .txt (or .log)")
    ap.add_argument("--tail", type=int, default=None, help="Show last N CollectData lines before summary")
    ap.add_argument("--no-summary", action="store_true", help="Hide CollectData summary")
    ap.add_argument("--raw", action="store_true", help="Dump raw TX/RX frame heads parsed from SPM text")
    ap.add_argument("--decode-public", action="store_true", help="Attempt to decode Public rows from RX frames")
    args = ap.parse_args(argv)

    # Read file
    try:
        txt = open(args.session_file, "r", encoding="utf-8", errors="replace").read()
    except Exception as e:
        print(f"[err] failed to read {args.session_file}: {e}", file=sys.stderr)
        return 1

    # 1) Optional raw frame dump
    if args.raw or args.decode_public:
        frames = extract_raw_frames(txt)
        if args.raw:
            print("---- raw PakBus frames (both) ----")
            for i, fr in enumerate(frames, 1):
                print(f"[{i:04d}] {fr.dir}  line~{fr.line_no:<5} len={len(fr.bytes_):>4}  head={_head(fr.bytes_, 24)}")
            print(f"[ok] total frames: {len(frames)}")
        if args.decode_public:
            rows = try_decode_public_from_frames(frames)
            if rows:
                print("\n---- Public rows decoded from RX (0x89) ----")
                for r in rows:
                    # compact row print
                    head = f"{r['TIMESTAMP']}  RN={r['RECORD']}"
                    rest = "  " + "  ".join(f"{k}={r[k]:.5g}" for k in PUBLIC_SCHEMA[2:])
                    print(head + rest)
                print(f"[ok] decoded {len(rows)} Public row(s).")
            else:
                print("[info] no decodable Public rows found in RX frames (either RX not captured or payload size mismatch).")

    # 2) Legacy CollectData line analyzer (only if present)
    entries = parse_entries(txt)
    if entries:
        to_show = entries[-args.tail:] if args.tail else entries
        print("\n---- CollectData ----" + (" (truncated)" if args.tail else ""))
        for e in to_show:
            print(e.raw_line)
        if not args.no_summary:
            print("\n---- summary ----")
            print(summarize(entries))
    else:
        if not (args.raw or args.decode_public):
            print("[info] No 'CollectData req/resp' lines in file. Try --raw or --decode-public for RX inspection.")

    return 0

if __name__ == "__main__":
    sys.exit(main())