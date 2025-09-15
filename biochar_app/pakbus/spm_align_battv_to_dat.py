#!/usr/bin/env python3
"""
Align BattV_Min values found in an SPM dump (frames.txt or raw .txt) to rows in a TOA5 .dat,
and emit TIMESTAMP / RECORD / BattV for each matched hit.

Examples:
  # Typical: frames + dat
  python -m biochar_app.pakbus.spm_align_battv_to_dat \
    --source "biochar_app/data-raw/spm_sessions/COM7 Monitoring Session 09_11_2025 10 am.frames.txt" \
    --dat "biochar_app/data-raw/spm_sessions/Table_1_test_Table1.dat"

  # Use the raw .txt instead of frames:
  python -m biochar_app.pakbus.spm_align_battv_to_dat \
    --source "biochar_app/data-raw/spm_sessions/COM7 Monitoring Session 09_11_2025 10 am.txt" \
    --dat "biochar_app/data-raw/spm_sessions/Table_1_test_Table1.dat"

  # Or feed the already-scanned hits CSV (offset,battv[,hex]) you generated earlier:
  python -m biochar_app.pakbus.spm_align_battv_to_dat \
    --hits-csv "biochar_app/data-raw/spm_sessions/decoded/battv_from_dump_scan.csv" \
    --dat "biochar_app/data-raw/spm_sessions/Table_1_test_Table1.dat"
"""

from __future__ import annotations
import argparse, csv, re
from pathlib import Path
from typing import List, Tuple, Optional

import math, struct

DECODED_DEFAULT = Path("biochar_app/data-raw/spm_sessions/decoded")
SESSIONS_DIR    = Path("biochar_app/data-raw/spm_sessions")

FloatHit = Tuple[int, float]  # (offset, battv)

def parse_toa5_dat(p: Path) -> List[Tuple[str, int, float]]:
    rows = []
    lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    hdr_idx = None
    for i, ln in enumerate(lines[:15]):
        if "TIMESTAMP" in ln and "RECORD" in ln and "BattV_Min" in ln:
            hdr_idx = i
            break
    if hdr_idx is None:
        raise SystemExit(f"[err] could not find TIMESTAMP/RECORD/BattV_Min header in {p}")

    # data usually starts 3 lines after the header block in TOA5
    for ln in lines[hdr_idx+3:]:
        if not ln.strip():
            continue
        m = re.match(r'^"([^"]+)"\s*,\s*([^,]+)\s*,\s*([^,]+)', ln)
        if not m:
            continue
        ts_str = m.group(1)
        rec_s  = m.group(2).strip()
        batt_s = m.group(3).strip()
        try:
            rec = int(rec_s)
            batt = float(batt_s)
        except:
            # skip NANs and non-numeric rows
            continue
        rows.append((ts_str, rec, batt))
    return rows

def harvest_hex_bytes(txt: Path) -> bytes:
    hex_token = re.compile(r'\b[0-9a-fA-F]{2}\b')
    out = bytearray()
    with txt.open("r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            toks = hex_token.findall(ln)
            for t in toks:
                out.append(int(t, 16))
    return bytes(out)

def scan_battv_be_from_bytes(b: bytes, vmin=10.0, vmax=16.0) -> List[FloatHit]:
    hits: List[FloatHit] = []
    n = len(b)
    for off in range(0, n - 3):
        fval = struct.unpack_from(">f", b, off)[0]
        if math.isfinite(fval) and vmin <= fval <= vmax:
            hits.append((off, float(fval)))
    return hits

def sibling_variant(p: Path) -> Optional[Path]:
    """Switch .frames.txt <-> .txt if possible."""
    s = p.suffixes
    if s[-2:] == ['.frames', '.txt']:
        alt = p.with_suffix('').with_suffix('').with_suffix('.txt')
        return alt if alt.exists() else None
    if s[-1:] == ['.txt'] and not p.name.endswith(".frames.txt"):
        alt = p.with_suffix('.frames.txt')
        return alt if alt.exists() else None
    return None

def align_sequences(dump_hits: List[FloatHit], dat_rows, eps=0.02, max_ahead=80):
    out = []
    j = 0
    m = len(dat_rows)
    for offset, vdump in dump_hits:
        matched = None
        upper = min(m, j + max_ahead)
        for k in range(j, upper):
            ts, rec, vdat = dat_rows[k]
            if abs(vdat - vdump) <= eps:
                matched = (k, ts, rec, vdat)
                break
        if matched:
            k, ts, rec, vdat = matched
            out.append({
                "offset": offset,
                "battv_dump": f"{vdump:.5f}",
                "battv_dat": f"{vdat:.5f}",
                "abs_diff": f"{abs(vdat - vdump):.5f}",
                "record": rec,
                "timestamp": ts
            })
            j = k + 1
        else:
            out.append({
                "offset": offset,
                "battv_dump": f"{vdump:.5f}",
                "battv_dat": "",
                "abs_diff": "",
                "record": "",
                "timestamp": ""
            })
    return out

def load_hits_csv(p: Path) -> List[FloatHit]:
    hits: List[FloatHit] = []
    with p.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                off = int(row.get("offset") or row.get("Offset") or row.get("byte_offset") or "0")
                val = float(row.get("battv") or row.get("BattV") or row.get("value"))
            except:
                continue
            hits.append((off, val))
    return hits

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path,
                    help="SPM frames.txt or raw .txt to scan (will try sibling variant if no hits found)")
    ap.add_argument("--hits-csv", type=Path,
                    help="Use a precomputed hits CSV (offset,battv[,hex]) instead of scanning text")
    ap.add_argument("--dat", type=Path, required=True,
                    help="TOA5 file (Table_1_test_Table1.dat)")
    ap.add_argument("--eps", type=float, default=0.02, help="match tolerance (V)")
    ap.add_argument("--max-ahead", type=int, default=80, help="lookahead rows during alignment")
    ap.add_argument("--out", type=Path, help="output CSV (default: decoded/battv_aligned.csv)")
    ap.add_argument("--vmin", type=float, default=10.0)
    ap.add_argument("--vmax", type=float, default=16.0)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_path = args.out or (DECODED_DEFAULT / "battv_aligned.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    dat_rows = parse_toa5_dat(args.dat)
    if not dat_rows:
        raise SystemExit(f"[err] parsed zero usable rows from {args.dat}")

    # Source of dump hits
    if args.hits_csv and args.hits_csv.exists():
        dump_hits = load_hits_csv(args.hits_csv)
        if not dump_hits:
            raise SystemExit(f"[err] no rows in {args.hits_csv}")
        print(f"[info] using precomputed hits from {args.hits_csv}: {len(dump_hits)} rows")
    else:
        src = args.source
        if not src or not src.exists():
            raise SystemExit("[err] --source is required if --hits-csv is not provided")

        b = harvest_hex_bytes(src)
        if args.debug:
            print(f"[debug] harvested {len(b)} bytes from {src}")
            if len(b) >= 32:
                print("[debug] head:", " ".join(f"{x:02x}" for x in b[:32]))

        dump_hits = scan_battv_be_from_bytes(b, vmin=args.vmin, vmax=args.vmax)
        if not dump_hits:
            alt = sibling_variant(src)
            if alt:
                b = harvest_hex_bytes(alt)
                if args.debug:
                    print(f"[debug] harvested {len(b)} bytes from sibling {alt}")
                dump_hits = scan_battv_be_from_bytes(b, vmin=args.vmin, vmax=args.vmax)

        if not dump_hits:
            raise SystemExit(f"[err] found no BattV hits in {src}")

        print(f"[ok] scanned {len(dump_hits)} BattV hits from {src}")

    aligned = align_sequences(dump_hits, dat_rows, eps=args.eps, max_ahead=args.max_ahead)

    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["offset","battv_dump","battv_dat","abs_diff","record","timestamp"])
        w.writeheader()
        for row in aligned:
            w.writerow(row)

    matched = sum(1 for r in aligned if r["record"] != "")
    print(f"[ok] matched {matched}/{len(dump_hits)} hits against {len(dat_rows)} dat rows")
    print(f"[ok] wrote → {out_path}")

if __name__ == "__main__":
    main()