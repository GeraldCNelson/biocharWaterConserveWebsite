#!/usr/bin/env python3
"""
Harvest TIMESTAMP (and best-effort RECORD) from SPM dump text/bytes
around BattV hits aligned to Table1.dat.

- Handles frames.txt (RX/TX lines, contiguous hex nybbles).
- Accepts aligned CSVs with columns: offset_byte | offset | offset_text.
- Scans ±window bytes for plausible Unix epoch seconds (LE/BE).
"""

import argparse, csv, re, bisect
import struct
from pathlib import Path
from datetime import datetime, timezone

# ---- Hex tokenizer (non-overlapping pairs of hex nybbles) ----
HEX_PAIR = re.compile(r'(?i)[0-9A-F]{2}')   # robust for contiguous hex strings

def parse_dat_timestamp_to_epoch(ts_raw: str) -> int | None:
    """
    Accepts either ISO-ish 'YYYY-MM-DDTHH:MM[:SS][Z|+00:00]' or 'M/D/YY H:MM' (UTC).
    Returns epoch seconds (UTC) or None.
    """
    try:
        if not ts_raw:
            return None
        s = ts_raw.strip()
        if "T" in s:  # ISO-ish
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        else:  # e.g. 9/10/25 10:45
            dt = datetime.strptime(s, "%m/%d/%y %H:%M").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def find_ts_near_value(b: bytes, center: int, halfwin: int, target_epoch: int,
                       slack_seconds: int = 6 * 3600) -> dict | None:
    """
    Scan ±halfwin around center for a 32-bit LE/BE int within slack of target_epoch.
    Returns {"off","u32","endian"} or None.
    """
    lo = max(0, center - halfwin)
    hi = min(len(b), center + halfwin)
    best = None
    best_score = None  # (abs(value-target), distance, prefer LE)
    i = lo
    while i + 3 < hi:
        le = int.from_bytes(b[i:i+4], "little", signed=False)
        be = int.from_bytes(b[i:i+4], "big", signed=False)
        for val, endian in ((le, "LE"), (be, "BE")):
            diff = abs(val - target_epoch)
            if diff <= slack_seconds:
                score = (diff, abs(i - center), 0 if endian == "LE" else 1)
                if best is None or score < best_score:
                    best = {"off": i, "u32": val, "endian": endian}
                    best_score = score
        i += 1
    return best


def find_ts_near_any(b: bytes, center: int, halfwin: int,
                     year_min: int = 2015, year_max: int = 2035) -> dict | None:
    """
    Scan ±halfwin around center for any plausible UNIX epoch (LE/BE) between year_min..year_max.
    Returns closest {"off","u32","endian"} or None.
    """
    lo = max(0, center - halfwin)
    hi = min(len(b), center + halfwin)

    min_epoch = int(datetime(year_min, 1, 1, tzinfo=timezone.utc).timestamp())
    max_epoch = int(datetime(year_max, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())

    best = None
    best_score = None  # (distance, prefer LE)
    i = lo
    while i + 3 < hi:
        le = int.from_bytes(b[i:i+4], "little", signed=False)
        be = int.from_bytes(b[i:i+4], "big", signed=False)
        for val, endian in ((le, "LE"), (be, "BE")):
            if min_epoch <= val <= max_epoch:
                score = (abs(i - center), 0 if endian == "LE" else 1)
                if best is None or score < best_score:
                    best = {"off": i, "u32": val, "endian": endian}
                    best_score = score
        i += 1
    return best

def load_dump_text_and_bytes(path_str):
    """
    Returns:
        src_path: Path
        dump_text: str
        b: bytes (full byte stream recovered from text)
        char_starts: list[(char_pos, byte_pos)] mapping each token's char index to byte index
    Works for:
      - frames.txt lines like: "[0002] RX bdaffd000d1ffd..."
      - raw hex text
      - mixed whitespace/newlines
    """
    src_path = Path(path_str)
    raw = src_path.read_bytes()
    # Try text as UTF-8 with replacement; we only care about hex nybbles
    dump_text = raw.decode("utf-8", errors="replace")

    tokens = []
    char_starts = []  # (char_pos, byte_pos)
    byte_acc = 0

    for m in HEX_PAIR.finditer(dump_text):
        tok = m.group(0)
        tokens.append(tok)
        char_starts.append((m.start(), byte_acc))
        byte_acc += 1  # one byte per token

    if not tokens:
        print(f"[err] no hex tokens parsed from {src_path}")
        return src_path, dump_text, b"", []

    b = bytes(int(t, 16) for t in tokens)
    print(f"[info] source parsed from text: {src_path}  (bytes={len(b)}, tokens={len(tokens)})")
    return src_path, dump_text, b, char_starts

# ---- Map a text-offset (character index in dump_text) to nearest byte index via char_starts ----
def map_text_offset_to_byte_index(char_starts, text_offset):
    """
    char_starts: list of (char_pos, byte_pos) sorted by char_pos
    Returns byte_pos at or immediately before text_offset.
    """
    if not char_starts:
        return None
    positions = [c for (c, _) in char_starts]
    i = bisect.bisect_right(positions, int(text_offset)) - 1
    i = max(0, min(i, len(char_starts)-1))
    return char_starts[i][1]

# ---- Safe 32-bit reads ----
def le_u32_at(b, off):
    if 0 <= off <= len(b)-4:
        return int.from_bytes(b[off:off+4], "little")
    return None

def be_u32_at(b, off):
    if 0 <= off <= len(b)-4:
        return int.from_bytes(b[off:off+4], "big")
    return None

# ---- Plausibility check for epoch seconds ----
def plausible_epoch(u32):
    # Accept roughly 2001–2038 to cover your 2018–2025 use case
    if u32 is None: return False
    return  978307200 <= u32 <= 2145916799  # 2001-01-01 .. 2038-01-18

# ---- Scan ±window bytes around center for plausible epoch seconds (LE/BE) ----
def find_ts_near(b, center, window):
    lo = max(0, center - window)
    hi = min(len(b), center + window)
    hits = []
    for i in range(lo, hi - 3):
        le = le_u32_at(b, i)
        if plausible_epoch(le):
            hits.append({"off": i, "u32": le, "endian": "LE"})
        be = be_u32_at(b, i)
        if plausible_epoch(be):
            hits.append({"off": i, "u32": be, "endian": "BE"})
    return hits

def parse_int_safe(v):
    try:
        return int(v)
    except Exception:
        return None

def scan_be_floats(b, vmin=9.0, vmax=18.0):
    """Return list of (offset, value) for all big-endian 32-bit floats in range."""
    hits = []
    for i in range(0, len(b) - 3):
        f = struct.unpack_from(">f", b, i)[0]
        if vmin <= f <= vmax:
            hits.append((i, f))
    return hits

def pick_centers_by_value(aligned_rows, be_hits, eps=0.006):
    """
    Match each aligned BattV (row order) to the next unused BE float hit within eps.
    Returns list of centers (byte offsets) or None if not found.
    """
    centers = []
    j = 0  # cursor into be_hits
    for row in aligned_rows:
        try:
            want = float(row.get("battv_dump") or row.get("battv") or "")
        except Exception:
            want = None
        if want is None:
            centers.append(None)
            continue
        found = None
        while j < len(be_hits):
            off, val = be_hits[j]
            j += 1
            if abs(val - want) <= eps:
                found = off
                break
        centers.append(found)
    return centers

def main():
    import struct  # needed for BE float scanning

    def scan_be_floats(b, vmin=9.0, vmax=18.0):
        """Return list of (offset, value) for all big-endian 32-bit floats in plausible BattV range."""
        hits = []
        for i in range(0, len(b) - 3):
            f = struct.unpack_from(">f", b, i)[0]
            if vmin <= f <= vmax:
                hits.append((i, f))
        return hits

    def pick_centers_by_value(aligned_rows, be_hits, eps=0.006):
        """
        Match each aligned BattV (row order) to the next unused BE float hit within eps.
        Returns list of centers (byte offsets) or None if not found.
        """
        centers = []
        j = 0
        for row in aligned_rows:
            want_raw = (row.get("battv_dump") or row.get("battv") or "").strip()
            try:
                want = float(want_raw) if want_raw != "" else None
            except Exception:
                want = None
            found = None
            if want is not None:
                while j < len(be_hits):
                    off, val = be_hits[j]
                    j += 1
                    if abs(val - want) <= eps:
                        found = off
                        break
            centers.append(found)
        return centers

    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, help="SPM dump (.frames.txt or .txt with hex)")
    ap.add_argument("--aligned", required=True, help="CSV produced by battv alignment step")
    ap.add_argument("--out", required=True, help="Output CSV with harvested TS/REC")
    ap.add_argument("--window", type=int, default=96, help="± bytes to scan around center")
    ap.add_argument("--year-min", type=int, default=2018,
                    help="Lower bound year for plausible UNIX epochs (default: 2018)")
    ap.add_argument("--year-max", type=int, default=2021,
                    help="Upper bound year for plausible UNIX epochs (default: 2021)")
    args = ap.parse_args()

    # Load dump as text/bytes and positions map
    src_path, dump_text, b, char_starts = load_dump_text_and_bytes(args.source)

    # Read aligned CSV (flexible column names)
    with open(args.aligned, newline="") as f:
        rdr = csv.DictReader(f)
        aligned_rows = list(rdr)

    out_cols = [
        "offset_text", "offset_byte", "battv_dump", "record_dat", "timestamp_dat",
        "ts_off", "ts_u32", "ts_endian", "ts_utc", "dt_minutes_vs_dat",
        "rec_off", "rec_u16_le",
    ]
    out_rows = []

    # 1) Try to map text offsets to byte offsets (works only if aligned CSV matches this source file)
    mapped_bytes = []
    for row in aligned_rows:
        offset_byte = parse_int_safe(row.get("offset_byte"))
        if offset_byte is None:
            offset_byte = parse_int_safe(row.get("offset"))  # earlier runs
        offset_text = parse_int_safe(row.get("offset_text"))

        if offset_byte is None and offset_text is not None:
            try:
                offset_byte = map_text_offset_to_byte_index(char_starts, offset_text)
            except Exception:
                offset_byte = None

        mapped_bytes.append(offset_byte)

    # 2) If mapping looks bogus (most are None or tiny), fall back to value-based centering
    bad_or_tiny = sum(1 for x in mapped_bytes if x is None or x <= 4)
    need_value_centers = (bad_or_tiny > len(mapped_bytes) * 0.6)

    value_centers = []
    if need_value_centers and b:
        be_hits = scan_be_floats(b, 9.0, 18.0)
        value_centers = pick_centers_by_value(aligned_rows, be_hits, eps=0.006)
        matched_count = sum(1 for c in value_centers if c is not None)
        print(f"[info] using value-based centers: matched {matched_count}/{len(value_centers)}")

    # 3) Harvest per row
    for idx, row in enumerate(aligned_rows):
        # Centers: prefer mapped byte if sane; otherwise use value-based center
        offset_text = parse_int_safe(row.get("offset_text"))
        offset_byte = mapped_bytes[idx]
        if need_value_centers and (offset_byte is None or offset_byte <= 4):
            offset_byte = value_centers[idx]

        battv_dump = row.get("battv_dump") or row.get("battv") or ""
        record_dat = parse_int_safe(row.get("record") or row.get("record_dat"))
        timestamp_dat_raw = row.get("timestamp") or row.get("timestamp_dat") or ""
        timestamp_dat_iso = timestamp_dat_raw  # write as-is; we’ll parse for delta if possible

        # ---- timestamp harvesting with fallback ----
        ts_off = None
        ts_u32 = None
        ts_endian = ""
        ts_utc_str = ""
        dt_minutes = ""

        if offset_byte is not None and b:
            best = None

            # 1) Try value-anchored search around the .dat timestamp
            target = parse_dat_timestamp_to_epoch(timestamp_dat_iso)
            if target is not None:
                best = find_ts_near_value(b, offset_byte, args.window, target, slack_seconds=6 * 3600)

            # 2) Fallback: any plausible epoch in the window (e.g., logger OS time 2018–2020)
            if best is None:
                best = find_ts_near_any(b, offset_byte, args.window, year_min=2015, year_max=2035)

            if best:
                ts_off, ts_u32, ts_endian = best["off"], best["u32"], best["endian"]
                try:
                    ts_utc = datetime.fromtimestamp(ts_u32, tz=timezone.utc)
                    ts_utc_str = ts_utc.isoformat()

                    # delta vs .dat (if parseable); will be large if the dump stores old OS time
                    dat_epoch = parse_dat_timestamp_to_epoch(timestamp_dat_iso)
                    if dat_epoch is not None:
                        dt_minutes_val = (ts_u32 - dat_epoch) / 60.0
                        dt_minutes = f"{dt_minutes_val:.2f}"
                except Exception:
                    ts_utc_str = ""

        # ---- best-effort record finder (u16-le == record_dat) ----
        rec_off = ""
        rec_u16_le = ""
        if offset_byte is not None and record_dat is not None and 0 <= record_dat < 65536 and b:
            lo = max(0, offset_byte - args.window)
            hi = min(len(b), offset_byte + args.window)
            best_i = None
            best_dist = None
            i = lo
            while i + 1 < hi:
                val = b[i] | (b[i+1] << 8)
                if val == record_dat:
                    d = abs(i - offset_byte)
                    if best_i is None or d < best_dist:
                        best_i, best_dist = i, d
                i += 1
            if best_i is not None:
                rec_off = best_i
                rec_u16_le = record_dat

        out_rows.append({
            "offset_text": "" if offset_text is None else offset_text,
            "offset_byte": "" if offset_byte is None else offset_byte,
            "battv_dump": battv_dump,
            "record_dat": "" if record_dat is None else record_dat,
            "timestamp_dat": timestamp_dat_iso,
            "ts_off": "" if ts_off is None else ts_off,
            "ts_u32": "" if ts_u32 is None else ts_u32,
            "ts_endian": ts_endian,
            "ts_utc": ts_utc_str,
            "dt_minutes_vs_dat": dt_minutes,
            "rec_off": rec_off,
            "rec_u16_le": rec_u16_le,
        })

    # Write CSV
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(out_rows)

    harvested_ts = sum(1 for r in out_rows if r["ts_u32"] != "")
    harvested_rec = sum(1 for r in out_rows if r["rec_u16_le"] != "")
    print(f"[ok] wrote → {out_path}")
    print(f"[ok] harvested {harvested_ts}/{len(out_rows)} timestamps and {harvested_rec}/{len(out_rows)} record IDs (window={args.window} bytes).")

if __name__ == "__main__":
    main()