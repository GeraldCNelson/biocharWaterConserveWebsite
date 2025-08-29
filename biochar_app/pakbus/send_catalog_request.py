#!/usr/bin/env python3
import os, sys, argparse, socket, time, math, csv, binascii
from pathlib import Path
from datetime import datetime, timezone, timedelta
import struct

# ---------------- tiny I/O helpers ----------------

def read_text(p: Path) -> str:
    with open(p, "r", encoding="utf-8") as f:
        return f.read()

def write_text(p: Path, s: str):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write(s)

def write_bin(p: Path, b: bytes):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        f.write(b)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

# ---------------- framing & CRC ----------------

def deframe_all(buf: bytes):
    """Return inner (deframed) messages between 0xBD ... 0xBD."""
    frames = []
    cur = bytearray()
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

def hex_to_bytes(h: str) -> bytes:
    h = h.strip().replace(" ", "").replace(":", "")
    if not h:
        return b""
    if not h.lower().startswith("bd"):
        h = "bd" + h
    if not h.lower().endswith("bd"):
        h = h + "bd"
    return bytes.fromhex(h)

def bytes_to_hex(b: bytes) -> str:
    return binascii.hexlify(b).decode("ascii")

def crc16_ccitt_false(data: bytes) -> int:
    crc = 0xFFFF
    for bt in data:
        crc ^= (bt << 8)
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if (crc & 0x8000) else (crc << 1) & 0xFFFF
    return crc

def crc16_x25(data: bytes) -> int:
    crc = 0xFFFF
    for bt in data:
        crc ^= bt
        for _ in range(8):
            crc = ((crc >> 1) ^ 0x8408) & 0xFFFF if (crc & 1) else (crc >> 1) & 0xFFFF
    return (~crc) & 0xFFFF

def crc16_ibm(data: bytes) -> int:
    crc = 0xFFFF
    for bt in data:
        crc ^= bt
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def compute_crc16_candidates(inner_wo_crc: bytes):
    cands = []
    for fn in (crc16_ccitt_false, crc16_x25, crc16_ibm):
        v = fn(inner_wo_crc)
        cands.append((v, "be"))
        cands.append((v, "le"))
    return cands

# ---------------- catalog decoding ----------------

def find_anchor_and_fields(inner: bytes):
    """
    Scan for pattern:
        [anchor LE32 secs] 00 FF ?? 00 00 FF B7 00
                         i         i+4...........
    Return (anchor_off, campbell_secs_le, count_be_u16, table_be_u16)
    """
    for i in range(4, len(inner)-8):
        if inner[i] == 0x00 and inner[i+1] == 0xFF and inner[i+3] == 0x00:
            if inner[i+4:i+8] == b"\x00\xff\xb7\x00":
                anchor_off = i-4
                if anchor_off < 0:
                    continue
                secs = int.from_bytes(inner[anchor_off:anchor_off+4], "little", signed=False)
                count_be = int.from_bytes(inner[i+1:i+3], "big", signed=False)
                table_be = int.from_bytes(inner[i+5:i+7], "big", signed=False)
                return (anchor_off, secs, count_be, table_be)
    return None

def extract_anchor_tuple(frame_hex: str):
    b = bytes.fromhex(frame_hex)
    if len(b) not in (27, 31):
        return None
    inner = b[1:-1]
    found = find_anchor_and_fields(inner)
    if not found:
        return None
    anchor_off, secs, count_be, table_be = found
    s16 = count_be if count_be < 0x8000 else count_be - 0x10000
    return (anchor_off, secs, s16, table_be, len(b) == 31)

def build_request_from_template_known_offsets(frame_hex: str,
                                              anchor_off: int,
                                              count_off: int,
                                              new_anchor_sec_le: int,
                                              new_count_be: int):
    """
    Patch a BD…BD request using offsets discovered earlier.
    """
    base = bytes.fromhex(frame_hex)
    if len(base) not in (27, 31):
        return []

    inner = bytearray(base[1:-1])
    # Patch anchor (LE32)
    inner[anchor_off:anchor_off+4] = int(new_anchor_sec_le).to_bytes(4, "little")
    # Patch count (BE16)
    inner[count_off:count_off+2] = int(new_count_be & 0xFFFF).to_bytes(2, "big")

    if len(inner) < 4:
        return []
    body_wo_crc = bytes(inner[:-2])

    frames = []
    for crc, mode in compute_crc16_candidates(body_wo_crc):
        patched = bytearray(body_wo_crc)
        if mode == "be":
            patched += crc.to_bytes(2, "big")
        else:
            patched += crc.to_bytes(2, "little")
        full = b"\xBD" + bytes(patched) + b"\xBD"
        frames.append(full.hex())
    return frames

# ---------------- decode bodies ----------------

FIELDS = ["BattV_Min",
          "VWC_1_Avg","EC_1_Avg","T_1_Avg",
          "VWC_2_Avg","EC_2_Avg","T_2_Avg",
          "VWC_3_Avg","EC_3_Avg","T_3_Avg"]

ROW_STRIDES = [44, 42, 40]
START_OFFSETS = list(range(0, 16))
MAX_ROWS_DEFAULT = 2000

def plausible_row(vals):
    if len(vals) != 10:
        return False
    b, v1, e1, t1, v2, e2, t2, v3, e3, t3 = vals
    def ok(x): return (not math.isnan(x)) and (not math.isinf(x)) and abs(x) < 1e6
    if not all(ok(x) for x in vals):
        return False
    if not (9.0 <= b <= 15.5):
        return False
    if not (0.0 <= v1 <= 1.5 and 0.0 <= v2 <= 1.5 and 0.0 <= v3 <= 1.5):
        return False
    if not (0.0 <= e1 <= 5.0 and 0.0 <= e2 <= 5.0 and 0.0 <= e3 <= 5.0):
        return False
    for t in (t1, t2, t3):
        if not (-30.0 <= t <= 60.0):
            return False
    return True

def decode_rows_from_body(body: bytes, max_rows=MAX_ROWS_DEFAULT, gap_tolerance: int = 3, verbose=False):
    """
    Try body and body[2:], sweep big-endian first, start offsets 0..15, strides 44/42/40.
    Allow up to `gap_tolerance` consecutive implausible rows inside a run.
    Return (rows, info) or (None, None).
    """
    candidates = [body]
    if len(body) >= 2:
        candidates.append(body[2:])  # strip common 2B table-hint prefix

    gap_tolerance = max(0, int(gap_tolerance))

    for cand in candidates:
        for endian in (">", "<"):
            for start in START_OFFSETS:
                for stride in ROW_STRIDES:
                    rows, pos = [], start
                    bad_run = 0
                    while pos + 40 <= len(cand) and len(rows) < max_rows:
                        try:
                            vals = list(struct.unpack(endian + "10f", cand[pos:pos+40]))
                        except Exception:
                            break
                        if plausible_row(vals):
                            rows.append(vals)
                            bad_run = 0
                        else:
                            bad_run += 1
                            if bad_run > gap_tolerance:
                                break
                        pos += stride
                    if rows:
                        return rows, dict(
                            endian=("BE" if endian == ">" else "LE"),
                            start_skip=start,
                            stride=stride,
                            body_len=len(cand),
                            used_prefix=(cand is body)
                        )
    return None, None

# ---------------- socket I/O ----------------

def send_and_read_multi(host, port, frames_hex, timeout=6.0, idle_gap=0.05):
    """
    Send a sequence of BD…BD frames (hex strings) in one TCP session.
    Return raw rx bytes.
    """
    frames = [hex_to_bytes(hx) for hx in frames_hex if hx]
    s = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port, 0, 0) if ":" in host else (host, port))
    try:
        for req in frames:
            s.sendall(req)
            time.sleep(idle_gap)
        s.settimeout(0.6)
        end = time.time() + timeout
        buf = bytearray()
        while time.time() < end:
            try:
                chunk = s.recv(8192)
                if not chunk:
                    break
                buf.extend(chunk)
            except socket.timeout:
                pass
        return bytes(buf)
    finally:
        try:
            s.close()
        except Exception:
            pass

# ---------------- robust catalog reader ----------------

def _detect_delimiter(header_line: str) -> str:
    return "\t" if "\t" in header_line and (header_line.count("\t") >= header_line.count(",")) else ","

def _normalize_keys(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if k is None:
            continue
        kn = k.strip().lstrip("\ufeff").lower()
        out[kn] = v.strip() if isinstance(v, str) else v
    return out

def _read_catalog_rows_any(csv_path: Path):
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        first = f.readline()
        if not first:
            return rows
        delim = _detect_delimiter(first)
        f.seek(0)
        rdr = csv.DictReader(f, delimiter=delim)
        for r in rdr:
            rows.append(_normalize_keys(r))
    return rows

# ---------------- catalog lookup ----------------

def load_catalog_row(catalog_dir: Path, leaf: str, table: int, want_last_n: int, verbose: bool = False):
    """
    Choose a BD…BD request template from the per-leaf/table catalog.
    """
    cat_path = Path(catalog_dir) / f"{leaf}_Table{table}_catalog.csv"
    if not cat_path.exists():
        print(f"[fatal] catalog not found: {cat_path}")
        return None

    rows_raw = _read_catalog_rows_any(cat_path)

    def to_int_or_none(x):
        try:
            if x is None:
                return None
            s = str(x).strip()
            if s == "":
                return None
            v = float(s)
            if math.isnan(v):
                return None
            return int(v)
        except Exception:
            return None

    def looks_hex_str(s_hex: str) -> bool:
        s = (s_hex or "").strip().lower()
        if not s or not s.startswith("bd"):
            return False
        return all(ch in "0123456789abcdef" for ch in s)

    rows = []
    for rr in rows_raw:
        frame_len = to_int_or_none(rr.get("frame_len")) or 0
        frame_hex = rr.get("frame_hex") or ""
        if not looks_hex_str(frame_hex):
            for _k, vstr in rr.items():
                if isinstance(vstr, str) and looks_hex_str(vstr):
                    frame_hex = vstr.strip()
                    break

        anchor_off = to_int_or_none(rr.get("anchor_off"))
        anchor_sec_le = to_int_or_none(rr.get("anchor_sec_le"))
        s20 = to_int_or_none(rr.get("signed16_be@20"))
        s22 = to_int_or_none(rr.get("signed16_be@22"))

        count_off = None
        signed_lastn = None
        if s20 is not None:
            count_off = 20
            signed_lastn = s20
        elif s22 is not None:
            count_off = 22
            signed_lastn = s22

        rows.append({
            "frame_len": frame_len,
            "frame_hex": (frame_hex or "").strip(),
            "anchor_off": anchor_off,
            "anchor_sec_le": anchor_sec_le,
            "signed_lastn": signed_lastn,
            "count_off": count_off,
            "frame_number": rr.get("frame_number"),
            "src_file": rr.get("src_file"),
        })

    if verbose:
        print(f"[catalog] {cat_path.name}: parsed {len(rows)} rows")

    def is_candidate(r1):
        return (
            r1["frame_hex"]
            and r1["frame_len"] in (27, 31)
            and r1["anchor_off"] is not None
            and r1["anchor_sec_le"] is not None
            and r1["count_off"] is not None
            and r1["signed_lastn"] is not None
            and r1["signed_lastn"] < 0
        )

    eligible = [r1 for r1 in rows if is_candidate(r1)]
    if verbose:
        print(f"[catalog] eligible rows: {len(eligible)} "
              f"(31B={sum(1 for rx in eligible if rx['frame_len'] == 31)}, "
              f"27B={sum(1 for rx in eligible if rx['frame_len'] == 27)})")

    if not eligible:
        print("[fatal] no qualifying rows (missing last-n / anchor fields)")
        return None

    pref31 = [r1 for r1 in eligible if r1["frame_len"] == 31]
    pref27 = [r1 for r1 in eligible if r1["frame_len"] == 27]

    def pick_from(cands):
        if not cands:
            return None
        want = abs(int(want_last_n))
        larger = [c for c in cands if abs(c["signed_lastn"]) >= want]
        if larger:
            return sorted(larger, key=lambda c: (abs(c["signed_lastn"]), c["frame_len"]))[0]
        return sorted(cands, key=lambda c: abs(c["signed_lastn"]), reverse=True)[0]

    pick = pick_from(pref31) or pick_from(pref27)
    if not pick:
        print("[fatal] no suitable template in catalog")
        return None

    if verbose:
        print(f"[pick] frame_len={pick['frame_len']} signed_lastn={pick['signed_lastn']} "
              f"count_off={pick['count_off']} anchor_off={pick['anchor_off']} anchor_sec_le={pick['anchor_sec_le']}")

    return pick

def build_variants_from_catalog_row(row: dict, want_last_n: int, anchor_override: int = None):
    hx = row["frame_hex"]
    anchor_off = int(row["anchor_off"])
    count_off = int(row["count_off"])
    anchor_secs = int(row["anchor_sec_le"]) if anchor_override is None else int(anchor_override)
    want_signed = -abs(int(want_last_n))
    return build_request_from_template_known_offsets(
        hx, anchor_off=anchor_off, count_off=count_off,
        new_anchor_sec_le=anchor_secs, new_count_be=want_signed
    )

# ---------------- prelude mining ----------------

def mine_prelude_from_tsv(tsv_path: Path, template_hex: str, k: int = 3):
    """
    Heuristic: scan TSV lines (assumes last column is tcp.payload or similar hex).
    Keep a rolling window of previous BD…BD frames; return K frames before template.
    """
    if not tsv_path.exists():
        return []
    template_hex = template_hex.lower()
    window = []

    def extract_frames_from_payload_hex(hs: str):
        hs = hs.strip().replace(" ", "").replace(":", "")
        if not hs:
            return []
        try:
            b = bytes.fromhex(hs)
        except Exception:
            return []
        inners = deframe_all(b)
        return [("bd" + i.hex() + "bd") for i in inners]

    try:
        with open(tsv_path, "r", encoding="utf-8") as f:
            for raw in f:
                cols = raw.rstrip("\n").split("\t")
                if not cols:
                    continue
                payload_hex = cols[-1].strip()
                frames = extract_frames_from_payload_hex(payload_hex)
                for fx in frames:
                    window.append(fx.lower())
                    if len(window) > 32:
                        window.pop(0)
                    if fx.lower() == template_hex:
                        return window[:-1][-k:]
    except Exception:
        return []
    return []

# ---------------- Campbell epoch helpers ----------------

_CAMPBELL_EPOCH = datetime(1990, 1, 1, tzinfo=timezone.utc)

def dt_to_campbell_secs(dt_utc: datetime) -> int:
    return int((dt_utc - _CAMPBELL_EPOCH).total_seconds())

def parse_dat_timestamp(ts_str: str):
    s = ts_str.strip().strip('"')
    fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z"]
    for fmt in fmts:
        try:
            if fmt.endswith("%z"):
                return datetime.strptime(s, fmt)
            else:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

# ---------------- .dat matching ----------------

def load_recent_dat_rows_tail(dat_path: Path, tail_k=10):
    rows = []
    try:
        with open(dat_path, "r", newline="") as f:
            rdr = csv.reader(f)
            lines = list(rdr)
    except Exception:
        return rows

    hdr_idx = None
    for i, line in enumerate(lines[:10]):
        if not line: continue
        if line[0].strip('"').upper() == "TIMESTAMP" or "TIMESTAMP" in [c.strip('"').upper() for c in line]:
            hdr_idx = i
            break
    if hdr_idx is None:
        return rows

    header = [c.strip('"') for c in lines[hdr_idx]]
    data_lines = lines[hdr_idx+4:] if len(lines) > hdr_idx+4 else lines[hdr_idx+1:]

    def idx(colname):
        try: return header.index(colname)
        except ValueError: return -1

    idx_ts = idx("TIMESTAMP")
    indices = [idx(f) for f in FIELDS]
    if idx_ts < 0 or any(i < 0 for i in indices):
        return rows

    def to_float(s):
        s = s.strip().strip('"')
        if s == "" or s.upper() == "NAN":
            return float("nan")
        try: return float(s)
        except Exception: return float("nan")

    data_tail = data_lines[-tail_k:] if tail_k and tail_k < len(data_lines) else data_lines

    for line in data_tail:
        if not line or len(line) <= max(indices + [idx_ts]):
            continue

        ts = line[idx_ts].strip().strip('"')
        vals = [to_float(line[i]) for i in indices]

        dt = parse_dat_timestamp(ts)
        if dt is not None and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        ts_secs = dt_to_campbell_secs(dt) if dt is not None else None

        rows.append({"ts_utc": ts, "ts_secs": ts_secs, "values": vals})
    return rows

def best_match_index(decoded_vals, dat_rows, target_anchor_secs: int | None, time_window_back: int,
                     forbidden_indices: set[int] | None = None):
    """
    Choose the .dat row that:
      - is not in the future w.r.t. target anchor, and
      - is within `time_window_back` seconds behind the anchor,
      - and is not in `forbidden_indices`,
    minimizing value L1 distance.

    If target_anchor_secs is None, fall back to global best by values (still avoiding forbidden).
    """
    if forbidden_indices is None:
        forbidden_indices = set()

    best_i, best_d = None, float("inf")
    for i, r in enumerate(dat_rows):
        if i in forbidden_indices:
            continue

        ts_secs = r.get("ts_secs")
        if target_anchor_secs is not None and ts_secs is not None:
            if ts_secs > target_anchor_secs:
                continue
            if target_anchor_secs - ts_secs > time_window_back:
                continue

        dv = r["values"]
        d = 0.0
        n = 0
        for a, b in zip(decoded_vals, dv):
            if math.isnan(b):
                continue
            d += abs(a - b)
            n += 1
        if n == 0:
            continue
        if d < best_d:
            best_d = d
            best_i = i
    return best_i


def match_rows_to_dat(rows, dat_rows, local_offset_hours=-6, anchor_secs=None, time_window_back=3600):
    """
    If anchor_secs is provided, prefer .dat rows with ts_secs in [anchor_secs - time_window_back, anchor_secs].
    Also avoid reusing the same .dat row index within a single call.
    """
    out = []
    matched = 0
    used_indices = set()

    # Build candidate indices once (windowed if anchor provided)
    if anchor_secs is not None:
        lo = int(anchor_secs) - int(time_window_back)
        hi = int(anchor_secs)
        windowed = [idx for idx, r in enumerate(dat_rows)
                    if r.get("ts_secs") is not None and lo <= int(r["ts_secs"]) <= hi]
        candidate_indices = windowed if windowed else list(range(len(dat_rows)))
    else:
        candidate_indices = list(range(len(dat_rows)))

    for vals in rows:
        # choose the best unused index among candidates
        best_i, best_d = None, float("inf")
        for i in candidate_indices:
            if i in used_indices:
                continue
            dv = dat_rows[i]["values"]
            d = 0.0
            n = 0
            for a, b in zip(vals, dv):
                if math.isnan(b):
                    continue
                d += abs(a - b)
                n += 1
            if n == 0:
                continue
            if d < best_d:
                best_d = d
                best_i = i

        if best_i is None:
            out.append(("", "", False))
            continue

        used_indices.add(best_i)
        ts_utc = dat_rows[best_i]["ts_utc"]
        dt = parse_dat_timestamp(ts_utc)
        if dt is None:
            out.append((ts_utc, "", True))
            matched += 1
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        loc = dt.astimezone(timezone(timedelta(hours=local_offset_hours))).strftime("%Y-%m-%d %H:%M:%S%z")
        out.append((dt.strftime("%Y-%m-%d %H:%M:%S"), loc, True))
        matched += 1

    return out, matched

# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog-dir", required=True, help="Directory with *_catalog.csv files")
    ap.add_argument("--leaf", required=True, help="Leaf (e.g., S2T)")
    ap.add_argument("--table", type=int, required=True, help="Table number (e.g., 1)")
    ap.add_argument("--last-n", type=int, default=300, help="Total rows to request (encoded as -N)")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=6785)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--prelude", help="Directory of prelude BD…BD *.hex frames to send first (lexicographic order)")
    ap.add_argument("--mine-prelude", help="TSV file to mine a prelude from (auto-picks K frames before the template)")
    ap.add_argument("--prelude-count", type=int, default=3, help="How many prelude frames to mine (default 3)")
    ap.add_argument("--dat-file", help="TOA5 .dat file for timestamp matching")
    ap.add_argument("--compare-last", type=int, default=200, help="How many .dat rows to consider for matching")
    ap.add_argument("--local-offset-hours", type=int, default=-6, help="Local tz offset hours for TimestampLocal")
    ap.add_argument("--gap-tolerance", type=int, default=3,
                    help="Allow up to this many consecutive implausible rows before stopping (default 3)")
    # network / paging tunables
    ap.add_argument("--timeout", type=float, default=8.0, help="Per-page socket timeout seconds (default 8.0)")
    ap.add_argument("--idle-gap", type=float, default=0.08, help="Sleep between frames (default 0.08s)")
    ap.add_argument("--max-pages", type=int, default=50, help="Max pages to fetch (default 50)")
    ap.add_argument("--fallback-backstep-sec", type=int, default=3600,
                    help="If no timestamps matched, step anchor back by this many seconds (default 3600)")
    ap.add_argument("--page-back-seconds", type=int, default=900,
                    help="When paginating, move anchor back this many seconds between pages (default 900s ~ 15 min)")
    ap.add_argument("--anchor-epsilon", type=int, default=2,
                    help="Subtract this small number of seconds from timestamp-derived anchors to avoid equality (default 2)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    catalog_dir = Path(args.catalog_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pick = load_catalog_row(catalog_dir, args.leaf, args.table, args.last_n, verbose=args.verbose)
    if not pick:
        return

    hx = pick["frame_hex"]
    s16 = pick["signed_lastn"]
    is31 = (pick["frame_len"] == 31)
    tag = "31B" if is31 else "27B"
    print(f"[info] using nearest template with |n|={abs(s16)}, {tag} "
          f"from src_file={pick.get('src_file','?')} frame_number={pick.get('frame_number','?')}")

    # Optional prelude (manual dir)
    prelude_frames = []
    if args.prelude:
        prel_dir = Path(args.prelude)
        if prel_dir.exists() and prel_dir.is_dir():
            for pth in sorted(prel_dir.glob("*.hex")):
                try:
                    s_hex = read_text(pth).strip()
                    if s_hex:
                        prelude_frames.append(s_hex)
                except Exception:
                    pass
            if prelude_frames:
                print(f"[info] loaded {len(prelude_frames)} prelude frames from {prel_dir}")
        else:
            print(f"[warn] prelude dir not found: {prel_dir}")

    # Or auto-mine prelude from TSV
    if not prelude_frames and getattr(args, "mine_predule", None):
        args.mine_prelude = args.mine_predule
    if not prelude_frames and args.mine_prelude:
        mined = mine_prelude_from_tsv(Path(args.mine_prelude), hx.lower(), k=max(1, args.prelude_count))
        if mined:
            prelude_frames = mined
            print(f"[info] mined {len(prelude_frames)} prelude frames from {args.mine_prelude}")
        else:
            print(f"[warn] could not mine a prelude from {args.mine_prelude}")

    # Optional .dat-based timestamp matching and initial anchor override
    dat_rows = []
    anchor_override = None
    if args.dat_file:
        dat_path = Path(args.dat_file)
        if dat_path.exists():
            tail = load_recent_dat_rows_tail(dat_path, tail_k=max(10, args.compare_last))
            dat_rows = tail
            if tail:
                newest = tail[-1]["ts_utc"]
                dt = parse_dat_timestamp(newest)
                if dt is not None:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    secs = dt_to_campbell_secs(dt)
                    anchor_override = secs + 10  # slight cushion forward
                    print(f"[dat] using last TIMESTAMP as anchor: {dt.strftime('%Y-%m-%d %H:%M:%S')} -> secs={secs}")
                    print(f"[dat] using: {dat_path}")
                    print(f"[dat] newest TIMESTAMP={dt.isoformat()}  -> override_anchor_sec_le={anchor_override}")
        else:
            print(f"[dat] not found: {dat_path}")

    # -------------- Automatic pagination --------------
    total_needed = abs(int(args.last_n))
    total_collected = []
    seen_keys = set()  # dedup by rounded tuple or timestamp

    remaining = total_needed
    page = 0
    last_anchor_used = None  # what we actually sent the page with

    while remaining > 0 and page < args.max_pages:
        page += 1

        # Cap what we ask per page (keeps responses small & consistent)
        remaining_page = min(120, remaining)

        # Build request for this page (use current anchor_override if set)
        variants = build_variants_from_catalog_row(
            pick,
            remaining_page,
            anchor_override=anchor_override
        )
        if not variants:
            print("[fatal] could not build CRC variants from template")
            break

        cur_anchor = int(pick["anchor_sec_le"]) if anchor_override is None else int(anchor_override)
        if args.verbose:
            print(f"[page {page}] using anchor_sec_le={cur_anchor}")

        # Always send prelude (some loggers expect the link/hello before each request)
        cur_prelude = prelude_frames

        # Try CRC variants until one decodes rows
        page_rows = []
        chosen_info = None
        variant_used = None

        for v in variants:
            vlen = len(bytes.fromhex(v))
            cur_tag = "31B" if vlen == 31 else "27B"
            if args.verbose:
                print(f"[info] sending 0xBD…0xBD (page {page}, {vlen} bytes) to {args.host}:{args.port}")

            tx_seq = (cur_prelude + [v]) if cur_prelude else [v]
            rx = send_and_read_multi(args.host, args.port, tx_seq,
                                     timeout=args.timeout, idle_gap=args.idle_gap)
            frames = deframe_all(rx)

            # Persist raw, hex, frames for this page/variant
            suffix = f"{args.leaf}_Table{args.table}_n{total_needed}_{cur_tag}_p{page}"
            write_bin(out_dir / f"{suffix}_raw.bin", rx)
            write_text(out_dir / f"{suffix}_raw.hex", bytes_to_hex(rx))
            meta = [
                f"time_utc={now_iso()}",
                f"host={args.host}",
                f"port={args.port}",
                f"template_len={len(bytes.fromhex(hx))}",
                f"variant_len={vlen}",
                f"frames_rx={len(frames)}",
                f"prelude_count={len(cur_prelude)}",
                f"anchor_used={cur_anchor}",
            ]
            write_text(out_dir / f"{suffix}_meta.txt", "\n".join(meta))

            frames_dir = out_dir / f"{suffix}_frames"
            frames_dir.mkdir(parents=True, exist_ok=True)
            for i, inner in enumerate(frames):
                write_text(frames_dir / f"frame_{i:03d}.hex", inner.hex())

            # Deep-scan 0x89 messages in every inner frame
            for inner in frames:
                data = bytes(inner)
                pscan = 0
                while pscan < len(data):
                    try:
                        idx = data.index(0x89, pscan)
                    except ValueError:
                        break
                    if idx + 3 <= len(data):
                        rc = data[idx+2]
                        body = data[idx+3:]
                        if rc == 0 and body:
                            rows, info = decode_rows_from_body(
                                body,
                                max_rows=MAX_ROWS_DEFAULT,
                                gap_tolerance=args.gap_tolerance,
                                verbose=args.verbose
                            )
                            if rows:
                                if chosen_info is None:
                                    chosen_info = info
                                page_rows.extend(rows)
                    pscan = idx + 1

            if page_rows:
                variant_used = cur_tag
                break  # accept the first successful variant

        # Dump a quick scan of decoded rows (with plausible flag)
        if page_rows:
            scan_path = out_dir / f"{args.leaf}_Table{args.table}_n{total_needed}_{variant_used}_p{page}_decode_scan.txt"
            with open(scan_path, "w", newline="") as f:
                w = csv.writer(f, delimiter="\t")
                w.writerow(["Row", "Plausible"] + FIELDS)
                for i, rvals in enumerate(page_rows):
                    w.writerow([i, "yes" if plausible_row(rvals) else "no"] + rvals)

        if not page_rows:
            if args.verbose:
                print(f"[page {page}] no rows decoded; stopping pagination")
            break

        # Timestamp matching (optional)
        timestamps = []
        matched_count = 0
        if dat_rows:
            # --- PATCH C: pass per-page anchor and time window to the matcher ---
            timestamps, matched_count = match_rows_to_dat(
                page_rows,
                dat_rows,
                local_offset_hours=args.local_offset_hours,
                anchor_secs=cur_anchor,
                time_window_back=3600  # try 7200 if needed
            )
            if args.verbose:
                print(f"[dat] timestamps matched for {matched_count}")

        # Dedup & accumulate (prefer timestamp key when available)
        new_this_page = 0
        for vals, ts in zip(page_rows, timestamps if timestamps else [("", "", False)]*len(page_rows)):
            # --- PATCH D: values-first key; include timestamp only as secondary ---
            val_key = tuple(round(x, 6) for x in vals)
            tsu = ts[0] if ts and len(ts) >= 1 else None
            key = (val_key, tsu)  # different values won't collide even with same timestamp
            # --- END PATCH D ---

            if key in seen_keys:
                continue
            seen_keys.add(key)
            total_collected.append((vals, ts))
            new_this_page += 1

        if args.verbose:
            ei = chosen_info or {}
            print(f"[ok] page {page}: decoded {len(page_rows)} rows "
                  f"(unique added {new_this_page}) "
                  f"(endian={ei.get('endian')} start={ei.get('start_skip')} stride={ei.get('stride')})")

        # Remaining target for the next page
        remaining = total_needed - len(total_collected)
        if remaining <= 0:
            break

        # ---- Compute the next anchor (INSIDE THE LOOP) ----
        # Strategy:
        #  - If we got timestamp matches, use the NEWEST matched UTC and step back a tiny epsilon.
        #  - Clamp the step so we never move back by more than --page-back-seconds per page.
        #  - If no timestamps, step back by --page-back-seconds from the CURRENT anchor.
        next_anchor = None

        if timestamps:
            newest_tsu = next((tsu for tsu, _tsl, matched in timestamps if tsu), None)
            if newest_tsu:
                dt_new = parse_dat_timestamp(newest_tsu)
                if dt_new is not None:
                    if dt_new.tzinfo is None:
                        dt_new = dt_new.replace(tzinfo=timezone.utc)
                    candidate = dt_to_campbell_secs(dt_new) - int(args.anchor_epsilon)
                    # clamp: do not jump back more than page_back_seconds from the anchor we just used
                    max_back = int(args.page_back_seconds)
                    if candidate > cur_anchor:
                        candidate = cur_anchor - 1  # never move forward
                    if (cur_anchor - candidate) > max_back:
                        candidate = cur_anchor - max_back
                    next_anchor = candidate

        if next_anchor is None:
            # fallback: uniform step back
            next_anchor = cur_anchor - int(args.page_back_seconds)

        # enforce strictly older than last used; remember it for the next loop
        if last_anchor_used is not None and next_anchor >= last_anchor_used:
            next_anchor = last_anchor_used - max(1, int(args.anchor_epsilon))

        if args.verbose:
            # rough human readout (Campbell secs -> UTC)
            dt_dbg = _CAMPBELL_EPOCH + timedelta(seconds=next_anchor)
            print(f"[page {page}] next_anchor={next_anchor} (~{dt_dbg.isoformat()})")

        anchor_override = next_anchor
        last_anchor_used = cur_anchor

    # ----------- write combined outputs -----------
    if total_collected:
        suffix_all = f"{args.leaf}_Table{args.table}_n{total_needed}_{tag}"
        out_csv = out_dir / f"{suffix_all}.csv"
        out_csv_collapsed = out_dir / f"{suffix_all}_collapsed.csv"
        out_csv_dupes = out_dir / f"{suffix_all}_dupes.csv"

        # 1) Write the raw (pre-collapsed) rows exactly as decoded
        with open(out_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Row", "TimestampUTC", "TimestampLocal"] + FIELDS)
            for i, (vals, ts) in enumerate(total_collected):
                # ts is a tuple like (tsu, tsl, meta)
                tsu, tsl, meta = ts
                w.writerow([i, tsu, tsl] + vals)
        print(f"[ok] decoded {len(total_collected)} total rows -> {out_csv}")

        # 2) Collapse to one row per TimestampUTC, preferring the highest match score
        #    If meta is missing or lacks 'score', default to 0.
        grouped = {}
        all_dupes = []  # for inspection: everything that lost a tie
        for i, (vals, ts) in enumerate(total_collected):
            tsu, tsl, meta = ts
            score = 0
            try:
                if isinstance(meta, dict):
                    score = meta.get("score", 0)
                elif hasattr(meta, "get"):
                    score = meta.get("score", 0)
            except Exception:
                score = 0

            current = grouped.get(tsu)
            candidate = {"i": i, "tsu": tsu, "tsl": tsl, "vals": vals, "score": float(score)}
            if current is None:
                grouped[tsu] = candidate
            else:
                # Prefer higher score; on tie, prefer the earlier row (smaller i)
                if (candidate["score"] > current["score"]) or (
                        candidate["score"] == current["score"] and candidate["i"] < current["i"]
                ):
                    all_dupes.append(current)  # the one we’re replacing becomes a dupe
                    grouped[tsu] = candidate
                else:
                    all_dupes.append(candidate)  # this candidate loses

        collapsed_rows = sorted(grouped.values(), key=lambda r: (r["tsu"], r["i"]))
        dropped_count = len(total_collected) - len(collapsed_rows)

        # 3) Write collapsed output (one row per TimestampUTC)
        with open(out_csv_collapsed, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Row", "TimestampUTC", "TimestampLocal", "MatchScore"] + FIELDS)
            for new_i, r in enumerate(collapsed_rows):
                w.writerow([new_i, r["tsu"], r["tsl"], r["score"]] + r["vals"])
        print(
            f"[dedup] collapsed to one row per TimestampUTC: kept {len(collapsed_rows)}, dropped {dropped_count} -> {out_csv_collapsed}")

        # 4) Optional: write the discarded duplicates for auditing
        if all_dupes:
            with open(out_csv_dupes, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["OrigRowIndex", "TimestampUTC", "TimestampLocal", "MatchScore"] + FIELDS)
                # sort for readability: by TimestampUTC then original index
                for r in sorted(all_dupes, key=lambda x: (x["tsu"], x["i"])):
                    w.writerow([r["i"], r["tsu"], r["tsl"], r["score"]] + r["vals"])
            print(f"[dedup] wrote {len(all_dupes)} discarded duplicate candidates -> {out_csv_dupes}")

        # 5) Head-5 from the collapsed set for quick glance
        out_csv5 = out_dir / f"{suffix_all}_head5.csv"
        with open(out_csv5, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Row", "TimestampUTC", "TimestampLocal", "MatchScore"] + FIELDS)
            for i, r in enumerate(collapsed_rows[:5]):
                w.writerow([i, r["tsu"], r["tsl"], r["score"]] + r["vals"])
        print(f"[ok] wrote first 5 rows (collapsed) -> {out_csv5}")
    else:
        print("[summary] nothing decoded in any page")

if __name__ == "__main__":
    main()