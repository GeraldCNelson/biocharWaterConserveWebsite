#!/usr/bin/env python3
import os, sys, re, socket, time, math, binascii, argparse, json, shutil, subprocess, csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
import struct
import io

# ------------ CONFIG ------------
PCAP_DIR = Path("biochar_app/pakbus/bdFiles")   # folder with *.pcapng
TCP_PORT = 6785                                 # PakBus-over-TCP port
MAX_ROWS_DEFAULT = 2000                         # decode this many rows per station (cap)
FIELDS = ["BattV_Min",
          "VWC_1_Avg","EC_1_Avg","T_1_Avg",
          "VWC_2_Avg","EC_2_Avg","T_2_Avg",
          "VWC_3_Avg","EC_3_Avg","T_3_Avg"]
ROW_STRIDES = [40, 42, 44]                      # 40 bytes payload + optional per-row padding
START_OFFSETS = list(range(0, 16))              # brute-force small start misalignments
# ---------------------------------

# ---- host:port from your project config ----
def load_host_port():
    try:
        from biochar_app.scripts.config import PAKBUS
        return PAKBUS.host, PAKBUS.port
    except Exception as e:
        print("FATAL: could not import PAKBUS host/port:", e)
        print("Tip: run with PYTHONPATH=. from repo root, e.g.")
        print("  PYTHONPATH=. python biochar_app/pakbus/replay_reqs.py --out-dir biochar_app/pakbus/bdFiles/out_csv")
        sys.exit(1)

HOST, PORT = load_host_port()

def have_tshark():
    exe = shutil.which("tshark")
    if exe:
        return exe
    cand = "/opt/homebrew/bin/tshark"  # common Homebrew path
    return cand if os.path.exists(cand) else None

TSHARK = have_tshark()

# ----------------- low-level helpers -----------------
# --- helpers to find anchor fields inside a 27/31-byte request ---
def extract_anchor_tuple(frame_hex: str):
    """
    Given a full BD…BD (27/31-byte) request hex (lower/upper ok),
    return (anchor_off, anchor_sec_le, count_be_signed, table_be, is_31b) or None.
    Operates on the *inner* bytes (without leading/trailing 0xBD).
    """
    b = bytes.fromhex(frame_hex)
    if len(b) not in (27,31):
        return None
    inner = b[1:-1]
    found = find_anchor_and_fields(inner)
    if not found:
        return None
    anchor_off, anchor_sec, count_be, table_be = found
    # convert 0..65535 into signed 16
    s16 = count_be if count_be < 0x8000 else count_be - 0x10000
    return (anchor_off, anchor_sec, s16, table_be, len(b)==31)

def find_anchor_and_fields(inner: bytes):
    """(shared with cataloger’s logic)"""
    for i in range(4, len(inner)-8):
        # pattern: 00 FF ?? 00 then 00 FF B7 00 around there
        if inner[i] == 0x00 and inner[i+1] == 0xFF and inner[i+3] == 0x00:
            if inner[i+4:i+8] == b"\x00\xff\xb7\x00":
                anchor_off = i-4
                if anchor_off < 0:
                    continue
                sec = int.from_bytes(inner[anchor_off:anchor_off+4], "little", signed=False)
                count_be = int.from_bytes(inner[i+1:i+3], "big", signed=False)  # we'll sign-fix later
                table_be = int.from_bytes(inner[i+5:i+7], "big", signed=False)
                return (anchor_off, sec, count_be, table_be)
    return None

# --- CRC builders (we’ll try several common 16-bit variants) ---
def crc16_ccitt_false(data: bytes) -> int:
    crc = 0xFFFF
    for bt in data:
        crc ^= (bt << 8)
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if (crc & 0x8000) else (crc << 1) & 0xFFFF
    return crc

def crc16_x25(data: bytes) -> int:  # (aka "Kermit" with final xor)
    crc = 0xFFFF
    for bt in data:
        crc ^= bt
        for _ in range(8):
            crc = ((crc >> 1) ^ 0x8408) & 0xFFFF if (crc & 1) else (crc >> 1) & 0xFFFF
    return (~crc) & 0xFFFF

def crc16_ibm(data: bytes) -> int:  # CRC-16/IBM (Modbus poly)
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
    """
    Return list of (crc_value, endian_mode) candidates.
    We’ll insert as BE or LE depending on mode when building the final frame.
    """
    candidates = []
    for fn in (crc16_ccitt_false, crc16_x25, crc16_ibm):
        crc = fn(inner_wo_crc)
        candidates.append((crc, "be"))
        candidates.append((crc, "le"))
    return candidates

def build_request_from_template(frame_hex: str, new_anchor_sec_le: int, new_count_be_signed: int):
    """
    Given a captured 27/31 byte BD…BD request, patch:
      - anchor LE32 seconds
      - count BE16 (signed!) the 0xff?? walking field in your traces. Pass a *signed* value.
    Recompute CRC with several variants; return a list of full BD…BD frames (hex).
    """
    base = bytes.fromhex(frame_hex)
    if len(base) not in (27,31):
        return []

    inner = bytearray(base[1:-1])
    found = find_anchor_and_fields(inner)
    if not found:
        return []
    anchor_off, prev_sec, prev_count_be, table_be = found

    # Patch fields
    inner[anchor_off:anchor_off+4] = int(new_anchor_sec_le).to_bytes(4, "little", signed=False)

    # Count goes at bytes (i+1,i+2) where i = anchor_off+4 (the 1st 00 after anchor)
    # We accept signed, but store as BE16 two's complement.
    neg16 = (int(new_count_be_signed) & 0xFFFF)
    i = anchor_off + 4
    inner[i+1:i+3] = neg16.to_bytes(2, "big", signed=False)

    # CRC sits in the last 2 bytes of inner in these short requests; drop them and rebuild
    if len(inner) < 4:
        return []
    # Use body_without_crc = everything except the final two bytes
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

def hex_to_bytes(h: str) -> bytes:
    h = h.strip().replace(" ", "")
    if not h:
        return b""
    if not h.lower().startswith("bd"):
        h = "bd" + h
    if not h.lower().endswith("bd"):
        h = h + "bd"
    return bytes.fromhex(h)

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

def plausible_row(vals):
    """Light plausibility filter for Table1 rows."""
    if len(vals) != 10:
        return False
    batt, v1,e1,t1, v2,e2,t2, v3,e3,t3 = vals
    def ok(x): return (not math.isnan(x)) and (not math.isinf(x)) and abs(x) < 1e6
    if not all(ok(x) for x in vals): return False
    if not (9.0 <= batt <= 15.5): return False
    if not (0.0 <= v1 <= 1.5 and 0.0 <= v2 <= 1.5 and 0.0 <= v3 <= 1.5): return False
    if not (0.0 <= e1 <= 5.0 and 0.0 <= e2 <= 5.0 and 0.0 <= e3 <= 5.0): return False
    for t in (t1,t2,t3):
        if not (-30.0 <= t <= 60.0):
            return False
    return True

def summarize_row(vals):
    b, v1,e1,t1, v2,e2,t2, v3,e3,t3 = vals
    return (f"BattV={b:.3f}  "
            f"VWC1={v1:.3f} EC1={e1:.3f} T1={t1:.2f}   "
            f"VWC2={v2:.3f} EC2={e2:.3f} T2={t2:.2f}   "
            f"VWC3={v3:.3f} EC3={e3:.3f} T3={t3:.2f}")

def send_and_read(req_bytes: bytes, timeout=6.0):
    """Send one BD…BD request, return bytes of whole TCP read window."""
    s = socket.socket(socket.AF_INET6 if ":" in HOST else socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(6)
    s.connect((HOST, PORT, 0, 0) if ":" in HOST else (HOST, PORT))
    try:
        s.sendall(req_bytes)
        s.settimeout(0.5)
        end = time.time() + timeout
        buf = bytearray()
        while time.time() < end:
            try:
                chunk = s.recv(4096)
                if not chunk: break
                buf.extend(chunk)
            except socket.timeout:
                pass
        return bytes(buf)
    finally:
        try: s.close()
        except: pass

def guess_timestamp_utc(body: bytes):
    """
    Heuristic: look for (rec?, secs, nsecs) since 1990-01-01 (Campbell epoch).
    Returns ISO 8601 string or None.
    """
    import struct
    epoch = datetime(1990,1,1,tzinfo=timezone.utc)
    for off in range(0, 16):
        for pat in (">HiI", ">iI"):
            try:
                tup = struct.unpack_from(pat, body, off)
            except Exception:
                continue
            if pat == ">HiI":
                _rec, secs, nsec = tup
            else:
                secs, nsec = tup
            if 0 <= secs < 3600*24*365*120:
                try:
                    ts = epoch + timedelta(seconds=int(secs), microseconds=int(nsec/1000))
                    return ts.isoformat()
                except Exception:
                    pass
    return None

def decode_rows_from_body(body: bytes, max_rows=MAX_ROWS_DEFAULT, require_two=False):
    """
    Try bodies: body and body[2:] (table-hint prefix).
    For each candidate, brute-force endian, start, stride (40/42/44) and harvest rows.
    Returns (rows, meta) or (None, None)
    """
    import struct
    candidates = [body]
    if len(body) >= 2:
        candidates.append(body[2:])

    for cand in candidates:
        for endian in (">", "<"):
            for start in START_OFFSETS:
                for stride in ROW_STRIDES:
                    rows = []
                    pos = start
                    while pos + 40 <= len(cand) and len(rows) < max_rows:
                        try:
                            vals = list(struct.unpack(endian + "10f", cand[pos:pos+40]))
                        except Exception:
                            break
                        if not plausible_row(vals):
                            break
                        rows.append(vals)
                        pos += stride
                    need = 2 if require_two else 1
                    if len(rows) >= need:
                        return rows, {
                            "endian": "BE" if endian==">" else "LE",
                            "start_skip": start,
                            "stride": stride,
                            "body_len": len(cand),
                            "prefixed": (cand is not body[2:]) is False
                        }
    return None, None

def collect_all_decoded_from_frames(frames, max_rows, verbose=False):
    """
    Iterate all deframed packets, and for every rc==0 CollectData body,
    attempt decoding and accumulate rows until max_rows is reached.
    """
    all_rows, metas, bodies = [], [], []
    for inner in frames:
        payload = inner[8:] if len(inner) > 8 else b""
        if not payload or payload[0] != 0x89:
            continue
        rc = payload[2] if len(payload) >= 3 else None
        bdy = payload[3:] if len(payload) > 3 else b""
        if rc != 0 or not bdy:
            continue
        rows, meta = decode_rows_from_body(bdy, max_rows=max_rows - len(all_rows), require_two=False)
        if rows:
            if verbose:
                print(f"    decoded: endian={meta['endian']} start_skip={meta['start_skip']} stride={meta['stride']} body_len={meta['body_len']}")
                print_rows(rows)
            all_rows.extend(rows)
            metas.append(meta)
            bodies.append(bdy)
        if len(all_rows) >= max_rows:
            break
    return all_rows, metas, bodies

def decode_for_request(req_hex, verbose=False, max_rows=MAX_ROWS_DEFAULT):
    if not req_hex:
        return None, None, None
    req = hex_to_bytes(req_hex)
    if verbose:
        print(f"\n  sending {len(req)}-byte request")
    rx = send_and_read(req)
    frames = deframe_all(rx)
    if verbose:
        print(f"    rx={len(rx)} bytes, frames={len(frames)}")

    rows, metas, bodies = collect_all_decoded_from_frames(frames, max_rows=max_rows, verbose=verbose)
    if not rows:
        if verbose:
            print("    no rc=0 CollectData body found or could not decode a plausible 10-float row")
        return None, None, None
    return rows, metas, bodies

# --- CSV writer (now with full provenance) ---
def write_csv(path: Path, rows, timestamps, source_tag="", matched_from="dat", dat_file=""):
    """
    Write decoded rows to CSV with timestamp matches and provenance.
    - source_tag: which request produced these rows ("31" or "27")
    - matched_from: provenance of the timestamp match ("dat" | "tsv" | "live")
    - dat_file: path to the reference file used for matching (or "")
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["Row", "TimestampUTC", "TimestampMT", "SourceReq", "Matched", "MatchedFrom", "MatchedFile"] + FIELDS
        )
        for i, vals in enumerate(rows):
            tsu, tsm, m = timestamps[i] if i < len(timestamps) else ("", "", False)
            w.writerow([i, tsu, tsm, source_tag, "yes" if m else "no", matched_from, dat_file if m else ""] + vals)

    # quick count
    try:
        with open(path, "r", newline="") as f:
            count = sum(1 for _ in f) - 1
    except Exception:
        count = len(rows)
    print(f"    [csv] wrote {path} ({max(0, count)} rows)")

# ----------------- .dat matching -----------------

def dat_filename_for(station_name: str, dat_dir: Path):
    """
    Canonical naming: e.g. S1M_Table1.dat for 'Table1S1M'
    """
    stn, tbl = parse_station_table_from_name(station_name)
    if stn and tbl:
        return dat_dir / f"{stn}_Table{tbl}.dat"
    return dat_dir / f"{station_name}.dat"

def load_recent_dat_rows_tail(dat_path: Path, tail_k=10, verbose=False):
    """
    Read TOA5 .dat file, but only return the LAST K rows (newest last).
    """
    rows = []
    try:
        with open(dat_path, "r", newline="") as f:
            rdr = csv.reader(f)
            lines = list(rdr)
    except Exception as e:
        if verbose:
            print(f"    [dat] failed to read {dat_path}: {e}")
        return rows

    # Find header row containing "TIMESTAMP"
    hdr_idx = None
    for i, line in enumerate(lines[:10]):
        if not line: continue
        if line[0].strip('"').upper() == "TIMESTAMP" or "TIMESTAMP" in [c.strip('"').upper() for c in line]:
            hdr_idx = i
            break
    if hdr_idx is None:
        if verbose:
            print(f"    [dat] TIMESTAMP header not found in {dat_path.name}")
        return rows

    header = [c.strip('"') for c in lines[hdr_idx]]
    data_lines = lines[hdr_idx+4:] if len(lines) > hdr_idx+4 else lines[hdr_idx+1:]

    def idx(colname):
        try: return header.index(colname)
        except ValueError: return -1

    idx_ts = idx("TIMESTAMP")
    indices = [idx(f) for f in FIELDS]
    if idx_ts < 0 or any(i < 0 for i in indices):
        if verbose:
            print(f"    [dat] missing expected columns in {dat_path.name}")
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
        rows.append({"ts_utc": ts, "values": vals})
    if verbose:
        print(f"    [dat] loaded tail {len(rows)} rows from {dat_path.name}")
    return rows

def best_match_index(decoded_vals, dat_rows):
    best_i, best_d = None, float("inf")
    for i, r in enumerate(dat_rows):
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

def match_rows_to_dat(rows, dat_rows, verbose=False):
    out = []
    matched = 0
    for vals in rows:
        i = best_match_index(vals, dat_rows)
        if i is None:
            out.append(("", "", False))
            continue
        ts_utc = dat_rows[i]["ts_utc"]
        try:
            dt = datetime.strptime(ts_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            ts_mt = dt.astimezone(timezone(timedelta(hours=-6))).strftime("%Y-%m-%d %H:%M:%S%z")
        except Exception:
            ts_mt = ""
        out.append((ts_utc, ts_mt, True))
        matched += 1
    if verbose:
        print(f"    [dat] matched {matched}/{len(rows)} decoded rows to .dat")
    return out

# ----------------- station naming (strict) -----------------

_STATION_PATTERN = re.compile(r"^S([1-9])([A-Z])(?:[A-Z0-9_]*)?$")

def parse_station_table_from_name(station_name: str):
    """
    Strict parse:
      - Must be 'Table<tbl><station>' where <tbl> is 1 or 2
      - <station> must start with 'S' then digit then capital letter (e.g., S4M, S1B, S3T)
    Returns (station_code, table_number) on success, else (None, None).
    """
    m = re.match(r"^Table([12])([A-Za-z0-9_]+)$", station_name)
    if not m:
        return (None, None)
    tbl = int(m.group(1))
    station = m.group(2)

    if not _STATION_PATTERN.match(station):
        return (None, None)

    if station[0] != 'S' or not station[1].isdigit() or not station[2].isupper():
        return (None, None)

    return (station, tbl)

def station_name_from_filename(p: Path):
    """
    Accept both 'Table1S3M.pcapng' (-> Table1S3M) and 'S3M_Table1.tsv' (-> Table1S3M)
    """
    stem = p.stem
    m = re.match(r"^([A-Za-z0-9_]+)_Table(\d+)$", stem, re.IGNORECASE)
    if m:
        stn = m.group(1).upper()  # e.g. S3M
        tbl = int(m.group(2))
        return f"Table{tbl}{stn}"
    return stem  # already like Table1S3M

# -------- request building / patching --------

def campbell_secs_from_utc_str(s: str) -> int:
    """
    Convert 'YYYY-mm-dd HH:MM:SS' (UTC) to Campbell epoch seconds (since 1990-01-01T00:00:00Z).
    """
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    epoch = datetime(1990, 1, 1, tzinfo=timezone.utc)
    return int((dt - epoch).total_seconds())

def make_candidates_with_lastn(template_hex: str, last_n: int, since_secs_le: int | None):
    """
    Build a list of candidate BD…BD frames (hex) from a captured template, with:
      - count = -last_n (signed16 BE)
      - anchor seconds = since_secs_le if provided, else preserved from template
    """
    t = extract_anchor_tuple(template_hex)
    if not t:
        return []
    _off, anchor_sec_le, _cnt_s16, _tbl, _is31 = t
    use_sec = since_secs_le if since_secs_le is not None else anchor_sec_le
    desired_signed = -int(last_n)  # negative for "last N"
    return build_request_from_template(template_hex, use_sec, desired_signed)

# -------- TSV/PCAP helpers --------

def extract_latest_reqs_from_pcap(pcap_path: Path):
    """
    From a .pcapng, return dict {"27": hex, "31": hex} for latest seen request of each length.
    """
    if not TSHARK:
        print("ERROR: tshark not found. Install Wireshark and ensure 'tshark' is in PATH.")
        return {}

    filt = f"tcp.port == {TCP_PORT} && tcp.len in {{27,31}} && data[0] == 0xbd"
    cmd = [TSHARK, "-r", str(pcap_path), "-Y", filt, "-T", "fields", "-e", "data"]
    try:
        out = subprocess.check_output(cmd, text=True)
    except subprocess.CalledProcessError:
        return {}

    latest = {}
    for line in out.strip().splitlines():
        h = line.strip()
        if not h:
            continue
        try:
            b = bytes.fromhex(h)
        except Exception:
            continue
        if len(b) not in (27,31):
            continue
        latest[str(len(b))] = h.lower()
    return latest

def extract_latest_reqs_from_tsv(tsv_path: Path):
    """
    Read a Wireshark/TShark TSV (-T fields) that includes a 'tcp.payload' column.
    Return {"27": hex, "31": hex} for the last-seen BD…BD request of each length.
    """
    latest = {}
    try:
        with open(tsv_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.rstrip("\n")
                if not line:
                    continue
                cols = line.split("\t")
                if not cols:
                    continue
                payload_hex = cols[-1].strip()
                if not payload_hex:
                    continue
                h = payload_hex.replace(":", "").replace(" ", "")
                try:
                    b = bytes.fromhex(h)
                except Exception:
                    continue

                # Look for BD…BD frames inside this TCP segment
                frames = deframe_all(b)  # strips 0xBD sentinels
                for inner in frames:
                    frame_bytes = b"\xBD" + inner + b"\xBD"
                    if len(frame_bytes) in (27, 31):
                        hx = frame_bytes.hex()
                        latest[str(len(frame_bytes))] = hx
    except Exception as e:
        print(f"[tsv] failed to parse {tsv_path.name}: {e}")
        return {}
    return latest

# ----------------- core flow -----------------

def print_rows(rows):
    print("\n    Row  " + "  ".join(["BattV","VWC1","EC1","T1","VWC2","EC2","T2","VWC3","EC3","T3"]))
    for i, vals in enumerate(rows):
        print(f"    {i:02d}  {summarize_row(vals)}")

def run_one_station(name, req_hex_27, req_hex_31, csv_dir: Path, dat_dir: Path,
                    verbose=False, max_rows=MAX_ROWS_DEFAULT, compare_last=10, args=None):
    # Station filter (leaf/table)
    leaf, tbl = parse_station_table_from_name(name)
    if leaf is None or tbl is None:
        # name not understood; skip
        return False
    if args and args.only_leaf and leaf.upper() != args.only_leaf.upper():
        return False
    if args and args.only_table and tbl != args.only_table:
        return False

    print(f"\n== {name} ==")
    station_ok = False
    per_req_outputs = {}  # tag -> (rows, timestamps, dat_file_used)

    # Resolve anchor seconds if user supplied --since-ts (UTC string)
    since_secs_le = None
    if args and args.since_ts:
        try:
            since_secs_le = campbell_secs_from_utc_str(args.since_ts)
            if verbose:
                print(f"    using since-ts={args.since_ts} -> campbell_secs={since_secs_le}")
        except Exception as e:
            print(f"    [warn] bad --since-ts value '{args.since_ts}': {e}")

    # Build candidate requests with patched last_n (negative BE16) and recomputed CRCs
    # We’ll try the 31-byte template first, then 27-byte, since 31 often carries more context.
    reqs = []
    if req_hex_31:
        reqs.append(("31", make_candidates_with_lastn(req_hex_31, args.last_n, since_secs_le)))
    if req_hex_27:
        reqs.append(("27", make_candidates_with_lastn(req_hex_27, args.last_n, since_secs_le)))

    for tag, cand_list in reqs:
        if not cand_list:
            continue
        if verbose:
            print(f"  trying {len(cand_list)} CRC variants from {tag}-byte template (last_n={args.last_n})")

        decoded_ok = False
        last_rows = last_metas = last_bodies = None

        for idx, hx in enumerate(cand_list, start=1):
            if verbose:
                print(f"    variant {idx}/{len(cand_list)}")
            decoded = decode_for_request(hx, verbose=verbose, max_rows=max_rows)
            if decoded is None or decoded[0] is None:
                continue
            rows, metas, bodies = decoded
            if rows:
                decoded_ok = True
                last_rows, last_metas, last_bodies = rows, metas, bodies
                # success; no need to try remaining CRC permutations for this tag
                break

        if not decoded_ok:
            if verbose:
                print(f"    no plausible rows decoded from any {tag}-template variant")
            continue

        rows, metas, bodies = last_rows, last_metas, last_bodies

        # Timestamp guess (first body only, informational)
        if bodies and verbose:
            ts_guess_utc = guess_timestamp_utc(bodies[0])
            if ts_guess_utc:
                try:
                    mt = (datetime.fromisoformat(ts_guess_utc)
                          .astimezone(timezone(timedelta(hours=-6)))).isoformat()
                    print(f"    timestamp guess (first row): {ts_guess_utc} / {mt}")
                except Exception:
                    print(f"    timestamp guess (first row): {ts_guess_utc}")

        # Match to .dat tail
        timestamps = []
        dat_file_used = ""
        if dat_dir:
            dat_path = dat_filename_for(name, dat_dir)
            if dat_path.exists():
                if verbose:
                    print(f"    [dat] using {dat_path}")
                dat_rows = load_recent_dat_rows_tail(dat_path, tail_k=compare_last, verbose=verbose)
                if dat_rows:
                    timestamps = match_rows_to_dat(rows, dat_rows, verbose=verbose)
                    dat_file_used = str(dat_path)
                else:
                    if verbose:
                        print(f"    [dat] no usable rows in {dat_path.name}")
            else:
                if verbose:
                    print(f"    [dat] NOT FOUND for {name}: expected {dat_path.name}")

        while len(timestamps) < len(rows):
            timestamps.append(("", "", False))

        if csv_dir:
            # Include last-n in the filename for clarity
            out_req_path = csv_dir / f"{name}_{tag}_n{args.last_n}.csv"
            write_csv(out_req_path, rows, timestamps,
                      source_tag=tag, matched_from="dat", dat_file=dat_file_used)

        per_req_outputs[tag] = (rows, timestamps, dat_file_used)
        station_ok = True

    # Combined CSV (dedup across 31/27 by (timestamp, rounded values))
    if station_ok and csv_dir:
        combined = []
        for tag in ("31","27"):
            if tag in per_req_outputs:
                rs, ts, df = per_req_outputs[tag]
                for r, t in zip(rs, ts):
                    combined.append((tag, r, t, df))

        seen, out_rows = set(), []
        for tag, vals, (tsu, tsm, m), df in combined:
            key = (tsu, tuple(round(x,6) for x in vals))
            if key in seen:
                continue
            seen.add(key)
            out_rows.append((tag, vals, tsu, tsm, m, df))

        out_combined = csv_dir / f"{name}_combined_n{args.last_n}.csv"
        out_combined.parent.mkdir(parents=True, exist_ok=True)
        with open(out_combined, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Row","TimestampUTC","TimestampMT","SourceReq","Matched","MatchedFrom","MatchedFile"] + FIELDS)
            for i,(src, vals, tsu, tsm, m, df) in enumerate(out_rows):
                w.writerow([i, tsu, tsm, src, "yes" if m else "no", "dat", df if m else ""] + vals)
        print(f"    [csv] wrote {out_combined} ({len(out_rows)} rows)")

    return station_ok

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pcap-dir", default="biochar_app/pakbus/bdFiles",
                        help="Directory containing .pcapng captures")
    parser.add_argument("--tsv-dir", default=None,
                        help="Directory containing tshark-exported .tsv payloads (optional)")
    parser.add_argument("--csv-dir", default="biochar_app/pakbus/bdFiles/out_csv",
                        help="(Deprecated by --out-dir) Output directory for per-station CSVs")
    parser.add_argument("--out-dir", default="biochar_app/pakbus/bdFiles/out_csv",
                        help="Output directory for per-station CSVs")
    parser.add_argument("--dat-dir", default=None,
                        help="Directory containing *.dat logger exports for timestamp comparison (optional)")
    parser.add_argument("--max-rows", type=int, default=2000,
                        help="Max rows to write per output CSV")
    parser.add_argument("--compare-last", type=int, default=10,
                        help="How many rows from the end of the .dat file to compare against")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose logging")

    # Fresh request build controls
    parser.add_argument("--since-ts", default=None,
                        help='UTC start time "YYYY-mm-dd HH:MM:SS"; if omitted, keep template anchor seconds')
    parser.add_argument("--last-n", type=int, default=1,
                        help="Download the last N rows (encoded as signed16 BE = -N)")

    # Station filtering
    parser.add_argument("--only-leaf", help="Limit to a single leaf (e.g. S2T)")
    parser.add_argument("--only-table", type=int, help="Limit to a single table number (e.g. 1)")

    args = parser.parse_args()

    # Output dir preference: --out-dir takes precedence
    out_dir = Path(args.out_dir) if args.out_dir else Path(args.csv_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    dat_dir = Path(args.dat_dir) if args.dat_dir else None
    if dat_dir:
        print(f"[dat] directory: {dat_dir}")

    # ---- discover station files (pcap + optional tsv) ----
    stations = {}  # name -> (req27_hex, req31_hex)

    # From PCAPs
    pcap_dir = Path(args.pcap_dir) if args.pcap_dir else None
    pcaps = sorted(pcap_dir.glob("*.pcapng")) if pcap_dir and pcap_dir.exists() else []
    for p in pcaps:
        name = station_name_from_filename(p)
        latest = extract_latest_reqs_from_pcap(p)
        has27 = "27" in latest
        has31 = "31" in latest
        print(f"[pcap] {name}: 27={'yes' if has27 else 'no '}  31={'yes' if has31 else 'no '}")
        if latest:
            stations[name] = (latest.get("27", ""), latest.get("31", ""))

    # From TSVs (optional)
    tsv_dir = Path(args.tsv_dir) if args.tsv_dir else None
    if tsv_dir and tsv_dir.exists():
        tsvs = sorted(tsv_dir.glob("*.tsv"))
        for t in tsvs:
            name = station_name_from_filename(t)
            latest = extract_latest_reqs_from_tsv(t)
            has27 = "27" in latest
            has31 = "31" in latest
            print(f"[tsv ] {name}: 27={'yes' if has27 else 'no '}  31={'yes' if has31 else 'no '}")
            if latest:
                prev27, prev31 = stations.get(name, ("", ""))
                stations[name] = (latest.get("27", prev27), latest.get("31", prev31))

    if not stations:
        print("No stations discovered in pcap or tsv sources.")
        sys.exit(0)

    # ---- run each station (apply leaf/table filter here) ----
    print(f"[info] host={HOST} port={PORT}")
    run_order = sorted(stations.keys())

    # Only list what we will actually run
    filtered = []
    for name in run_order:
        leaf, tbl = parse_station_table_from_name(name)
        if leaf is None or tbl is None:
            continue
        if args.only_leaf and leaf.upper() != args.only_leaf.upper():
            continue
        if args.only_table and tbl != args.only_table:
            continue
        filtered.append(name)

    if not filtered:
        print("[info] nothing to run after applying filters.")
        sys.exit(0)

    print(f"[info] running stations: {', '.join(filtered)}")

    results = []  # (name, ok_bool)
    for name in filtered:
        req27_hex, req31_hex = stations[name]
        ok = run_one_station(
            name,
            req27_hex, req31_hex,
            csv_dir=out_dir,
            dat_dir=dat_dir,
            max_rows=args.max_rows,
            compare_last=args.compare_last,
            verbose=args.verbose,
            args=args,
        )
        results.append((name, ok))

    # ---- summary ----
    print("\n=== summary ===")
    for name, ok in results:
        print(f"{name:<12} -> {'ok' if ok else '-'}")

if __name__ == "__main__":
    main()