#!/usr/bin/env python3
"""
bd_read_table1_downloader.py

Forge a fresh BD "read Table1" request by patching a known-good seed TX frame
(or a list of seed frames). Auto-detects the FD 09 slot, patches (count,start),
recomputes CRC (tries IBM & CCITT), sends, and decodes Table1 from the reply.

Examples:
  # Try all seeds from a file (one hex frame per line)
  python bd_read_table1_downloader.py \
    --addr 2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd --port 6785 \
    --seeds-file biochar_app/pakbus/pakbus_data/bdFiles/bd_frames_list_reads_try20.txt \
    --since-ts "2025-09-28 16:00:00" --tz "America/Denver" \
    --count 20 --out-dir pakbus_runs/forged
"""

import argparse, socket, time, sys, pathlib, csv, struct, datetime
from typing import Optional, Tuple, List

HELLO = bytes.fromhex("bd 90 01 0f fd 73 d3 c2 d6 bd")

DATA_PREFIXES = [
    bytes.fromhex("bd af fd 00 01 1f fd 20 03"),
    bytes.fromhex("bd af fd 00 01 1f fd 20 02"),
    bytes.fromhex("bd af fd 00 01 1f fd 20 01"),
]
ACK_PREFIX = bytes.fromhex("bd af fd 70")

def crc16_ibm(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def crc16_ccitt_false(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= (b << 8) & 0xFFFF
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc

def split_bd_frames(buf: bytes) -> List[bytes]:
    out, cur = [], bytearray()
    for b in buf:
        cur.append(b)
        if b == 0xBD:
            out.append(bytes(cur)); cur.clear()
    if cur:
        out.append(bytes(cur))
    return out

def is_data_frame(b: bytes) -> bool:
    return any(b.startswith(p) for p in DATA_PREFIXES) and len(b) >= 40

def recv_all(sock: socket.socket, idle_timeout=0.6, max_wait=1.5) -> bytes:
    sock.settimeout(idle_timeout)
    chunks, start = [], time.time()
    while True:
        try:
            data = sock.recv(65536)
            if data:
                chunks.append(data); start = time.time()
            else:
                break
        except socket.timeout:
            if time.time() - start >= max_wait:
                break
            continue
        except Exception:
            break
    return b"".join(chunks)

def to_epoch1990_from_local(ts_str: str, tz_name: Optional[str]) -> int:
    # Convert local "YYYY-MM-DD HH:MM:SS" to seconds since 1990-01-01 UTC
    naive = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name) if tz_name else None
        local = naive.replace(tzinfo=tz) if tz else naive
        utc = local.astimezone(datetime.timezone.utc)
    except Exception:
        utc = naive.replace(tzinfo=datetime.timezone.utc)
    epoch1990 = datetime.datetime(1990,1,1,tzinfo=datetime.timezone.utc)
    return int((utc - epoch1990).total_seconds())

def iter_fd09_slots(core_wo_crc: bytes) -> List[Tuple[int,int]]:
    """
    Find all positions matching: FD 09 ?? 00 00  [then at least 6 bytes available]
    Return list of (pos_count, pos_startkey).
    """
    res = []
    i = 0
    n = len(core_wo_crc)
    while True:
        j = core_wo_crc.find(b"\xFD\x09", i)
        if j < 0: break
        if j + 2 < n and j + 4 < n and j + 10 <= n:
            if core_wo_crc[j+3:j+5] == b"\x00\x00":
                pos_count = j + 5           # LE16
                pos_start = pos_count + 2    # LE32
                res.append((pos_count, pos_start))
        i = j + 1
    return res

def make_patched_variants(seed_hex: str, count: int, start_key_le32: int) -> List[bytes]:
    """
    From a seed hex frame, create patched frames (for each FD09 slot) with placeholder CRC.
    Returns list of BD-framed bytes with 0 CRC (we'll fill later).
    """
    b = bytes.fromhex(seed_hex.strip())
    if b[0] != 0xBD or b[-1] != 0xBD or len(b) < 16:
        raise ValueError("Seed does not look like a BD frame (must start/end with 0xBD).")
    core = b[1:-1]       # includes old CRC at last two bytes
    core_wo_crc = core[:-2]

    slots = iter_fd09_slots(core_wo_crc)
    if not slots:
        raise ValueError("No FD 09 ?? 00 00 slots found in seed.")

    variants = []
    for (pos_count, pos_start) in slots:
        buf = bytearray(core_wo_crc)
        buf[pos_count:pos_count+2] = struct.pack("<H", max(1, min(2000, count)))   # count (LE16)
        buf[pos_start:pos_start+4] = struct.pack("<I", start_key_le32)             # start key (LE32)
        variants.append(bytes([0xBD]) + bytes(buf) + b"\x00\x00" + bytes([0xBD]))
    return variants

def apply_crc(frame_with_placeholder: bytes, flavor: str) -> bytes:
    if frame_with_placeholder[0] != 0xBD or frame_with_placeholder[-1] != 0xBD:
        raise ValueError("Bad frame boundaries.")
    core = frame_with_placeholder[1:-1]
    core_wo_crc = core[:-2]
    crc = crc16_ibm(core_wo_crc) if flavor == "ibm" else crc16_ccitt_false(core_wo_crc)
    return bytes([0xBD]) + core_wo_crc + struct.pack("<H", crc) + bytes([0xBD])

def decode_table1_blocks_from_reply(buf: bytes) -> List[Tuple[str, list]]:
    """
    Scan for (epoch1990 UInt32 BE + 10 * float32 BE) blocks.
    Return [(ISO8601Z, values[10]), ...]
    """
    out = []
    n = len(buf)
    for i in range(0, n - (4 + 4*10)):
        sec = struct.unpack_from(">I", buf, i)[0]
        if not (900_000_000 <= sec <= 1_700_000_000):
            continue
        vals = struct.unpack_from(">" + "f"*10, buf, i+4)
        # quick plausibility: accept if >=6 look sane
        ranges = [
            (9.0,16.5),
            (0.0,0.6),(0.0,5.0),(-40,60),
            (0.0,0.6),(0.0,5.0),(-40,60),
            (0.0,0.6),(0.0,5.0),(-40,60),
        ]
        ok = sum(1 for v,(lo,hi) in zip(vals,ranges) if (v==v) and (lo-2 <= v <= hi+50))
        if ok >= 6:
            ts = datetime.datetime(1990,1,1,tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=sec)
            out.append((ts.isoformat().replace("+00:00","Z"), list(vals)))
    return out

def try_send(addr, port, frame_bytes: bytes,
             hello_gap_ms: int, post_wait_ms: int,
             connect_timeout: float, idle_timeout: float,
             af_family: int, retries: int, retry_sleep: float):
    """
    Send one frame with retries. Returns (raw_rx, frames, acks, datas).
    On connect/IO errors, returns (None, [], [], []).
    """
    for attempt in range(1, retries+1):
        try:
            with socket.socket(af_family, socket.SOCK_STREAM) as s:
                s.settimeout(connect_timeout)
                # IPv6 tuple uses (addr, port, flow, scope)
                if af_family == socket.AF_INET6:
                    s.connect((addr, port, 0, 0))
                else:
                    s.connect((addr, port))
                # Hello + gap
                s.sendall(HELLO)
                time.sleep(hello_gap_ms/1000.0)
                _ = recv_all(s, idle_timeout=min(0.4, idle_timeout), max_wait=0.9)
                # Send the forged frame
                s.sendall(frame_bytes)
                rx = recv_all(s, idle_timeout=idle_timeout, max_wait=post_wait_ms/1000.0)
            frames = split_bd_frames(rx) if rx else []
            acks, datas = [], []
            for bf in frames:
                if bf.startswith(ACK_PREFIX) and len(bf) == 18:
                    acks.append(bf)
                elif is_data_frame(bf):
                    datas.append(bf)
            return rx, frames, acks, datas
        except (socket.timeout, TimeoutError):
            if attempt < retries:
                print(f"[WARN] connect/recv timeout (attempt {attempt}/{retries}); retrying in {retry_sleep}s...")
                time.sleep(retry_sleep)
                continue
            else:
                print("[ERR] connect/recv timed out; giving up this variant.")
                return None, [], [], []
        except OSError as e:
            print(f"[ERR] socket error: {e}")
            return None, [], [], []
        except Exception as e:
            print(f"[ERR] unexpected send error: {e}")
            return None, [], [], []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--seed-hex", help="A single known-good TX frame in ASCII hex")
    ap.add_argument("--seeds-file", help="Path to file with one TX hex per line (tries all)")
    ap.add_argument("--count", type=int, default=20, help="Records to request (LE16)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--since-epoch-1990", type=int, help="Start key seconds since 1990-01-01 UTC")
    g.add_argument("--since-ts", type=str, help='Local "YYYY-MM-DD HH:MM:SS" to convert')
    ap.add_argument("--tz", default=None, help="IANA TZ for --since-ts (e.g., America/Denver)")
    ap.add_argument("--hello-gap-ms", type=int, default=150)
    ap.add_argument("--post-wait-ms", type=int, default=1500)
    ap.add_argument("--connect-timeout", type=float, default=8.0)
    ap.add_argument("--idle-timeout", type=float, default=0.6)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--retry-sleep", type=float, default=1.0)
    ap.add_argument("--af", choices=["inet6","inet"], default=None,
                    help="Force address family; default auto (inet6 if addr contains ':')")
    ap.add_argument("--out-dir", default="pakbus_runs/forged")
    args = ap.parse_args()

    outdir = pathlib.Path(args.out_dir); outdir.mkdir(parents=True, exist_ok=True)

    # Build start key
    if args.since_epoch_1990 is not None:
        start_key = int(args.since_epoch_1990) & 0xFFFFFFFF
    else:
        start_key = to_epoch1990_from_local(args.since_ts, args.tz)

    # Address family
    if args.af:
        af = socket.AF_INET6 if args.af == "inet6" else socket.AF_INET
    else:
        af = socket.AF_INET6 if ":" in args.addr else socket.AF_INET

    # Collect seeds
    seeds: List[str] = []
    if args.seed_hex:
        seeds.append(args.seed_hex.strip())
    if args.seeds_file:
        for line in pathlib.Path(args.seeds_file).read_text().splitlines():
            s = line.strip().lower().replace(" ", "")
            if s and all(c in "0123456789abcdef" for c in s):
                seeds.append(s)
    if not seeds:
        print("[ERR] Provide --seed-hex or --seeds-file")
        sys.exit(2)

    # Try seeds -> slots -> crc flavors with retries
    for si, seed in enumerate(seeds, 1):
        try:
            variants = make_patched_variants(seed, args.count, start_key)
        except Exception as e:
            print(f"[SEED {si}] skipped: {e}")
            continue

        for vi, v in enumerate(variants, 1):
            f_ibm   = apply_crc(v, "ibm")
            f_ccitt = apply_crc(v, "ccitt")

            for flavor, frame in (("IBM", f_ibm), ("CCITT", f_ccitt)):
                rx, frames, acks, datas = try_send(
                    args.addr, args.port, frame,
                    args.hello_gap_ms, args.post_wait_ms,
                    args.connect_timeout,  # <- fix: use underscore, not hyphen
                    args.idle_timeout, af,
                    args.retries, args.retry_sleep
                )
                total = len(rx) if rx else 0
                print(f"[SEED {si}/VAR {vi}/{flavor}] rx={total}B, frames={len(frames)}, data={len(datas)}, acks={len(acks)}")

                if not datas:
                    continue

                datas.sort(key=len, reverse=True)
                best = datas[0]
                # Save raw for inspection
                (outdir / f"reply_seed{si}_var{vi}_{flavor}.bin").write_bytes(best)

                # Decode Table1 blocks
                hits = decode_table1_blocks_from_reply(best)
                if not hits:
                    print(f"[SEED {si}/VAR {vi}/{flavor}] No epoch+10 floats found in payload; trying next.")
                    continue

                # Write CSV (append)
                csv_path = outdir / "table1_forged.csv"
                write_header = not csv_path.exists()
                with csv_path.open("a", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    if write_header:
                        w.writerow([
                            "TIMESTAMP","BattV_Min","VWC_1_Avg","EC_1_Avg","T_1_Avg",
                            "VWC_2_Avg","EC_2_Avg","T_2_Avg","VWC_3_Avg","EC_3_Avg","T_3_Avg",
                            "_seed_index","_slot_index","_crc_flavor","_count","_start_epoch1990"
                        ])
                    for iso, vals in hits:
                        w.writerow([iso] + [f"{v:.6f}" for v in vals] +
                                   [si, vi, flavor, args.count, start_key])

                print(f"[OK] Seed#{si} Slot#{vi} CRC={flavor}: {len(hits)} rows -> {csv_path}")
                print("[INFO] Downloader succeeded.")
                return

    print("[FAIL] Tried all seeds/slots/CRC flavors with no data frames decoded.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)