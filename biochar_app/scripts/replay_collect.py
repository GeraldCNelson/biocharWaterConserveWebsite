#!/usr/bin/env python3
import argparse, binascii, csv, pathlib, socket, struct, sys, time
from datetime import datetime, timezone, timedelta

# ---------- helpers ----------
def deframe_all(buf: bytes):
    frames, cur, in_frame = [], bytearray(), False
    for b in buf:
        if b == 0xBD:
            if in_frame and cur:
                frames.append(bytes(cur))
            cur.clear()
            in_frame = not in_frame
            continue
        if in_frame:
            cur.append(b)
    return frames

def list_collect_requests(stream: bytes):
    found, frames = [], deframe_all(stream)
    for idx, inner in enumerate(frames):
        # PakBus link header is 8 bytes on TCP; payload after that
        payload = inner[8:] if len(inner) > 8 else b""
        if not payload or payload[0] != 0x09:  # CollectData request
            continue
        tran  = payload[1] if len(payload) >= 2 else None
        mode  = payload[2] if len(payload) >= 3 else None
        table = struct.unpack(">H", payload[3:5])[0] if len(payload) >= 5 else None
        pbytes = payload[5:]
        found.append({"i": idx, "inner": inner, "tran": tran, "mode": mode, "table": table, "pbytes": pbytes})
    return found

# Table1 schema you’ve been using (10 IEEE floats)
FIELDS = ["BattV_Min",
          "VWC_1_Avg","EC_1_Avg","T_1_Avg",
          "VWC_2_Avg","EC_2_Avg","T_2_Avg",
          "VWC_3_Avg","EC_3_Avg","T_3_Avg"]

def decode_table1_row(data: bytes):
    """Heuristic: find 10 big-endian floats aligned within the first 16 bytes."""
    for off in range(0, 16):
        try:
            vals = struct.unpack_from(">"+ "f"*len(FIELDS), data, off)
            row = dict(zip(FIELDS, vals))
            # light sanity: battery plausible, temperature plausible
            if 8.0 <= row["BattV_Min"] <= 16.5 and -40 <= row["T_1_Avg"] <= 60:
                return off, row
        except Exception:
            pass
    return None, None

def find_pakbus_time_1990(data: bytes, search_window=64):
    """
    Try to find (secs, nsecs) since 1990-01-01.
    Only accept if 2014 <= year <= (now + 1 day).
    """
    epoch = datetime(1990,1,1,tzinfo=timezone.utc)
    now_plus = datetime.now(timezone.utc) + timedelta(days=1)
    for off in range(0, max(0, search_window-7)):
        try:
            secs, nsecs = struct.unpack_from(">ii", data, off)
        except Exception:
            continue
        if secs < 0 or not (0 <= nsecs < 1_000_000_000):
            continue
        dt = epoch + timedelta(seconds=secs, microseconds=nsecs/1000)
        if 2014 <= dt.year and dt <= now_plus:
            return off, dt
    return None, None

def last_timestamp_from_dat(dat_path: pathlib.Path):
    """Return the last timestamp (UTC) found in a CRBasic .dat file, or None."""
    if not dat_path or not dat_path.exists():
        return None
    text = dat_path.read_text(errors="ignore").splitlines()
    if len(text) <= 5:
        return None
    # find CSV header line (usually line 4), then data after
    header = None
    for i in range(4, len(text)):
        if text[i].strip():
            header = next(csv.reader([text[i]]))
            data_start = i + 1
            break
    if not header:
        return None
    # choose a likely time column
    norm = [h.strip() for h in header]
    time_candidates = [c for c in norm if c.lower() in ("time","timestamp","tmstamp","datetime")]
    t_idx = norm.index(time_candidates[0]) if time_candidates else 0
    # scan from bottom
    for j in range(len(text)-1, data_start-1, -1):
        row = text[j].strip()
        if not row:
            continue
        cells = next(csv.reader([row]))
        if t_idx >= len(cells):
            continue
        ts = cells[t_idx].strip()
        fmts = ("%Y-%m-%d %H:%M:%S",
                "%m/%d/%Y %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y/%m/%d %H:%M:%S")
        for f in fmts:
            try:
                dt_naive = datetime.strptime(ts, f)
                return dt_naive.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None

def align_to_interval(base_dt: datetime, interval_min: int):
    if not base_dt.tzinfo:
        base_dt = base_dt.replace(tzinfo=timezone.utc)
    epoch = datetime(1970,1,1,tzinfo=timezone.utc)
    secs = int((base_dt - epoch).total_seconds())
    interval_s = interval_min * 60
    aligned = epoch + timedelta(seconds=(secs // interval_s) * interval_s)
    return aligned

def send_and_read(ipv6_host: str, port: int, req_inner: bytes, wait_s: float = 8.0):
    tx = b"\xBD" + req_inner + b"\xBD"
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect((ipv6_host, port, 0, 0))
    s.sendall(tx)
    s.settimeout(0.5)
    end = time.time() + wait_s
    buf = bytearray()
    while time.time() < end:
        try:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf.extend(chunk)
        except socket.timeout:
            pass
    s.close()
    frames = deframe_all(bytes(buf))
    return bytes(buf), frames

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Replay a CollectData request from a PCAP and decode Table1 row.")
    ap.add_argument("--pcap", required=True, help="Path to .pcapng with PakBus/TCP traffic")
    ap.add_argument("--dat", help="Optional .dat file to anchor time (same table/site)")
    ap.add_argument("--host", required=True, help="IPv6 address of CR800 (e.g. 2605:59C0:...)")
    ap.add_argument("--port", type=int, default=6785, help="PakBus/TCP port (default 6785)")
    ap.add_argument("--interval", type=int, default=15, help="Table1 interval minutes (default 15)")
    ap.add_argument("--idx", type=int, default=None, help="Optional index of request to replay (from listing)")
    args = ap.parse_args()

    pcap = pathlib.Path(args.pcap)
    dat  = pathlib.Path(args.dat) if args.dat else None

    raw = pcap.read_bytes()
    reqs = list_collect_requests(raw)
    if not reqs:
        print(f"No CollectData (0x09) requests found in {pcap}")
        sys.exit(2)

    print(f"Found {len(reqs)} CollectData requests in {pcap}:")
    for r in reqs:
        phex = r["pbytes"].hex(" ")
        print(f"  [idx {r['i']:>3}] len={len(r['inner']):>3}  Tran={r['tran']:>3}  Mode=0x{(r['mode'] or 0):02X}  "
              f"Table={r['table']}  P={phex}")

    rq = None
    if args.idx is not None:
        # choose by exact frame index
        rq = next((r for r in reqs if r["i"] == args.idx), None)
        if not rq:
            print(f"--idx {args.idx} not found among CollectData frames.")
            sys.exit(2)
    else:
        # pick the longest request (usually the bulk one)
        rq = max(reqs, key=lambda r: len(r["inner"]))

    print("\nReplaying request:")
    print(f"  idx={rq['i']}  inner_len={len(rq['inner'])}  Tran={rq['tran']}  Mode=0x{rq['mode']:02X}  Table={rq['table']}")

    rx, frames = send_and_read(args.host, args.port, rq["inner"], wait_s=8.0)
    print("received bytes:", len(rx), "framed messages:", len(frames))

    # Find matching 0x89 response
    best = None
    for inner in frames:
        payload = inner[8:] if len(inner) > 8 else b""
        if not payload or payload[0] != 0x89:
            continue
        tn = payload[1] if len(payload) >= 2 else None
        rc = payload[2] if len(payload) >= 3 else None
        data = payload[3:] if len(payload) > 3 else b""
        if tn == rq["tran"]:
            best = (tn, rc, data)
            break
        if best is None:  # keep a fallback
            best = (tn, rc, data)

    if not best:
        print("No 0x89 CollectData response seen.")
        sys.exit(3)

    tn, rc, data = best
    print(f"CollectData response: Tran={tn} rc={rc} data_bytes={len(data)}")
    if rc != 0 or not data:
        print("Logger returned non-zero rc or empty data.")
        sys.exit(4)

    # Timestamp: embedded → .dat → now-rounded
    toff, dt = find_pakbus_time_1990(data)
    if dt:
        stamp = dt
        anchor_src = "embedded"
        print(f"Detected embedded TimeOfRecord: offset={toff}  time={dt.isoformat()}")
    else:
        anchor = last_timestamp_from_dat(dat) if dat else None
        if anchor:
            stamp = align_to_interval(anchor, args.interval)
            print(f"Anchored time from .dat last timestamp: {anchor.isoformat()} -> aligned {stamp.isoformat()}")
            anchor_src = ".dat"
        else:
            now_aligned = align_to_interval(datetime.now(timezone.utc), args.interval)
            print(f"No embedded time and no .dat anchor; using now-rounded: {now_aligned.isoformat()}")
            stamp = now_aligned
            anchor_src = "now-rounded"

    # Decode a Table1 row
    foff, row = decode_table1_row(data)
    if not row:
        print("Could not decode a float row; data head:", binascii.hexlify(data[:64]).decode())
        sys.exit(5)

    print("\nDecoded Table1 row:")
    print(
        f"{stamp.isoformat()}  "
        f"BattV={row['BattV_Min']:.3f}  "
        f"VWC1={row['VWC_1_Avg']:.4f} EC1={row['EC_1_Avg']:.3f} T1={row['T_1_Avg']:.2f}  "
        f"VWC2={row['VWC_2_Avg']:.4f} EC2={row['EC_2_Avg']:.3f} T2={row['T_2_Avg']:.2f}  "
        f"VWC3={row['VWC_3_Avg']:.4f} EC3={row['EC_3_Avg']:.3f} T3={row['T_3_Avg']:.2f}"
    )
    print(f"(timestamp source: {anchor_src})")

if __name__ == "__main__":
    main()