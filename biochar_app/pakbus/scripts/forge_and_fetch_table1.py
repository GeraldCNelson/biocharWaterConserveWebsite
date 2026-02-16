#!/usr/bin/env python3
"""
forge_and_fetch_table1.py

Use discovered offsets to craft BD “replay/data” request frames for any time window, send to logger,
and decode Table1 (epoch+10 float BE) rows.

Inputs:
  --addr, --port
  --offsets pakbus_runs/offsets/bd_table1_offsets.json  (from the probe script)
  --start, --end     (UTC like 2024-06-01T00:00:00Z; end is exclusive)
  --step  15min      (request cadence; 15min matches Table1)
  --count 4          (# records per request; tune to fit reply sizes)
  --hello-gap-ms, --post-wait-ms  network timing knobs

Outputs:
  pakbus_runs/forged/raw/replay_YYYYmmdd_HHMMSS.hex  (raw reply bytes)
  pakbus_runs/forged/table1/table1_forged.csv        (appended rows)
"""

import argparse, json, pathlib, socket, time, struct, datetime, csv

HELLO = bytes.fromhex("bd90010ffd73d3c2d6bd")

DATA_PREFIXES = [
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x03',
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x02',
    b'\xbd\xaf\xfd\x00\x01\x1f\xfd\x20\x01',
]

FIELDS = ["BattV_Min",
          "VWC_1_Avg","EC_1_Avg","T_1_Avg",
          "VWC_2_Avg","EC_2_Avg","T_2_Avg",
          "VWC_3_Avg","EC_3_Avg","T_3_Avg"]

def split_bd_frames(buf: bytes):
    out, cur = [], bytearray()
    for b in buf:
        cur.append(b)
        if b == 0xBD:
            out.append(bytes(cur)); cur.clear()
    if cur: out.append(bytes(cur))
    return out

def is_data_frame(frame: bytes) -> bool:
    return any(frame.startswith(p) for p in DATA_PREFIXES) and len(frame) >= 40

def iter_table1_blocks(b: bytes):
    n = len(b)
    for i in range(0, n - (4 + 4*10)):
        sec = struct.unpack_from(">I", b, i)[0]
        if not (900_000_000 <= sec <= 1_800_000_000):
            continue
        vals = struct.unpack_from(">" + "f"*10, b, i+4)
        yield i, sec, list(vals)

def to_epoch1990(ts_utc: datetime.datetime) -> int:
    base = datetime.datetime(1990,1,1,tzinfo=datetime.timezone.utc)
    return int((ts_utc - base).total_seconds())

def send_one(addr, port, tx: bytes, hello_gap_ms=120, wait_ms=1200) -> bytes:
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(2.0)
        s.connect((addr, port, 0, 0))
        s.sendall(HELLO)
        time.sleep(hello_gap_ms/1000.0)
        _ = s.recv(4096)  # best-effort consume hello ack (ignore contents)
        s.sendall(tx)
        s.settimeout(0.25)
        chunks, start = [], time.time()
        while True:
            try:
                data = s.recv(65536)
                if data:
                    chunks.append(data); start = time.time()
                else:
                    break
            except socket.timeout:
                if time.time() - start >= (wait_ms/1000.0):
                    break
                continue
        return b"".join(chunks)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--offsets", default="pakbus_runs/offsets/bd_table1_offsets.json")
    ap.add_argument("--start", required=True, help="UTC start, e.g., 2024-06-01T00:00:00Z")
    ap.add_argument("--end",   required=True, help="UTC end (exclusive)")
    ap.add_argument("--step",  default="15min", help="Step between requests (15min)")
    ap.add_argument("--count", type=int, default=4, help="records per request")
    ap.add_argument("--hello-gap-ms", type=int, default=120)
    ap.add_argument("--post-wait-ms", type=int, default=1400)
    args = ap.parse_args()

    cfg = json.loads(pathlib.Path(args.offsets).read_text())
    tmpl_path = pathlib.Path(cfg["template_file"])
    tmpl = tmpl_path.read_bytes() if tmpl_path.exists() else bytes.fromhex(tmpl_path.read_text())
    epoch_off = cfg.get("epoch_offset")
    count_off = cfg.get("count_offset")

    if epoch_off is None:
        print("[FATAL] offsets config missing epoch_offset. Run probe script first.")
        return

    # Outputs
    raw_dir = pathlib.Path("pakbus_runs/forged/raw"); raw_dir.mkdir(parents=True, exist_ok=True)
    out_dir = pathlib.Path("pakbus_runs/forged/table1"); out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "table1_forged.csv"
    write_header = not out_csv.exists()

    # parse start/end UTC
    def parse_utc(x: str) -> datetime.datetime:
        s = x.strip()
        if s.endswith("Z"): s = s[:-1]
        if " " in s and "T" not in s:
            s = s.replace(" ","T")
        dt = datetime.datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)

    t0 = parse_utc(args.start)
    t1 = parse_utc(args.end)

    # step parsing (simple: only minutes/hours)
    step = args.step.lower()
    if step.endswith("min"):
        minutes = int(step[:-3])
        delta = datetime.timedelta(minutes=minutes)
    elif step.endswith("h"):
        hours = int(step[:-1])
        delta = datetime.timedelta(hours=hours)
    else:
        raise SystemExit(f"Unsupported step {args.step}; use 15min, 5min, 1h, etc.")

    with out_csv.open("a", newline="", encoding="utf-8") as fcsv:
        w = csv.writer(fcsv)
        if write_header:
            w.writerow(["TIMESTAMP"] + FIELDS + ["_source_file","_request_epoch","_count","_epoch_offset","_count_offset"])

        t = t0
        while t < t1:
            epoch = to_epoch1990(t)
            tx = bytearray(tmpl)
            tx[epoch_off:epoch_off+4] = epoch.to_bytes(4,"big")
            if count_off is not None:
                tx[count_off:count_off+2] = int(args.count).to_bytes(2,"big")

            rx = send_one(args.addr, args.port, bytes(tx), args.hello_gap_ms, args.post_wait_ms)
            tslabel = t.strftime("%Y%m%d_%H%M%S")
            (raw_dir / f"replay_{tslabel}.hex").write_bytes(rx)

            # decode table1 rows found in reply
            frames = split_bd_frames(rx)
            datas = [fr for fr in frames if is_data_frame(fr)]
            total_hits = 0
            for df in datas:
                for off, sec, vals in iter_table1_blocks(df):
                    dt = datetime.datetime(1990,1,1,tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=sec)
                    iso = dt.isoformat().replace("+00:00","Z")
                    w.writerow([iso] + [f"{v:.6f}" for v in vals] + [f"replay_{tslabel}.hex", epoch, args.count, epoch_off, count_off])
                    total_hits += 1

            print(f"[{tslabel}] epoch={epoch} -> data_frames={len(datas)}, table1_hits={total_hits}")
            t += delta

if __name__ == "__main__":
    main()