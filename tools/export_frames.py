#!/usr/bin/env python3
"""
Minimal exporter: read a Table1<LEAF>.pcapng, extract BD…BD TCP payloads (port 6785),
and write a frames CSV to out_fetch/<LEAF>_Table1_n<COUNT>_p01_frames/frames.csv.

Usage:
  python tools/export_frames.py --pcapng biochar_app/pakbus/bdFiles/Table1S3T.pcapng --out-dir biochar_app/pakbus/bdFiles/out_fetch
"""

import argparse, csv, re, subprocess, sys
from pathlib import Path
from datetime import datetime, timezone

COMMON_TSHARK_PATHS = [
    "/Applications/Wireshark.app/Contents/MacOS/tshark",
    "/opt/homebrew/bin/tshark",
    "/usr/local/bin/tshark",
    "tshark",
]

DISPLAY_FILTER = 'tcp.port == 6785 && data && data[0] == 0xbd'

def find_tshark() -> str:
    for p in COMMON_TSHARK_PATHS:
        try:
            subprocess.run([p, "-v"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            return p
        except Exception:
            pass
    raise RuntimeError("tshark not found; install Wireshark or add tshark to PATH.")

def station_from_filename(path: str) -> str:
    name = Path(path).name
    m = re.search(r"(S\d+[A-Z])", name.upper())
    return m.group(1) if m else Path(path).stem

def export_rows(tshark: str, pcap: Path):
    cmd = [
        tshark, "-r", str(pcap),
        "-Y", DISPLAY_FILTER,
        "-T", "fields",
        "-e", "frame.time_epoch",
        "-e", "data",
        "-Eheader=y", "-Eseparator=,", "-Equote=d",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(f"[warn] tshark rc={proc.returncode} for {pcap}\n")

    out = []
    first = True
    for line in proc.stdout.splitlines():
        if first:
            first = False
            continue
        line = line.strip()
        if not line:
            continue
        parts = [p.strip().strip('"') for p in line.split(",")]
        if len(parts) != 2:
            continue
        try:
            epoch = float(parts[0])
        except Exception:
            continue
        hx = parts[1].replace(":", "").lower()
        if not (len(hx) >= 4 and len(hx) % 2 == 0 and hx[:2] == "bd" and hx[-2:] == "bd"):
            continue
        out.append((epoch, hx))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pcapng", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--prelude-tsv")  # accepted but unused in this minimal version
    args = ap.parse_args()

    pcap = Path(args.pcapng)
    if not pcap.exists():
        print(f"[error] missing pcapng: {pcap}", file=sys.stderr)
        sys.exit(2)

    try:
        tshark = find_tshark()
    except RuntimeError as e:
        print(f"[error] {e}", file=sys.stderr)
        sys.exit(2)

    leaf = station_from_filename(str(pcap))
    rows = export_rows(tshark, pcap)
    count = len(rows)

    out_base = Path(args.out_dir)
    out_base.mkdir(parents=True, exist_ok=True)
    out_dir = out_base / f"{leaf}_Table1_n{count}_p01_frames"
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "frames.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["DateTime", "Epoch", "ByteLen", "PayloadHex"])
        for epoch, hx in rows:
            dt = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
            w.writerow([dt, f"{epoch:.6f}", len(hx)//2, hx])

    print(f"[ok] {leaf}: wrote {csv_path} ({count} rows)")

if __name__ == "__main__":
    main()