#!/usr/bin/env python3
"""
Extract BD…BD request frames (27/31-byte payloads) from Wireshark .pcapng files.

Usage examples:
  # Folder (recommended)
  python biochar_app/pakbus/extract_bd_frames.py biochar_app/pakbus/bdFiles

  # Glob (no shell expansion needed)
  python biochar_app/pakbus/extract_bd_frames.py "biochar_app/pakbus/bdFiles/*.pcapng"

  # Custom output dir
  python biochar_app/pakbus/extract_bd_frames.py biochar_app/pakbus/bdFiles --outdir biochar_app/pakbus/bdFiles

Outputs:
  - reqs.json  (map of station -> {"27": "bd..bd", "31": "bd..bd"})
  - reqs.py    (same as a Python dict: REQ_BY_STATION = {...})
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from glob import glob
from collections import defaultdict

# Display filter to pull only candidate frames (we'll still validate BD...BD in Python)
DISPLAY_FILTER = 'tcp.port == 6785 && tcp.len in {27,31} && data[0] == 0xbd'

# Try common macOS path if tshark not in PATH
COMMON_TSHARK_PATHS = [
    "/Applications/Wireshark.app/Contents/MacOS/tshark",
    "/opt/homebrew/bin/tshark",
    "/usr/local/bin/tshark",
    "tshark",
]

def find_tshark() -> str:
    for p in COMMON_TSHARK_PATHS:
        try:
            subprocess.run([p, "-v"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            return p
        except Exception:
            continue
    raise RuntimeError(
        "tshark not found. Install Wireshark and ensure tshark is in PATH.\n"
        "On macOS it is usually at /Applications/Wireshark.app/Contents/MacOS/tshark."
    )

def expand_inputs(targets):
    files = []
    for t in targets:
        t = os.path.expanduser(t)
        p = Path(t)
        if p.is_dir():
            files.extend(sorted(str(x) for x in p.glob("*.pcapng")))
        else:
            # treat as a glob pattern (even if quoted)
            files.extend(sorted(glob(t)))
    return [f for f in files if f.lower().endswith(".pcapng")]

def station_from_filename(path: str) -> str:
    """
    Heuristic: grab the 'S1B' / 'S2T' / similar token from the filename.
    e.g., Table1S1M.pcapng -> S1M
    """
    name = Path(path).name
    m = re.search(r"(S\d+[A-Z])", name.upper())
    if m:
        return m.group(1)
    # fallback: stem
    return Path(path).stem

def run_tshark(tshark: str, pcap: str) -> list[str]:
    """
    Run tshark and return hex payload lines (no spaces), one per TCP segment matching our display filter.
    """
    cmd = [
        tshark, "-r", pcap,
        "-Y", DISPLAY_FILTER,
        "-T", "fields",
        "-e", "data",
        "-n",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(f"[WARN] tshark returned {proc.returncode} for {pcap}\n")
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    return lines

def looks_bd_frame(hexstr: str) -> bool:
    # hex string should start/end with 'bd' and be even-length
    if len(hexstr) < 4 or len(hexstr) % 2 != 0:
        return False
    return hexstr[:2].lower() == "bd" and hexstr[-2:].lower() == "bd"

def main():
    ap = argparse.ArgumentParser(description="Extract BD…BD request frames from pcapng files.")
    ap.add_argument("inputs", nargs="+", help="Directories and/or glob patterns for .pcapng files.")
    ap.add_argument("--outdir", help="Directory where reqs.py/json will be written. Defaults to the first input dir or CWD.")
    ap.add_argument("--max", type=int, default=8, help="Max frames to keep per file per length (27,31).")
    args = ap.parse_args()

    files = expand_inputs(args.inputs)
    if not files:
        print("[WARN] No .pcapng files matched.")
        # still write empty reqs to a sensible place
        outdir = Path(args.outdir or os.getcwd())
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "reqs.py").write_text("REQ_BY_STATION = {\n}\n", encoding="utf-8")
        (outdir / "reqs.json").write_text(json.dumps({}, indent=2), encoding="utf-8")
        print(f"[OK] wrote empty reqs.py/reqs.json to {outdir}")
        return

    # Default outdir: the directory of the first input (if it’s a directory),
    # else the parent folder of that first file; otherwise CWD.
    if args.outdir:
        outdir = Path(args.outdir)
    else:
        fp = Path(files[0])
        outdir = fp if fp.is_dir() else fp.parent
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        tshark = find_tshark()
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # Collect latest FRAMES by station; keep the last (most recent) up to --max, per len
    req_by_station: dict[str, dict[str, list[str]]] = defaultdict(lambda: {"27": [], "31": []})

    for pcap in files:
        station = station_from_filename(pcap)
        print(f"[...] scanning {pcap}  (station={station})")
        lines = run_tshark(tshark, pcap)
        # TShark lists packets in capture order → latest are usually LAST
        for hx in lines:
            if not hx:
                continue
            # normalize: remove colons if any (shouldn't be there with -e data)
            hx = hx.replace(":", "").lower()
            if not looks_bd_frame(hx):
                continue
            byte_len = len(hx) // 2
            if byte_len == 27 and hx not in req_by_station[station]["27"]:
                req_by_station[station]["27"].append(hx)
                req_by_station[station]["27"] = req_by_station[station]["27"][-args.max:]
            elif byte_len == 31 and hx not in req_by_station[station]["31"]:
                req_by_station[station]["31"].append(hx)
                req_by_station[station]["31"] = req_by_station[station]["31"][-args.max:]
            # ignore other sizes

    # For convenience in the runner, collapse lists to “most recent” single string where present
    final_map = {}
    for st, sizes in req_by_station.items():
        entry = {}
        if sizes["27"]:
            entry["27"] = sizes["27"][-1]
        if sizes["31"]:
            entry["31"] = sizes["31"][-1]
        if entry:
            final_map[st] = entry

    # Write JSON + Python
    json_path = outdir / "reqs.json"
    py_path   = outdir / "reqs.py"

    json_path.write_text(json.dumps(final_map, indent=2), encoding="utf-8")
    py_path.write_text(
        "REQ_BY_STATION = " + json.dumps(final_map, indent=2) + "\n",
        encoding="utf-8"
    )

    print(f"[OK] wrote {json_path}")
    print(f"[OK] wrote {py_path}")
    print("\nTip: in your runner you can do:")
    print(f"  from pathlib import Path")
    print(f"  import json; reqs = json.loads(Path('{json_path}').read_text())")
    print("  # then reqs['S1B']['31'] etc.")

if __name__ == "__main__":
    main()