#!/usr/bin/env python3
# Decode (or at least extract) PakBus frames from SPM .txt dumps and save them.
# For now, CSV decoding is stubbed; main value is robust frame extraction.
#
# Usage:
#   python -m biochar_app.pakbus.spm_decode_table12 "<session>.txt" --table 1 --out table1.csv --save-frames
#   python -m biochar_app.pakbus.spm_decode_table12 "<session>.txt" --table 2 --out table2.csv --save-frames
#
# --save-frames with no value writes "<session>.frames.txt" next to the input.
# You can also pass a custom path: --save-frames path/to/frames.txt

import argparse, os, sys, re, pathlib, csv

HEX_PAIR = re.compile(r"\b[0-9A-Fa-f]{2}\b")

def _extract_hex_pairs(s: str):
    # Return only clean hex-byte pairs from a line (ignores ASCII column etc.)
    return HEX_PAIR.findall(s)

def parse_spm_txt_frames(spm_txt_path: str):
    """
    Parse SPM Dump View .txt:
      [timestamp] Written data (...)   -> TX
      [timestamp] Read data (...)      -> RX
      Next one or more lines contain hex bytes; stop at blank or next bracketed timestamp line.
    Also supports lines with 'head='... (our earlier --raw dump) as a fallback.
    Returns list of (dir, hexstring) where dir in {'TX','RX'}.
    """
    frames = []
    pending_dir = None
    with open(spm_txt_path, "r", errors="ignore") as f:
        lines = f.readlines()

    i = 0
    n = len(lines)
    while i < n:
        line = lines[i].rstrip("\n")
        # Fallback: our earlier tool's "head=" lines
        m_head = re.search(r"head=([0-9A-Fa-f ]+)", line)
        if m_head:
            hexstr = m_head.group(1).replace(" ", "")
            frames.append(("?", hexstr))
            i += 1
            continue

        # Detect new SPM block headers
        if "Written data" in line:
            pending_dir = "TX"
            i += 1
            # consume following hex lines
            hexbytes = []
            while i < n:
                l2 = lines[i].rstrip("\n")
                if l2.startswith("[") and "]" in l2:
                    break  # next block header
                pairs = _extract_hex_pairs(l2)
                if pairs:
                    hexbytes.extend(pairs)
                    i += 1
                else:
                    # stop at non-hex payload line
                    i += 1
                    break
            if hexbytes:
                frames.append((pending_dir, "".join(pairs for pairs in hexbytes)))
            pending_dir = None
            continue

        if "Read data" in line:
            pending_dir = "RX"
            i += 1
            hexbytes = []
            while i < n:
                l2 = lines[i].rstrip("\n")
                if l2.startswith("[") and "]" in l2:
                    break
                pairs = _extract_hex_pairs(l2)
                if pairs:
                    hexbytes.extend(pairs)
                    i += 1
                else:
                    i += 1
                    break
            if hexbytes:
                frames.append((pending_dir, "".join(pairs for pairs in hexbytes)))
            pending_dir = None
            continue

        # Otherwise, just advance
        i += 1

    return frames

def default_frames_path_for(session_path: str) -> pathlib.Path:
    p = pathlib.Path(session_path)
    # Put next to the session file, with .frames.txt
    return p.with_suffix("").with_name(p.stem + ".frames.txt")

def decode_spm(session_path, table, out_csv, save_frames):
    # Determine if we are parsing text dump (we are, per your workflow)
    # We don’t try to open .spm (binary) anymore here.
    if not os.path.isfile(session_path):
        print(f"[err] not found: {session_path}", file=sys.stderr)
        sys.exit(1)

    frames_dir = parse_spm_txt_frames(session_path)

    if not frames_dir:
        print(f"[warn] no frames found in {session_path}")
    else:
        print(f"[ok] extracted {len(frames_dir)} frames")

    # --- Handle save_frames ---
    if save_frames is not None:
        # save_frames may be True (flag only) or a path string
        if save_frames is True:
            save_path = default_frames_path_for(session_path)
        else:
            sp = pathlib.Path(str(save_frames))
            # If relative, still write next to session file (as requested)
            if not sp.is_absolute():
                save_path = default_frames_path_for(session_path)
            else:
                save_path = sp
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w") as f:
            for i, (d, hexs) in enumerate(frames_dir, 1):
                f.write(f"[{i:04d}] {d} {hexs}\n")
        print(f"[ok] frames dumped → {save_path}")

    # --- Stub CSV (we don’t have reliable decoding yet) ---
    # Writes headers so downstream tooling doesn’t break.
    with open(out_csv, "w", newline="") as fout:
        w = csv.writer(fout)
        if table in (1, 2):
            w.writerow([
                "TIMESTAMP","RECORD","BattV_Min",
                "VWC_1_Avg","EC_1_Avg","T_1_Avg",
                "VWC_2_Avg","EC_2_Avg","T_2_Avg",
                "VWC_3_Avg","EC_3_Avg","T_3_Avg"
            ])
        else:
            w.writerow(["TIMESTAMP","RECORD"])  # safe minimal header
    print(f"[ok] wrote 0 rows → {out_csv}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("spmfile", help="Path to SPM Dump View .txt")
    ap.add_argument("--table", type=int, required=True, help="Table number (1 or 2 for your CR2 setup)")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--save-frames", nargs="?", const=True,
                    help="Save hex frames; default path is '<session>.frames.txt' next to the input")
    args = ap.parse_args()

    # Normalize save_frames (may be True, a string path, or None)
    save_frames = args.save_frames if args.save_frames is not None else None
    decode_spm(args.spmfile, args.table, args.out, save_frames)

if __name__ == "__main__":
    main()