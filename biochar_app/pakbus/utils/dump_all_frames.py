# biochar_app/pakbus/utils/dump_all_frames.py
from __future__ import annotations
from pathlib import Path
import argparse, sys
import re, binascii

HEX_RE = re.compile(r"^[0-9a-fA-F\s]+$")

def load_frame_bytes(p: Path) -> bytes:
    """Load a frame from file p, accepting either binary or ascii hex."""
    raw = p.read_bytes()
    # If content is short ASCII and matches hex charset, decode as hex
    try:
        text = raw.decode("ascii", errors="strict").strip()
        if text and HEX_RE.match(text) and len(text.replace(" ", "").replace("\n", "")) % 2 == 0:
            hexstr = text.replace(" ", "").replace("\n", "")
            return bytes(int(hexstr[i:i+2], 16) for i in range(0, len(hexstr), 2))
    except Exception:
        pass
    # Otherwise treat as binary payload (already deframed)
    return raw

def is_dir_with_frames(p: Path) -> bool:
    return p.is_dir() and p.name.endswith("_frames")

def collect_frame_files(root: Path, glob_pat: str) -> list[Path]:
    # We want files inside *_frames directories; user passed "*_frames/*"
    # Use rglob to recurse and only include files
    return [q for q in root.rglob(glob_pat) if q.is_file()]

def dump_all(root: Path, glob_pat: str, out_file: Path, verbose: bool) -> int:
    files = collect_frame_files(root, glob_pat)
    if not files:
        print(f"ERROR: no files matched {root}/{glob_pat}")
        return 2

    # Group by parent *_frames directory; sort files within each group
    groups: dict[Path, list[Path]] = {}
    for f in files:
        base = f
        while base.parent != root and not is_dir_with_frames(base.parent):
            base = base.parent
        grp = base.parent if base.name.endswith("_frames") else base
        if not grp.name.endswith("_frames"):
            grp = f.parent
        groups.setdefault(grp, []).append(f)

    total_frames = 0
    hello_candidates: list[tuple[str, int, bytes]] = []

    for grp, flist in sorted(groups.items(), key=lambda x: str(x[0])):
        flist.sort()  # chronological by filename
        if verbose:
            print(f"\n=== {grp} ===")
        frames: list[bytes] = []
        for fp in flist:
            b = load_frame_bytes(fp)
            frames.append(b)
            total_frames += 1
            if verbose:
                op = b[0] if b else 0
                print(f"{fp.name} op=0x{op:02X} len={len(b):03d} hex={binascii.hexlify(b).decode()[:128]}{'...' if len(b)>64 else ''}")

        # locate pre-0x09
        for i in range(1, len(frames)):
            if frames[i] and frames[i][0] == 0x09:
                pre = frames[i-1]
                if pre:
                    hello_candidates.append((grp.name, i-1, pre))

    # write candidates
    with out_file.open("w", encoding="utf-8") as w:
        for gname, idx, fr in hello_candidates:
            w.write(f"{gname} #{idx:03d} op=0x{fr[0]:02X} len={len(fr):03d} hex={binascii.hexlify(fr).decode()}\n")

    # print summary + first candidate to stdout
    print("\n--- Summary ---")
    print(f"Scanned files : {len(files)}")
    print(f"Total frames  : {total_frames}")
    print(f"Hello candids : {len(hello_candidates)}  (saved to {out_file})")
    if hello_candidates:
        first_hex = binascii.hexlify(hello_candidates[0][2]).decode()
        print(f"\nFirst hello candidate hex:\n{first_hex}")
    else:
        print("Note: No 0x09 found; try a different directory or ensure files are the deframed payloads.")
    return 0

def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Dump deframed *_frames; extract hello (frame before 0x09).")
    p.add_argument("--dir", default="biochar_app/pakbus/pakbus_data/bdFiles/out_fetch",
                   help="Directory containing *_frames folders (default set to project path).")
    p.add_argument("--glob", default="*_frames/*",
                   help="Glob for files inside *_frames folders (default: *_frames/*).")
    p.add_argument("--out", default="hello_candidates.txt",
                   help="Output file with pre-0x09 frames.")
    p.add_argument("--quiet", action="store_true", help="Less per-file printing.")
    args = p.parse_args(argv)

    root = Path(args.dir)
    if not root.exists():
        print(f"ERROR: directory not found: {root}")
        return 2
    return dump_all(root, args.glob, Path(args.out), verbose=not args.quiet)

if __name__ == "__main__":
    raise SystemExit(main())