#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPM/PakBus helper used by spm_analyze_all.zsh

- Works with .spm binaries (raw SPM capture) and .txt text exports
- Reassembles frames: scans for BD ... BD across line breaks
- Unescapes DB DC -> BD and DB DD -> DB
- Knows CollectData req (0x09) and resp (0x89)
- Provides three subcommands: list-frames, extract-tabledefs (stub), collectdata-dump

Usage:
  python spm_pyhelper.py list-frames <path>
  python spm_pyhelper.py collectdata-dump <path>
  python spm_pyhelper.py extract-tabledefs <path>   # placeholder
"""

import sys, pathlib, struct, re, binascii

BD  = 0xBD
ESC = 0xDB

def read_blob(path: pathlib.Path) -> bytes:
    p = pathlib.Path(path)
    raw = p.read_bytes()
    # Heuristic: if it looks like a text dump (lots of ASCII, "Read data"/"Written data"),
    # treat it as text and extract hex tokens — otherwise keep binary.
    if b"Read data" in raw or b"Written data" in raw or b"Dump view" in raw:
        # Extract EVERY pair of hex digits, but keep BD-framed segments intact.
        # We first normalize to uppercase hex with spaces to make regex simpler.
        hexish = re.findall(rb'\b[0-9A-Fa-f]{2}\b', raw)
        return bytes.fromhex(b' '.join(hexish).decode('ascii'))
    return raw

def find_bd_frames(blob: bytes):
    """Return iterable of raw frames that start and end with BD (may include escaped bytes)."""
    frames = []
    i = 0
    n = len(blob)
    while i < n:
        try:
            start = blob.index(BD.to_bytes(1,'big'), i)
        except ValueError:
            break
        j = start + 1
        while j < n:
            if blob[j] == BD:
                frames.append(blob[start:j+1])
                i = j + 1
                break
            j += 1
        else:
            # no closing BD
            break
    return frames

def unescape(frame: bytes) -> bytes:
    """Strip leading/trailing BD, undo byte-stuffing."""
    if not (frame and frame[0]==BD and frame[-1]==BD):
        return None
    out = bytearray()
    i = 1
    end = len(frame)-1
    while i < end:
        b = frame[i]
        if b == ESC and i+1 < end:
            nxt = frame[i+1]
            if nxt == 0xDC:
                out.append(BD)
            elif nxt == 0xDD:
                out.append(ESC)
            else:
                # tolerant: preserve unknown escape
                out.extend((ESC, nxt))
            i += 2
        else:
            out.append(b)
            i += 1
    return bytes(out)

def find_collectdata(payload: bytes):
    """
    Return small dict if a CollectData msg is found inside payload.
    We search for 0x09 (req) or 0x89 (resp) then pull table/mode nearby.
    PakBus link-layer header length varies; we pattern-scan.
    """
    for i, b in enumerate(payload):
        if b in (0x09, 0x89):
            # typical shapes we see in your capture:
            #   09 <table_hi> <table_lo> <mode> ...
            #   89 <table_hi> <table_lo> <mode> ...
            if i+3 < len(payload):
                table = (payload[i+1] << 8) | payload[i+2]
                mode  = payload[i+3]
            else:
                table = None
                mode = None
            # try to guess a 16-bit signature in the next ~16 bytes (best-effort)
            sig = None
            for j in range(i+4, min(i+20, len(payload)-1)):
                val = (payload[j] << 8) | payload[j+1]
                # filter out obvious non-sig values (small counters/zeros)
                if val not in (0,1,2,3,4,5,6,7,8,9,0x0101):
                    sig = val
                    break
            return {
                "off": i,
                "msg": b,
                "table": table,
                "mode": mode,
                "sig": sig,
            }
    return None

def cmd_list_frames(path: str):
    blob = read_blob(pathlib.Path(path))
    raw_frames = find_bd_frames(blob)
    print(f"[diag] bytes={len(blob)}  frames={len(raw_frames)}")
    shown = 0
    for idx, rf in enumerate(raw_frames):
        pl = unescape(rf) or b""
        info = find_collectdata(pl)
        if info:
            m = "CollectData resp" if info["msg"]==0x89 else ("CollectData req" if info["msg"]==0x09 else f"0x{info['msg']:02X}")
            t = info["table"] if info["table"] is not None else -1
            print(f"[{idx:04d}] {m}  table={t}  mode=0x{(info['mode'] or 0):02X}  sig={('0x%04X'%info['sig']) if info['sig'] is not None else 'n/a'}")
            shown += 1
        else:
            # generic
            head = pl[:12].hex(' ')
            print(f"[{idx:04d}] (other) head={head}")
    if shown == 0:
        print("[note] No CollectData frames detected (0x09/0x89).")

def cmd_collectdata_dump(path: str):
    blob = read_blob(pathlib.Path(path))
    raw_frames = find_bd_frames(blob)
    print(f"[diag] raw frames: {len(raw_frames)}")
    hits = 0
    for i, rf in enumerate(raw_frames):
        pl = unescape(rf) or b""
        info = find_collectdata(pl)
        if not info:
            continue
        hits += 1
        m = "req" if info["msg"]==0x09 else "resp"
        t = info["table"] if info["table"] is not None else -1
        sig = f"0x{info['sig']:04X}" if info["sig"] is not None else "n/a"
        print(f"[{i:04d}] {m} msg=0x{info['msg']:02X} table={t} mode=0x{(info['mode'] or 0):02X} sig={sig}")
        start = info["off"]
        head = pl[start:start+64]
        print(f"       payload_head={head.hex(' ')}")
    print(f"[ok] parsed frames with CollectData msg: {hits}")

def cmd_extract_tabledefs(path: str):
    # Placeholder: keeps interface parity with zsh driver.
    # We still write a tiny marker file so the pipeline doesn't fail.
    p = pathlib.Path(path)
    out = p.with_suffix("").with_name(p.stem + ".tabledefs.bin")
    out.write_bytes(b"")  # stub
    print(f"[ok] wrote table defs blob → {out}  (stub)")

def main(argv):
    if len(argv) != 3 or argv[1] not in ("list-frames","collectdata-dump","extract-tabledefs"):
        print(__doc__.strip())
        return 2
    cmd = argv[1]
    path = argv[2]
    if cmd == "list-frames":
        cmd_list_frames(path)
    elif cmd == "collectdata-dump":
        cmd_collectdata_dump(path)
    elif cmd == "extract-tabledefs":
        cmd_extract_tabledefs(path)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))