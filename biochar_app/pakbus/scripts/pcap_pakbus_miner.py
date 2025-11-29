#!/usr/bin/env python3
from __future__ import annotations
import argparse, binascii, csv, shutil, subprocess, sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Iterable, List, Tuple, Optional

# Known patterns we’ve been seeing
HELLO_HEX = "bdefff10010fff00010e00ddf0bd"
SHORT_89_PREFIX = "bdaffd20010ffd000189050101fffd"
SHORT_60_PREFIX = "bdaffd60010ffd000189050101fffd"
NEIGHBOR_70_PREFIX = "bdaffd70010ffd00010a6"
LONG_BASE_PREFIX = "bdaffd00011ffd"  # long replies start like this, byte(s) after vary by table

def hexify(b: bytes, maxlen: Optional[int] = None) -> str:
    h = binascii.hexlify(b).decode()
    return h if maxlen is None else h[:maxlen]

def find_bd_frames(payload: bytes) -> List[bytes]:
    """Split PakBus frames delimited by 0xBD...0xBD and drop very short fragments."""
    out = []
    bd = 0xBD
    idxs = [i for i, bb in enumerate(payload) if bb == bd]
    for a, b in zip(idxs, idxs[1:]):
        if b > a:
            frag = payload[a:b+1]
            if frag and frag[0] == bd and frag[-1] == bd and len(frag) > 4:
                out.append(frag)
    return out

def classify_frame(hx: str) -> str:
    if hx == HELLO_HEX:
        return "hello"
    if hx.startswith(SHORT_89_PREFIX):
        return "reply-89"
    if hx.startswith(SHORT_60_PREFIX):
        return "reply-60"
    if hx.startswith(NEIGHBOR_70_PREFIX):
        return "neighbor-70"
    if hx.startswith(LONG_BASE_PREFIX):
        # surface some extra context (next few nibbles) to separate tables
        return "long-" + hx[:len(LONG_BASE_PREFIX)+2]
    return "other"

# ----------------------------- Scapy backend ---------------------------------
def scapy_iter_frames(pcap_path: str) -> Iterable[Tuple[float, str, bytes]]:
    """
    Yields (epoch_ts, "src->dst", payload_bytes) for TCP packets.
    Requires scapy; if not installed, caller should choose tshark path.
    """
    try:
        from scapy.all import rdpcap, TCP, Raw
    except Exception as e:
        raise RuntimeError("Scapy not available") from e

    pkts = rdpcap(pcap_path)
    for p in pkts:
        if not p.haslayer(TCP):
            continue
        tcp = p[TCP]
        if not tcp.payload:
            continue
        raw = bytes(tcp.payload)
        # Try to extract IPs for direction string
        try:
            src = p[0].src
            dst = p[0].dst
        except Exception:
            src, dst = "?", "?"
        ts = float(getattr(p, "time", 0.0))
        yield ts, f"{src}->{dst}", raw

# ----------------------------- tshark backend --------------------------------
def tshark_iter_frames(pcap_path: str) -> Iterable[Tuple[float, str, bytes]]:
    """
    Yields (epoch_ts, "src->dst", payload_bytes) for TCP packets via tshark.
    Requires tshark in PATH.
    """
    if not shutil.which("tshark"):
        raise RuntimeError("tshark not found in PATH")

    # We ask tshark to dump: frame.time_epoch, ip.src, ip.dst, tcp.payload (hex)
    cmd = [
        "tshark",
        "-r", pcap_path,
        "-Y", "tcp && tcp.len > 0",
        "-T", "fields",
        "-e", "frame.time_epoch",
        "-e", "ip.src",
        "-e", "ip.dst",
        "-e", "tcp.payload",
        "-E", "separator=,",
        "-E", "quote=n",
        "-E", "occurrence=f",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) < 4:
            continue
        try:
            ts = float(parts[0])
        except ValueError:
            continue
        src = parts[1] or "?"
        dst = parts[2] or "?"
        hexpayload = parts[3].replace(":", "")  # tshark uses colon-delimited hex bytes
        if not hexpayload:
            continue
        try:
            raw = binascii.unhexlify(hexpayload)
        except binascii.Error:
            continue
        yield ts, f"{src}->{dst}", raw
    proc.wait()

# ------------------------------- Main mining ---------------------------------
def mine_pcap(
    pcap_path: str,
    out_csv: str,
    prefer_scapy: bool = True,
    min_frame_len: int = 8,
    only_bd: bool = False,
) -> None:
    # Choose backend
    frames_iter = None
    backend = ""
    if prefer_scapy:
        try:
            frames_iter = scapy_iter_frames(pcap_path)
            backend = "scapy"
        except Exception:
            pass
    if frames_iter is None:
        frames_iter = tshark_iter_frames(pcap_path)
        backend = "tshark"

    print(f"[INFO] Using backend: {backend}")

    # Collect and write CSV
    counts = Counter()
    long_prefix_counts = Counter()  # for candidate long signatures
    totals = 0

    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_utc", "dir", "frame_len", "frame_hex", "class"])

        for ts_epoch, direction, payload in frames_iter:
            totals += 1

            # Split payload into PakBus frames (0xBD ... 0xBD)
            bd_frames = find_bd_frames(payload)
            if only_bd and not bd_frames:
                continue

            # If no BD frames found, optionally still log the TCP payload (skipped if only_bd)
            if not bd_frames:
                ts_iso = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).isoformat()
                hx = hexify(payload)
                klass = "tcp-payload"
                counts[klass] += 1
                w.writerow([ts_iso, direction, len(payload), hx, klass])
                continue

            for fr in bd_frames:
                hx = hexify(fr)
                if len(fr) < min_frame_len:
                    continue
                klass = classify_frame(hx)
                counts[klass] += 1

                # If looks like a long reply, keep a few candidate prefix lengths
                if hx.startswith(LONG_BASE_PREFIX):
                    for L in (16, 18, 20, 22, 24, 28):
                        long_prefix_counts[hx[:L]] += 1

                ts_iso = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).isoformat()
                w.writerow([ts_iso, direction, len(fr), hx, klass])

    # Print summary
    print("\n[SUMMARY classes]")
    total_frames = sum(v for k, v in counts.items() if k != "tcp-payload")
    for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {k:12s}: {v}")
    print(f"  {'(frames total)':12s}: {total_frames}")

    # Report candidate long signatures
    if long_prefix_counts:
        print("\n[CANDIDATE long reply signatures (by frequency)]")
        for pref, n in long_prefix_counts.most_common(20):
            print(f"  {pref}  (x{n})")
        print("\nPick the *shortest* prefix that uniquely identifies the long reply,")
        print("and try it as --reply-sig in probe_and_pull.py.")
    else:
        print("\n[NOTE] No frames matched the LONG_BASE_PREFIX pattern.")
        print("If you know a different long signature from S1M, search for that instead.")

def main():
    ap = argparse.ArgumentParser(description="Mine PakBus frames from a PCAP and suggest a reliable reply signature.")
    ap.add_argument("--pcap", required=True, help="Path to a .pcap / .pcapng")
    ap.add_argument("--out-csv", default="pcap_frames.csv")
    ap.add_argument("--no-scapy", action="store_true", help="Skip scapy and use tshark backend")
    ap.add_argument("--only-bd", action="store_true", help="Only log BD…BD frames, skip raw TCP payload rows")
    ap.add_argument("--min-frame-len", type=int, default=8, help="Drop frames shorter than this")
    args = ap.parse_args()

    try:
        mine_pcap(
            args.pcap,
            args.out_csv,
            prefer_scapy=not args.no_scapy,
            min_frame_len=args.min_frame_len,
            only_bd=args.only_bd,
        )
    except RuntimeError as e:
        print(f"[ERROR] {e}")
        sys.exit(2)

if __name__ == "__main__":
    main()