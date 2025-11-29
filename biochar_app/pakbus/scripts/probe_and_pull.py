#!/usr/bin/env python3
from __future__ import annotations
import argparse, binascii, csv, socket, struct, time
from datetime import datetime, timezone
from typing import List, Tuple, Optional, Literal
import struct

# --- BD framing helpers -------------------------------------------------------

# Rolling buffer used to handle frames split across multiple TCP reads
_FRAMER_BUF = bytearray()

def _drain_bd_frames_from_buffer() -> list[bytes]:
    """
    Pull complete BD-framed packets out of _FRAMER_BUF.
    A BD frame starts with 0xBD and ends with 0xBD.
    Leaves any trailing partial frame bytes in _FRAMER_BUF.
    """
    frames: list[bytes] = []
    buf = _FRAMER_BUF

    i = 0
    while True:
        # find start 0xBD
        s = buf.find(b"\xbd", i)
        if s < 0:
            if i > 0:
                del buf[:i]
            break

        # find closing 0xBD after start
        e = buf.find(b"\xbd", s + 1)
        if e < 0:
            if s > 0:
                del buf[:s]
            break

        # we have [s..e] inclusive
        frames.append(bytes(buf[s:e + 1]))
        i = e + 1
        if i >= len(buf):
            buf.clear()
            break

    return frames

def split_frames(raw: bytes) -> list[bytes]:
    """
    Stateless splitter if you already have the entire payload containing
    complete frames. For streamed sockets, prefer recv_some() below which
    uses the rolling buffer.
    """
    frames: list[bytes] = []
    i = 0
    while True:
        s = raw.find(b"\xbd", i)
        if s < 0:
            break
        e = raw.find(b"\xbd", s + 1)
        if e < 0:
            break
        frames.append(raw[s:e + 1])
        i = e + 1
    return frames

# --- socket helper ------------------------------------------------------------

def recv_some(sock, idle_timeout: float = 5.0):
    """
    Read from socket and return (raw_bytes, frames_list).
    Never returns None. Uses a rolling buffer so frames can span reads.
    """
    sock.settimeout(idle_timeout)
    try:
        raw = sock.recv(65536)
    except socket.timeout:
        return b"", []  # idle
    except Exception:
        return b"", []  # be defensive

    if not raw:
        return b"", []  # peer closed

    # Accumulate and extract complete BD frames
    _FRAMER_BUF.extend(raw)
    try:
        frames = _drain_bd_frames_from_buffer()
    except Exception:
        frames = []

    return raw, frames

# --- Constants & “known good” shapes from your pcap sessions -----------------

HELLO_HEX = "bdefff10010fff00010e00ddf0bd"

# The TX that matches your pcap (routered read; ctl=0x0F, no FD shim):
TX_PCAP_GOOD = "bda0016ffd00010ffd09050102ffff2810bd"

# “Short” replies routinely seen:
#   reply-89    : bdaffd20010ffd000189050101fffd....
#   alt-ack(?)  : bdaffd60010ffd000189050101fffd....
#   neighbor-70 : bdaffd70010ffd00010a6... (varies)

# Default signature of the *long* data frames you care about (logger→PC):
DEFAULT_LONG_SIG = "bdaffd00011ffd20"

# ---------------------------------------------------------------------------

def hex_to_bytes(s: str) -> bytes:
    return binascii.unhexlify(s.replace(" ", "").strip())

def send_all(sock: socket.socket, data: bytes):
    total = 0
    while total < len(data):
        n = sock.send(data[total:])
        if n <= 0:
            raise OSError("socket send failed")
        total += n

def open_tcp(addr: str, port: int, connect_timeout: float = 5.0) -> socket.socket:
    family = socket.AF_INET6 if ":" in addr else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.settimeout(connect_timeout)
    sock.connect((addr, port))
    return sock

def write_frame_log_row(fh, ts_iso: str, fr: bytes):
    fh.write(f"{ts_iso},{len(fr)},{binascii.hexlify(fr).decode()}\n")

def longest_float_run(
    b: bytes,
    step: int = 1,
    endian: Literal[">","<"] = ">",
    min_count: int = 6,
) -> tuple[Optional[int], list[float]]:
    """
    Sliding scan for the longest plausible run of 32-bit floats.
    step: advance per probe (1=every byte, 4=word aligned)
    endian: '>' big-endian, '<' little-endian
    min_count: only return if run meets threshold
    """
    best_off, best = None, []
    n = len(b)

    for off in range(0, max(0, n - 4), step):
        seq: list[float] = []
        i = off
        while i + 4 <= n:
            try:
                # tuple-unpack avoids PyCharm's indexing complaint
                (f,) = struct.unpack_from(f"{endian}f", b, i)
            except struct.error:
                break
            # Filter: finite-ish, avoid NaNs and wild values
            if (f != f) or abs(f) > 1e9:
                break
            seq.append(f)
            i += 4
        if len(seq) > len(best):
            best_off, best = off, seq

    if len(best) < min_count:
        return None, []
    return best_off, best

# ---------------------- Shared helpers to remove duplication ------------------

def _preview_batch(frames: list[bytes], batch_idx: int, total_frames: int) -> int:
    """Print the standard RX batch preview and return updated total_frames."""
    total_frames += len(frames)
    print(f"[RX batch {batch_idx}] +{len(frames)} frames (total {total_frames})")
    fr0 = frames[0]
    preview = binascii.hexlify(fr0[:16]).decode()
    print(f"   Frame {len(fr0):3d}B: {preview}...")
    return total_frames

def _log_and_classify_frames(
        frames: list[bytes],
        frame_fh,
        hex_fh,
        summary: Optional[dict] = None
):
    """Common logging + optional summary classification for probe phase."""
    for fr in frames:
        ts = datetime.now(timezone.utc).isoformat()
        hx = binascii.hexlify(fr).decode()
        if frame_fh:
            write_frame_log_row(frame_fh, ts, fr)
        if hex_fh:
            hex_fh.write(hx + "\n")
        if summary is not None:
            if hx.startswith("bdefff10010fff00010e00ddf0bd"):
                summary["hello"] += 1
            elif hx.startswith("bdaffd20010ffd000189050101fffd"):
                summary["reply_89"] += 1
            elif hx.startswith("bdaffd60010ffd000189050101fffd"):
                summary["reply_60"] += 1
            elif hx.startswith("bdaffd70010ffd00010a6"):
                summary["neighbor_70"] += 1
            else:
                summary["other"] += 1

def _write_raw_hex_once(hex_fh, raw: bytes):
    if hex_fh and raw:
        try:
            hex_fh.write(binascii.hexlify(raw).decode() + "\n")
        except Exception:
            pass

# ---------------------- Probe helpers (routered “replay”) --------------------

def run_probe_once(
        addr: str,
        port: int,
        tx_hex: str,
        *,
        hello: bool = False,
        hello_gap_ms: int = 100,
        reads_per_tx: int = 4,
        read_gap_ms: int = 400,
        tx_gap_ms: int = 3000,
        idle_timeout: float = 8.0,
        rx_frame_log: Optional[str] = None,
        rx_hex_log: Optional[str] = None,
) -> dict:
    """
    Sends hello (optional), then tx_hex once; listens in a small loop with idle gaps.
    Returns a summary: counts of short acks, neighbor-70, etc.
    """
    sock = open_tcp(addr, port)
    print(f"[CONNECT] [{addr}]:{port}")

    # open logs
    frame_fh = open(rx_frame_log, "a", buffering=1) if rx_frame_log else None
    hex_fh   = open(rx_hex_log, "a", buffering=1)   if rx_hex_log   else None
    now_iso  = datetime.now(timezone.utc).isoformat()

    if frame_fh:
        frame_fh.write(f"# --- run @ {now_iso} ---\n")
    if hex_fh:
        hex_fh.write(f"# --- run @ {now_iso} ---\n")

    try:
        if hello:
            hello_b = hex_to_bytes(HELLO_HEX)
            print(f"[TX hello] {len(hello_b)}B: {HELLO_HEX}")
            send_all(sock, hello_b)
            time.sleep(hello_gap_ms / 1000.0)
            raw, frames = recv_some(sock, idle_timeout=1.0)
            frames = frames or split_frames(raw)
            _log_and_classify_frames(frames, frame_fh, hex_fh)
            if any(fr == hello_b for fr in frames):
                print("[RX after hello] 14 B [hello]")

        tx = hex_to_bytes(tx_hex)
        print(f"[TX] {len(tx)}B raw-hex: {tx_hex}")
        send_all(sock, tx)

        summary = dict(reply_89=0, reply_60=0, neighbor_70=0, other=0, hello=0, timeout=0)
        total_frames = 0

        for i in range(1, reads_per_tx + 1):
            raw, frames = recv_some(sock, idle_timeout=idle_timeout)
            if not raw and not frames:
                summary["timeout"] += 1

            if frames:
                # Preview + log + classify
                total_frames = _preview_batch(frames, i, total_frames)
                _log_and_classify_frames(frames, frame_fh, hex_fh, summary)

            time.sleep(read_gap_ms / 1000.0)

        time.sleep(tx_gap_ms / 1000.0)
        return summary
    finally:
        sock.close()
        if frame_fh: frame_fh.close()
        if hex_fh:   hex_fh.close()
        print("[CLOSE] socket closed")

# ---------------------------- Pull & decode phase ----------------------------

def pull_and_decode(
        addr: str,
        port: int,
        *,
        tx_hex: str,
        hello: bool,
        reads: int,
        read_gap_ms: int,
        resend_every: float,
        idle_timeout: float,
        reply_sig_hex: str,
        seek_after_sig: int,
        scan_step: int,
        min_floats: int,
        endian: Literal[">", "<"],
        include_raw: bool,
        out_csv: str,
        rx_frame_log: Optional[str],
        rx_hex_log: Optional[str],
) -> Tuple[int, int]:
    """
    Sends hello (optional) + repeated TXs and collects frames.
    Looks for frames that contain reply_sig_hex, then scans after that offset
    for plausible float runs. Writes CSV of decoded rows.
    Returns (#rows_written, max_cols).
    """
    sock = open_tcp(addr, port)
    print(f"[CONNECT] [{addr}]:{port}")

    # logs
    frame_fh = open(rx_frame_log, "a", buffering=1) if rx_frame_log else None
    hex_fh   = open(rx_hex_log, "a", buffering=1)   if rx_hex_log   else None
    now_iso  = datetime.now(timezone.utc).isoformat()

    if frame_fh:
        frame_fh.write(f"# --- pull @ {now_iso} ---\n")
    if hex_fh:
        hex_fh.write(f"# --- pull @ {now_iso} ---\n")

    tx  = hex_to_bytes(tx_hex)
    sig = hex_to_bytes(reply_sig_hex)

    rows: List[Tuple[str, List[float], Optional[str]]] = []
    max_cols = 0

    try:
        if hello:
            hello_b = hex_to_bytes(HELLO_HEX)
            print(f"[TX hello] 14B")
            send_all(sock, hello_b)
            time.sleep(0.1)

        print(f"[TX] {len(tx)}B")
        send_all(sock, tx)
        last_tx = time.time()

        total_frames = 0
        idle_strikes = 0
        IDLE_LIMIT = 3  # stop early if we're repeatedly getting nothing

        for i in range(1, reads + 1):
            if (time.time() - last_tx) >= resend_every:
                print("[TX resend]")
                send_all(sock, tx)
                last_tx = time.time()

            raw, frames = recv_some(sock, idle_timeout=idle_timeout)

            _write_raw_hex_once(hex_fh, raw)

            if frames:
                total_frames = _preview_batch(frames, i, total_frames)
                idle_strikes = 0
            else:
                idle_strikes += 1
                if idle_strikes >= IDLE_LIMIT:
                    break
                time.sleep(read_gap_ms / 1000.0)
                continue

            # process frames
            for fr in frames:
                ts = datetime.now(timezone.utc).isoformat()
                hx = binascii.hexlify(fr).decode()

                if frame_fh:
                    write_frame_log_row(frame_fh, ts, fr)

                k = fr.find(sig)
                if k == -1:
                    continue
                start = k + seek_after_sig
                if start >= len(fr):
                    continue
                pay = fr[start:]

                off, floats = longest_float_run(
                    pay, step=scan_step, endian=endian, min_count=min_floats
                )
                if not floats:
                    continue

                max_cols = max(max_cols, len(floats))
                rows.append((ts, floats, hx if include_raw else None))

            time.sleep(read_gap_ms / 1000.0)

    finally:
        sock.close()
        if frame_fh: frame_fh.close()
        if hex_fh:   hex_fh.close()
        print("[CLOSE] socket closed")

    if not rows:
        print("[INFO] No decodable float runs found. Check rx_frames_log.csv for captured frames.")
        return 0, 0

    # Build headers
    headers = ["timestamp_utc"] + [f"col_{i+1}" for i in range(max_cols)]
    if include_raw:
        headers.append("raw_frame_hex")

    # Normalize row lengths & write CSV
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["# pulled_at_utc", datetime.now(timezone.utc).isoformat()])
        w.writerow(headers)
        for ts, vals, rawhex in rows:
            vals = list(vals) + [""] * (max_cols - len(vals))
            row = [ts] + vals
            if include_raw:
                row.append(rawhex or "")
            w.writerow(row)

    print(f"[SAVED] {len(rows)} decoded rows, {max_cols} cols → {out_csv}")
    if rx_frame_log:
        print(f"[NOTE] Frame log: {rx_frame_log}")
    if rx_hex_log:
        print(f"[NOTE] Raw hex log: {rx_hex_log}")
    return len(rows), max_cols

# ----------------------------------- main ------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Probe PakBus path (hello/acks/neighbors) and then pull long replies into CSV."
    )
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", required=True, type=int)
    ap.add_argument("--hello", action="store_true")

    # Probe knobs
    ap.add_argument("--probe-reads", type=int, default=4)
    ap.add_argument("--probe-read-gap-ms", type=int, default=400)
    ap.add_argument("--probe-tx-gap-ms", type=int, default=3000)
    ap.add_argument("--probe-idle-timeout", type=float, default=8.0)

    # Pull knobs
    ap.add_argument("--reads", type=int, default=360)
    ap.add_argument("--read-gap-ms", type=int, default=200)
    ap.add_argument("--resend-every", type=float, default=6.0)
    ap.add_argument("--idle-timeout", type=float, default=12.0)

    ap.add_argument("--out", required=True, help="CSV to write decoded rows")
    ap.add_argument("--rx-hex-log", default="rx_dump.hex")
    ap.add_argument("--rx-frame-log", default="rx_frames_log.csv")

    # TX, signature, and decode tuning
    ap.add_argument("--tx-hex", default=TX_PCAP_GOOD)
    ap.add_argument("--reply-sig", default=DEFAULT_LONG_SIG)
    ap.add_argument("--seek-after-sig", type=int, default=16,
                    help="Bytes to skip after signature before scanning for floats")
    ap.add_argument("--scan-step", type=int, default=1, choices=(1,2,4))
    ap.add_argument("--min-floats", type=int, default=6)
    ap.add_argument("--endian", default=">", choices=(">","<"))
    ap.add_argument("--include-raw", action="store_true")

    args = ap.parse_args()

    print(f"[START] {datetime.now(timezone.utc).isoformat()} addr={args.addr} port={args.port}")
    print(f"[LOG] frames → {args.rx_frame_log}")
    print(f"[LOG] hex    → {args.rx_hex_log}")

    # --- 1) Quick probe with the known-good TX shape (pcap-derived) ----------
    print("\n[probe] known-good routed (pcap shape)")
    probe_summary = run_probe_once(
        args.addr, args.port, args.tx_hex,
        hello=args.hello,
        reads_per_tx=args.probe_reads,
        read_gap_ms=args.probe_read_gap_ms,
        tx_gap_ms=args.probe_tx_gap_ms,
        idle_timeout=args.probe_idle_timeout,
        rx_frame_log=args.rx_frame_log,
        rx_hex_log=args.rx_hex_log,
    )
    print("\n[SUMMARY probe]")
    for k, v in probe_summary.items():
        print(f"  {k.replace('_','-'):12s}: {v}")

    # --- 2) Pull & decode into CSV ------------------------------------------
    print("\n[pull] scanning for long data frames and decoding floats")
    rows, cols = pull_and_decode(
        args.addr, args.port,
        tx_hex=args.tx_hex,
        hello=args.hello,
        reads=args.reads,
        read_gap_ms=args.read_gap_ms,
        resend_every=args.resend_every,
        idle_timeout=args.idle_timeout,
        reply_sig_hex=args.reply_sig,
        seek_after_sig=args.seek_after_sig,
        scan_step=args.scan_step,
        min_floats=args.min_floats,
        endian=args.endian,
        include_raw=args.include_raw,
        out_csv=args.out,
        rx_frame_log=args.rx_frame_log,
        rx_hex_log=args.rx_hex_log,
    )

    if rows == 0:
        print("[DONE] No decoded rows this pass. Use rx_frames_log.csv to refine signature/seek/endian].")

if __name__ == "__main__":
    main()