#!/usr/bin/env python3
import socket, time, sys, binascii, argparse, math
from datetime import datetime, timezone, timedelta

# ---- import your BD request dictionary ----
try:
    from reqs import REQ_BY_STATION
except Exception as e:
    print("FATAL: could not import REQ_BY_STATION from reqs.py:", e)
    sys.exit(1)

# ---- your CR800/CR200X host:port (what you already use) ----
try:
    from biochar_app.scripts.config import PAKBUS
    HOST, PORT = PAKBUS.host, PAKBUS.port
except Exception as e:
    print("FATAL: could not import PAKBUS host/port:", e)
    print("Tip: ensure biochar_app.scripts.config.PAKBUS has host/port")
    sys.exit(1)

FIELDS = ["BattV_Min",
          "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
          "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
          "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"]

def hex_to_bytes(h: str) -> bytes:
    h = h.strip().replace(" ", "")
    if not h:
        return b""
    # Allow with/without leading 'bd' and trailing 'bd'
    if not h.lower().startswith("bd"):
        h = "bd" + h
    if not h.lower().endswith("bd"):
        h = h + "bd"
    return bytes.fromhex(h)

def deframe_all(buf: bytes):
    """Return inner (deframed) messages between 0xBD ... 0xBD."""
    frames = []
    cur = bytearray()
    in_frame = False
    for b in buf:
        if b == 0xBD:
            if in_frame and cur:
                frames.append(bytes(cur))
            cur = bytearray()
            in_frame = not in_frame
            continue
        if in_frame:
            cur.append(b)
    return frames

def plausible_row(vals):
    """Very light plausibility filter for Table1 (adjust as needed)."""
    if len(vals) != 10:
        return False
    batt, v1,e1,t1, v2,e2,t2, v3,e3,t3 = vals
    def ok(x): return (not math.isnan(x)) and (not math.isinf(x)) and abs(x) < 1e6
    if not all(ok(x) for x in vals): return False
    if not (9.0 <= batt <= 15.5): return False
    # soil moisture fractions typically 0..1 (allow slight over)
    if not (0.0 <= v1 <= 1.5 and 0.0 <= v2 <= 1.5 and 0.0 <= v3 <= 1.5): return False
    # EC, be generous
    if not (0.0 <= e1 <= 5.0 and 0.0 <= e2 <= 5.0 and 0.0 <= e3 <= 5.0): return False
    # temps
    for t in (t1,t2,t3):
        if not (-30.0 <= t <= 60.0):
            return False
    return True

def try_decode_10floats(payload: bytes):
    """
    Brute-force a small set of alignments:
      start_skip in [0..12]
      per-row stride = 40 bytes (10 floats)
      endianness: big & little
    Return the first plausible row list or None.
    """
    import struct
    N = 10
    for endian in (">","<"):  # CR200X often big-endian, but we’ve seen LE in your captures too
        for start in range(0, 13):
            if len(payload) < start + 40:  # need at least one 10-float row
                continue
            try:
                vals = list(struct.unpack(endian + "10f", payload[start:start+40]))
            except Exception:
                continue
            if plausible_row(vals):
                return vals, ("BE" if endian==">" else "LE"), start
    return None, None, None

def summarize_row(vals):
    batt, v1,e1,t1, v2,e2,t2, v3,e3,t3 = vals
    return (f"BattV={batt:.3f}  "
            f"VWC1={v1:.3f} EC1={e1:.3f} T1={t1:.2f}   "
            f"VWC2={v2:.3f} EC2={e2:.3f} T2={t2:.2f}   "
            f"VWC3={v3:.3f} EC3={e3:.3f} T3={t3:.2f}")

def send_and_read(req_bytes: bytes, timeout=6.0):
    """Send one BD…BD request, return bytes of whole TCP read window."""
    s = socket.socket(socket.AF_INET6 if ":" in HOST else socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(6)
    s.connect((HOST, PORT, 0, 0) if ":" in HOST else (HOST, PORT))
    try:
        s.sendall(req_bytes)
        s.settimeout(0.5)
        end = time.time() + timeout
        buf = bytearray()
        while time.time() < end:
            try:
                chunk = s.recv(4096)
                if not chunk: break
                buf.extend(chunk)
            except socket.timeout:
                pass
        return bytes(buf)
    finally:
        try: s.close()
        except: pass

def handle_station(name, req27_hex, req31_hex, verbose=False):
    print(f"\n== {name} ==")
    # Prefer the 31-byte “history” request if present; otherwise use 27-byte one
    choices = []
    if req31_hex: choices.append(("31", req31_hex))
    if req27_hex: choices.append(("27", req27_hex))
    if not choices:
        print("  (no requests found)")
        return

    for tag, hx in choices:
        req = hex_to_bytes(hx)
        print(f"\n  sending {tag}-byte request: {len(req)} bytes")
        rx = send_and_read(req)
        frames = deframe_all(rx)
        if verbose:
            print(f"    rx={len(rx)} bytes, frames={len(frames)}")

        # find 0x89 responses
        got = None
        for inner in frames:
            payload = inner[8:] if len(inner) > 8 else b""
            if not payload:
                continue
            if payload[0] == 0x89:  # CollectData response
                rc = payload[2] if len(payload) >= 3 else None
                body = payload[3:] if len(payload) > 3 else b""
                if rc == 0 and body:
                    got = body
                    break

        if not got:
            print("    no rc=0 CollectData body found")
            continue

        # Some logs include a 2-byte “table hint” prefix; try both
        candidates = [got]
        if len(got) >= 2:
            candidates.append(got[2:])

        decoded = None
        for cand in candidates:
            vals, endian, start = try_decode_10floats(cand)
            if vals:
                decoded = (vals, endian, start, len(cand))
                break

        if not decoded:
            print("    could not decode a plausible 10-float row from body")
            continue

        vals, endian, start, blen = decoded
        print(f"    decoded: endian={endian} start_skip={start} body_len={blen}")
        print("    00  " + summarize_row(vals))
        return  # stop after first success

    print("  (no request produced a plausible row)")

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Replay BD…BD CollectData requests from reqs.py and print a decoded row.")
    ap.add_argument("--all", action="store_true", help="run all stations in reqs.py")
    ap.add_argument("--stations", nargs="*", help="subset of station names to run")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    # Default to all if no selection provided
    if not args.all and not args.stations:
        print("[info] no --all/--stations provided; defaulting to all stations in reqs.py")
        args.all = True

    if args.all:
        todo = list(REQ_BY_STATION.keys())
    else:
        todo = args.stations

    print(f"[info] host={HOST} port={PORT}")
    print(f"[info] running stations: {', '.join(todo)}")

    for name in todo:
        obj = REQ_BY_STATION.get(name)
        if not obj:
            print(f"\n== {name} ==\n  (not found in reqs.py)")
            continue
        req27 = obj.get("27", "")
        req31 = obj.get("31", "")
        handle_station(name, req27, req31, verbose=args.verbose)

if __name__ == "__main__":
    main()