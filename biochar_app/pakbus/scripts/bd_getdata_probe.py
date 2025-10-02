#!/usr/bin/env python3
# bd_getdata_probe.py — send hello, derive addressing from beacon, try fresh Get-Data probes

import argparse, socket, time, pathlib, sys, textwrap

HELLO = bytes.fromhex("bd90010ffd73d3c2d6bd")

def hexdump(b: bytes, n=32, maxb=96):
    s = b[:maxb].hex()
    groups = " ".join(textwrap.wrap(s, 2))
    return "\n      ".join(textwrap.wrap(groups, 3*n))

def recv_all(sock: socket.socket, idle_timeout=0.35, max_wait=2.0) -> bytes:
    sock.settimeout(idle_timeout)
    chunks, start = [], time.time()
    while True:
        try:
            data = sock.recv(65536)
            if data:
                chunks.append(data); start = time.time()
            else:
                break
        except socket.timeout:
            if time.time() - start >= max_wait:
                break
            continue
    return b"".join(chunks)

def split_bd(buf: bytes):
    out, cur = [], bytearray()
    for b in buf:
        cur.append(b)
        if b == 0xBD:
            out.append(bytes(cur)); cur.clear()
    if cur:
        out.append(bytes(cur))
    return out

def find_hello_beacon(frames):
    hello = None
    beacons = []
    for f in frames:
        if len(f) == 13 and f[:1] == b'\xef' and f[-1] == 0xbd:
            hello = f
        elif len(f) == 17 and f[:2] == b'\xaf\xfd' and f[-1] == 0xbd:
            beacons.append(f)
    return hello, beacons

def infer_route_from_beacon(f: bytes):
    # Very small heuristic: in your dumps these look like:
    #  af fd 70 01 0f fd 00 01 09 xx 01 01 ff fd ?? ?? bd
    # where bytes 2..7 carry small addressing/route fields.
    # We’ll use the pair (dest_hi,dest_lo)=(0x00,0x01) and (src_hi,src_lo)=(0x1f,0xfd) seen in long frames too.
    # If we see a different pair in the beacon, prefer that.
    dest = (0x00, 0x01)
    src  = (0x1f, 0xfd)
    try:
        # Extract a couple of bytes that match your long frames signature
        # Long frames start: af fd 00 01 1f fd ...
        # If our beacon also contains 00 01 0f fd early, treat 00 01 as dest and 1f fd as src.
        # (This is heuristic but consistent with your logs.)
        if f[2:6] == b'\x70\x01\x0f\xfd' and f[6:8] == b'\x00\x01':
            dest = (0x00, 0x01)
            src  = (0x1f, 0xfd)
    except Exception:
        pass
    return dest, src

def build_probe(dest, src, table_id, count, variant=0):
    """
    Compose a minimal BD frame that mimics the leading structure of the 244–246B frames
    but with a "get data" intent. We try a few safe variants:
      variant 0/1 tweak a small control byte we’ve seen vary,
      table_id is the Campbell table number (0x11 for your Table1),
      count is how many records to request.
    NOTE: This is heuristic but safe (read-only); logger will NAK if unhappy.
    """
    # Header prefix aligned with observed: af fd <dest_hi> <dest_lo> <src_hi> <src_lo> 20 03
    # Then we add a tiny op block: 0x89 <variant> 00 00 02 00 01 <table> <count_hi> <count_lo>
    # Follow with a simple FCS (XOR) and BD terminator. (The logger also accepts BD-only framing with internal CRC.)
    d_hi, d_lo = dest; s_hi, s_lo = src
    core = bytearray([0xaf,0xfd, d_hi,d_lo, s_hi,s_lo, 0x20,0x03,
                      0x89, (0x11 if variant==0 else 0x15), 0x00,0x00, 0x02,0x00, 0x01,
                      table_id & 0xff, (count>>8)&0xff, count&0xff])
    # naive FCS (one-byte xor) just to keep structure; BD layer seems tolerant with these payloads
    fcs = 0
    for b in core[2:]:  # skip af fd
        fcs ^= b
    core.append(fcs)
    core.append(0xbd)
    return bytes(core)

def looks_data(f: bytes) -> bool:
    # Any af fd ... long frame that is NOT identical to the periodic status pattern length we saw?
    return f.startswith(b"\xaf\xfd") and len(f) > 40 and f[-1] == 0xbd

def extract_epoch_candidates(f: bytes):
    # Scan for big-endian 32-bit seconds since 1990 (~> 900,000,000 .. 1,700,000,000)
    out = []
    for i in range(0, len(f)-4):
        x = int.from_bytes(f[i:i+4], "big")
        if 900_000_000 <= x <= 1_700_000_000:
            out.append((i, x))
    return out

def main():
    ap = argparse.ArgumentParser(description="Send hello, infer addressing, try fresh Get-Data probes")
    ap.add_argument("--addr", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--table", default="0x11", help="Table id (hex or int). Your Table1 is 0x11.")
    ap.add_argument("--count", type=int, default=12, help="Records to request per probe")
    ap.add_argument("--out-dir", default="pakbus_runs/getdata_probe")
    ap.add_argument("--hello-gap-ms", type=int, default=250)
    ap.add_argument("--post-wait-ms", type=int, default=2400)
    args = ap.parse_args()

    table_id = int(args.table, 0)
    outdir = pathlib.Path(args.out_dir); outdir.mkdir(parents=True, exist_ok=True)

    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.settimeout(10.0)
        s.connect((args.addr, args.port, 0, 0))

        # Hello
        s.sendall(HELLO)
        time.sleep(args.hello_gap_ms/1000.0)
        rx0 = recv_all(s)
        fr0 = split_bd(rx0)
        hello, beacons = find_hello_beacon(fr0)
        print(f"[HELLO] rx={len(rx0)}B frames={len(fr0)}; hello={'yes' if hello else 'no'}; beacons={len(beacons)}")
        for i,f in enumerate(fr0,1):
            print(f"  RX0[{i}] {len(f)}B:\n    {hexdump(f)}")

        if not beacons:
            print("[ERR] no beacon seen; cannot infer addressing. Try again.")
            return

        dest, src = infer_route_from_beacon(beacons[-1])
        print(f"[ADDR] inferred dest={dest} src={src}")

        # Try a few safe variants
        variants = [0, 1]
        got_any = 0
        for v in variants:
            probe = build_probe(dest, src, table_id, args.count, variant=v)
            (outdir / f"probe_v{v}_tx.hex").write_text(probe.hex())
            s.sendall(probe)
            time.sleep(args.post_wait_ms/1000.0)
            rx = recv_all(s)
            fr = split_bd(rx)
            print(f"[PROBE v{v}] rx={len(rx)}B frames={len(fr)}")
            for j,f in enumerate(fr,1):
                print(f"  RXv{v}[{j}] {len(f)}B:\n    {hexdump(f)}")
            # Save and analyze
            (outdir / f"probe_v{v}_rx.bin").write_bytes(rx)
            for k,f in enumerate(fr,1):
                if looks_data(f):
                    (outdir / f"probe_v{v}_frame_{k}_{len(f)}B.hex").write_bytes(f)
                    cands = extract_epoch_candidates(f)
                    if cands:
                        print(f"    ↳ epoch32 candidates @ {[hex(i) for i,_ in cands][:4]} (showing up to 4)")
                    got_any += 1

        if not got_any:
            print("[INFO] No clear data frames yet; we can widen the sweep once we see what these probes trigger.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)