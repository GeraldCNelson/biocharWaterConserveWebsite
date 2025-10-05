#!/usr/bin/env python3
import argparse, binascii, socket, time

HELLO = bytes.fromhex("bdefff10010fff00010e00ddf0bd")

def build_getdata_frame(our_id, dest_id, table, count, start_key):
    # PakBus-ish skeleton used in your working frames:
    # bd <our-id> <dest-id> 6f fd 00 01 0f fd 09 <table> <count> 02 <start_key> bd
    # We maintain the exact working layout/lengths you’ve been sending.
    hdr = bytes([0xbd, our_id, dest_id, 0x6f, 0xfd, 0x00, 0x01, 0x0f, 0xfd, 0x09])
    body = bytes([table, count, 0x02]) + start_key.to_bytes(4, "big")
    return hdr + body + b"\xbd"

def parse_pkt(pkt: bytes):
    """Return (label, detail) for a received logger packet."""
    if pkt == HELLO:
        return "hello", ""
    if pkt.startswith(b"\xbd\xaf\xfd"):
        # Known short ACK we care about: contains 0x89 0x05 and is typically 18B
        if b"\x89\x05" in pkt and len(pkt) == 18:
            return "reply-89", ""
        # Common neighbor/topology advert: starts with ... 0x70 ...
        if len(pkt) >= 4 and pkt[3] == 0x70:
            return "neighbor-70", ""
        # Everything else that looks like a PakBus payload from logger
        return "data-ish", ""
    # If it begins with BD and ends with BD, also consider data-ish
    if len(pkt) >= 2 and pkt[0] == 0xbd and pkt[-1] == 0xbd:
        return "data-ish", ""
    return "other", ""

def recv_with_gaps(sock, n_reads, gap_ms, rx_limit, suppress_neighbor=False, prefix=""):
    """Read up to n_reads packets, spaced by gap_ms, classify & print; return counts and if reply-89 was seen."""
    counts = {"hello":0, "reply-89":0, "neighbor-70":0, "data-ish":0, "other":0, "timeout":0}
    saw_reply_89 = False
    for i in range(1, n_reads + 1):
        try:
            rx = sock.recv(rx_limit)
        except socket.timeout:
            counts["timeout"] += 1
            print(f"[RX {prefix}{i}] timeout")
            time.sleep(gap_ms/1000.0)
            continue

        label, _ = parse_pkt(rx)
        counts[label] += 1
        hex_str = binascii.hexlify(rx).decode()
        # logging policy
        if label == "neighbor-70" and suppress_neighbor:
            print(f"[RX {prefix}{i}] {len(rx):d}B [neighbor-70] (suppressed)")
        else:
            print(f"[RX {prefix}{i}] {len(rx):d}B [{label}]")
            print(hex_str)

        if label == "reply-89":
            saw_reply_89 = True

        time.sleep(gap_ms/1000.0)

    return counts, saw_reply_89

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True, help="IPv6/IPv4 address")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--hello", action="store_true")
    ap.add_argument("--hello-gap-ms", type=int, default=120, help="gap after hello")
    ap.add_argument("--connect-timeout", type=float, default=3.0)
    ap.add_argument("--idle-timeout", type=float, default=6.0)

    ap.add_argument("--our-id", type=lambda x:int(x,0), required=True)
    ap.add_argument("--dest-id", type=lambda x:int(x,0), required=True)
    ap.add_argument("--table", type=lambda x:int(x,0), required=True)
    ap.add_argument("--count", type=lambda x:int(x,0), required=True)
    ap.add_argument("--start-key", type=lambda s:int(s,16), required=True)

    ap.add_argument("--reads-per-tx", type=int, default=20)
    ap.add_argument("--read-gap-ms", type=int, default=500)
    ap.add_argument("--rx-limit", type=int, default=2048)

    # NEW knobs:
    ap.add_argument("--auto-followups", type=int, default=2, help="extra polls after first reply-89")
    ap.add_argument("--followup-gap-ms", type=int, default=300)
    ap.add_argument("--suppress-neighbor", action="store_true")

    args = ap.parse_args()

    frame = build_getdata_frame(args.our_id, args.dest_id, args.table, args.count, args.start_key)
    print(f"[TX frame] {len(frame):d}B:", binascii.hexlify(frame).decode())

    # Connect
    print(f"[CONNECT] [{args.addr}]:{args.port}")
    sock = socket.socket(socket.AF_INET6 if ":" in args.addr else socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(args.connect_timeout)
    sock.connect((args.addr, args.port))
    sock.settimeout(args.idle_timeout)

    try:
        # Optional hello (keeps consistent with your capture)
        if args.hello:
            print(f"[TX hello] {len(HELLO)}B:", binascii.hexlify(HELLO).decode())
            sock.sendall(HELLO)
            try:
                rx = sock.recv(args.rx_limit)
                label, _ = parse_pkt(rx)
                print(f"[RX after hello] {len(rx):2d} B [{label}]")
                print(binascii.hexlify(rx).decode())
            except socket.timeout:
                print("[RX after hello] timeout")
            time.sleep(args.hello_gap_ms/1000.0)

        # First poll
        print(f"[TX 1] {len(frame)}B:", binascii.hexlify(frame).decode())
        sock.sendall(frame)
        totals = {"hello":0, "reply-89":0, "neighbor-70":0, "data-ish":0, "other":0, "timeout":0}

        counts, saw = recv_with_gaps(
            sock,
            args.reads_per_tx,
            args.read_gap_ms,
            args.rx_limit,
            suppress_neighbor=args.suppress_neighbor,
            prefix="1."
        )
        for k,v in counts.items(): totals[k] = totals.get(k,0)+v

        # Auto follow-ups: resend the same getdata if we saw a short ACK (reply-89)
        for fidx in range(2, args.auto_followups + 2):  # e.g., 2 and 3 when auto-followups=2
            if not saw:
                break
            time.sleep(args.followup_gap_ms/1000.0)
            print(f"[TX {fidx}] {len(frame)}B:", binascii.hexlify(frame).decode())
            sock.sendall(frame)
            counts, saw_more = recv_with_gaps(
                sock,
                max(8, args.reads_per_tx//3),  # shorter window for follow-ups
                args.read_gap_ms,
                args.rx_limit,
                suppress_neighbor=args.suppress_neighbor,
                prefix=f"{fidx}."
            )
            for k,v in counts.items(): totals[k] = totals.get(k,0)+v
            saw = saw or saw_more  # keep going if we see additional reply-89 (rare but possible)

        # Summary
        print("\n[SUMMARY]")
        for k in ("reply-89","neighbor-70","data-ish","hello","other","timeout"):
            print(f"  {k:12s}: {totals.get(k,0)}")

    finally:
        try:
            sock.close()
        except:
            pass
        print("[CLOSE] socket closed")

if __name__ == "__main__":
    main()