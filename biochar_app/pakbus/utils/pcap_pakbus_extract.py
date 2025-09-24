# pcap_pakbus_extract.py
# Parse hex-framed PakBus lines (one line per TCP payload), save TDF blobs,
# and write decoded CollectData rows to CSV using cr200_client_utils.

from __future__ import annotations
import sys, csv, argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Your utils
from biochar_app.pakbus import cr200_client_utils as u

def iter_frames_from_line(hexline: str) -> List[bytes]:
    """
    From a single hex line, return a list of framed packets (each one starts and ends with 0xBD).
    Some lines may contain exactly one frame; we still scan in case.
    """
    s = "".join(hexline.strip().split())  # remove spaces/newlines
    if not s:
        return []
    try:
        buf = bytes.fromhex(s)
    except ValueError:
        return []

    frames: List[bytes] = []
    i = 0
    n = len(buf)
    FRAME = 0xBD
    while i < n:
        # find start
        while i < n and buf[i] != FRAME:
            i += 1
        if i >= n: break
        j = i + 1
        # find end
        while j < n and buf[j] != FRAME:
            j += 1
        if j >= n:
            # no closing frame on this line
            break
        frames.append(buf[i:j+1])  # include both 0xBD
        i = j + 1
    return frames

def unframe_and_check(frame: bytes) -> Optional[bytes]:
    """
    Strip 0xBD..0xBD, unquote 0xBC escapes, verify signature-nullifier (sum==0),
    and return the raw packet (without the final 2 nullifier bytes).
    """
    if len(frame) < 4 or frame[0] != 0xBD or frame[-1] != 0xBD:
        return None
    body = frame[1:-1]
    raw = u.unquote(body)
    # signature check (sum over all bytes should be 0 with nullifier included)
    if u.calc_sig_for(raw) != 0:
        return None
    # strip 2-byte nullifier
    if len(raw) < 2:
        return None
    return raw[:-2]

def parse_collect_req(raw: bytes) -> Optional[Dict]:
    """
    Parse a CollectData REQUEST (MsgType 0x09) for logging: mode, table, sig, etc.
    """
    try:
        # header is 8 bytes link + [MsgType, TranNbr, Security(2), Mode(1)]
        # then TableNbr(2), TableDefSig(2)
        # we don't care about field list & params for logging here
        # raw is the packet AFTER link header (we’ll slice later)
        (mtype, tran), off = u.decode_bin(("Byte","Byte"), raw)
        if mtype != 0x09:
            return None
        (sec, mode), off2 = u.decode_bin(("UInt2","Byte"), raw[off:])
        off += off2
        (tablenbr, sig), off3 = u.decode_bin(("UInt2","UInt2"), raw[off:])
        return {"TranNbr": tran, "Security": sec, "Mode": mode,
                "TableNbr": tablenbr, "TableSig": sig}
    except Exception:
        return None

def parse_collect_resp(msg: Dict) -> Tuple[int, bytes]:
    """
    Given a decoded message dict, return (rc, payload-bytes) for CollectData responses.
    """
    m = u.msg_collectdata_response(msg)
    rc = int(m.get("RespCode", 0xEE)) & 0xFF
    data = m.get("RecData", b"") or b""
    return rc, data

def iso_utc(nsec: Optional[Tuple[int,int]]) -> str:
    if not isinstance(nsec, tuple) or len(nsec) != 2:
        return ""
    try:
        ts = u.nsec_to_time(nsec)
        # format like Campbell .dat
        from datetime import datetime, timezone
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""

def write_csv(rows: List[Dict], outcsv: Path) -> None:
    # Collect field names in order of first appearance
    base = ["TIMESTAMP", "RECORD"]
    seen = set()
    dyn: List[str] = []
    for r in rows:
        for k in r.keys():
            if k in ("TableName","RecNbr","TimeOfRec"): continue
            if k not in seen:
                seen.add(k); dyn.append(k)
    cols = base + dyn
    outcsv.parent.mkdir(parents=True, exist_ok=True)
    with outcsv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            row = {
                "TIMESTAMP": iso_utc(r.get("TimeOfRec")),
                "RECORD": r.get("RecNbr"),
            }
            for k in dyn:
                row[k] = r.get(k)
            w.writerow(row)

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Decode PakBus hex payload lines (from tshark) into TDF + CSV."
    )
    ap.add_argument("hex_file", help="Path to tcp_payload_hex_*.txt produced by tshark")
    ap.add_argument("--outdir", default=None,
                    help="Output directory (default: alongside hex_file)")
    ap.add_argument("--tdf", default=None,
                    help="Fallback TDF file to use if no device TDF blob was captured")
    ap.add_argument("--ignore-sig", action="store_true",
                    help="Ignore signature mismatches by zeroing all table signatures in the TDF")
    ap.add_argument("--csv-out", default=None,
                    help="Optional single combined CSV path (in addition to per-table CSVs)")
    args = ap.parse_args()

    inpath = Path(args.hex_file)
    outdir = Path(args.outdir) if args.outdir else inpath.with_suffix("").parent
    outdir.mkdir(parents=True, exist_ok=True)

    # Collect artifacts as we stream the file
    tdf_blobs: List[bytes] = []
    last_tdf: Optional[bytes] = None

    # For CollectData responses, store payloads until we have a TDF to decode with
    collected_payloads: List[bytes] = []

    # Optional: remember most recent tabledef signature seen in requests (for logging)
    last_req_info: Dict[int, Dict] = {}  # tran -> info

    # Process each line → frames → packets
    # Robust to weird encodings: read as binary then splitlines and decode 'latin-1'.
    raw_bytes = inpath.read_bytes()
    for line in raw_bytes.splitlines():
        try:
            s = line.decode("utf-8", "strict")
        except UnicodeDecodeError:
            # tshark sometimes writes with CRLF / odd bytes; fall back gently
            s = line.decode("latin-1", "ignore")

        frames = iter_frames_from_line(s)
        for fr in frames:
            body = unframe_and_check(fr)
            if not body:
                continue

            # Decode link header + message tuple
            try:
                hdr, msg = u.decode_pkt(body)
            except Exception:
                continue
            if not msg:
                continue

            mtype = int(msg.get("MsgType", -1))
            tran  = int(msg.get("TranNbr", -1))
            raw   = msg.get("raw", b"")

            # BMP5 GetTableDefs response 0x17
            if mtype == 0x17 and len(raw) >= 3:
                rc = raw[2]
                blob = raw[3:]
                print(f"[RECV] GetTableDefs RC=0x{rc:02X}  blob={len(blob)} bytes")
                if rc == 0 and blob:
                    tdf_blobs.append(blob)
                    last_tdf = blob
                    idx = len(tdf_blobs)
                    out_tdf = outdir / f"device_tabledefs_{idx}.tdf"
                    out_tdf.write_bytes(blob)
                    print(f"       saved → {out_tdf}")

            # CollectData REQUEST 0x09 (for context logging)
            elif mtype == 0x09:
                info = parse_collect_req(raw)
                if info:
                    last_req_info[tran] = info
                    print(f"[SEND] CollectData mode=0x{info['Mode']:02X} table={info['TableNbr']} "
                          f"sig=0x{info['TableSig']:04X} sec=0x{info['Security']:04X}")

            # CollectData RESPONSE 0x89
            elif mtype == 0x89:
                rc, payload = parse_collect_resp(msg)
                print(f"[RECV] CollectData RC=0x{rc:02X}  payload={len(payload)} bytes")
                if rc == 0 and payload:
                    collected_payloads.append(payload)

            # (other message types ignored here)

    # --- Decide which TDF to use ---
    tdf_bytes: Optional[bytes] = None
    tdf_label = ""

    # If the user supplied a TDF, always use it (ignore captured blobs)
    if args.tdf:
        try:
            tdf_bytes = Path(args.tdf).read_bytes()
            tdf_label = f"fallback file: {args.tdf}"
            print(f"[INFO] Using fallback TDF: {args.tdf}")
        except Exception as e:
            print(f"[ERROR] Could not read --tdf {args.tdf}: {e}")
            return
    else:
        # No --tdf provided: only trust a captured blob if it looks big enough
        if last_tdf and len(last_tdf) >= 64:  # heuristic minimum for real TDFs
            tdf_bytes = last_tdf
            tdf_label = f"device-captured ({len(last_tdf)} bytes)"
            print(f"[INFO] Using device-captured TDF ({len(last_tdf)} bytes)")
        else:
            print("[WARN] No valid device TDF captured and no --tdf provided; cannot decode row payloads.")
            return

    # Parse TDF
    try:
        tdefs = u.parse_tabledef(tdf_bytes)
    except Exception as e:
        print(f"[ERROR] Failed to parse TDF ({tdf_label}): {e}")
        return

    # Optionally ignore signatures by zeroing them
    if args.ignore_sig:
        try:
            for t in tdefs:
                if "Signature" in t:
                    t["Signature"] = 0
            print("[INFO] --ignore-sig active: zeroed all TDF table signatures.")
        except Exception as e:
            print(f"[WARN] Could not zero signatures: {e}")

    # Decode all CollectData payloads we saved
    all_rows_by_table: Dict[str, List[Dict]] = {}
    total_rows = 0

    for payload in collected_payloads:
        try:
            rec_frags, more = u.parse_collectdata(payload, tdefs, ())
        except Exception as e:
            # If the user asked to ignore signatures, report once more clearly
            if args.ignore_sig:
                print(f"[WARN] Failed to parse a CollectData payload ({len(payload)} bytes) "
                      f"even with --ignore-sig: {e}")
            else:
                print(f"[WARN] Failed to parse a CollectData payload ({len(payload)} bytes): {e}")
            continue

        # flatten into row dicts
        flat = u.flatten_records(rec_frags)
        total_rows += len(flat)
        # group by table name
        for r in flat:
            tname = str(r.get("TableName") or "Table?")
            all_rows_by_table.setdefault(tname, []).append(r)

    if total_rows == 0:
        print("[INFO] No rows decoded from captured payloads.")
        return

    # Write one CSV per table
    out_csv_dir = outdir / "csv"
    out_csv_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Per-table CSVs will be written under: {out_csv_dir}")

    for tname, rows in all_rows_by_table.items():
        safe = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in tname)
        outcsv = out_csv_dir / f"{safe}.csv"
        try:
            write_csv(rows, outcsv)
            print(f"[OK] wrote {outcsv}  ({len(rows)} rows)")
        except Exception as e:
            print(f"[ERROR] writing {outcsv}: {e}")

    # Optionally also write a single combined CSV
    if args.csv_out:
        try:
            # Union of all fieldnames across tables; TIMESTAMP/RECORD first (if present)
            all_rows: List[Dict] = []
            all_keys: List[str] = []
            seen = set()

            for tname, rows in all_rows_by_table.items():
                for r in rows:
                    rr = dict(r)  # shallow copy
                    rr.setdefault("TableName", tname)
                    all_rows.append(rr)
                    for k in rr.keys():
                        if k not in seen:
                            seen.add(k); all_keys.append(k)

            # keep a friendly ordering
            ordered = []
            for k in ("TIMESTAMP", "RECORD", "TableName"):
                if k in seen: ordered.append(k)
            for k in all_keys:
                if k not in ordered:
                    ordered.append(k)

            out_combined = Path(args.csv_out)
            out_combined.parent.mkdir(parents=True, exist_ok=True)
            with out_combined.open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=ordered)
                w.writeheader()
                for r in all_rows:
                    # normalize timestamp field name if present in rows as TimeOfRec
                    rr = dict(r)
                    if "TimeOfRec" in rr and "TIMESTAMP" in ordered:
                        rr.setdefault("TIMESTAMP", iso_utc(rr.get("TimeOfRec")))
                    w.writerow(rr)

            print(f"[OK] wrote combined CSV → {out_combined}  ({len(all_rows)} rows)")
        except Exception as e:
            print(f"[ERROR] writing combined CSV '{args.csv_out}': {e}")

    print(f"[DONE] decoded {total_rows} rows across {len(all_rows_by_table)} table(s).")

if __name__ == "__main__":
    main()