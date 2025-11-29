#!/usr/bin/env python3
"""
cr200_client.py
Fetch rows from a CR2xx leaf (PakBus) via a CR800 router (PakBus/TCP), using CollectData.

Improvements in this revision:
- Signature negotiation: try TDF signature, then 0x0000 (runtime) automatically.
- Robust mode fallback: MostRecent (0x04) → LastN (0x05) → Records (0x03).
- Graceful error handling: RC errors log + continue instead of raising.
- Optional freshness gate (--max-age-hours).
- Optional removal of RECORD column (--drop-record).

Outputs CSV to:
  biochar_app/pakbus/bdFiles/out_fetch/<SITE_OR_LEAF>_<TABLE>_decoded.csv

Notes:
- Quote IPv6 without []: '2605:59C0:...:1DDD'
- Ensure PC400 isn’t connected simultaneously (router busy).
"""

from __future__ import annotations

import sys
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timedelta, timezone
import argparse
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

# Local helpers
from . import cr200_client_utils as utils

OUT_DIR = Path("biochar_app/pakbus/bdFiles/out_fetch")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- small helpers ----------

def _iso_utc(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def normalize_host(host: str | None) -> str | None:
    """Accept hostnames, literal IPv6 (with/without brackets), or full URLs, return bare host."""
    if not host:
        return None
    h = host.strip()
    if "://" in h:
        parsed = urlparse(h)
        h = parsed.hostname or h.split("://", 1)[-1]
    if h.startswith("[") and h.endswith("]"):
        h = h[1:-1].strip()
    return h

def _decode_name(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("ascii", "ignore")
        except Exception:
            return repr(x)
    return str(x)

def _resolve_table_entry(tdefs: List[Dict[str, Any]], requested: str) -> Tuple[Dict[str, Any], str, int]:
    """
    Find a table entry whose decoded name matches requested (case-sensitive),
    return (entry, wire_name, signature).
    """
    # exact match first
    for t in tdefs:
        wire = t.get("Header", {}).get("TableName")
        name = _decode_name(wire)
        if name == requested:
            return t, name, int(t.get("Signature", 0)) & 0xFFFF
    # try case-insensitive
    for t in tdefs:
        wire = t.get("Header", {}).get("TableName")
        name = _decode_name(wire)
        if name.lower() == requested.lower():
            return t, name, int(t.get("Signature", 0)) & 0xFFFF
    raise RuntimeError(f"table {requested!r} not found in TDF")

def _flatten_records(recs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Accept a list of CollectData responses (dicts with RecFrag) and normalize to rows.
    """
    rows: List[Dict[str, Any]] = []
    field_order: List[str] = []
    for frag in recs:
        if not isinstance(frag, dict):
            continue
        for r in frag.get("RecFrag", []) or []:
            ts_nsec = r.get("TimeOfRec")
            ts = utils.nsec_to_time(ts_nsec) if isinstance(ts_nsec, tuple) else None
            fields: Dict[str, Any] = r.get("Fields", {}) or {}
            norm: Dict[str, Any] = {}
            for k, v in fields.items():
                key = _decode_name(k)
                if isinstance(v, (bytes, bytearray)):
                    try:
                        norm[key] = v.decode("ascii", "ignore")
                    except Exception:
                        norm[key] = repr(v)
                else:
                    norm[key] = v
            if not field_order:
                field_order = list(norm.keys())

            rec_no = r.get("RecNo") or r.get("RecNbr") or r.get("Record")
            row = {"TIMESTAMP": _iso_utc(ts), **norm}
            if rec_no is not None:
                try:
                    row["RECORD"] = int(rec_no)
                except Exception:
                    row["RECORD"] = rec_no
            rows.append(row)
    return rows, field_order

PARQUET_ORDER = [
    "TIMESTAMP", "RECORD",
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
]

def normalize_to_parquet_schema(df: pd.DataFrame, drop_record: bool = False) -> pd.DataFrame:
    if df.empty:
        out = pd.DataFrame(columns=PARQUET_ORDER)
    else:
        out = df.copy()
        # types/casts
        if "RECORD" in out.columns:
            with pd.option_context("future.no_silent_downcasting", True):
                out["RECORD"] = pd.to_numeric(out["RECORD"], errors="coerce").astype("Int64")
        # TIMESTAMP already iso; ensure coerce
        if "TIMESTAMP" in out.columns:
            ts = pd.to_datetime(out["TIMESTAMP"], errors="coerce", utc=True)
            out["TIMESTAMP"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        # float-ish columns (best-effort)
        for c in [
            "BattV_Min",
            "VWC_1_Avg","EC_1_Avg","T_1_Avg",
            "VWC_2_Avg","EC_2_Avg","T_2_Avg",
            "VWC_3_Avg","EC_3_Avg","T_3_Avg",
        ]:
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce")
        for c in PARQUET_ORDER:
            if c not in out.columns:
                out[c] = pd.NA
        out = out[PARQUET_ORDER]

    if drop_record and "RECORD" in out.columns:
        out = out.drop(columns=["RECORD"])

    return out

# ---------- core fetch ----------

def do_collect(
    *,
    sock,
    leaf: int,
    src: int,
    router: Optional[int],
    tdefs: List[Dict[str, Any]],
    table_name_req: str,
    num: int,
    collect_mode: str,
    tdef_sig_override: Optional[int],
    timeout: float,
    max_age_hours: Optional[float] = None,
    drop_record: bool = False,
) -> pd.DataFrame:
    """
    Perform CollectData with robust signature+mode fallback.
    Returns a normalized DataFrame (optionally filtered by max_age_hours).
    """
    entry, wire_name, tdf_sig = _resolve_table_entry(tdefs, table_name_req)

    # Build signature attempts: prefer explicit override if given, else TDF, then 0x0000
    sig_candidates: List[int] = []
    if tdef_sig_override is not None:
        sig_candidates.append(int(tdef_sig_override) & 0xFFFF)
    else:
        sig_candidates.append(int(tdf_sig) & 0xFFFF)
    if 0 not in sig_candidates:
        sig_candidates.append(0)

    def _collect_once(mode_code: int, p1, p2=0, sig_val: int = 0, note: str = "") -> List[Dict[str, Any]]:
        print(f"[DEBUG] CollectData call: DstNodeId={leaf} SrcNodeId={src} RouterPhyAddr={router} "
              f"Table={wire_name} sig=0x{sig_val:04X} mode=0x{mode_code:02X} P1={p1} P2={p2} {note}")
        try:
            resp = utils.collect_data(
                sock,
                DstNodeId=leaf,
                SrcNodeId=src,
                TableDef=tdefs,
                TableName=wire_name,
                FieldNames=(),
                CollectMode=mode_code,
                P1=p1, P2=p2,
                RouterPhyAddr=router,
                timeout=timeout,
                TableDefSigOverride=sig_val,
            )
        except Exception as e:
            print(f"[WARN] CollectData failed: {e}")
            return []
        # unify to list[dict]
        if isinstance(resp, dict):
            recs = [resp]
        elif isinstance(resp, (list, tuple)):
            recs = list(resp)
        else:
            recs = []
        rows, _ = _flatten_records(recs)
        return rows

    # ordered mode attempts per signature
    def _try_all(sig_val: int) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if collect_mode == "records":
            rows = _collect_once(0x03, int(num), 0, sig_val)
        elif collect_mode == "mostrecent":
            rows = _collect_once(0x04, int(num), 0, sig_val)
        elif collect_mode == "lastn":
            rows = _collect_once(0x05, int(num), 0, sig_val)
        elif collect_mode == "time":
            now = datetime.now(timezone.utc)
            start = now - timedelta(hours=24)
            p1 = utils.time_to_nsec(start.timestamp(), epoch=utils.nsec_base, tick=utils.nsec_tick)
            p2 = utils.time_to_nsec(now.timestamp(),   epoch=utils.nsec_base, tick=utils.nsec_tick)
            rows = _collect_once(0x07, p1, p2, sig_val)
        else:  # auto
            # 0x04 → 0x05 → 0x03 (ask for num recs; if 0x03, start unknown → just 1 record w/ p1=0xFFFFFFFF as “most recent” if supported by fw)
            rows = _collect_once(0x04, int(num), 0, sig_val, note="[auto 0x04]")
            if not rows:
                rows = _collect_once(0x05, int(num), 0, sig_val, note="[auto 0x05]")
            if not rows:
                # A few firmwares accept startRec=0xFFFFFFFF to mean "tail".
                rows = _collect_once(0x03, 0xFFFFFFFF, int(num), sig_val, note="[auto 0x03 tail]")
        return rows

    all_rows: List[Dict[str, Any]] = []
    for sig_val in sig_candidates:
        attempt = _try_all(sig_val)
        if attempt:
            all_rows = attempt
            break

    df = pd.DataFrame(all_rows)

    # Freshness gate (optional)
    if max_age_hours and not df.empty and "TIMESTAMP" in df.columns:
        now = datetime.now(timezone.utc)
        ts = pd.to_datetime(df["TIMESTAMP"], errors="coerce", utc=True)
        fresh = ts >= (now - timedelta(hours=float(max_age_hours)))
        before = len(df)
        df = df.loc[fresh].reset_index(drop=True)
        if before and df.empty:
            print(f"[DEBUG] All {before} rows rejected by freshness gate ({max_age_hours}h).")

    df = normalize_to_parquet_schema(df, drop_record=drop_record)
    return df

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Fetch rows from CR2xx leaf via CR800 PakBus/TCP")
    ap.add_argument("--host", required=True, help="CR800 host/IP (IPv6/IPv4/hostname; no [] needed)")
    ap.add_argument("--port", type=int, default=6785, help="PakBus/TCP port (default 6785)")
    ap.add_argument("--router", type=int, default=1, help="CR800 PakBus address (router). Default 1")
    ap.add_argument("--src", type=int, default=1, help="This client's PakBus address. Default 1")

    # leaf selection
    ap.add_argument("--leaf", type=int, help="Leaf PakBus address (CR2xx; e.g., 4 for S1B)")
    ap.add_argument("--site", help="Optional label used only for output filename")

    ap.add_argument("--table", default="Table1", help="Table name (Table1, Table2, Table3, Status, Public)")
    ap.add_argument("--num", type=int, default=1, help="How many records to fetch")

    # modes
    ap.add_argument("--collect-mode",
                    choices=["auto", "records", "mostrecent", "lastn", "time"],
                    default="auto",
                    help="Collection mode: auto (default), records(0x03), mostrecent(0x04), lastn(0x05), time(0x07 last 24h)")

    # table definitions source
    ap.add_argument("--tdf-file", required=True, help="Path to *.TDF created by CRBasic Editor / PC400")
    ap.add_argument("--tdef-sig", help="Override signature (decimal or hex like 0xF091)")

    # output labeling & options
    ap.add_argument("--site-label", help="Label for output filename; defaults to --site or 'leaf<id>'")
    ap.add_argument("--max-age-hours", type=float, help="Drop rows older than this many hours")
    ap.add_argument("--drop-record", action="store_true", help="Drop RECORD column from output")

    args = ap.parse_args()

    if args.leaf is None:
        print("[ERROR] --leaf is required (no site->leaf mapping here).", file=sys.stderr)
        sys.exit(2)

    host = normalize_host(args.host)
    port = int(args.port)
    router = int(args.router) if args.router is not None else None
    src = int(args.src)
    leaf = int(args.leaf)
    table = args.table

    # parse signature override
    tdef_sig_override: Optional[int] = None
    if args.tdef_sig:
        s = args.tdef_sig.strip().lower()
        tdef_sig_override = int(s, 16) if s.startswith("0x") else int(s)

    # load table definitions
    tdf_path = Path(args.tdf_file)
    if not tdf_path.exists():
        print(f"[ERROR] TDF file not found: {tdf_path}", file=sys.stderr)
        sys.exit(2)
    blob = tdf_path.read_bytes()
    tdefs = utils.parse_tabledef(blob)
    print(f"[INFO] Using tabledefs from {tdf_path} ({len(tdefs)} tables)")

    # connect
    s = utils.open_socket(host or "", Port=port, Timeout=15.0)
    if not s:
        print(f"[ERROR] Unable to open PakBus socket to {host}:{port}", file=sys.stderr)
        sys.exit(2)

    try:
        # warm the route (ignore failures)
        for dst, rphy in ((router, None), (leaf, router)):
            try:
                if dst is not None:
                    utils.ping_node(s, DstNodeId=dst, SrcNodeId=src, RouterPhyAddr=rphy, timeout=4.0)
            except Exception:
                pass

        df = do_collect(
            sock=s,
            leaf=leaf,
            src=src,
            router=router,
            tdefs=tdefs,
            table_name_req=table,
            num=args.num,
            collect_mode=args.collect_mode,
            tdef_sig_override=tdef_sig_override,
            timeout=12.0,
            max_age_hours=args.max_age_hours,
            drop_record=args.drop_record,
        )

        # write CSV
        label = args.site_label or (args.site if args.site else f"leaf{leaf}")
        out = OUT_DIR / f"{label}_{table}_decoded.csv"
        df.to_csv(out, index=False)
        print(f"wrote {out}  ({len(df)} rows)")
    finally:
        try:
            s.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()