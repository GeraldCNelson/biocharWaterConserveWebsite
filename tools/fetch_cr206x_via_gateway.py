#!/usr/bin/env python3
"""
Generic CR206X fetch via CR800 PakBus/TCP router.

- Loads routing + defaults from config/sites.yaml (by --site).
- Fetches last-N records from <table> using PakBus Collect*.
- Verifies:
  (1) Program signature via Public.ProgramSignature (or ProgSig)
  (2) Table definition hash vs a baseline in _trust_logs/
- Appends to: biochar_app/pakbus/bdFiles/out_fetch/<SITE>_<TABLE>_decoded.csv
"""

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

# ---- Paths ----
OUT_DIR = Path("biochar_app/pakbus/bdFiles/out_fetch")
TRUST_DIR = OUT_DIR / "_trust_logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
TRUST_DIR.mkdir(parents=True, exist_ok=True)

# ---- pypakbus imports ----
try:
    # NOTE: adjust these imports if your pypakbus layout differs
    from pypakbus.transport.tcp import TcpTransport
    from pypakbus.link import Link
    from pypakbus.node import Node
    from pypakbus.messages.get_table_def import GetTableDef
    from pypakbus.messages.collect_data import CollectData, CollectRecord
except Exception as e:
    print(f"[ERROR] pypakbus not available: {e}", file=sys.stderr)
    sys.exit(2)


# ---------- helpers ----------

def load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def compute_tdef_hash(tdef) -> str:
    """
    Build a stable hash from table definition (field name/type/size/order).
    """
    items = []
    for f in tdef.fields:
        items.append((f.name, getattr(f, "data_type", None), getattr(f, "size", None)))
    payload = json.dumps({"table": tdef.name, "fields": items}, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def iso_utc(ts) -> str:
    if isinstance(ts, (int, float)):
        try:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            dt = datetime.now(timezone.utc)
    elif isinstance(ts, datetime):
        dt = ts.astimezone(timezone.utc)
    else:
        try:
            dt = pd.to_datetime(ts, utc=True).to_pydatetime()
        except Exception:
            dt = datetime.now(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def read_public_signature(link: Link, leaf: Node, expect_field_names=("ProgramSignature", "ProgSig")) -> str | None:
    """
    Try to read signature from Public table.
    """
    try:
        gtd = GetTableDef(node=leaf, table_name="Public")
        rsp = link.send_and_wait(gtd, timeout=6.0)
        if rsp and rsp.tables:
            fields = [f.name for f in rsp.tables[0].fields]
            target = next((n for n in expect_field_names if n in fields), None)
            if not target:
                return None
            # Fetch newest record from Public
            rec_rsp = link.send_and_wait(CollectRecord(node=leaf, table_name="Public", recno=-1), timeout=6.0)
            if not rec_rsp or getattr(rec_rsp, "data", None) is None:
                return None
            row = rec_rsp.data
            # Normalize to dict
            if isinstance(row, dict):
                return str(row.get(target))
            # else tuple/seq in table field order
            vals = list(row)
            idx = fields.index(target)
            return str(vals[idx])
    except Exception:
        return None
    return None


def append_dedupe_csv(out_path: Path, df_new: pd.DataFrame, key="TimestampUTC") -> int:
    """
    Append df_new to out_path and drop duplicates by key.
    Returns total rows after write.
    """
    if out_path.exists():
        try:
            df_old = pd.read_csv(out_path)
        except Exception:
            df_old = pd.DataFrame(columns=df_new.columns)
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=[key], keep="last").sort_values(key).reset_index(drop=True)
    else:
        df = df_new.drop_duplicates(subset=[key], keep="last").sort_values(key).reset_index(drop=True)
    df.to_csv(out_path, index=False)
    return len(df)


# ---------- core fetch ----------

def fetch_last_n(host: str, port: int, base_addr: int, leaf_addr: int, table: str, n: int, *,
                 expect_prog_sig: str | None,
                 site: str,
                 timeout: float = 6.0) -> pd.DataFrame:
    """
    Connect TCP->CR800 (base) and route to CR206X (leaf). Use CollectData/CollectRecord.
    Also performs the two verifications (program signature + table hash).
    """
    transport = TcpTransport(host=host, port=port, timeout=timeout)
    link = Link(transport)
    base = Node(address=base_addr, link=link, is_router=True)
    leaf = Node(address=leaf_addr, link=link, parent=base)  # routed via base

    link.start()
    try:
        # --- Verification 1: program signature ---
        actual_sig = read_public_signature(link, leaf)
        if expect_prog_sig:
            if actual_sig is None:
                print(f"[WARN] {site}: Public.ProgramSignature not readable; expected '{expect_prog_sig}'.", file=sys.stderr)
            elif str(actual_sig) != str(expect_prog_sig):
                print(f"[WARN] {site}: ProgramSignature mismatch: device='{actual_sig}' expected='{expect_prog_sig}'.", file=sys.stderr)

        # --- Get table definition ---
        gtd = GetTableDef(node=leaf, table_name=table)
        rsp = link.send_and_wait(gtd, timeout=timeout)
        if rsp is None or not getattr(rsp, "tables", None):
            raise RuntimeError(f"Unable to get table definition for {table} (leaf {leaf_addr})")

        tdef = rsp.tables[0]
        fields = [f.name for f in tdef.fields]

        # --- Verification 2: table hash ---
        tdef_hash = compute_tdef_hash(tdef)
        hash_file = TRUST_DIR / f"{site}_{table}.tdef.sha1"
        if hash_file.exists():
            known = hash_file.read_text().strip()
            if known != tdef_hash:
                print(f"[WARN] {site}: TableDef hash changed! known={known[:8]}.. new={tdef_hash[:8]}..  (schema drift?)",
                      file=sys.stderr)
        else:
            # establish baseline
            hash_file.write_text(tdef_hash + "\n")

        # --- Collect last-N records (Prefer CollectData) ---
        rows = []
        try:
            req = CollectData(node=leaf, table_name=table, start_recno=-n, records=n)
            data_rsp = link.send_and_wait(req, timeout=timeout)
            if data_rsp and getattr(data_rsp, "data", None):
                rows = data_rsp.data
        except Exception:
            rows = []

        if not rows:
            # fallback: CollectRecord newest, loop
            tmp = []
            for _ in range(n):
                cr = CollectRecord(node=leaf, table_name=table, recno=-1)
                r = link.send_and_wait(cr, timeout=timeout)
                if r is None or getattr(r, "data", None) is None:
                    break
                tmp.append(r.data)
                time.sleep(0.05)
            rows = tmp[::-1]

        if not rows:
            return pd.DataFrame(columns=["TimestampUTC"] + fields)

        # Normalize to DF
        recs = []
        for row in rows:
            if isinstance(row, dict):
                ts = row.get("time") or row.get("Timestamp") or row.get("timestamp")
                vals = [row.get(k) for k in fields]
            else:
                seq = list(row)
                ts = seq[0]
                vals = seq[1:1 + len(fields)]
            recs.append({"TimestampUTC": iso_utc(ts), **dict(zip(fields, vals))})

        df = pd.DataFrame.from_records(recs)
        df = df.drop_duplicates(subset=["TimestampUTC"], keep="last").sort_values("TimestampUTC").reset_index(drop=True)
        return df

    finally:
        link.stop()


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Fetch last-N rows from CR206X leaf via CR800 PakBus/TCP (generic)")
    ap.add_argument("--site", required=True, help="Site label from config/sites.yaml (e.g., S3B)")
    ap.add_argument("--config", default="config/sites.yaml", help="Path to YAML config")
    ap.add_argument("--table", default="Table1", help="Table name (default Table1)")
    ap.add_argument("--num", type=int, default=200, help="How many recent records to fetch")
    # Optional overrides (rarely needed)
    ap.add_argument("--host", help="Override CR800 host/IP")
    ap.add_argument("--port", type=int, help="Override CR800 PakBus/TCP port")
    ap.add_argument("--base", type=int, help="Override CR800 PakBus address")
    ap.add_argument("--leaf", type=int, help="Override leaf PakBus address (CR206X)")
    args = ap.parse_args()

    cfg = load_cfg(Path(args.config))
    defaults = cfg.get("defaults", {})
    gw = cfg.get("gateway", {})

    if args.leaf is None or args.host is None or args.base is None or args.port is None:
        # populate from config by --site
        leaves = {s["name"]: s for s in cfg.get("leaves", [])}
        if args.site not in leaves:
            print(f"[ERROR] site '{args.site}' not found in {args.config}", file=sys.stderr)
            sys.exit(2)
        site_row = leaves[args.site]
        host = args.host or gw.get("host")
        port = args.port or int(gw.get("port", 6785))
        base = args.base or int(gw.get("pakbus_id", 1))
        leaf = args.leaf or int(site_row["pakbus_id"])
    else:
        host, port, base, leaf = args.host, args.port, args.base, args.leaf

    expect_prog_sig = defaults.get("program_signature")

    df = fetch_last_n(
        host=host,
        port=port or 6785,
        base_addr=base or 1,
        leaf_addr=leaf,
        table=args.table or defaults.get("table", "Table1"),
        n=args.num,
        expect_prog_sig=expect_prog_sig,
        site=args.site,
    )

    out = OUT_DIR / f"{args.site}_{args.table}_decoded.csv"
    total = append_dedupe_csv(out, df)
    print(f"wrote {out}  (+{len(df)} new, total {total})")


if __name__ == "__main__":
    main()