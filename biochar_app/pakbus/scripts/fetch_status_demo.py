from pathlib import Path
from datetime import datetime, timezone
from typing import cast
from biochar_app.pakbus import cr200_client_utils as u

HOST="2605:59C0:30F3:2500:2D0:2CFF:FE02:1DDD"
PORT=6785
ROUTER=1
SRC=4094
LEAF=2

TDF = "biochar_app/pakbus/CSU_3depths.tdf"
TABLE_NAME = "Status"          # <-- plain name (not b'\x01Status')

# Load the ENTIRE table-def list
tdefs = u.parse_tabledef(Path(TDF).read_bytes())

# (optional) find and print the signature we’ll use for sanity
sig = None
for td in tdefs:
    nm = td["Header"]["TableName"]
    nm = nm.decode("ascii", "ignore") if isinstance(nm, (bytes, bytearray)) else nm
    if nm.lstrip("\x01") == TABLE_NAME:
        sig = int(td["Signature"]) & 0xFFFF
        break
print(f"Using table: {TABLE_NAME!r}  sig=0x{sig:04X}" if sig is not None else "Signature not found")

s = u.open_socket(HOST, Port=PORT, Timeout=12.0)
try:
    # keep the route warm
    u.ping_node(s, DstNodeId=LEAF, SrcNodeId=SRC, RouterPhyAddr=ROUTER, timeout=4)

    # IMPORTANT: pass the full list (tdefs) and the PLAIN table name
    recs, _ = u.collect_data(
        s,
        DstNodeId=LEAF, SrcNodeId=SRC,
        TableDef=tdefs,                     # <-- list, not a single dict
        TableName=TABLE_NAME,               # <-- "Status" (no 0x01 prefix)
        FieldNames=(),                      # all fields
        CollectMode=0x04,                   # MostRecent (last-N)
        P1=5,                               # last 5 records
        RouterPhyAddr=ROUTER,
        timeout=8.0,
        TableDefSigOverride=sig             # ok to be None; include if found
    )

    rows = []
    for frag in recs:
        for r in frag.get("RecFrag", []):
            ts_iso = None
            ts = r.get("TimeOfRec")
            if isinstance(ts, tuple) and len(ts) == 2:
                t = u.nsec_to_time(cast("tuple[int,int]", ts))
                ts_iso = datetime.fromtimestamp(t, tz=timezone.utc).isoformat().replace("+00:00","Z")
            row = {"TIMESTAMP": ts_iso}
            for k, v in r.get("Fields", {}).items():
                k = k.decode() if isinstance(k, (bytes, bytearray)) else k
                row[k] = v
            if "RecNbr" in r:
                row["RECORD"] = r["RecNbr"]
            rows.append(row)

    print(f"rows: {len(rows)}")
    if rows:
        for r in rows[:3]:
            print(r)
finally:
    try: s and s.close()
    except: pass