#!/usr/bin/env python3
import argparse, sys, struct
import pandas as pd
from pathlib import Path

FIELDS = [
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
]

def try_unpack_10_be_floats(blob: bytes, start: int) -> tuple[float, ...] | None:
    """Try to unpack 10 big-endian float32 starting at offset start.
       Returns tuple if looks sane, else None."""
    end = start + 40
    if end > len(blob): return None
    vals = struct.unpack(">10f", blob[start:end])
    # sanity ranges (broad): BattV 8..16, VWC 0..1.2, EC 0..5, T -20..80
    b, v1, e1, t1, v2, e2, t2, v3, e3, t3 = vals
    if not (8.0 <= b <= 16.0): return None
    if not (0.0 <= v1 <= 1.2 and 0.0 <= v2 <= 1.2 and 0.0 <= v3 <= 1.2): return None
    if not (0.0 <= e1 <= 5.0 and 0.0 <= e2 <= 5.0 and 0.0 <= e3 <= 5.0): return None
    if not (-20.0 <= t1 <= 80.0 and -20.0 <= t2 <= 80.0 and -20.0 <= t3 <= 80.0): return None
    return vals

def decode_row(payload_hex: str) -> dict | None:
    """Heuristic: scan the payload for a plausible 10-float block (big-endian)."""
    if not isinstance(payload_hex, str): return None
    h = payload_hex.replace(" ", "").replace(":", "").lower()
    # fast path: many dumps already have spaces; len should be even
    try:
        b = bytes.fromhex(h)
    except Exception:
        return None

    # Scan for a window that looks like 10 float32s in our ranges.
    # These frames often contain housekeeping header then data block.
    for i in range(0, max(0, len(b) - 40) + 1):
        vals = try_unpack_10_be_floats(b, i)
        if vals is not None:
            btt, v1, e1, t1, v2, e2, t2, v3, e3, t3 = vals
            return {
                "BattV_Min": btt,
                "VWC_1_Avg": v1, "EC_1_Avg": e1, "T_1_Avg": t1,
                "VWC_2_Avg": v2, "EC_2_Avg": e2, "T_2_Avg": t2,
                "VWC_3_Avg": v3, "EC_3_Avg": e3, "T_3_Avg": t3,
            }
    return None

def main():
    ap = argparse.ArgumentParser(description="Decode BD PayloadHex frames to Table1 fields.")
    ap.add_argument("--collapsed", required=True, help="*_collapsed.csv with PayloadHex")
    ap.add_argument("--out", help="Output CSV (default: alongside input, *_decoded.csv)")
    ap.add_argument("--limit", type=int, default=None, help="Optional limit for testing")
    args = ap.parse_args()

    df = pd.read_csv(args.collapsed)
    if "PayloadHex" not in df.columns or "DateTime" not in df.columns:
        print(f"ERROR: {args.collapsed} must have DateTime and PayloadHex columns", file=sys.stderr)
        sys.exit(2)

    if args.limit:
        df = df.head(args.limit).copy()

    rows = []
    misses = 0
    for _, r in df.iterrows():
        d = decode_row(r["PayloadHex"])
        if d:
            dout = {"TimestampUTC": pd.to_datetime(r["DateTime"]).isoformat()}
            dout.update(d)
            rows.append(dout)
        else:
            misses += 1

    if not rows:
        print("ERROR: failed to decode any rows.", file=sys.stderr)
        sys.exit(3)

    out = Path(args.out) if args.out else Path(args.collapsed).with_name(
        Path(args.collapsed).stem.replace("_collapsed", "_decoded") + ".csv"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(rows)[["TimestampUTC"] + FIELDS]
    out_df.sort_values("TimestampUTC").drop_duplicates("TimestampUTC", keep="last").to_csv(out, index=False)

    total = len(df)
    print(f"[decode] wrote {out}  rows={len(out_df)}  misses={misses}/{total}")

if __name__ == "__main__":
    main()