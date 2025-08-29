#!/usr/bin/env python3
import argparse, pandas as pd
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--collapsed", required=True, help="Frame-only CSV (DateTime, Epoch, ByteLen, PayloadHex)")
    ap.add_argument("--n", type=int, default=8, help="How many rows to show")
    args = ap.parse_args()

    df = pd.read_csv(args.collapsed)
    keep = df.head(args.n).copy()
    # print a narrow view of payload with grouped bytes to see patterns
    def group2(hexs): return " ".join(hexs[i:i+2] for i in range(0, len(hexs), 2))
    keep["PayloadGrouped"] = keep["PayloadHex"].str.lower().str.replace(":", "", regex=False).apply(group2)
    print(keep[["DateTime","Epoch","ByteLen","PayloadGrouped"]].to_string(index=False))

if __name__ == "__main__":
    main()