#!/usr/bin/env python3
# decode_table1_from_bd.py — extract one clean Table 1 record per data_*.hex
# Pattern (strict): UInt32 BE epoch seconds since 1990-01-01 + 10 x float32 BE
# We scan only a reasonable payload window and require 15-min aligned timestamps.

import pathlib, struct, csv, datetime, math

IN_DIR  = pathlib.Path("pakbus_runs/replies")
OUT_DIR = pathlib.Path("pakbus_runs/decoded_table1"); OUT_DIR.mkdir(parents=True, exist_ok=True)
MASTER  = OUT_DIR / "table1_master.csv"

FIELDS = [
    "BattV_Min",
    "VWC_1_Avg","EC_1_Avg","T_1_Avg",
    "VWC_2_Avg","EC_2_Avg","T_2_Avg",
    "VWC_3_Avg","EC_3_Avg","T_3_Avg"
]

# Reasonable field ranges for plausibility checks
RANGES = {
  "BattV_Min": (9.0, 16.0),
  "VWC_1_Avg": (0.0, 0.6), "VWC_2_Avg": (0.0, 0.6), "VWC_3_Avg": (0.0, 0.6),
  "EC_1_Avg":  (0.0, 5.0), "EC_2_Avg":  (0.0, 5.0), "EC_3_Avg":  (0.0, 5.0),
  "T_1_Avg":   (-40.0, 60.0), "T_2_Avg": (-40.0, 60.0), "T_3_Avg": (-40.0, 60.0),
}

EPOCH_1990 = datetime.datetime(1990, 1, 1, tzinfo=datetime.timezone.utc)

def in_range(name: str, v: float) -> bool:
    lo, hi = RANGES.get(name, (-math.inf, math.inf))
    if not math.isfinite(v):
        return False
    return lo <= v <= hi

def read_hex_bytes(p: pathlib.Path) -> bytes:
    # The data_*.hex files we produced are **raw bytes** of the BD frame.
    return p.read_bytes()

def plausible_timestamp(sec: int) -> datetime.datetime | None:
    """Return UTC datetime if seconds are in a sane range and aligned to 15-min boundary."""
    # Widened range to cover 1998–~2041 (safe upper bound)
    if not (900_000_000 <= sec <= 1_600_000_000):
        return None
    ts = EPOCH_1990 + datetime.timedelta(seconds=sec)
    # Require 00/15/30/45 minutes, seconds==0 — matches CR2 Table1 schedule
    if ts.second != 0 or (ts.minute % 15) != 0:
        return None
    return ts

def score_values(vals: list[float]) -> tuple[int, float]:
    """
    Score a 10-float vector:
      - count_in_range: how many fields pass plausibility ranges
      - penalty: sum of absolute distance outside ranges (0 if in range)
    Higher count_in_range is better; if tied, lower penalty is better.
    """
    count = 0
    penalty = 0.0
    for i, name in enumerate(FIELDS):
        v = vals[i]
        lo, hi = RANGES[name]
        if math.isfinite(v) and lo <= v <= hi:
            count += 1
        else:
            # distance outside range (use 0 if NaN/inf)
            if math.isfinite(v):
                if v < lo: penalty += (lo - v)
                elif v > hi: penalty += (v - hi)
            else:
                penalty += 10.0  # harsh penalty for NaN/inf
    return count, penalty

def find_best_table1(b: bytes) -> tuple[int, datetime.datetime, list[float]] | None:
    """
    Scan a bounded window for (epoch BE + 10 float32 BE) and pick the single best hit.
    We limit scanning to offsets [scan_start .. scan_end) to avoid false positives.
    """
    n = len(b)
    # Heuristic window covering all offsets we've seen (19, 64) and some margin.
    scan_start = 16
    scan_end   = min(n, 256)

    best = None  # (score_tuple, offset, ts_dt, vals)
    for i in range(scan_start, max(scan_start, scan_end - (4 + 4*10))):
        try:
            sec = struct.unpack_from(">I", b, i)[0]
        except struct.error:
            continue
        ts = plausible_timestamp(sec)
        if ts is None:
            continue
        try:
            vals = list(struct.unpack_from(">" + "f"*10, b, i + 4))
        except struct.error:
            continue

        count, penalty = score_values(vals)
        # Require at least 8/10 in range to accept as a candidate
        if count < 8:
            continue

        key = (count, -penalty)  # higher count, lower penalty
        if (best is None) or (key > best[0]):
            best = (key, i, ts, vals)

    if best is None:
        return None
    _, off, ts, vals = best
    return off, ts, vals

def main():
    per_rows_total = 0
    master_rows = []

    files = sorted(IN_DIR.glob("data_*.hex"))
    if not files:
        print(f"[WARN] No data_*.hex files found under {IN_DIR}")
        return

    for p in files:
        b = read_hex_bytes(p)
        found = find_best_table1(b)
        if not found:
            print(f"[SKIP] {p.name}: no valid Table1 record found")
            continue

        off, ts, vals = found
        iso = ts.isoformat().replace("+00:00", "Z")

        # write per-frame CSV (ONE clean record per file)
        per = OUT_DIR / (p.stem + "_table1.csv")
        with per.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["TIMESTAMP"] + FIELDS + ["_source_file", "_offset"])
            w.writerow([iso] + [f"{v:.6f}" for v in vals] + [p.name, off])
        per_rows_total += 1
        print(f"[OK] {p.name} -> {per.name}  ({iso}, offset {off})")

        # add to master (keep numeric values, we format only when writing CSV)
        master_rows.append((iso, *vals, p.name, off))

    if not master_rows:
        print("[WARN] No rows to write.")
        return

    # Write a fresh master each run (overwrite)
    with MASTER.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["TIMESTAMP"] + FIELDS + ["_source_file", "_offset"])
        for iso, *rest in master_rows:
            vals = rest[:10]
            src  = rest[10]
            off  = rest[11]
            # keep numbers as strings with fixed precision to avoid Excel auto-formatting,
            # but they will still be parseable by pandas
            w.writerow([iso] + [f"{v:.6f}" for v in vals] + [src, off])

    print(f"[INFO] wrote {MASTER} (rows={len(master_rows)}, per-file CSVs={per_rows_total})")

if __name__ == "__main__":
    main()