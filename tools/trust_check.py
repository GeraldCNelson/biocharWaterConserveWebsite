#!/usr/bin/env python3
import argparse, csv, sys
from datetime import datetime, timedelta, timezone

# These are the expected channel names in order.
FIELDS = [
    "BattV_Min",
    "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
    "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
    "VWC_3_Avg", "EC_3_Avg", "T_3_Avg",
]

REQUIRED_DAT_HEADER = ["TIMESTAMP", "RECORD"] + FIELDS

def parse_ts_utc(s: str):
    """
    Accepts broad ISO-8601 variants:
      - with/without fractional seconds
      - trailing 'Z' or explicit offset like +00:00
      - 'T' or space between date and time
    Returns a pandas.Timestamp tz-aware (UTC).
    """
    import pandas as pd
    from datetime import datetime, timezone

    s = str(s).strip()
    if not s:
        raise ValueError("Empty timestamp")

    # normalize a few common variants
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    if 'T' not in s and ' ' in s:
        # allow "YYYY-MM-DD HH:MM:SS(.fffff)[offset]"
        s = s.replace(' ', 'T', 1)

    # first try pandas (handles most cases)
    try:
        ts = pd.to_datetime(s, utc=True)
        if pd.isna(ts):
            raise ValueError
        return ts
    except Exception:
        # fallback: Python stdlib
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return pd.Timestamp(dt.astimezone(timezone.utc))
        except Exception:
            raise ValueError(f"Unrecognized timestamp: {s!r}")

def read_collapsed_csv(path, fields=FIELDS):
    """
    Expect headers: Row,TimestampUTC,TimestampLocal,<FIELDS...>
    Returns dict[ts_utc] = [values...], list of ts for order check.
    Skips rows with missing/blank TimestampUTC.
    """
    data = {}
    order = []
    with open(path, newline="") as f:
        r = csv.DictReader(f, delimiter=",")
        # If it looks like single-column (e.g., tabs), retry with \t
        if r.fieldnames and len(r.fieldnames) == 1:
            f.seek(0)
            r = csv.DictReader(f, delimiter="\t")

        cols = r.fieldnames or []
        missing = [c for c in (["TimestampUTC"] + list(fields)) if c not in cols]
        if missing:
            raise RuntimeError(f"{path}: missing expected columns: {missing}")

        for row in r:
            ts_raw = (row.get("TimestampUTC") or "").strip()
            if not ts_raw:
                # gracefully skip blank rows
                continue
            try:
                ts = parse_ts_utc(ts_raw)
            except ValueError:
                # surface the bad row position and continue with a clear error
                raise

            vals = []
            for c in fields:
                v = row.get(c, "")
                try:
                    vals.append(float(v))
                except Exception:
                    vals.append(float("nan"))
            data[ts] = vals
            order.append(ts)
    return data, order

def _strip_cells(row):
    return [c.strip().strip('"') for c in row]

def read_dat_file(path, fields=FIELDS):
    """
    Read CRBasic/TOA5 .dat file with 4-line header block.
    Finds the header names line (the one containing TIMESTAMP and RECORD),
    validates required columns, then streams data rows.

    Returns dict[ts_utc] = [values...] (values are in the order of `fields`).
    """
    # First, peek top ~8 lines to detect the "names" line
    with open(path, newline="") as f:
        r = csv.reader(f)
        peek = []
        for _ in range(8):
            try:
                peek.append(next(r))
            except StopIteration:
                break

        header_idx = None
        header_names = None
        for i, row in enumerate(peek):
            cells = _strip_cells(row)
            if "TIMESTAMP" in cells and "RECORD" in cells:
                header_idx = i
                header_names = cells
                break

        if header_idx is None or header_names is None:
            found0 = _strip_cells(peek[0]) if peek else []
            raise RuntimeError(
                f"{path}: could not find TIMESTAMP/RECORD header line in first {len(peek)} rows "
                f"(top row looked like: {found0})"
            )

        # Validate required columns are present on the detected names line
        missing_required = [c for c in REQUIRED_DAT_HEADER if c not in header_names]
        if missing_required:
            raise RuntimeError(
                f"{path}: missing required header columns: {missing_required} "
                f"(found: {header_names})"
            )

        # Build index map for fields we want
        try:
            idx_ts = header_names.index("TIMESTAMP")
        except ValueError:  # shouldn't happen after validation
            idx_ts = 0

        idxs = []
        for name in fields:
            try:
                idxs.append(header_names.index(name))
            except ValueError:
                # Should not happen after required check; keep message explicit
                raise RuntimeError(f"{path}: missing field in header: {name}")

        # Rewind and stream all rows, skipping non-data and header/unit lines
        f.seek(0)
        r = csv.reader(f)
        data = {}
        row_num = -1
        for row in r:
            row_num += 1
            cells = _strip_cells(row)
            if not cells or len(cells) == 0:
                continue
            first = (cells[0] if cells else "").upper()

            # Skip any header-ish rows
            if row_num <= header_idx:
                continue
            # The next 1–2 rows are usually units and stats (e.g., TS/RN, then blanks/min/avg labels)
            if first in ("TOA5", "TIMESTAMP", "TS", ""):
                continue

            # Data row
            ts_raw = cells[idx_ts] if idx_ts < len(cells) else ""
            if not ts_raw:
                continue

            # Most TOA5 logs use "YYYY-mm-dd HH:MM:SS"
            try:
                ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                # Be liberal and reuse the CSV parser
                ts = parse_ts_utc(ts_raw)

            vals = []
            for i in idxs:
                try:
                    vals.append(float(cells[i]))
                except Exception:
                    vals.append(float("nan"))
            data[ts] = vals

    return data

def cadence_stats(timestamps, cadence_min):
    if not timestamps:
        return dict(count=0, gaps=0, dupes=0, min_gap=None, max_gap=None)
    ts_sorted = sorted(timestamps)
    gaps = 0
    dupes = len(timestamps) - len(set(timestamps))
    expected = timedelta(minutes=cadence_min)
    min_gap = None
    max_gap = None
    for prev, cur in zip(ts_sorted, ts_sorted[1:]):
        delta = cur - prev
        if min_gap is None or delta < min_gap:
            min_gap = delta
        if max_gap is None or delta > max_gap:
            max_gap = delta
        # Count gaps > expected by a small epsilon (30s)
        if delta > expected + timedelta(seconds=30):
            gaps += 1
    return dict(count=len(ts_sorted), gaps=gaps, dupes=dupes, min_gap=min_gap, max_gap=max_gap)

def compare_pair(collapsed, dat, fields, value_tol):
    """
    Returns summary dict and list of outlier rows.
    """
    ts_c = set(collapsed.keys())
    ts_d = set(dat.keys())
    overlap = sorted(ts_c & ts_d)
    only_c = sorted(ts_c - ts_d)
    only_d = sorted(ts_d - ts_c)

    # compute deltas
    max_abs = [0.0] * len(fields)
    outliers = []  # (ts, field_name, dat_val, fetched_val, abs_delta)
    for ts in overlap:
        v_c = collapsed[ts]
        v_d = dat[ts]
        for j, fname in enumerate(fields):
            dv = v_d[j]
            fv = v_c[j]
            # NaN check
            if (dv != dv) or (fv != fv):
                continue
            delta = abs(fv - dv)
            if delta > max_abs[j]:
                max_abs[j] = delta
            if delta > value_tol:
                outliers.append((ts, fname, dv, fv, delta))

    summary = {
        "overlap_count": len(overlap),
        "only_in_collapsed": len(only_c),
        "only_in_dat": len(only_d),
        "max_abs_delta_by_field": dict(zip(fields, max_abs)),
        "only_collapsed_head": [t.isoformat() for t in only_c[:5]],
        "only_dat_head": [t.isoformat() for t in only_d[:5]],
    }
    return summary, outliers

def human_td(td):
    if td is None:
        return "n/a"
    secs = int(td.total_seconds())
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def main():
    p = argparse.ArgumentParser(description="Compare collapsed fetch vs .dat for trustworthiness")
    p.add_argument("--collapsed", required=True, help="Path to *_collapsed.csv from fetch")
    p.add_argument("--dat", required=True, help="Path to .dat file")
    p.add_argument("--cadence-min", type=int, default=15, help="Expected cadence in minutes (default 15)")
    p.add_argument("--value-tol", type=float, default=0.005, help="Absolute tolerance for value deltas (default 0.005)")
    p.add_argument("--cadence-only", action="store_true",
                    help="Skip value comparisons; only check cadence/overlap.")
    p.add_argument(
        "--fields", nargs="*",
        default=FIELDS,
        help="Subset of fields to check; defaults to all"
    )
    args = p.parse_args()

    # Resolve fields to check
    requested_fields = list(args.fields) if args.fields is not None else list(FIELDS)

    # Read inputs
    try:
        collapsed, _order = read_collapsed_csv(args.collapsed, requested_fields)
    except RuntimeError as e:
        # Graceful skip if this looks like a raw BD-frames CSV
        import pandas as _pd
        _df = _pd.read_csv(args.collapsed, nrows=1)
        if "PayloadHex" in _df.columns:
            print(f"[skip] {args.collapsed}: raw BD frames (PayloadHex present). "
                  f"Decode to Table1 first, then rerun trust check.")
            sys.exit(0)
        raise
    dat = read_dat_file(args.dat, requested_fields)

    # A) cadence / gaps / dupes
    cad_c = cadence_stats(list(collapsed.keys()), args.cadence_min)
    cad_d = cadence_stats(list(dat.keys()), args.cadence_min)

    # B) overlap deltas
    summary, outliers = compare_pair(collapsed, dat, requested_fields, args.value_tol)

    print("\n=== TRUST CHECK SUMMARY ===")
    print(f"collapsed file : {args.collapsed}")
    print(f"dat file       : {args.dat}")

    print("\n-- Collapsed cadence --")
    print(
        f"count={cad_c['count']} gaps={cad_c['gaps']} dupes={cad_c['dupes']} "
        f"min_gap={human_td(cad_c['min_gap'])} max_gap={human_td(cad_c['max_gap'])}"
    )
    print("-- .dat cadence --")
    print(
        f"count={cad_d['count']} gaps={cad_d['gaps']} dupes={cad_d['dupes']} "
        f"min_gap={human_td(cad_d['min_gap'])} max_gap={human_td(cad_d['max_gap'])}"
    )

    print("\n-- Overlap --")
    print(f"overlap rows  : {summary['overlap_count']}")
    print(f"only collapsed: {summary['only_in_collapsed']}  head={summary['only_collapsed_head']}")
    print(f"only in .dat  : {summary['only_in_dat']}        head={summary['only_dat_head']}")

    print("\n-- Max |Δ| by field --")
    for k, v in summary["max_abs_delta_by_field"].items():
        print(f"{k:>12}: {v:.6g}")

    if outliers:
        print(f"\n-- Outliers > {args.value_tol} ({len(outliers)} rows) --")
        for ts, name, dv, fv, d in outliers[:20]:
            print(f"{ts.isoformat()}  {name}: dat={dv:.6g} fetched={fv:.6g} |Δ|={d:.6g}")
    else:
        print(f"\nNo per-field deltas exceeded {args.value_tol} in the overlap window.")

    # Non-zero exit when something looks off (useful in CI)
    bad = cad_c["gaps"] > 0 or cad_c["dupes"] > 0 or len(outliers) > 0
    sys.exit(1 if bad else 0)

if __name__ == "__main__":
    main()