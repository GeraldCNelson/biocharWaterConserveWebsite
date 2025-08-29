#!/usr/bin/env python3
"""
make_tsvs.py
- Refresh catalog for each leaf/table by calling send_catalog_request.py
- Ensure a TSV template exists for each leaf/table by copying from a reference TSV
  (since all leaves share identical hardware/table schema)

Examples:
  PYTHONPATH=. python tools/make_tsvs.py \
    --leaves S1T S1M S1B S2T S2M S2B \
    --table 1 \
    --host 2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd --port 6785 \
    --catalog-dir biochar_app/pakbus/bdFiles/out_catalog \
    --tsv-dir biochar_app/pakbus/bdFiles \
    --ref-leaf S2T

Notes:
- We pass --last-n 0 so no data pages are fetched; only the catalog is refreshed.
- If a TSV for a leaf is missing, we copy from <ref-leaf>_Table<Table>.tsv.
- If --ref-leaf is not provided, we auto-pick any existing *_Table<Table>.tsv in --tsv-dir.
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

def run_catalog_only(pythonpath_root: Path,
                     send_script: Path,
                     leaf: str,
                     table: int,
                     host: str,
                     port: int,
                     catalog_dir: Path) -> int:
    """
    Call send_catalog_request.py in 'catalog-only' mode.
    We achieve that by --last-n 0 and an out-dir that we don't care about for this step.
    """
    # Dummy out dir (required by the script's interface, but unused here)
    dummy_out = catalog_dir / "_tmp_noop_out"
    dummy_out.mkdir(parents=True, exist_ok=True)

    env = {**dict(**{"PYTHONPATH": str(pythonpath_root)}), **dict(**{"PATH": str(Path.cwd())})}

    cmd = [
        sys.executable or "python",
        str(send_script),
        "--catalog-dir", str(catalog_dir),
        "--leaf", leaf,
        "--table", str(table),
        "--last-n", "0",
        "--host", host,
        "--port", str(port),
        "--out-dir", str(dummy_out),
    ]

    print(f"[catalog] {leaf}: running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, env=env)
    return proc.returncode

def pick_reference_tsv(tsv_dir: Path, table: int, ref_leaf: str | None) -> Path | None:
    """
    Choose a reference TSV to copy from.
    Priority:
      1) --ref-leaf provided and exists
      2) Any *_Table<Table>.tsv present in tsv_dir
    """
    if ref_leaf:
        ref = tsv_dir / f"{ref_leaf}_Table{table}.tsv"
        if ref.is_file():
            return ref
        print(f"[warn] --ref-leaf given but TSV not found: {ref}")

    candidates = sorted(tsv_dir.glob(f"*_*Table{table}.tsv")) + sorted(tsv_dir.glob(f"*Table{table}.tsv"))
    if candidates:
        return candidates[0]

    return None

def ensure_tsv(tsv_dir: Path, leaf: str, table: int, ref_tsv: Path) -> bool:
    """
    Ensure {leaf}_Table{table}.tsv exists. If missing, copy from ref_tsv.
    Returns True if the TSV exists or was created, False on failure.
    """
    dest = tsv_dir / f"{leaf}_Table{table}.tsv"
    if dest.is_file():
        print(f"[tsv] {leaf}: already present -> {dest}")
        return True

    try:
        shutil.copy2(ref_tsv, dest)
        print(f"[tsv] {leaf}: created by copying reference -> {dest}")
        return True
    except Exception as e:
        print(f"[error] {leaf}: failed to copy TSV from {ref_tsv}: {e}")
        return False

def main():
    ap = argparse.ArgumentParser(description="Refresh catalogs and ensure TSVs exist for leaves/tables.")
    ap.add_argument("--leaves", nargs="+", required=True, help="Leaf names, e.g., S1T S1M S1B S2T S2M S2B")
    ap.add_argument("--table", type=int, required=True, help="Table number (e.g., 1)")
    ap.add_argument("--host", required=True, help="Logger host (IPv6/IPv4)")
    ap.add_argument("--port", type=int, required=True, help="Logger port")
    ap.add_argument("--catalog-dir", required=True, help="Directory to store/read catalogs")
    ap.add_argument("--tsv-dir", required=True, help="Directory where TSV templates live")
    ap.add_argument("--ref-leaf", help="Reference leaf to copy TSV from if missing (e.g., S2T)")
    ap.add_argument("--pythonpath-root", default=".", help="Project root to add to PYTHONPATH")
    ap.add_argument("--send-script", default="biochar_app/pakbus/send_catalog_request.py",
                    help="Path to send_catalog_request.py")
    args = ap.parse_args()

    leaves = args.leaves
    table = args.table
    host = args.host
    port = args.port
    catalog_dir = Path(args.catalog_dir)
    tsv_dir = Path(args.tsv_dir)
    pythonpath_root = Path(args.pythonpath_root).resolve()
    send_script = Path(args.send_script)

    # sanity
    catalog_dir.mkdir(parents=True, exist_ok=True)
    tsv_dir.mkdir(parents=True, exist_ok=True)
    if not send_script.is_file():
        print(f"[fatal] send_catalog_request.py not found at: {send_script}")
        sys.exit(2)

    # choose reference TSV once
    ref_tsv = pick_reference_tsv(tsv_dir, table, args.ref_leaf)
    if ref_tsv is None:
        print(f"[fatal] Could not locate a reference TSV for Table {table}. "
              f"Provide --ref-leaf or place any *_Table{table}.tsv in {tsv_dir}.")
        sys.exit(3)

    print(f"[info] Using reference TSV: {ref_tsv}")

    overall_rc = 0

    for leaf in leaves:
        # Step 1: refresh catalog (non-fatal if it fails, but we report)
        rc = run_catalog_only(pythonpath_root, send_script, leaf, table, host, port, catalog_dir)
        if rc != 0:
            print(f"[warn] Catalog refresh returned non-zero for {leaf} (rc={rc}). Continuing…")
            overall_rc = 1

        # Step 2: ensure TSV exists (copy from reference if missing)
        ok = ensure_tsv(tsv_dir, leaf, table, ref_tsv)
        if not ok:
            overall_rc = 1

    sys.exit(overall_rc)

if __name__ == "__main__":
    main()