#!/usr/bin/env python3
"""
Rebuild 2023 SWC volumes & all ratio columns, then regenerate every granularity.
"""
import zipfile
import tempfile
import logging
import re
from pathlib import Path
import numpy as np

import pandas as pd
from biochar_app.config import DATA_PROCESSED_DIR
from biochar_app.process_data import (
    add_swc_cylinder_volumes,
    calculate_ratios,
    aggregate,
    save_outputs,
)

# ─────────────────────────────────────────────────────────────────────────────
# instead of a hard‐coded relative path, compute it based on this script’s location
# reuse DATA_PROCESSED_DIR from config (string) and convert to Path
DATA_PROCESSED_DIR = Path(DATA_PROCESSED_DIR)
IN_ZIP = DATA_PROCESSED_DIR / "dataloggerData_2023-01-01_2023-12-31_15min.zip"

logging.basicConfig(level=logging.INFO, format="%(message)s")

def drop_old_swc_and_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop any old SWC columns (both raw‐volume and cylinder‐volume)
    and any *_ratio_* columns so we can re-compute them cleanly.
    """
    # anything starting with "SWC_" (raw or vol) or containing "_ratio_"
    pattern = re.compile(r'^(SWC_.*|.*_ratio_.*)$')
    to_drop = [c for c in df.columns if pattern.match(c)]
    if to_drop:
        df = df.drop(columns=to_drop)
    return df

def rebuild_15min():
    year = 2023

    # sanity check
    if not IN_ZIP.exists():
        logging.error(f"❌ Could not find expected file: {IN_ZIP}")
        return

    logging.info(f"🔍 Found ZIP: {IN_ZIP}")

    out_zip = IN_ZIP.with_name(
        f"dataloggerData_{year}-01-01_{year}-12-31_15min.zip"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        # 1) unzip
        with zipfile.ZipFile(IN_ZIP, "r") as zin:
            zin.extractall(tmpdir)
        csvs = list(Path(tmpdir).glob("*.csv"))
        if not csvs:
            raise RuntimeError(f"No CSV inside {IN_ZIP}")
        csv_path = csvs[0]

        # 2) load
        df = pd.read_csv(csv_path, parse_dates=["timestamp"])
        logging.info(f"Loaded {len(df)} rows from {csv_path.name}")

        # 3) drop old SWC & ratio columns
        df = drop_old_swc_and_ratios(df)

        # 4) recompute cylinder‐volume SWC
        df = add_swc_cylinder_volumes(df)
        logging.info("  • Recomputed SWC_vol_L_… & SWC_vol_gal_… columns")

        # 4a) recompute ratios
        df = calculate_ratios(df)
        df = df.replace([np.inf, -np.inf], pd.NA)
        logging.info("  • Recomputed all *_ratio_* columns")

        # 5) overwrite CSV
        df.to_csv(csv_path, index=False, float_format="%.4f")
        logging.info(f"  • Wrote revised CSV to {csv_path.name}")

        # 6) re‐zip
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zout:
            zout.write(csv_path, arcname=csv_path.name)
        logging.info(f"✅ Rebuilt 15 min ZIP → {out_zip}")

    # 7) regenerate all other granularities
    aggregated = aggregate(df, year)
    save_outputs(year, aggregated)
    logging.info("✅ All granularities regenerated.")

# drop_old_swc_and_ratios() definition goes here (same as before)...

if __name__ == "__main__":
    rebuild_15min()