#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

# allow importing config.py from the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import config

def main():
    # root of all summary parquet files
    summary_root = config.PARQUET_DIR / "summary"

    # pull years and granularities straight from config.py
    years = [str(y) for y in config.YEARS]
    granularities = config.GRANULARITIES

    for gran in granularities:
        gran_dir = summary_root / gran
        print(f"\n=== Inspecting: {gran_dir} ===\n")

        if not gran_dir.exists():
            print(f"⚠️  Directory not found: {gran_dir}\n")
            continue

        # look for each year's file under this granularity
        for year in years:
            fname = f"{year}_{gran}.parquet"
            path = gran_dir / fname
            if not path.exists():
                print(f"   ⚠️  Missing file: {fname}")
                continue

            print(f"--- File: {fname} ---")
            df = pd.read_parquet(path)

            # same diagnostics as before
            print(f"Columns: {list(df.columns)}")
            print(f"Shape: {df.shape}")
            print("Dtypes:")
            print(df.dtypes)
            print(df.head(), "\n")

    print("Done.")

if __name__ == "__main__":
    main()