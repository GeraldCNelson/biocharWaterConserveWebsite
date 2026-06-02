#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

# allow importing config.py from the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import config


def summarize_file(path: Path) -> None:
    """
    Print diagnostics for a single parquet file: filename, columns, shape, dtypes, and first rows.
    """
    print(f"--- File: {path.name} ---")
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"Error reading {path.name}: {e}\n")
        return

    print(f"Columns: {list(df.columns)}")
    print(f"Shape: {df.shape}")
    print("Dtypes:")
    print(df.dtypes)
    print("Head:")
    print(df.head(), "\n")


def main():
    # ── 1) Raw logger parquet files (by year directories)
    raw_root = config.PARQUET_DIR
    print("\n🔥 Raw logger parquet files summary 🔥")
    for year_dir in sorted(raw_root.iterdir()):
        if not year_dir.is_dir():
            continue
        # look for any raw-logger parquet under this year folder
        for raw_file in sorted(year_dir.glob(f"*raw_logger.parquet")):
            summarize_file(raw_file)

    # ── 2) Fixed-frequency summary parquet files
    summary_root = config.PARQUET_DIR / "summary"
    print("\n📊 Summary parquet files summary 📊")
    if not summary_root.exists():
        print(f"No summary directory found at {summary_root}")
    else:
        for freq_dir in sorted(summary_root.iterdir()):
            if not freq_dir.is_dir():
                continue
            print(f"\n➡️  Section: {freq_dir.name}")
            for pq in sorted(freq_dir.glob("*.parquet")):
                summarize_file(pq)

    print("Done.")


if __name__ == "__main__":
    main()