from pathlib import Path
import re
import sqlite3

import pandas as pd

ROOT = Path("biochar_app")
IRRIGATION_ROOT = ROOT / "data-processed" / "management" / "irrigation"

BAD_PATTERNS = [
    "2026-06-19_S1_S2",
    "2026-06-19_S3_S4",
]

TEXT_EXTENSIONS = {
    ".csv",
    ".txt",
    ".json",
    ".md",
    ".py",
    ".html",
    ".js",
    ".css",
    ".log",
}

def audit_text_files() -> None:
    print("\n=== TEXT FILE / CSV REFERENCES ===")

    for path in IRRIGATION_ROOT.rglob("*"):
        if not path.is_file():
            continue

        if path.suffix.lower() not in TEXT_EXTENSIONS:
            continue

        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        hits = [pattern for pattern in BAD_PATTERNS if pattern in text]
        if hits:
            print(f"\n{path}")
            for pattern in hits:
                print(f"  contains: {pattern}")

def audit_filenames() -> None:
    print("\n=== FILENAME / PATH REFERENCES ===")

    for path in IRRIGATION_ROOT.rglob("*"):
        path_s = str(path)
        hits = [pattern for pattern in BAD_PATTERNS if pattern in path_s]
        if hits:
            print(path)

def audit_csv_event_id_columns() -> None:
    print("\n=== CSV event_id COLUMNS ===")

    for path in IRRIGATION_ROOT.rglob("*.csv"):
        try:
            df = pd.read_csv(path, nrows=5000)
        except Exception:
            continue

        if "event_id" not in df.columns:
            continue

        event_ids = df["event_id"].astype(str)
        mask = event_ids.str.contains(
            "|".join(re.escape(p) for p in BAD_PATTERNS),
            na=False,
        )

        if mask.any():
            print(f"\n{path}")
            print(df.loc[mask, ["event_id"]].drop_duplicates().to_string(index=False))

def audit_sqlite_databases() -> None:
    print("\n=== SQLITE DATABASE REFERENCES ===")

    for path in IRRIGATION_ROOT.rglob("*.db"):
        try:
            con = sqlite3.connect(path)
            tables = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table'",
                con,
            )["name"].tolist()

            for table in tables:
                try:
                    df = pd.read_sql_query(f"SELECT * FROM {table}", con)
                except Exception:
                    continue

                for col in df.columns:
                    s = df[col].astype(str)
                    mask = s.str.contains(
                        "|".join(re.escape(p) for p in BAD_PATTERNS),
                        na=False,
                    )
                    if mask.any():
                        print(f"\n{path} :: table={table} :: column={col}")
                        print(df.loc[mask].head(20).to_string(index=False))
        except Exception:
            continue
        finally:
            try:
                con.close()
            except Exception:
                pass

def main() -> None:
    audit_text_files()
    audit_filenames()
    audit_csv_event_id_columns()
    audit_sqlite_databases()

if __name__ == "__main__":
    main()