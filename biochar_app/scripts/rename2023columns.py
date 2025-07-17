import os
import zipfile
import pandas as pd

# === Configuration ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data-processed")
TARGET_YEAR = "2023"
dry_run = False  # ✅ Set True to preview changes only

# === Helper: Extract granularity from filename ===
def extract_granularity(filename):
    filename = filename.lower().replace(".zip", "")
    for part in ["15min", "1hour", "daily", "monthly", "gseason", "growingseason"]:
        if part in filename:
            return part.replace("growingseason", "gseason")
    return None

# === Helper: Rename columns intelligently ===
def rename_columns(cols):
    suffixes = ["15min", "1hour", "daily", "monthly", "gseason"]
    new_cols = []
    for col in cols:
        if col == "timestamp":
            new_cols.append(col)
        else:
            for old_suffix in suffixes:
                if col.endswith(f"_{old_suffix}"):
                    base_col = col.rsplit(f"_{old_suffix}", 1)[0]
                    new_cols.append(base_col)  # ✅ Drop the suffix
                    break
            else:
                new_cols.append(col)  # ✅ Leave as-is if no matching suffix
    return new_cols

# === Main script ===
def process_zip_files():
    print(f"🔍 Scanning {DATA_PROCESSED_DIR} for {TARGET_YEAR} zip files...")

    for filename in sorted(os.listdir(DATA_PROCESSED_DIR)):
        if not filename.startswith(f"dataloggerData_{TARGET_YEAR}") or not filename.endswith(".zip"):
            continue

        granularity = extract_granularity(filename)
        if not granularity:
            print(f"⚠️  Could not determine granularity from filename: {filename}")
            continue

        full_path = os.path.join(DATA_PROCESSED_DIR, filename)
        with zipfile.ZipFile(full_path, 'r') as zipf:
            csv_files = [f for f in zipf.namelist() if f.endswith(".csv")]
            if not csv_files:
                print(f"⚠️  No CSV found inside {filename}")
                continue

            csv_name = csv_files[0]
            df = pd.read_csv(zipf.open(csv_name))

            print(f"\n🛠️ Processing {filename} ({csv_name})")
            print(f"🔍 First 5 columns BEFORE rename: {list(df.columns[:5])}")

            new_columns = rename_columns(df.columns)
            df.columns = new_columns

            print(f"✅ First 5 columns AFTER rename: {list(df.columns[:5])}")

            if not dry_run:
                temp_csv_path = os.path.join(DATA_PROCESSED_DIR, csv_name)
                df.to_csv(temp_csv_path, index=False)

                with zipfile.ZipFile(full_path, 'w', zipfile.ZIP_DEFLATED) as new_zipf:
                    new_zipf.write(temp_csv_path, arcname=csv_name)

                os.remove(temp_csv_path)
                print(f"✅ Overwrote {filename} with updated columns.")

if __name__ == "__main__":
    process_zip_files()