#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from biochar_app.config.paths import DATA_PROCESSED_DIR, FERTILIZER_DIR, FERTILIZER_CSV
from biochar_app.config.core import STRIP_AREA_ACRES, STRIP_GROUP_AREA_ACRES


DEFAULT_INPUT_WORKBOOK = FERTILIZER_DIR / "fertilizer_data_raw.xlsx"
DEFAULT_OUTPUT_CSV = FERTILIZER_CSV
DEFAULT_BACKUP_DIR = FERTILIZER_DIR / "backup"

# No application dates exist in the workbook, so use a consistent seasonal date.
DEFAULT_APPLICATION_MM_DD = "05-01"

FERTILIZER_AREA_ACRES = {
    **STRIP_AREA_ACRES,
    **STRIP_GROUP_AREA_ACRES,
}

OUTPUT_COLUMNS = [
    "year",
    "date",
    "strip_group",
    "location",
    "product",
    "applied_total_lb",
    "applied_lb_per_acre",
    "notes",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_WORKBOOK)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--backup-dir", type=Path, default=DEFAULT_BACKUP_DIR)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def make_backup(path: Path, backup_dir: Path) -> Path | None:
    if not path.exists():
        return None

    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{path.stem}_backup_{stamp}{path.suffix}"
    backup_path.write_bytes(path.read_bytes())
    return backup_path


def location_for_group(strip_group: str) -> str:
    if strip_group == "S1_S2":
        return "west"
    if strip_group == "S3_S4":
        return "east"
    return ""


def seasonal_date(year: int) -> str:
    return f"{year}-{DEFAULT_APPLICATION_MM_DD}"


def compute_rate(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["area_acres"] = out["strip_group"].map(FERTILIZER_AREA_ACRES)
    out["applied_total_lb"] = pd.to_numeric(out["applied_total_lb"], errors="coerce")
    out["area_acres"] = pd.to_numeric(out["area_acres"], errors="coerce")

    out["applied_lb_per_acre"] = out["applied_total_lb"] / out["area_acres"]

    out = out.drop(columns=["area_acres"], errors="ignore")
    out["applied_lb_per_acre"] = pd.to_numeric(out["applied_lb_per_acre"], errors="coerce")

    return out


def parse_2023_sheet(workbook: Path, sheet_name: str) -> pd.DataFrame:
    year = 2023
    raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)

    rows: list[dict[str, object]] = []

    # 2023 layout:
    # row 1 = FIELD N1 (STRIPS 1 & 2)
    # rows 2:4 products
    # row 5 = FIELD N2 (STRIPS 3 & 4)
    # rows 6:8 products
    group_blocks = [
        ("S1_S2", 2, 4),
        ("S3_S4", 6, 8),
    ]

    for strip_group, start_row, end_row in group_blocks:
        for r in range(start_row, end_row + 1):
            product = str(raw.iat[r, 0]).strip()
            values = pd.to_numeric(raw.iloc[r, 1:4], errors="coerce")
            applied_total_lb = float(values.sum(skipna=True))

            if applied_total_lb <= 0:
                continue

            rows.append(
                {
                    "year": year,
                    "date": seasonal_date(year),
                    "strip_group": strip_group,
                    "location": location_for_group(strip_group),
                    "product": product.replace("LBS OF ", "").strip(),
                    "applied_total_lb": applied_total_lb,
                    "notes": "Derived from 2023 fertilizer recommendation sheet; workbook has no explicit application date.",
                }
            )

    return compute_rate(pd.DataFrame(rows))


def parse_2024_2025_sheet(workbook: Path, sheet_name: str, year: int) -> pd.DataFrame:
    raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)

    rows: list[dict[str, object]] = []

    # Workbook columns:
    # product = col 0
    # source-to-apply columns:
    # S1 = 6, S2 = 11, S3 = 16, S4 = 21
    strip_apply_cols = {
        "S1": 6,
        "S2": 11,
        "S3": 16,
        "S4": 21,
    }

    group_members = {
        "S1_S2": ["S1", "S2"],
        "S3_S4": ["S3", "S4"],
    }

    # Product rows are 2:5 inclusive. Row 6 is total.
    for r in range(2, 6):
        product = str(raw.iat[r, 0]).strip()
        if not product or product.lower().startswith("nan"):
            continue

        for strip_group, strips in group_members.items():
            total_lb = 0.0

            for strip in strips:
                col = strip_apply_cols[strip]
                value = pd.to_numeric(pd.Series([raw.iat[r, col]]), errors="coerce").iloc[0]
                if pd.notna(value):
                    total_lb += float(value)

            if total_lb <= 0:
                continue

            rows.append(
                {
                    "year": year,
                    "date": seasonal_date(year),
                    "strip_group": strip_group,
                    "location": location_for_group(strip_group),
                    "product": product,
                    "applied_total_lb": total_lb,
                    "notes": (
                        f"Derived from {year} fertilizer sheet by summing strip-level "
                        f"source-to-apply values for {strip_group}; workbook has no explicit application date."
                    ),
                }
            )

    return compute_rate(pd.DataFrame(rows))


def build_fertilizer_clean(input_workbook: Path) -> pd.DataFrame:
    if not input_workbook.exists():
        raise FileNotFoundError(f"Input fertilizer workbook not found: {input_workbook}")

    xls = pd.ExcelFile(input_workbook, engine="openpyxl")
    frames: list[pd.DataFrame] = []

    for sheet_name in xls.sheet_names:
        sheet_upper = str(sheet_name).upper()

        if "2023" in sheet_upper:
            frames.append(parse_2023_sheet(input_workbook, sheet_name))
        elif "2024" in sheet_upper:
            frames.append(parse_2024_2025_sheet(input_workbook, sheet_name, 2024))
        elif "2025" in sheet_upper:
            frames.append(parse_2024_2025_sheet(input_workbook, sheet_name, 2025))

    frames = [df for df in frames if df is not None and not df.empty]

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    out = pd.concat(frames, ignore_index=True)

    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["applied_total_lb"] = pd.to_numeric(out["applied_total_lb"], errors="coerce").round(3)
    out["applied_lb_per_acre"] = pd.to_numeric(out["applied_lb_per_acre"], errors="coerce").round(3)

    out = out.loc[out["year"].notna()].copy()
    out["year"] = out["year"].astype(int)

    out = out[OUTPUT_COLUMNS]
    out = out.sort_values(["year", "strip_group", "product"]).reset_index(drop=True)

    return out


def main() -> int:
    args = parse_args()

    df = build_fertilizer_clean(args.input)

    print("\n--- Fertilizer clean summary ---")
    print(f"Rows: {len(df)}")
    if not df.empty:
        print(f"Years: {sorted(df['year'].unique().tolist())}")
        print(df.to_string(index=False))

    if args.dry_run:
        print("\nDry run only. No files written.")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    backup_path = make_backup(args.output, args.backup_dir)

    df.to_csv(args.output, index=False)

    if backup_path:
        print(f"\nBackup written: {backup_path}")

    print(f"Output written: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())