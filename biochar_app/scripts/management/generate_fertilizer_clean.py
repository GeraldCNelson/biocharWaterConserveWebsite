#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from biochar_app.config.core import STRIP_AREA_ACRES
from biochar_app.config.fertilizer import FERTILIZER_PRODUCT_ANALYSIS
from biochar_app.config.paths import (
    FERTILIZER_CSV_OUT,
    FERTILIZER_DATA_IN,
    FERTILIZER_DIR,
)

DEFAULT_INPUT_WORKBOOK = FERTILIZER_DATA_IN
DEFAULT_OUTPUT_CSV = FERTILIZER_CSV_OUT
DEFAULT_BACKUP_DIR = FERTILIZER_DIR / "backup"

# No application dates exist in the workbook, so use a consistent seasonal date.
DEFAULT_APPLICATION_MM_DD = "05-01"

NUTRIENTS = ["n", "p", "k", "mn", "s"]

OUTPUT_COLUMNS = [
    "year",
    "date",
    "strip",
    "strip_group",
    "location",
    "product",
    "applied_total_lb",
    "applied_lb_per_acre",
    "n_applied_total_lb",
    "n_applied_lb_per_acre",
    "p_applied_total_lb",
    "p_applied_lb_per_acre",
    "k_applied_total_lb",
    "k_applied_lb_per_acre",
    "mn_applied_total_lb",
    "mn_applied_lb_per_acre",
    "s_applied_total_lb",
    "s_applied_lb_per_acre",
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


def seasonal_date(year: int) -> str:
    return f"{year}-{DEFAULT_APPLICATION_MM_DD}"


def strip_group_for_strip(strip: str) -> str:
    if strip in {"S1", "S2"}:
        return "S1_S2"
    if strip in {"S3", "S4"}:
        return "S3_S4"
    return ""


def location_for_strip(strip: str) -> str:
    if strip in {"S1", "S2"}:
        return "west"
    if strip in {"S3", "S4"}:
        return "east"
    return ""


def strip_area_acres(strip: str) -> float | None:
    value = STRIP_AREA_ACRES.get(strip)
    if value is None:
        return None

    try:
        return float(value)
    except Exception:
        return None


def normalize_product_name(value: object) -> str:
    product = str(value).strip().upper()
    product = product.replace("LBS OF ", "").strip()
    product = " ".join(product.split())
    return product


def clean_product_name(value: object) -> str:
    return normalize_product_name(value)


def numeric_value(value: object) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return 0.0
    return float(parsed)


def product_analysis_for(product: str) -> dict[str, float]:
    key = normalize_product_name(product)
    analysis = FERTILIZER_PRODUCT_ANALYSIS.get(key)

    if analysis is None:
        return {nutrient: 0.0 for nutrient in NUTRIENTS}

    return {
        nutrient: float(analysis.get(nutrient, 0.0) or 0.0)
        for nutrient in NUTRIENTS
    }


def product_analysis_note(product: str) -> str:
    key = normalize_product_name(product)
    if key in FERTILIZER_PRODUCT_ANALYSIS:
        return (
            "Nutrient amounts were estimated from product analysis fractions "
            "in biochar_app.config.fertilizer."
        )

    return (
        "No product analysis fraction was found in biochar_app.config.fertilizer "
        "for this product, so nutrient-specific applied amounts were set to 0."
    )


def add_nutrient_totals(row: dict[str, object]) -> dict[str, object]:
    product = str(row.get("product") or "")
    applied_total_lb = numeric_value(row.get("applied_total_lb"))
    analysis = product_analysis_for(product)

    for nutrient in NUTRIENTS:
        total_col = f"{nutrient}_applied_total_lb"
        rate_col = f"{nutrient}_applied_lb_per_acre"

        row[total_col] = applied_total_lb * analysis.get(nutrient, 0.0)
        row[rate_col] = pd.NA

    return row


def add_rate_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["area_acres"] = out["strip"].map(strip_area_acres)
    out["applied_total_lb"] = pd.to_numeric(out["applied_total_lb"], errors="coerce")
    out["area_acres"] = pd.to_numeric(out["area_acres"], errors="coerce")

    out["applied_lb_per_acre"] = out["applied_total_lb"] / out["area_acres"]

    for nutrient in NUTRIENTS:
        total_col = f"{nutrient}_applied_total_lb"
        rate_col = f"{nutrient}_applied_lb_per_acre"

        if total_col not in out.columns:
            out[total_col] = 0.0
        if rate_col not in out.columns:
            out[rate_col] = pd.NA

        out[total_col] = pd.to_numeric(out[total_col], errors="coerce")
        out[rate_col] = pd.to_numeric(out[rate_col], errors="coerce")

        missing_rate = (
            out[rate_col].isna()
            & out[total_col].notna()
            & out["area_acres"].notna()
        )
        out.loc[missing_rate, rate_col] = (
            out.loc[missing_rate, total_col] / out.loc[missing_rate, "area_acres"]
        )

        missing_total = (
            out[total_col].isna()
            & out[rate_col].notna()
            & out["area_acres"].notna()
        )
        out.loc[missing_total, total_col] = (
            out.loc[missing_total, rate_col] * out.loc[missing_total, "area_acres"]
        )

    out = out.drop(columns=["area_acres"], errors="ignore")
    return out


def strip_amounts_from_row_values(
    raw: pd.DataFrame,
    row_index: int,
    strips: list[str],
    value_start_col: int = 1,
) -> dict[str, float]:
    values = [
        numeric_value(v)
        for v in raw.iloc[row_index, value_start_col:].tolist()
    ]
    values = [v for v in values if v > 0]

    if len(values) >= len(strips):
        return {
            strip: values[i]
            for i, strip in enumerate(strips)
        }

    if len(values) == 1 and strips:
        per_strip_value = values[0] / len(strips)
        return {
            strip: per_strip_value
            for strip in strips
        }

    return {
        strip: 0.0
        for strip in strips
    }


def parse_2023_sheet(workbook: Path, sheet_name: str) -> pd.DataFrame:
    year = 2023
    raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)

    rows: list[dict[str, object]] = []

    # 2023 layout:
    # row 1 = FIELD N1 (STRIPS 1 & 2)
    # rows 2:4 products
    # row 5 = FIELD N2 (STRIPS 3 & 4)
    # rows 6:8 products
    #
    # The source workbook has strip-specific entries. This parser reads
    # strip-specific product amounts where present. If a row contains only
    # one positive value for a paired strip group, that value is split across
    # the strips as a fallback.
    group_blocks = [
        ("S1_S2", ["S1", "S2"], 2, 4),
        ("S3_S4", ["S3", "S4"], 6, 8),
    ]

    for strip_group, strips, start_row, end_row in group_blocks:
        for r in range(start_row, end_row + 1):
            product = clean_product_name(raw.iat[r, 0])
            if not product or product.lower().startswith("nan"):
                continue

            strip_amounts = strip_amounts_from_row_values(
                raw=raw,
                row_index=r,
                strips=strips,
                value_start_col=1,
            )

            for strip, applied_total_lb in strip_amounts.items():
                if applied_total_lb <= 0:
                    continue

                row: dict[str, object] = {
                    "year": year,
                    "date": seasonal_date(year),
                    "strip": strip,
                    "strip_group": strip_group,
                    "location": location_for_strip(strip),
                    "product": product,
                    "applied_total_lb": applied_total_lb,
                    "notes": (
                        "Derived from 2023 fertilizer recommendation sheet using "
                        "strip-specific values where available. Workbook has no explicit "
                        f"application date. {product_analysis_note(product)}"
                    ),
                }

                row = add_nutrient_totals(row)
                rows.append(row)

    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return add_rate_columns(pd.DataFrame(rows))


def parse_2024_2025_sheet(workbook: Path, sheet_name: str, year: int) -> pd.DataFrame:
    raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)

    rows: list[dict[str, object]] = []

    # Workbook columns by strip:
    # S1 source-to-apply column = 6
    # S2 source-to-apply column = 11
    # S3 source-to-apply column = 16
    # S4 source-to-apply column = 21
    #
    # The nutrient recommendation columns in the workbook are not used for
    # nutrient totals here. Nutrient totals are estimated from applied product
    # amount and the product analysis fractions in config/fertilizer.py.
    strip_source_cols = {
        "S1": 6,
        "S2": 11,
        "S3": 16,
        "S4": 21,
    }

    # Product rows are 2:5 inclusive. Row 6 is total.
    for r in range(2, 6):
        product = clean_product_name(raw.iat[r, 0])
        if not product or product.lower().startswith("nan"):
            continue

        for strip, source_col in strip_source_cols.items():
            applied_total_lb = numeric_value(raw.iat[r, source_col])

            if applied_total_lb <= 0:
                continue

            row: dict[str, object] = {
                "year": year,
                "date": seasonal_date(year),
                "strip": strip,
                "strip_group": strip_group_for_strip(strip),
                "location": location_for_strip(strip),
                "product": product,
                "applied_total_lb": applied_total_lb,
                "notes": (
                    f"Derived from {year} fertilizer sheet using strip-level "
                    "source-to-apply values. Workbook has no explicit application date. "
                    f"{product_analysis_note(product)}"
                ),
            }

            row = add_nutrient_totals(row)
            rows.append(row)

    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return add_rate_columns(pd.DataFrame(rows))


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
    out = out.loc[out["year"].notna()].copy()
    out["year"] = out["year"].astype(int)

    numeric_cols = [
        "applied_total_lb",
        "applied_lb_per_acre",
        "n_applied_total_lb",
        "n_applied_lb_per_acre",
        "p_applied_total_lb",
        "p_applied_lb_per_acre",
        "k_applied_total_lb",
        "k_applied_lb_per_acre",
        "mn_applied_total_lb",
        "mn_applied_lb_per_acre",
        "s_applied_total_lb",
        "s_applied_lb_per_acre",
    ]

    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).round(3)

    out = out[OUTPUT_COLUMNS]
    out = out.sort_values(["year", "strip", "product"]).reset_index(drop=True)

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