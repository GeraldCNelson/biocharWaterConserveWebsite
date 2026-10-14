#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from biochar_app.config.core import STRIP_AREA_ACRES
from biochar_app.config.fertilizer import (
    FERTILIZER_PRODUCT_ALIASES,
    FERTILIZER_PRODUCT_ANALYSIS,
    FERTILIZER_PRODUCT_GRADE,
)
from biochar_app.config.paths import (
    BIOCHAR_MASTER_WORKBOOK,
    FERTILIZER_CSV_OUT,
    FERTILIZER_DIR,
)

DEFAULT_INPUT_WORKBOOK = BIOCHAR_MASTER_WORKBOOK
DEFAULT_OUTPUT_CSV = FERTILIZER_CSV_OUT
DEFAULT_BACKUP_DIR = FERTILIZER_DIR / "backup"

NUTRIENTS = ["n", "p", "k", "mn", "s"]
SHEET_PATTERN = re.compile(r"^(2023|2024|2025|2026)\s+FERTILIZING$", re.I)

OUTPUT_COLUMNS = [
    "year",
    "date",
    "record_type",
    "activity",
    "scope",
    "strip",
    "strip_group",
    "location",
    "source_sheet",
    "source_cell",
    "source_product",
    "product",
    "recommended_product_lb_per_acre",
    "recommended_product_total_lb",
    "estimated_n_lb_per_acre",
    "estimated_n_total_lb",
    "estimated_p_lb_per_acre",
    "estimated_p_total_lb",
    "estimated_k_lb_per_acre",
    "estimated_k_total_lb",
    "estimated_mn_lb_per_acre",
    "estimated_mn_total_lb",
    "estimated_s_lb_per_acre",
    "estimated_s_total_lb",
    "nutrient_basis",
    "calculation_method",
    "notes",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build recommendation-derived fertilizer product requirements from "
            "the annual FERTILIZING sheets in the master workbook."
        )
    )
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


def numeric_value(value: object) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return 0.0
    return float(parsed)


def normalize_product_name(value: object) -> str:
    product = str(value or "").strip().upper()
    return " ".join(product.split())


def canonical_product_name(value: object) -> str:
    normalized = normalize_product_name(value)
    return FERTILIZER_PRODUCT_ALIASES.get(normalized, normalized)


def _excel_column_name(column_number: int) -> str:
    name = ""
    while column_number:
        column_number, remainder = divmod(column_number - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _product_rates_from_targets(
    *,
    n_lb_per_acre: float,
    p2o5_lb_per_acre: float,
    k2o_lb_per_acre: float,
    mn_lb_per_acre: float,
) -> dict[str, float]:
    """Return product rates needed to meet one strip's nutrient targets."""
    phosphate_grade = FERTILIZER_PRODUCT_GRADE["11-52-0"]
    phosphate_rate = p2o5_lb_per_acre / phosphate_grade["p2o5"]

    # The phosphate source also supplies N. Subtract that N before calculating
    # the urea requirement, then prevent negative product recommendations.
    n_from_phosphate = phosphate_rate * phosphate_grade["n"]
    urea_rate = max(
        (n_lb_per_acre - n_from_phosphate)
        / FERTILIZER_PRODUCT_GRADE["UREA"]["n"],
        0.0,
    )

    return {
        "UREA": urea_rate,
        "11-52-0": phosphate_rate,
        "SULFATE OF POTASH": (
            k2o_lb_per_acre
            / FERTILIZER_PRODUCT_GRADE["SULFATE OF POTASH"]["k2o"]
        ),
        "MANGANESE SULFATE (MNSO4)": (
            mn_lb_per_acre
            / FERTILIZER_PRODUCT_GRADE["MANGANESE SULFATE (MNSO4)"]["mn"]
        ),
    }


def _recommendation_row(
    *,
    year: int,
    strip: str,
    source_sheet: str,
    source_cell: str,
    source_product: object,
    product: str,
    product_rate: float,
) -> dict[str, object]:
    area_acres = float(STRIP_AREA_ACRES[strip])
    analysis = FERTILIZER_PRODUCT_ANALYSIS[product]

    row: dict[str, object] = {
        "year": year,
        # The workbook contains no explicit application date. A blank is
        # intentional: recommendations are not evidence of applications.
        "date": pd.NA,
        "record_type": "recommendation",
        "activity": "fertilizer_recommendation",
        "scope": "strip",
        "strip": strip,
        "strip_group": strip_group_for_strip(strip),
        "location": location_for_strip(strip),
        "source_sheet": source_sheet,
        "source_cell": source_cell,
        "source_product": str(source_product or "").strip(),
        "product": product,
        "recommended_product_lb_per_acre": product_rate,
        "recommended_product_total_lb": product_rate * area_acres,
        "nutrient_basis": (
            "elemental mass; P and K converted from label-basis P2O5 and K2O"
        ),
        "calculation_method": (
            "recalculated from strip nutrient targets and stated product grades"
        ),
        "notes": (
            "Recommendation-derived product requirement; not evidence of an "
            "application. Date is intentionally blank; the field-level activity "
            "date does not confirm that this product and rate were applied to "
            "this strip."
        ),
    }

    for nutrient in NUTRIENTS:
        nutrient_rate = product_rate * float(analysis.get(nutrient, 0.0) or 0.0)
        row[f"estimated_{nutrient}_lb_per_acre"] = nutrient_rate
        row[f"estimated_{nutrient}_total_lb"] = nutrient_rate * area_acres

    return row


def _append_strip_rows(
    rows: list[dict[str, object]],
    *,
    year: int,
    strip: str,
    source_sheet: str,
    targets: tuple[float, float, float, float],
    source_products: dict[str, object],
    source_cells: dict[str, str],
) -> None:
    rates = _product_rates_from_targets(
        n_lb_per_acre=targets[0],
        p2o5_lb_per_acre=targets[1],
        k2o_lb_per_acre=targets[2],
        mn_lb_per_acre=targets[3],
    )

    for product, product_rate in rates.items():
        if product_rate <= 0:
            continue
        rows.append(
            _recommendation_row(
                year=year,
                strip=strip,
                source_sheet=source_sheet,
                source_cell=source_cells[product],
                source_product=source_products[product],
                product=product,
                product_rate=product_rate,
            )
        )


def parse_2023_sheet(workbook: Path, sheet_name: str) -> pd.DataFrame:
    raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)
    rows: list[dict[str, object]] = []

    # The 2023 sheet supplies group-level N and P2O5 targets for each strip
    # pair, plus separate K2O targets for the two strips in each pair.
    group_specs = [
        (1, ("S1", "S2"), (3, 4)),
        (5, ("S3", "S4"), (3, 4)),
    ]

    for target_row, strips, potassium_columns in group_specs:
        n_target = numeric_value(raw.iat[target_row, 1])
        p2o5_target = numeric_value(raw.iat[target_row, 2])
        source_products = {
            "UREA": raw.iat[target_row + 1, 0],
            "11-52-0": raw.iat[target_row + 2, 0],
            "SULFATE OF POTASH": raw.iat[target_row + 3, 0],
            "MANGANESE SULFATE (MNSO4)": "",
        }
        excel_target_row = target_row + 1

        for strip, potassium_col in zip(strips, potassium_columns):
            k2o_target = numeric_value(raw.iat[target_row, potassium_col])
            column_letter = "D" if potassium_col == 3 else "E"
            _append_strip_rows(
                rows,
                year=2023,
                strip=strip,
                source_sheet=sheet_name,
                targets=(n_target, p2o5_target, k2o_target, 0.0),
                source_products=source_products,
                source_cells={
                    "UREA": f"B{excel_target_row}:C{excel_target_row}",
                    "11-52-0": f"C{excel_target_row}",
                    "SULFATE OF POTASH": f"{column_letter}{excel_target_row}",
                    "MANGANESE SULFATE (MNSO4)": "",
                },
            )

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def parse_modern_sheet(workbook: Path, sheet_name: str, year: int) -> pd.DataFrame:
    raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)
    rows: list[dict[str, object]] = []

    # Each strip block has N, P, K, and Mn target columns. Source-product cells
    # are deliberately ignored because several workbook formulas are wrong.
    strip_specs = {
        "S1": (2, ("C3:D4", "D4", "E5", "F6")),
        "S2": (7, ("H3:I4", "I4", "J5", "K6")),
        "S3": (12, ("M3:N4", "N4", "O5", "P6")),
        "S4": (17, ("R3:S4", "S4", "T5", "U6")),
    }
    source_products = {
        "UREA": raw.iat[2, 0],
        "11-52-0": raw.iat[3, 0],
        "SULFATE OF POTASH": raw.iat[4, 0],
        "MANGANESE SULFATE (MNSO4)": raw.iat[5, 0],
    }

    for strip, (start_col, cell_refs) in strip_specs.items():
        targets = tuple(
            numeric_value(raw.iat[row_index, start_col + nutrient_index])
            for nutrient_index, row_index in enumerate((2, 3, 4, 5))
        )
        source_cells = dict(zip(source_products, cell_refs))
        _append_strip_rows(
            rows,
            year=year,
            strip=strip,
            source_sheet=sheet_name,
            targets=targets,
            source_products=source_products,
            source_cells=source_cells,
        )

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def parse_fertilization_activity_dates(workbook: Path) -> pd.DataFrame:
    """Read field-level fertilization dates without inferring products or strips."""
    xls = pd.ExcelFile(workbook, engine="openpyxl")
    sheet_name = next(
        (
            name
            for name in xls.sheet_names
            if str(name).strip().upper() == "FIELD ACTIVITY DATES"
        ),
        None,
    )
    if sheet_name is None:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    raw = pd.read_excel(workbook, sheet_name=sheet_name, header=None)
    activity_row = next(
        (
            row_index
            for row_index, value in raw.iloc[:, 0].items()
            if normalize_product_name(value) == "FERTILIZATION"
        ),
        None,
    )
    if activity_row is None:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    rows: list[dict[str, object]] = []
    for column_index in range(1, raw.shape[1]):
        year_value = pd.to_numeric(
            pd.Series([raw.iat[0, column_index]]), errors="coerce"
        ).iloc[0]
        activity_date = pd.to_datetime(
            raw.iat[activity_row, column_index], errors="coerce"
        )
        if pd.isna(year_value) or pd.isna(activity_date):
            continue

        year = int(year_value)
        if int(activity_date.year) != year:
            source_cell = f"{_excel_column_name(column_index + 1)}{activity_row + 1}"
            raise ValueError(
                f"Fertilization date year mismatch in {sheet_name}!{source_cell}: "
                f"header={year}, date={activity_date.date().isoformat()}"
            )

        row = {column: pd.NA for column in OUTPUT_COLUMNS}
        row.update(
            {
                "year": year,
                "date": activity_date.date().isoformat(),
                "record_type": "fertilization_activity",
                "activity": "fertilization",
                "scope": "field",
                "location": "field",
                "source_sheet": sheet_name,
                "source_cell": (
                    f"{_excel_column_name(column_index + 1)}{activity_row + 1}"
                ),
                "calculation_method": "recorded field activity date",
                "notes": (
                    "Field-level fertilization activity date. The source does not "
                    "identify products, rates, amounts, or strip-specific scope."
                ),
            }
        )
        rows.append(row)

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def build_fertilizer_clean(input_workbook: Path) -> pd.DataFrame:
    if not input_workbook.exists():
        raise FileNotFoundError(f"Input fertilizer workbook not found: {input_workbook}")

    xls = pd.ExcelFile(input_workbook, engine="openpyxl")
    frames: list[pd.DataFrame] = []

    for sheet_name in xls.sheet_names:
        match = SHEET_PATTERN.match(str(sheet_name).strip())
        if not match:
            continue
        year = int(match.group(1))
        if year == 2023:
            frames.append(parse_2023_sheet(input_workbook, sheet_name))
        else:
            frames.append(parse_modern_sheet(input_workbook, sheet_name, year))

    frames.append(parse_fertilization_activity_dates(input_workbook))

    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Drop frame-local all-missing columns before concatenation. The union is
    # restored below, and this avoids pandas' deprecated dtype inference for
    # the intentionally sparse activity rows.
    out = pd.concat(
        [frame.dropna(axis=1, how="all") for frame in frames],
        ignore_index=True,
    )
    numeric_columns = [
        column
        for column in OUTPUT_COLUMNS
        if column.endswith("_lb_per_acre") or column.endswith("_total_lb")
    ]
    for column in numeric_columns:
        out[column] = pd.to_numeric(out[column], errors="coerce").round(3)

    out = out[OUTPUT_COLUMNS]
    out["_record_order"] = out["record_type"].map(
        {"fertilization_activity": 0, "recommendation": 1}
    )
    out = out.sort_values(
        ["year", "_record_order", "strip", "product"], na_position="last"
    )
    return out.drop(columns="_record_order").reset_index(drop=True)


def write_csv_atomic(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".csv", dir=destination.parent
    )
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        frame.to_csv(temporary, index=False)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def build_and_install_fertilizer(
    input_workbook: Path = DEFAULT_INPUT_WORKBOOK,
    output_csv: Path = DEFAULT_OUTPUT_CSV,
) -> dict[str, object]:
    frame = build_fertilizer_clean(input_workbook)
    write_csv_atomic(frame, output_csv)

    activities = frame.loc[frame["record_type"] == "fertilization_activity"]
    recommendations = frame.loc[frame["record_type"] == "recommendation"]
    activity_dates = pd.to_datetime(activities["date"], errors="coerce").dropna()
    return {
        "rows": len(frame),
        "recommendation_rows": len(recommendations),
        "activity_rows": len(activities),
        "latest_activity_date": (
            activity_dates.max().date().isoformat() if not activity_dates.empty else None
        ),
        "output_csv": str(output_csv),
    }


def main() -> int:
    args = parse_args()
    df = build_fertilizer_clean(args.input)

    print("\n--- Fertilizer data summary ---")
    print(f"Rows: {len(df)}")
    if not df.empty:
        print(f"Years: {sorted(df['year'].unique().tolist())}")
        print(df.groupby(["year", "record_type"]).size().to_string())

    if args.dry_run:
        print("\nDry run only. No files written.")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    backup_path = make_backup(args.output, args.backup_dir)
    write_csv_atomic(df, args.output)

    if backup_path:
        print(f"\nBackup written: {backup_path}")
    print(f"Output written: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
