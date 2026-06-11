#!/usr/bin/env python3

"""

extract_management_data.py

Extract clean irrigation and fertilizer CSVs from the raw

biochar-data-master.xlsx workbook.

Outputs

-------

- IRRIGATION_CSV

- FERTILIZER_CSV

Design

------

- Source workbook remains the raw authority.

- This script extracts only the rows/columns needed by the app.

- Output CSVs become the stable app-facing datasets.

Run

---

python tools/extract_management_data.py

"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from biochar_app.config.paths import (
    BIOCHAR_MASTER_WORKBOOK,
    MANAGEMENT_PROCESSED_DIR,
    IRRIGATION_CSV,
    FERTILIZER_CSV,
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def make_machine_name(name: object) -> str:
    s = str(name).strip().lower()
    s = s.replace("%", "pct")
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def choose_first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def drop_empty_rows_and_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.dropna(axis=0, how="all")
    out = out.dropna(axis=1, how="all")
    return out


def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    orig = list(out.columns)

    counts: dict[str, int] = {}
    new_cols: list[str] = []

    for c in orig:
        m = make_machine_name(c)
        if m in counts:
            counts[m] += 1
            m = f"{m}_{counts[m]}"
        else:
            counts[m] = 0
        new_cols.append(m)

    out.columns = new_cols
    return out


def try_find_header_row(raw: pd.DataFrame, required_tokens: list[str], max_scan_rows: int = 30) -> int:
    """
    Heuristic: find the first row whose values collectively contain several
    expected header tokens.
    """
    limit = min(max_scan_rows, len(raw))
    best_idx = 0
    best_score = -1

    for i in range(limit):
        vals = [make_machine_name(v) for v in raw.iloc[i].tolist()]
        row_text = " ".join(vals)
        score = sum(1 for tok in required_tokens if tok in row_text)
        if score > best_score:
            best_score = score
            best_idx = i

    return best_idx


def load_sheet_with_detected_header(sheet_name: str, required_tokens: list[str]) -> pd.DataFrame:
    raw = pd.read_excel(BIOCHAR_MASTER_WORKBOOK, sheet_name=sheet_name, header=None)
    raw = drop_empty_rows_and_cols(raw)

    header_row = try_find_header_row(raw, required_tokens=required_tokens)
    header_vals = raw.iloc[header_row].tolist()

    df = raw.iloc[header_row + 1 :].copy()
    df.columns = header_vals
    df = drop_empty_rows_and_cols(df)
    df = clean_headers(df)

    return df


def extract_year_from_sheet_name(sheet_name: str) -> int | None:
    m = re.search(r"(20\d{2})", str(sheet_name))
    if not m:
        return None
    return int(m.group(1))


def normalize_strip_group(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    s = str(value).strip().upper()
    if not s:
        return None

    compact = s.replace(" ", "").replace("_", "").replace("-", "")

    if compact in {"1&2", "1AND2", "S1&S2", "S1ANDS2"}:
        return "S1_S2"
    if compact in {"3&4", "3AND4", "S3&S4", "S3ANDS4"}:
        return "S3_S4"

    return None


def normalize_strip(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    s = str(value).strip().upper()
    if not s:
        return None

    compact = s.replace(" ", "").replace("_", "").replace("-", "")

    if "STRIP1" in compact or compact == "S1":
        return "S1"
    if "STRIP2" in compact or compact == "S2":
        return "S2"
    if "STRIP3" in compact or compact == "S3":
        return "S3"
    if "STRIP4" in compact or compact == "S4":
        return "S4"

    return None


def strip_treatment_map(strip: str) -> str:
    return {
        "S1": "biochar_half_water",
        "S2": "control_half_water",
        "S3": "biochar_full_water",
        "S4": "control_full_water",
    }.get(strip, "")


def _coerce_time_string(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()


def _build_event_timestamp(date_series: pd.Series, time_series: pd.Series) -> pd.Series:
    date_part = pd.to_datetime(date_series, errors="coerce").dt.strftime("%Y-%m-%d")
    return pd.to_datetime(date_part + " " + _coerce_time_string(time_series), errors="coerce")


# ---------------------------------------------------------------------
# Workbook sheet discovery
# ---------------------------------------------------------------------

def get_workbook_sheets() -> list[str]:
    xls = pd.ExcelFile(BIOCHAR_MASTER_WORKBOOK, engine="openpyxl")
    return list(xls.sheet_names)


def irrigation_sheet_names() -> list[str]:
    return [s for s in get_workbook_sheets() if "IRRIGATION" in str(s).upper()]


def fertilizer_sheet_names() -> list[str]:
    return [s for s in get_workbook_sheets() if "FERTIL" in str(s).upper()]


# ---------------------------------------------------------------------
# Irrigation extraction
# ---------------------------------------------------------------------

def extract_irrigation_sheet(sheet_name: str) -> pd.DataFrame:
    year = extract_year_from_sheet_name(sheet_name)
    if year is None:
        raise ValueError(f"Could not determine year from irrigation sheet name: {sheet_name}")

    df = load_sheet_with_detected_header(
        sheet_name,
        required_tokens=["date", "time", "gal", "location"],
    ).copy()

    date_col = choose_first_existing(df, ["date"])
    strip_group_col = choose_first_existing(df, ["strip_i_d", "strip_id"])
    location_col = choose_first_existing(df, ["location"])
    time_on_col = choose_first_existing(df, ["time_on"])
    time_off_col = choose_first_existing(df, ["time_off"])
    gallons_col = choose_first_existing(df, ["gal_used_x_100"])
    notes_col = choose_first_existing(df, ["notes"])

    missing = [
        name for name, val in [
            ("date", date_col),
            ("strip_i_d", strip_group_col),
            ("time_on", time_on_col),
            ("time_off", time_off_col),
            ("gal_used_x_100", gallons_col),
        ]
        if val is None
    ]
    if missing:
        raise ValueError(f"Irrigation sheet {sheet_name!r} missing required columns: {missing}")

    out = pd.DataFrame()
    out["year"] = year
    out["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date.astype("string")
    out["start_timestamp"] = _build_event_timestamp(df[date_col], df[time_on_col])
    out["end_timestamp"] = _build_event_timestamp(df[date_col], df[time_off_col])
    out["strip_group"] = df[strip_group_col].map(normalize_strip_group)

    if location_col is not None:
        out["location"] = (
            df[location_col]
            .astype("string")
            .str.strip()
            .str.lower()
        )
    else:
        out["location"] = pd.Series([""] * len(df), dtype="string")

    # Enforce canonical location from strip_group
    location_from_group = {
        "S1_S2": "west",
        "S3_S4": "east",
    }

    out["location"] = out["strip_group"].map(location_from_group).astype("string")

    # Keep gallons exactly as recorded in workbook for now.
    out["gallons"] = pd.to_numeric(df[gallons_col], errors="coerce")

    if notes_col is not None:
        out["notes"] = df[notes_col].astype("string").str.strip()
    else:
        out["notes"] = pd.Series([""] * len(df), dtype="string")

    # Handle overnight irrigation: if end < start, roll end forward one day
    mask = (
        out["start_timestamp"].notna()
        & out["end_timestamp"].notna()
        & (out["end_timestamp"] < out["start_timestamp"])
    )
    out.loc[mask, "end_timestamp"] = out.loc[mask, "end_timestamp"] + pd.Timedelta(days=1)

    # Keep only valid irrigation events
    out = out.dropna(subset=["strip_group", "start_timestamp"]).copy()

    keep_mask = out["gallons"].notna() | out["notes"].fillna("").astype(str).str.strip().ne("")
    out = out.loc[keep_mask].copy()

    out = out.sort_values(["start_timestamp", "strip_group"], kind="stable").reset_index(drop=True)
    return out


def extract_irrigation() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for sheet_name in irrigation_sheet_names():
        try:
            frames.append(extract_irrigation_sheet(sheet_name))
        except Exception as e:
            print(f"⚠️ Skipping irrigation sheet {sheet_name!r}: {e}")

    if not frames:
        raise ValueError("No usable irrigation sheets found in workbook.")

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["year", "start_timestamp", "strip_group"], kind="stable").reset_index(drop=True)
    return out


# ---------------------------------------------------------------------
# Fertilizer extraction
# ---------------------------------------------------------------------

def extract_fertilizer_sheet(sheet_name: str) -> pd.DataFrame:
    year = extract_year_from_sheet_name(sheet_name)
    if year is None:
        raise ValueError(f"Could not determine year from fertilizer sheet name: {sheet_name}")

    df = load_sheet_with_detected_header(
        sheet_name,
        required_tokens=["fertilizer", "source", "strip", "lbs", "apply"],
    ).copy()

    product_col = choose_first_existing(
        df,
        ["element_lbs_acre_rate", "element_lbs_acre"],
    )
    s1_col = choose_first_existing(df, ["lbs_of_source_to_apply"])
    s2_col = choose_first_existing(df, ["lbs_of_source_to_apply_1"])
    s3_col = choose_first_existing(df, ["lbs_of_source_to_apply_2"])
    s4_col = choose_first_existing(df, ["lbs_of_source_to_apply_3"])

    missing = [
        name for name, val in [
            ("product", product_col),
            ("s1", s1_col),
            ("s2", s2_col),
            ("s3", s3_col),
            ("s4", s4_col),
        ]
        if val is None
    ]
    if missing:
        raise ValueError(f"Fertilizer sheet {sheet_name!r} missing required columns: {missing}")

    base = df[[product_col, s1_col, s2_col, s3_col, s4_col]].copy()
    base = base.rename(columns={product_col: "product"})

    # Remove subtotal/summary rows
    product_text = base["product"].astype(str).str.strip()
    bad_rows = product_text.eq("") | product_text.str.contains(
        r"TOTAL|BAGS|COST|RECOMMENDATION|ELEMENT",
        case=False,
        regex=True,
        na=False,
    )
    base = base.loc[~bad_rows].copy()

    rows: list[dict[str, object]] = []
    strip_map = {
        "S1": s1_col,
        "S2": s2_col,
        "S3": s3_col,
        "S4": s4_col,
    }

    for _, row in base.iterrows():
        product = str(row["product"]).strip()
        for strip, col in strip_map.items():
            lbs_product = pd.to_numeric(pd.Series([row[col]]), errors="coerce").iloc[0]
            if pd.isna(lbs_product):
                continue
            rows.append(
                {
                    "year": year,
                    "strip": strip,
                    "treatment": strip_treatment_map(strip),
                    "product": product,
                    "lbs_product": float(lbs_product),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["year", "strip", "treatment", "product", "lbs_product"])

    out = out.sort_values(["year", "strip", "product"], kind="stable").reset_index(drop=True)
    return out


def extract_fertilizer() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for sheet_name in fertilizer_sheet_names():
        try:
            frames.append(extract_fertilizer_sheet(sheet_name))
        except Exception as e:
            print(f"⚠️ Skipping fertilizer sheet {sheet_name!r}: {e}")

    if not frames:
        raise ValueError("No usable fertilizer sheets found in workbook.")

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["year", "strip", "product"], kind="stable").reset_index(drop=True)
    return out


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    if not BIOCHAR_MASTER_WORKBOOK.exists():
        raise FileNotFoundError(f"Workbook not found: {BIOCHAR_MASTER_WORKBOOK}")

    MANAGEMENT_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    irrigation_df = None
    fertilizer_df = None

    # --- irrigation ---
    try:
        irrigation_df = extract_irrigation()
        irrigation_df.to_csv(IRRIGATION_CSV, index=False)

        print(f"✅ Wrote irrigation CSV: {IRRIGATION_CSV}")
        print(f"   rows={len(irrigation_df)} cols={len(irrigation_df.columns)}")
        print(
            f"   strip groups="
            f"{sorted(irrigation_df['strip_group'].dropna().unique().tolist()) if not irrigation_df.empty else []}"
        )
    except Exception as e:
        print(f"❌ Irrigation extraction failed: {e}")

    # --- fertilizer ---
    try:
        fertilizer_df = extract_fertilizer()
        fertilizer_df.to_csv(FERTILIZER_CSV, index=False)

        print(f"✅ Wrote fertilizer CSV: {FERTILIZER_CSV}")
        print(f"   rows={len(fertilizer_df)} cols={len(fertilizer_df.columns)}")
        print(
            f"   years="
            f"{sorted(fertilizer_df['year'].dropna().unique().tolist()) if not fertilizer_df.empty else []}"
        )
    except Exception as e:
        print(f"⚠️ Fertilizer extraction skipped/failed: {e}")

    if irrigation_df is None and fertilizer_df is None:
        raise ValueError("Neither irrigation nor fertilizer extraction succeeded.")


if __name__ == "__main__":
    main()