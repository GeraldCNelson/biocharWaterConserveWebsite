#!/usr/bin/env python3
"""
clean_ward_master_common.py

Shared helpers for cleaning Ward/Lobato compiled lab files into a consistent,
machine-readable format for the Biochar dashboard.

Design goals
------------
* Support both:
  - Ward CSV exports that contain TWO header rows (human + machine)
  - Compiled Excel workbooks (single header row)
  - Already-clean one-header CSV files
* Produce:
  - a cleaned CSV with stable machine column names
  - a machine->human header map JSON (for UI labels / debugging)
* Normalize:
  - strip/sample id variants -> canonical lab-master values: strip_1..strip_4
  - date fields -> ISO "YYYY-MM-DD"
  - special Ward values like "Not Reported" / "<0.01" -> numeric 0 when desired

Project strip conventions
-------------------------
* ETL/logger internals: S1, S2, S3, S4
* lab/master CSV internals: strip_1, strip_2, strip_3, strip_4
* display/UI labels: STRIP 1, STRIP 2, STRIP 3, STRIP 4

Compatibility goals (important)
-------------------------------
* Preserve legacy/stable column names expected by downstream code/UI, even when
  Ward/Lobato source naming shifts. Example: ensure `nitrate_n_ppm` exists.

Notes
-----
This file is intentionally dataset-agnostic. Dataset-specific scripts should
supply:
* which source columns contain strip/date information
* which extra admin columns to drop
* any dataset-specific renaming/mapping
* optional expected-column validation
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Mapping, Sequence, TypeAlias

import json
import re

import pandas as pd


ScalarValue: TypeAlias = str | int | float | None


# ---------------------------------------------------------------------
# Admin columns to drop (case-insensitive) across ALL Ward/Lobato masters
# ---------------------------------------------------------------------
ADMIN_DROP_COLS = {
    # Common admin fields
    "customer_no", "cust_no", "cust_id", "customer_id", "account_id",
    "first_name", "last_name", "name", "company",
    "address_1", "address_2", "address", "city", "st", "state", "zip",
    "lab_no", "lab_id",
    "kind_of_sample", "feed_description", "feeder",
    "cust", "customer",
    "report_type", "report",
    "sample_id_1", "sample_id_2",

    # Soil-specific admin-ish fields
    "grower", "field_id",
    "results_for",

    # Depth columns we don’t want when enforcing fixed depth later
    "b_depth", "e_depth", "beginning_depth", "ending_depth",
}


# ---------------------------------------------------------------------
# Special Ward/Lobato string values
# ---------------------------------------------------------------------
SPECIAL_NUMERIC_ZERO_STRINGS = {
    "NOT REPORTED",
    "NOT_REPORTED",
    "<0.01",
    "< 0.01",
    "<0.1",
    "< 0.1",
    "BDL",  # below detection limit
}

COMMON_EMPTY_STRINGS = {
    "",
    "NA",
    "N/A",
    "NAN",
    "NULL",
    "NONE",
    ".",
    "-",
    "--",
}


# ---------------------------------------------------------------------
# Machine name normalization (Excel headers -> snake_case)
# ---------------------------------------------------------------------
def make_machine_name(name: str) -> str:
    """
    Convert headers like:
      "CEC/Sum of Cations me/100g" -> "cec_sum_of_cations_me_100g"
      "1:1 Soil pH"               -> "1_1_soil_ph"
      "Gram(+):Gram(-)"           -> "gram_gram"
    """
    s = str(name).strip().lower()

    # normalize a few common tokens before regex collapsing
    s = s.replace("%", "pct")
    s = s.replace("meq/100g", "meq_100g")
    s = s.replace("me/100g", "me_100g")
    s = s.replace("mg/kg", "mg_kg")

    # collapse punctuation to underscores
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# ---------------------------------------------------------------------
# Strip normalization helpers
# ---------------------------------------------------------------------
def normalize_strip(value: object) -> Optional[str]:
    """
    Convert common strip/sample-id variants to canonical lab-master strings:
      strip_1 .. strip_4

    Handles examples like:
      - "STRIP 1", "Strip1", "strip_1"
      - "S1", "S1HAY", "S1BIO"
      - "N1 STRIP 1"
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip().upper()
    if not s:
        return None

    compact = re.sub(r"[\s_\-]+", "", s)

    # explicit STRIP#
    if "STRIP" in compact:
        for d in ("1", "2", "3", "4"):
            if f"STRIP{d}" in compact:
                return f"strip_{d}"

    # S# anywhere (covers S1, S1HAY, S1BIO, etc.)
    m = re.search(r"S([1-4])", compact)
    if m:
        return f"strip_{m.group(1)}"

    return None


def canonical_strip_to_display(value: object) -> str:
    """strip_1 -> STRIP 1"""
    if value is None:
        return ""
    s = str(value).strip().lower()
    m = re.fullmatch(r"strip_([1-4])", s)
    if m:
        return f"STRIP {m.group(1)}"
    return str(value).strip()


def canonical_strip_to_etl(value: object) -> Optional[str]:
    """strip_1 -> S1"""
    if value is None:
        return None
    s = str(value).strip().lower()
    m = re.fullmatch(r"strip_([1-4])", s)
    if m:
        return f"S{m.group(1)}"
    return None


def normalize_strip_column(df: pd.DataFrame, *, strip_col: str = "strip") -> pd.DataFrame:
    """Return a copy with df[strip_col] normalized to canonical strip_n strings."""
    out = df.copy()
    if strip_col not in out.columns:
        return out
    out[strip_col] = out[strip_col].apply(normalize_strip)
    return out


def derive_strip_column(
    df: pd.DataFrame,
    *,
    source_candidates: Sequence[str],
    target_col: str = "strip",
) -> pd.DataFrame:
    """
    Create/overwrite a canonical `strip` column from the first available source
    column among `source_candidates`.
    """
    out = df.copy()
    src = first_present(out.columns, *source_candidates)
    if src is None:
        return out
    out[target_col] = out[src].apply(normalize_strip)
    return out


# ---------------------------------------------------------------------
# Date normalization
# ---------------------------------------------------------------------
def parse_to_iso_date(value: object) -> Optional[str]:
    """Parse a value into an ISO date (YYYY-MM-DD) string, or None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip()
    if not s or s.upper() in COMMON_EMPTY_STRINGS:
        return None

    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return None

    iso_date: str = ts.date().isoformat()
    return iso_date


def normalize_date_columns(
    df: pd.DataFrame,
    *,
    date_cols: Mapping[str, str],
) -> pd.DataFrame:
    """
    Rename + normalize date columns to ISO 'YYYY-MM-DD'.

    Parameters
    ----------
    date_cols:
        Mapping from *existing* column name -> *desired* column name.
        Columns missing in df are ignored.
    """
    out = df.copy()
    for src, dst in date_cols.items():
        if src not in out.columns:
            continue
        if src != dst and dst not in out.columns:
            out = out.rename(columns={src: dst})
        out[dst] = out[dst].apply(parse_to_iso_date)
    return out


# ---------------------------------------------------------------------
# Special Ward value normalization / numeric coercion
# ---------------------------------------------------------------------
def normalize_special_ward_value(
    value: ScalarValue,
    *,
    below_detection_to_zero: bool = True,
) -> ScalarValue:
    """
    Normalize special strings used in Ward/Lobato files.

    Examples:
      "Not Reported" -> 0
      "<0.01"        -> 0
      "< 0.01"       -> 0

    If not a recognized special value, the original value is returned.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value

    s = str(value).strip()
    if not s:
        return value

    sup = s.upper()
    if below_detection_to_zero and sup in SPECIAL_NUMERIC_ZERO_STRINGS:
        return 0

    return value


def normalize_special_ward_values(
    df: pd.DataFrame,
    *,
    below_detection_to_zero: bool = True,
    exclude_cols: Iterable[str] = ("strip", "date_rec", "date_rept", "date_recd"),
) -> pd.DataFrame:
    """
    Apply special-value normalization across most columns.
    """
    out = df.copy()
    exclude = {str(c) for c in exclude_cols}

    def _normalize_cell(x: ScalarValue) -> ScalarValue:
        return normalize_special_ward_value(
            x,
            below_detection_to_zero=below_detection_to_zero,
        )

    for c in out.columns:
        if c in exclude:
            continue
        out[c] = out[c].map(_normalize_cell)

    return out


def coerce_numeric_series(s: pd.Series) -> pd.Series:
    """
    Best-effort numeric coercion for columns that might include commas,
    whitespace, or stray symbols.

    Important:
    - "Not Reported" and "<0.01" should ideally be normalized *before* this
      via normalize_special_ward_values().
    """
    if s is None:
        return s

    # Ensure string, strip whitespace
    ss = s.astype(str).str.strip()

    # Treat common empties as NA
    ss = ss.replace({v: pd.NA for v in COMMON_EMPTY_STRINGS})
    ss = ss.replace({v.lower(): pd.NA for v in COMMON_EMPTY_STRINGS})

    # Remove commas
    ss = ss.str.replace(",", "", regex=False)

    return pd.to_numeric(ss, errors="coerce")


def coerce_numeric_columns(
    df: pd.DataFrame,
    *,
    exclude_cols: Iterable[str] = ("strip", "date_rec", "date_rept", "date_recd"),
) -> pd.DataFrame:
    """
    Apply numeric coercion to all non-excluded columns.
    Leaves excluded columns untouched.
    """
    out = df.copy()
    exclude = {str(c) for c in exclude_cols}

    for c in out.columns:
        if c in exclude:
            continue
        out[c] = coerce_numeric_series(out[c])

    return out


# ---------------------------------------------------------------------
# Compatibility aliases (stabilize downstream expectations)
# ---------------------------------------------------------------------
def first_present(columns: Iterable[str], *candidates: str) -> Optional[str]:
    cols = set(columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def ensure_compatibility_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add stable/legacy column names expected by downstream code/UI if they are
    missing but can be derived from newer/alternative Ward/Lobato naming.

    Key case right now:
      - Ensure `nitrate_n_ppm` exists (table expects it).
        If missing, populate it from common alternatives like `h2o_no3_n`.
    """
    out = df.copy()

    # --- NITRATE: keep UI stable ---
    if "nitrate_n_ppm" not in out.columns:
        src = first_present(
            out.columns,
            "h2o_no3_n",
            "no3_n_ppm",
            "no3_n",
            "nitrate_n",
            "nitrate_ppm",
            "no3_ppm",
        )
        if src is not None:
            out["nitrate_n_ppm"] = out[src]

    # --- pH: optional stable alias ---
    if "soil_ph" not in out.columns:
        src = first_present(out.columns, "1_1_soil_ph", "soil_ph_1_1", "ph", "soil_pH")
        if src is not None:
            out["soil_ph"] = out[src]

    # --- CEC: optional stable alias ---
    if "cec_meq_100g" not in out.columns:
        src = first_present(out.columns, "cec_sum_of_cations_me_100g", "cec_meq_100g", "cec")
        if src is not None:
            out["cec_meq_100g"] = out[src]

    return out


# ---------------------------------------------------------------------
# Depth (fixed interval)
# ---------------------------------------------------------------------
def add_fixed_depth_columns(df: pd.DataFrame, *, begin_in: int, end_in: int) -> pd.DataFrame:
    out = df.copy()
    out["begin_depth_in"] = begin_in
    out["end_depth_in"] = end_in
    return out


# ---------------------------------------------------------------------
# Admin column dropping
# ---------------------------------------------------------------------
def drop_admin_columns(
    df: pd.DataFrame,
    *,
    extra_drop: Iterable[str] | None = None,
    preserve_cols: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Drop known admin columns (case-insensitive) if present."""
    drop_set = {c.lower() for c in ADMIN_DROP_COLS}
    if extra_drop:
        drop_set |= {c.lower() for c in extra_drop}

    preserve_set = {str(c).strip().lower() for c in (preserve_cols or [])}

    to_drop = [
        c for c in df.columns
        if str(c).strip().lower() in drop_set
        and str(c).strip().lower() not in preserve_set
    ]
    return df.drop(columns=to_drop, errors="ignore")


# ---------------------------------------------------------------------
# Validation / reporting helpers
# ---------------------------------------------------------------------
def print_strip_summary(df: pd.DataFrame, *, strip_col: str = "strip") -> None:
    if strip_col not in df.columns:
        print(f"\nNo '{strip_col}' column present.")
        return

    vals = sorted(v for v in df[strip_col].dropna().astype(str).unique().tolist())
    print(f"\nUnique strip values in '{strip_col}':")
    for v in vals:
        print(f"  {v}")


def print_date_summary(df: pd.DataFrame, *, date_col: str = "date_rept") -> None:
    if date_col not in df.columns:
        print(f"\nNo '{date_col}' column present.")
        return

    s = df[date_col].dropna().astype(str)
    if s.empty:
        print(f"\nNo populated values in '{date_col}'.")
        return

    print(f"\nUnique dates in '{date_col}':")
    unique_dates: list[str] = [str(v) for v in sorted(s.unique().tolist())]
    for unique_date in unique_dates:
        print(f"  {unique_date}")

    print(f"\nCounts by '{date_col}':")
    counts = s.value_counts().sort_index()
    for count_key, count_value in counts.items():
        print(f"  {str(count_key)}: {int(count_value)}")


def report_missing_columns(df: pd.DataFrame, expected: Iterable[str]) -> list[str]:
    missing = [c for c in expected if c not in df.columns]
    if missing:
        print("\nMissing expected columns:")
        for c in missing:
            print(f"  - {c}")
    else:
        print("\nAll expected columns are present.")
    return missing


def report_unmatched_source_columns(
    df: pd.DataFrame,
    *,
    matched_output_columns: Iterable[str],
    ignore_columns: Iterable[str] = (),
) -> list[str]:
    matched = set(matched_output_columns)
    ignored = set(ignore_columns)
    unmatched = [c for c in df.columns if c not in matched and c not in ignored]

    if unmatched:
        print("\nUnmatched source columns:")
        for c in unmatched:
            print(f"  - {c}")
    else:
        print("\nNo unmatched source columns.")
    return unmatched


# ---------------------------------------------------------------------
# Ward CSV readers
# ---------------------------------------------------------------------
def read_ward_two_header_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Ward export format:
      row 0 = human headers
      row 1 = machine headers
      row 2+ = data

    Returns:
      df (machine columns), header_map (machine -> human)
    """
    raw = pd.read_csv(
        path,
        header=None,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )
    if raw.shape[0] < 3:
        raise ValueError(f"Expected >=3 rows (human header, machine header, data). Got {raw.shape[0]}")

    human = [str(x).strip() for x in raw.iloc[0, :].tolist()]
    machine = [str(x).strip() for x in raw.iloc[1, :].tolist()]
    data = raw.iloc[2:, :].copy()

    if len(machine) != data.shape[1]:
        raise ValueError("Machine header width does not match data width.")

    data.columns = machine
    header_map = {m: h for m, h in zip(machine, human)}

    # Enforce compatibility column names for downstream consumers
    data = ensure_compatibility_columns(data)

    return data, header_map


def read_clean_one_header_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Clean export format:
      single header row (already machine-readable)

    Returns:
      df, header_map where map is identity (machine -> machine)
    """
    df = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        engine="python",
    )
    header_map = {c: c for c in df.columns}

    # Enforce compatibility column names for downstream consumers
    df = ensure_compatibility_columns(df)

    return df, header_map


def read_ward_master_csv(path: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Auto-detect:
      - Try one-header clean CSV first (works if file is already cleaned)
      - If that doesn't look like a cleaned file, fall back to two-header reader
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    try:
        df1, hm1 = read_clean_one_header_csv(path)
        cols = set(df1.columns)

        if {"strip", "date_rec"} & cols:
            return df1, hm1

        if any(c.endswith("_pct_db") for c in cols):
            return df1, hm1
    except Exception:
        pass

    return read_ward_two_header_csv(path)


# ---------------------------------------------------------------------
# Excel workbook helpers (compiled soil chem / soil bio / nir)
# ---------------------------------------------------------------------
def clean_compiled_workbook(
    df: pd.DataFrame,
    *,
    admin_drop_cols: Iterable[str] = ADMIN_DROP_COLS,
    preserve_cols: Iterable[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Normalize a compiled one-header workbook.

    This is used for the Lobato-compiled soil chemistry/biology Excel files.

    - Drops purely empty columns
    - Converts headers to machine names (snake_case)
    - De-dupes collisions with suffixes (_1, _2, ...)
    - Returns (df_clean, header_map) where header_map maps machine->original
    """
    df = df.copy()
    df = df.dropna(axis=1, how="all")

    orig_cols = list(df.columns)
    new_cols: list[str] = []
    counts: dict[str, int] = {}

    for c in orig_cols:
        m = make_machine_name(c)
        if m in counts:
            counts[m] += 1
            m2 = f"{m}_{counts[m]}"
            new_cols.append(m2)
        else:
            counts[m] = 0
            new_cols.append(m)

    df.columns = new_cols

    header_map = {m: str(h) for m, h in zip(new_cols, orig_cols)}
    df = drop_admin_columns(
        df,
        extra_drop=list(admin_drop_cols),
        preserve_cols=preserve_cols,
    )

    # Enforce compatibility column names for downstream consumers
    df = ensure_compatibility_columns(df)

    return df, header_map


# ---------------------------------------------------------------------
# Higher-level cleaning helpers
# ---------------------------------------------------------------------
def standardize_ward_dataframe(
    df: pd.DataFrame,
    *,
    strip_source_candidates: Sequence[str] = ("strip", "sample_id", "sample_id_1"),
    date_cols: Mapping[str, str] | None = None,
    below_detection_to_zero: bool = True,
    extra_drop_cols: Iterable[str] | None = None,
    fixed_depth: tuple[int, int] | None = None,
    numeric_exclude_cols: Iterable[str] = ("strip", "date_rec", "date_rept", "date_recd"),
    add_compatibility_aliases: bool = True,
) -> pd.DataFrame:
    """
    Apply the common normalization steps used by Ward/Lobato cleaners.

    Important:
    Strip derivation must happen BEFORE admin-column dropping, because some
    source columns such as sample_id_1 are admin-like but still needed to
    construct canonical `strip`.
    """
    out = df.copy()

    # 1) Derive/normalize strip FIRST
    out = derive_strip_column(out, source_candidates=strip_source_candidates, target_col="strip")

    # 2) Drop admin columns AFTER strip is built
    out = drop_admin_columns(out, extra_drop=extra_drop_cols)

    # 3) Normalize dates
    if date_cols:
        out = normalize_date_columns(out, date_cols=date_cols)

    # 4) Normalize special Ward strings
    out = normalize_special_ward_values(
        out,
        below_detection_to_zero=below_detection_to_zero,
        exclude_cols=numeric_exclude_cols,
    )

    # 5) Optional fixed depth
    if fixed_depth is not None:
        out = add_fixed_depth_columns(out, begin_in=int(fixed_depth[0]), end_in=int(fixed_depth[1]))

    # 6) Numeric coercion
    out = coerce_numeric_columns(out, exclude_cols=numeric_exclude_cols)

    # 7) Compatibility aliases last
    if add_compatibility_aliases:
        out = ensure_compatibility_columns(out)

    return out


def validate_and_report(
    df: pd.DataFrame,
    *,
    strip_col: str = "strip",
    date_col: str = "date_rept",
    expected_columns: Iterable[str] | None = None,
    matched_output_columns: Iterable[str] | None = None,
    ignore_unmatched_columns: Iterable[str] = (),
) -> dict[str, list[str]]:
    """
    Convenience reporting wrapper for cleaners.
    """
    print_strip_summary(df, strip_col=strip_col)
    print_date_summary(df, date_col=date_col)

    missing: list[str] = []
    unmatched: list[str] = []

    if expected_columns is not None:
        missing = report_missing_columns(df, expected_columns)

    if matched_output_columns is not None:
        unmatched = report_unmatched_source_columns(
            df,
            matched_output_columns=matched_output_columns,
            ignore_columns=ignore_unmatched_columns,
        )

    return {
        "missing_columns": missing,
        "unmatched_columns": unmatched,
    }


# ---------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------
def write_clean_outputs(
    df: pd.DataFrame,
    header_map: dict[str, str],
    *,
    out_csv: Path,
    out_headers_json: Optional[Path] = None,
) -> None:
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_csv, index=False)

    if out_headers_json is not None:
        out_headers_json = Path(out_headers_json)
        out_headers_json.parent.mkdir(parents=True, exist_ok=True)
        with out_headers_json.open("w", encoding="utf-8") as f:
            json.dump(header_map, f, indent=2, ensure_ascii=False)