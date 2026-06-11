#!/usr/bin/env python3
"""
update_ward_master_soilchem.py

Update the compiled Soil Chemistry workbook into a canonical machine-readable CSV
for the Biochar dashboard.

Code to run in a terminal:

    python -m biochar_app.scripts.lab.update_ward_master_soilchem

Conventions
-----------
* lab/master CSV strip values are canonicalized to:
    strip_1, strip_2, strip_3, strip_4

Key behaviors
-------------
* Reads the compiled soil chemistry workbook
* Preserves Sample ID through the first pass
* Builds canonical strip from Sample ID
* Drops non-project rows whose Sample ID does not resolve to strip_1..strip_4
* Normalizes date fields to YYYY-MM-DD
* Enforces fixed depth 0–8 inches
* Creates stable columns used by downstream table builders
* Optionally merges known supplemental 2026 chemistry/SHA files
* Writes:
    - cleaned master CSV
    - machine->human headers JSON
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence
import re
import pandas as pd

from biochar_app.config.paths import SOIL_CHEM_RAW_DIR, SOIL_CHEM_PROCESSED_DIR
from biochar_app.config.lab_source_mappings import (
    RAW_TO_CANONICAL_SOILCHEM,
    DROP_COLUMNS_SOILCHEM,
    EXPECTED_SOILCHEM_COLUMNS,
)

from biochar_app.scripts.lab.clean_ward_master_common import (
    clean_compiled_workbook,
    standardize_ward_dataframe,
    validate_and_report,
    write_clean_outputs,
    normalize_strip,
)

MASTER_XLSX = SOIL_CHEM_RAW_DIR / "Lobato - Soil chemistry results compiled.xlsx"
OUT_CLEAN_CSV = SOIL_CHEM_PROCESSED_DIR / "ward_master_soilchem_clean.csv"
OUT_HEADERS_JSON = SOIL_CHEM_PROCESSED_DIR / "ward_master_soilchem_headers_machine_to_human.json"

# Add known 2026 supplemental files here.
# The script skips any listed file that does not exist.
SUPPLEMENTAL_SOILCHEM_FILES: Sequence[Path] = [
    SOIL_CHEM_RAW_DIR / "Soil_2025-11-03.csv",
    SOIL_CHEM_RAW_DIR / "Soil_2026-04-28.csv",
    ]


def _snake_col(name: str) -> str:
    text = str(name).strip().lower()
    text = text.replace("%", " pct ")
    text = text.replace(":", " ")
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    text = text.replace("(", " ")
    text = text.replace(")", " ")
    text = text.replace(".", " ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text

def _apply_raw_to_canonical_map(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply RAW_TO_CANONICAL by coalescing source values into canonical columns.

    This follows the soil-bio pattern conceptually:
    raw/source-specific columns are used to populate one canonical field.

    If the canonical destination already exists, fill its missing values from
    the source column instead of creating duplicate columns.
    """
    out = df.copy()

    for src, dst in RAW_TO_CANONICAL_SOILCHEM.items():
        if src not in out.columns:
            continue

        if dst in out.columns:
            out[dst] = out[dst].where(out[dst].notna(), out[src])
            if src != dst:
                out = out.drop(columns=[src], errors="ignore")
        else:
            out = out.rename(columns={src: dst})

    out = out.drop(
        columns=[c for c in out.columns if c in DROP_COLUMNS_SOILCHEM],
        errors="ignore",
    )

    out = out.loc[:, ~out.columns.duplicated()].copy()

    return out

def _find_machine_col_by_human_header(
        header_map: dict[str, str],
        human_name: str,
) -> Optional[str]:
    target = " ".join(str(human_name).strip().split()).lower()

    for machine, human in header_map.items():
        h = " ".join(str(human).strip().split()).lower()
        if h == target:
            return machine

    return None


def _ensure_sample_id_column(
        df_clean: pd.DataFrame,
        header_map: dict[str, str],
) -> pd.DataFrame:
    out = df_clean.copy()

    sample_candidates = [
        "sample_id",
        "sample_id_1",
        "sample_id_2",
        "sample_id_3",
    ]

    for col in sample_candidates:
        if col not in out.columns:
            continue

        normalized = out[col].apply(normalize_strip)
        if normalized.notna().sum() > 0:
            out["sample_id"] = out[col].astype(str)
            print(f"🔎 Using '{col}' as soil chem Sample ID source")
            return out

    machine_candidates = []
    for human_candidate in ("Sample ID", "Sample ID 1", "Sample ID 2", "Sample ID 3"):
        machine = _find_machine_col_by_human_header(header_map, human_candidate)
        if machine is not None:
            machine_candidates.append(machine)

    for machine in machine_candidates:
        if machine not in out.columns:
            continue

        normalized = out[machine].apply(normalize_strip)
        if normalized.notna().sum() > 0:
            out["sample_id"] = out[machine].astype(str)
            print(f"🔎 Using '{machine}' as soil chem Sample ID source")
            return out

    sampleish = [c for c in out.columns if "sample" in c.lower()]
    raise ValueError(
        "Could not locate a Sample ID column that resolves to strip_1..strip_4. "
        f"Columns containing 'sample': {sampleish}"
    )


def _filter_to_project_rows(df_clean: pd.DataFrame) -> pd.DataFrame:
    out = df_clean.copy()

    if "sample_id" not in out.columns:
        raise ValueError("Expected 'sample_id' before filtering project rows.")

    out["strip"] = out["sample_id"].apply(normalize_strip)
    mask = out["strip"].notna()

    kept = int(mask.sum())
    dropped = int((~mask).sum())
    print(f"🔎 Soil Chem project-row filter: kept={kept}, dropped={dropped}")

    out = out.loc[mask].copy()
    out["sample_id"] = out["sample_id"].astype(str)

    return out


def _drop_blank_key_rows(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Remove footer/comment/blank rows that do not have both strip and date_rec.
    """
    out = df_clean.copy()

    if "strip" not in out.columns or "date_rec" not in out.columns:
        return out

    mask = (
            out["strip"].fillna("").astype(str).str.strip().ne("")
            & out["date_rec"].fillna("").astype(str).str.strip().ne("")
    )

    dropped = int((~mask).sum())
    if dropped:
        print(f"🧹 Dropped {dropped} blank/footer rows after standardization")

    return out.loc[mask].copy()


def _ensure_expected_soilchem_columns(df_clean: pd.DataFrame) -> pd.DataFrame:
    out = df_clean.copy()

    if "1_1_soil_ph" in out.columns:
        if "soil_ph_1_1" not in out.columns:
            out["soil_ph_1_1"] = out["1_1_soil_ph"]
        else:
            out["soil_ph_1_1"] = out["soil_ph_1_1"].where(
                out["soil_ph_1_1"].notna(),
                out["1_1_soil_ph"],
            )

    if "1_1_s_salts_mmho_cm" in out.columns:
        if "ec_1_1" not in out.columns:
            out["ec_1_1"] = out["1_1_s_salts_mmho_cm"]
        else:
            out["ec_1_1"] = out["ec_1_1"].where(
                out["ec_1_1"].notna(),
                out["1_1_s_salts_mmho_cm"],
            )

    if "cec_sum_of_cations_me_100g" in out.columns:
        if "cec_meq_100g" not in out.columns:
            out["cec_meq_100g"] = out["cec_sum_of_cations_me_100g"]
        else:
            out["cec_meq_100g"] = out["cec_meq_100g"].where(
                out["cec_meq_100g"].notna(),
                out["cec_sum_of_cations_me_100g"],
            )

        if "sum_of_cations_meq_100g" not in out.columns:
            out["sum_of_cations_meq_100g"] = out["cec_sum_of_cations_me_100g"]
        else:
            out["sum_of_cations_meq_100g"] = out["sum_of_cations_meq_100g"].where(
                out["sum_of_cations_meq_100g"].notna(),
                out["cec_sum_of_cations_me_100g"],
            )

        if "h2o_no3_n" in out.columns:
            if "nitrate_n_ppm" not in out.columns:
                out["nitrate_n_ppm"] = out["h2o_no3_n"]
            else:
                out["nitrate_n_ppm"] = out["nitrate_n_ppm"].where(
                    out["nitrate_n_ppm"].notna(),
                    out["h2o_no3_n"],
                )

        if "nh4_no3_ratio" not in out.columns:
            out["nh4_no3_ratio"] = pd.NA

    return out


def _standardize_soilchem_dataframe(
    df_raw: pd.DataFrame,
    compiled_master: bool = True,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Shared cleaning logic for both the compiled master and supplemental files.
    """
    if compiled_master:
        df_clean, header_map = clean_compiled_workbook(df_raw, admin_drop_cols=[])
    else:
        header_map = {_snake_col(c): str(c) for c in df_raw.columns}
        df_clean = df_raw.rename(columns={c: _snake_col(c) for c in df_raw.columns}).copy()

    df_clean = _ensure_sample_id_column(df_clean, header_map)
    df_clean = _filter_to_project_rows(df_clean)

    df_clean = standardize_ward_dataframe(
        df_clean,
        strip_source_candidates=("strip", "sample_id", "sample_id_1", "sample_id_2", "sample_id_3"),
        date_cols={
            "date_recd": "date_rec",
            "date_received": "date_rec",
            "date_rec": "date_rec",
            "date_rept": "date_rept",
            "date_reported": "date_rept",
        },
        below_detection_to_zero=True,
        extra_drop_cols=(),
        fixed_depth=(0, 8),
        numeric_exclude_cols=(
            "strip",
            "date_rec",
            "date_rept",
            "date_recd",
            "date_received",
            "date_reported",
            "sample_id",
            "sample_id_1",
            "sample_id_2",
            "sample_id_3",
            "past_crop",
            "excess_lime",
            "excess_lime_rating",
        ),
        add_compatibility_aliases=True,
    )

    # Convert source-specific Ward names to canonical dashboard names.
    df_clean = _apply_raw_to_canonical_map(df_clean)

    # Remove bad/footer rows.
    df_clean = _drop_blank_key_rows(df_clean)

    # Add compatibility aliases used by downstream table builders.
    df_clean = _ensure_expected_soilchem_columns(df_clean)

    key_cols = [
        c
        for c in ["strip", "date_rec", "date_rept", "begin_depth_in", "end_depth_in"]
        if c in df_clean.columns
    ]
    other_cols = [c for c in df_clean.columns if c not in key_cols]
    df_clean = df_clean[key_cols + other_cols]

    return df_clean, header_map


def _read_supplemental_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=0)

    if suffix == ".csv":
        return pd.read_csv(path)

    raise ValueError(f"Unsupported supplemental file type: {path}")


def _print_date_counts(df: pd.DataFrame, label: str) -> None:
    if "date_rec" not in df.columns:
        return

    date_counts = (
        df["date_rec"]
        .fillna("")
        .astype(str)
        .value_counts(dropna=False)
        .sort_index()
    )

    print(f"\nCounts by 'date_rec' {label}:")
    for date_value, count in date_counts.items():
        print(f"  {date_value}: {count}")


def _prepare_soilchem_csv(
        clean_csv: Path,
        output_csv: Path,
        supplemental_raw_csv: Optional[Path] = None,
) -> Path:
    """
    Prepare the canonical soil chemistry CSV.

    Mirrors the soil-bio pattern:
    - read existing clean compiled master
    - optionally read one supplemental raw Ward soil chemistry/SHA file
    - standardize supplemental rows
    - keep only canonical/shared columns
    - append and de-duplicate by strip/date_rec
    - write prepared output
    """
    clean_df = pd.read_csv(clean_csv)

    if supplemental_raw_csv is None or not supplemental_raw_csv.exists():
        clean_df.to_csv(output_csv, index=False)
        return output_csv

    print(f"\n➕ Merging supplemental soil chem file: {supplemental_raw_csv}")

    raw_supp = _read_supplemental_file(supplemental_raw_csv)
    supp_clean, _ = _standardize_soilchem_dataframe(
        raw_supp,
        compiled_master=False,
    )

    print("\n=== Excess lime diagnostics ===")

    for col in supp_clean.columns:
        if "lime" in col.lower():
            print(f"Column: {col}")
            print(supp_clean[col].unique()[:10])

    supp_clean = supp_clean.dropna(axis=1, how="all").copy()
    shared_cols = [c for c in clean_df.columns if c in supp_clean.columns]
    supp_clean = supp_clean[shared_cols].copy()
    supp_clean = supp_clean.reindex(columns=clean_df.columns, fill_value=pd.NA)

    print(f"Rows from supplemental file after cleaning: {len(supp_clean)}")
    _print_date_counts(supp_clean, label=f"in {supplemental_raw_csv.name}")

    merged_df = pd.concat([clean_df, supp_clean], ignore_index=True, sort=False)

    if {"strip", "date_rec"}.issubset(merged_df.columns):
        before = len(merged_df)
        merged_df = merged_df.drop_duplicates(
            subset=["strip", "date_rec"],
            keep="last",
        )
        after = len(merged_df)
        if before != after:
            print(f"🧹 Dropped {before - after} duplicate rows by strip/date_rec")
    sort_cols = [c for c in ("date_rec", "strip") if c in merged_df.columns]
    if sort_cols:
        merged_df = merged_df.sort_values(sort_cols, kind="stable").reset_index(drop=True)
    merged_df.to_csv(output_csv, index=False)
    _print_date_counts(merged_df, label=f"after merging {supplemental_raw_csv.name}")
    return output_csv


def update_ward_master_soilchem(sheet: Optional[str] = None) -> None:
    if not MASTER_XLSX.exists():
        raise FileNotFoundError(f"Input not found: {MASTER_XLSX}")

    df_raw = pd.read_excel(MASTER_XLSX, sheet_name=(0 if sheet is None else sheet))
    print(f"📥 Reading Soil Chem master: {MASTER_XLSX}")
    print(f"🧠 Loaded {df_raw.shape[0]} rows × {df_raw.shape[1]} columns")

    df_clean, header_map = _standardize_soilchem_dataframe(df_raw)

    validate_and_report(
        df_clean,
        strip_col="strip",
        date_col="date_rept",
        expected_columns=EXPECTED_SOILCHEM_COLUMNS,
        matched_output_columns=df_clean.columns,
        ignore_unmatched_columns=(),
    )

    write_clean_outputs(
        df_clean,
        header_map,
        out_csv=OUT_CLEAN_CSV,
        out_headers_json=OUT_HEADERS_JSON,
    )

    print(f"✅ Wrote compiled-master soil chem clean CSV: {OUT_CLEAN_CSV}")
    print(f"✅ Wrote headers map JSON:                 {OUT_HEADERS_JSON}")

    for supplemental_csv in SUPPLEMENTAL_SOILCHEM_FILES:
        if supplemental_csv.exists():
            _prepare_soilchem_csv(
                clean_csv=OUT_CLEAN_CSV,
                output_csv=OUT_CLEAN_CSV,
                supplemental_raw_csv=supplemental_csv,
            )
        else:
            print(f"ℹ️ Supplemental soil chem file not found, skipping: {supplemental_csv}")

    final_df = pd.read_csv(OUT_CLEAN_CSV)

    validate_and_report(
        final_df,
        strip_col="strip",
        date_col="date_rept",
        expected_columns=EXPECTED_SOILCHEM_COLUMNS,
        matched_output_columns=final_df.columns,
        ignore_unmatched_columns=(),
    )

    print(f"✅ Final soil chem clean CSV includes supplemental rows: {OUT_CLEAN_CSV}")


# Backward-compatible alias in case old imports still use the prior name.
clean_ward_master_soilchem = update_ward_master_soilchem


if __name__ == "__main__":
    update_ward_master_soilchem()