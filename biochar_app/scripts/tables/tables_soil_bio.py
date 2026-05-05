#!/usr/bin/env python3
"""
tables_soil_bio.py

Soil Biological Health table builders.

This file defines variable groups and now also includes a small preparation
step for the cleaned soil-bio CSV so the dashboard can tolerate two real-world
issues we have seen in the Ward/processed files:

1. Missing strip values for the 2025-03-31 rows in the cleaned CSV.
2. A newer late-2025 raw Ward PLFA file that has not yet been merged into the
   cleaned master CSV.

The preparation logic:
- backfills blank strip values when a date has exactly four blank strip rows
  that correspond to STRIP 1..4 in order
- optionally discovers / loads a matching raw Biological_*.csv file
- normalizes raw Ward columns into the cleaned machine-readable schema
- appends new rows and de-duplicates by (strip, date_rec), keeping the latest row

Payload envelope conventions (top-level note + set building) are standardized
via tables_common.py.

Run in the project terminal

python - <<'PY'
from pathlib import Path
from biochar_app.scripts.tables_soil_bio import _prepare_soilbio_csv

clean = Path("biochar_app/data-processed/lab-tests/soil-tests-bio/csv-files/ward_master_soilbio_clean_plus_Biological_2025-11-03_v5.csv")
raw   = Path("biochar_app/data-raw/lab-tests/soil-tests-bio/csv-files/Biological_2025-11-03.csv")
out   = clean.parent / "ward_master_soilbio_clean_plus_Biological_2025-11-03_v6.csv"

_prepare_soilbio_csv(
    clean_csv=clean,
    output_csv=out,
    supplemental_raw_csv=raw,
)

print(f"Created: {out}")
PY
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from biochar_app.scripts.tables.tables_common import build_grouped_tab_payload
from biochar_app.scripts.tables.tables_soil_common import VariableSpec, build_soil_table_payload

# -----------------------------------------------------------------------------
# Shared top-level note (STANDARD)
# -----------------------------------------------------------------------------
SOIL_TABLE_TOP_NOTE = "Rows: STRIP 1–4 (0–8 in). Columns: sampling events. Values shown are strip means."

# -----------------------------------------------------------------------------
# Raw Ward biological column aliases -> cleaned machine-readable columns
# -----------------------------------------------------------------------------
CLEAN_TO_RAW_ALIASES: Dict[str, Sequence[str]] = {
    "begin_depth_in": (
        "Begin Depth",
    ),
    "end_depth_in": (
        "End Depth",
    ),
    "total_biomass": (
        "Total Living Microbial Biomass ng/g",
    ),
    "protozoa_biomass": (
        "Protozoan ng/g",
        "Protozoan",
    ),
    "protozoan_pct": (
        "Protozoan ng/g % Biomass",
        "Protozoan % Biomass",
    ),
    "cyclo_19_0": (
        "Cyclo 19:0 ng/g",
    ),
    "polyunsaturated": (
        "PolyUnsaturated ng/g",
        "Polyunsaturated ng/g",
    ),
    "cyclo_17_0": (
        "Cyclo 17:0 ng/g",
    ),
    "monounsaturated": (
        "MonoUnsaturated ng/g",
        "Monounsaturated ng/g",
    ),
    "saturated": (
        "Saturated ng/g",
    ),
    "unsaturated": (
        "Unsaturated ng/g",
    ),
    "pre_18_1w7c_cy19_0": (
        "Pre 18:1w7c:cy19:0 ng/g",
        "Pre 18:1 w7c:cy19:0 ng/g",
    ),
    "pre_18_1_w7c": (
        "Pre 18:1 w7c ng/g",
        "Pre 18:1w7c ng/g",
    ),
    "pre_16_1w7c_cy17_0": (
        "Pre 16:1w7c:cy17:0 ng/g",
        "Pre 16:1 w7c:cy17:0 ng/g",
    ),
    "pre_16_1_w7c": (
        "Pre 16:1 w7c ng/g",
        "Pre 16:1w7c ng/g",
    ),
    "mono_poly": (
        "Monounsaturated:Polyunsaturated ng/g",
        "Monounsaturated:Polyunsaturated",
    ),
    "sat_unsat": (
        "Saturated:Unsaturated ng/g",
        "Saturated:Unsaturated",
    ),
    "gram_pos_gram": (
        "Gram(+):Gram(-) ng/g",
        "Gram(+):Gram(-)",
        "Gram (+):Gram (-) ng/g",
    ),
    "predator_prey": (
        "Predator:Prey ng/g",
        "Predator:Prey",
    ),
    "fungi_bacteria": (
        "Fungi:Bacteria ng/g",
        "Fungi:Bacteria",
    ),
    "undifferentiated_biomass": (
        "Undifferentiated ng/g",
        "Undifferentiated",
    ),
    "undifferentiated_pct": (
        "Undifferentiated ng/g % Biomass",
        "Undifferentiated % Biomass",
    ),
    "gram_pos_biomass": (
        "Gram (+) ng/g",
        "Gram(+) ng/g",
        "Gram Positive ng/g",
    ),
    "gram_pos_pct": (
        "Gram (+) ng/g % Biomass",
        "Gram(+) ng/g % Biomass",
        "Gram (+) % Biomass",
    ),
    "saprophytes_biomass": (
        "Saprophytes ng/g",
        "Saprophytes",
    ),
    "saprophytic_pct": (
        "Saprophytes ng/g % Biomass",
        "Saprophytes % Biomass",
    ),
    "arbuscular_mycorrhizal_biomass": (
        "Arbuscular Mycorrhizal ng/g",
        "Arbuscular Mycorrhizal",
    ),
    "arbusular_mycorrhizal_pct": (
        "Arbuscular Mycorrhizal ng/g % Biomass",
        "Arbuscular Mycorrhizal % Biomass",
    ),
    "total_fungi_biomass": (
        "Total Fungi ng/g",
        "Total Fungi",
    ),
    "total_fungi_pct": (
        "Total Fungi ng/g % Biomass",
        "Total Fungi % Biomass",
    ),
    "rhizobia_biomass": (
        "Rhizobia ng/g",
        "Rhizobia",
    ),
    "rhizobia_pct": (
        "Rhizobia % Biomass",
        "Rhizobia ng/g % Biomass",
        "Rhizobia Percent Biomass",
    ),
    "gram_biomass": (
        "Gram (-) ng/g",
        "Gram(-) ng/g",
        "Gram Negative ng/g",
    ),
    "gram_pct": (
        "Gram (-) ng/g % Biomass",
        "Gram(-) ng/g % Biomass",
        "Gram (-) % Biomass",
    ),
    "actinomycetes_biomass": (
        "Actinomycetes ng/g",
        "Actinomycetes",
    ),
    "actinomycetes_pct": (
        "Actinomycetes ng/g % Biomass",
        "Actinomycetes % Biomass",
    ),
    "total_bacteria_biomass": (
        "Total Bacteria ng/g",
        "Total Bacteria",
    ),
    "bacteria_pct": (
        "Total Bacteria ng/g % Biomass",
        "Total Bacteria % Biomass",
    ),
    "diversity_index": (
        "Functional Group Diversity Index ng/g",
        "Functional Group Diversity Index",
    ),
}

# -----------------------------------------------------------------------------
# Soil Bio variable groups
# -----------------------------------------------------------------------------
SOILBIO_VARIABLE_GROUPS: List[Dict[str, Any]] = [
    {
        "group_key": "soilbio_micro_biomass",
        "group_label": "Microbial Biomass & Community",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="total_biomass",
                label="Total Biomass (ng/g)",
                candidates=("total_biomass", "total_biomass_ng_per_g", "total_biomass_ng_g"),
                note=(
                    "Total microbial biomass from phospholipid fatty acids (PLFA) (ng/g). "
                    "PLFAs are found in cell membranes of living organisms; different groups "
                    "have characteristic PLFA fingerprints."
                ),
                reference_key="total_biomass",
            ),
            VariableSpec(
                key="bacteria_biomass",
                label="Bacteria Biomass (ng/g)",
                candidates=(
                    "total_bacteria_biomass",
                    "bacteria_biomass",
                    "total_bacteria_ng_per_g",
                    "bacteria_biomass_ng_per_g",
                    "bacteria_ng_per_g",
                ),
                note="Bacterial biomass from PLFA (ng/g).",
                reference_key="bacteria_biomass",
            ),
            VariableSpec(
                key="fungi_biomass",
                label="Fungi Biomass (ng/g)",
                candidates=(
                    "total_fungi_biomass",
                    "fungi_biomass",
                    "total_fungi_ng_per_g",
                    "fungi_biomass_ng_per_g",
                    "fungi_ng_per_g",
                ),
                note="Fungal biomass from PLFA (ng/g).",
                reference_key="fungi_biomass",
            ),
            VariableSpec(
                key="fungi_bacteria",
                label="Fungi : Bacteria",
                candidates=("fungi_bacteria", "fungi_bacteria_ratio"),
                note="Fungal-to-bacterial biomass ratio (unitless).",
                reference_key="fungi_bacteria",
            ),
            VariableSpec(
                key="actinobacteria_biomass",
                label="Actinobacteria Biomass (ng/g)",
                candidates=(
                    "actinomycetes_biomass",
                    "actinobacteria_biomass",
                    "actino_biomass",
                    "actinobacteria_ng_per_g",
                ),
                note="Actinobacteria (actinomycetes) biomass from PLFA (ng/g).",
                reference_key=None,
            ),
            VariableSpec(
                key="rhizobia_biomass",
                label="Rhizobia Biomass (ng/g)",
                candidates=("rhizobia_biomass", "rhizobia_ng_per_g"),
                note="Rhizobia biomass from PLFA (ng/g).",
                reference_key=None,
            ),
            VariableSpec(
                key="mycorrhizae_biomass",
                label="Mycorrhizae Biomass (ng/g)",
                candidates=(
                    "arbuscular_mycorrhizal_biomass",
                    "arbusular_mycorrhizal_biomass",
                    "mycorrhizae_biomass",
                    "mycorrhizae_ng_per_g",
                ),
                note="Arbuscular mycorrhizal fungi biomass from PLFA (ng/g).",
                reference_key="mycorrhizae_biomass",
            ),
        ],
    },
    {
        "group_key": "soilbio_functional_groups",
        "group_label": "Functional Groups",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="gram_pos_biomass",
                label="Gram+ Biomass (ng/g)",
                candidates=("gram_pos_biomass", "gram_pos_ng_per_g", "gram_positive_ng_per_g"),
                note="Gram-positive bacterial biomass (ng/g).",
                reference_key="gram_pos_gram",
            ),
            VariableSpec(
                key="gram_neg_biomass",
                label="Gram− Biomass (ng/g)",
                candidates=("gram_neg_biomass", "gram_neg_ng_per_g", "gram_negative_ng_per_g", "gram_biomass"),
                note="Gram-negative bacterial biomass (ng/g).",
                reference_key="gram_pos_gram",
            ),
            VariableSpec(
                key="protozoan_biomass",
                label="Protozoan Biomass (ng/g)",
                candidates=("protozoa_biomass", "protozoan_biomass", "protozoan_ng_per_g"),
                note="Protozoan biomass (ng/g).",
                reference_key="predator_prey",
            ),
            VariableSpec(
                key="saprophytes_biomass",
                label="Saprophytes Biomass (ng/g)",
                candidates=("saprophytes_biomass",),
                note="Saprophytic fungi biomass (ng/g).",
                reference_key=None,
            ),
            VariableSpec(
                key="undifferentiated_biomass",
                label="Undifferentiated Biomass (ng/g)",
                candidates=("undifferentiated_biomass",),
                note="Undifferentiated biomass (ng/g).",
                reference_key=None,
            ),
        ],
    },
    {
        "group_key": "soilbio_stress_diversity",
        "group_label": "Stress Indicators & Diversity",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="diversity_index",
                label="Diversity Index",
                candidates=("diversity_index",),
                note="PLFA diversity index (unitless).",
                reference_key="diversity_index",
            ),
            VariableSpec(
                key="pre_16_1w7c_cy17_0",
                label="Pre 16:1w7c : cy17:0",
                candidates=("pre_16_1w7c_cy17_0",),
                note="Stress indicator ratio (interpret in context).",
                reference_key=None,
            ),
            VariableSpec(
                key="pre_18_1w7c_cy19_0",
                label="Pre 18:1w7c : cy19:0",
                candidates=("pre_18_1w7c_cy19_0",),
                note="Stress indicator ratio (interpret in context).",
                reference_key=None,
            ),
            VariableSpec(
                key="sat_unsat",
                label="Saturated : Unsaturated",
                candidates=("sat_unsat",),
                note="Ratio of saturated to unsaturated fatty acids (unitless).",
                reference_key=None,
            ),
            VariableSpec(
                key="mono_poly",
                label="Monounsaturated : Polyunsaturated",
                candidates=("mono_poly",),
                note="Ratio of mono- to polyunsaturated fatty acids (unitless).",
                reference_key=None,
            ),
        ],
    },
    {
        "group_key": "soilbio_community_ratios",
        "group_label": "Community Composition Ratios",
        "notes": SOIL_TABLE_TOP_NOTE,
        "variables": [
            VariableSpec(
                key="predator_prey",
                label="Predator : Prey",
                candidates=("predator_prey", "predator_pre", "predator_prey_ratio"),
                note="Often expressed as protozoa:bacteria; interpret in context.",
                reference_key="predator_prey",
            ),
            VariableSpec(
                key="gram_pos_gram",
                label="Gram+ : Gram−",
                candidates=("gram_pos_gram", "gram_pos_neg", "gram_pos_gram_neg", "gram_pos_to_neg"),
                note="Ratio of Gram+ to Gram− bacterial biomass (unitless).",
                reference_key="gram_pos_gram",
            ),
        ],
    },
]


def _normalize_strip(value: Any) -> str:
    """
    Normalize common strip variants to the dashboard-standard form: strip_1..strip_4
    """
    if value is None:
        return ""

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""

    match = re.search(r"(?i)\bstrip\s*[_ -]?\s*(\d+)\b", text)
    if not match:
        match = re.search(r"(?i)\bs\s*[_ -]?\s*(\d+)\b", text)

    if match:
        return f"strip_{int(match.group(1))}"

    return ""


def _coerce_numeric(value: Any) -> Optional[float]:
    """
    Convert Ward-style numeric strings to float.

    Rules:
    - blank / NaN -> None
    - '< 0.01' style values -> 0.0
    - otherwise parse float after stripping commas
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    if text.startswith("<"):
        return 0.0

    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date_iso(value: Any) -> str:
    if value is None:
        return ""
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%d")


def _normalize_raw_header(header: Any) -> str:
    """
    Normalize raw Ward headers so small punctuation/spacing differences do not
    break matching.
    """
    if header is None:
        return ""
    text = str(header).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def _build_raw_header_lookup(columns: Iterable[Any]) -> Dict[str, str]:
    """
    Build normalized_header -> actual_header lookup.
    If duplicates normalize to the same key, the first one wins.
    """
    lookup: Dict[str, str] = {}
    for col in columns:
        norm = _normalize_raw_header(col)
        if norm and norm not in lookup:
            lookup[norm] = str(col)
    return lookup


def _find_actual_raw_col(
    raw_lookup: Dict[str, str],
    aliases: Sequence[str],
) -> Optional[str]:
    """
    Find the actual raw column name for one cleaned field.

    Matching strategy:
    1. exact normalized alias match
    2. startswith fallback to catch pandas-mangled duplicate headers like '.1'
    3. token-subset fallback for minor formatting differences
    """
    normalized_items = list(raw_lookup.items())

    for alias in aliases:
        norm_alias = _normalize_raw_header(alias)
        if not norm_alias:
            continue
        actual = raw_lookup.get(norm_alias)
        if actual is not None:
            return actual

    for alias in aliases:
        norm_alias = _normalize_raw_header(alias)
        if not norm_alias:
            continue
        for norm_actual, actual_col in normalized_items:
            if norm_actual.startswith(norm_alias) or norm_alias.startswith(norm_actual):
                return actual_col

    drop_tokens = {"ng", "g"}
    for alias in aliases:
        alias_tokens = set(re.findall(r"[a-z0-9]+", str(alias).lower())) - drop_tokens
        if not alias_tokens:
            continue

        for actual_col in raw_lookup.values():
            actual_tokens = set(re.findall(r"[a-z0-9]+", actual_col.lower())) - drop_tokens
            if alias_tokens.issubset(actual_tokens):
                return actual_col

    return None


def _first_matching_raw_value(
    row: pd.Series,
    raw_lookup: Dict[str, str],
    aliases: Sequence[str],
) -> Any:
    """
    Return the first raw value whose alias exists in the raw file.
    """
    actual_col = _find_actual_raw_col(raw_lookup=raw_lookup, aliases=aliases)
    if actual_col is None:
        return None
    return row.get(actual_col)


def _backfill_blank_strips(clean_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix existing cleaned rows that have blank strip values.
    """
    out = clean_df.copy()

    if "strip" not in out.columns or "date_rec" not in out.columns:
        return out

    strip_series = out["strip"].fillna("").astype(str).str.strip()
    blank_mask = strip_series.eq("")

    if not blank_mask.any():
        return out

    for _, date_rows in out.loc[blank_mask].groupby("date_rec", sort=False):
        idxs = list(date_rows.index)
        if len(idxs) == 4:
            for i, idx in enumerate(idxs, start=1):
                out.at[idx, "strip"] = f"strip_{i}"

    out["strip"] = out["strip"].map(_normalize_strip)
    return out


def _infer_supplemental_raw_path(clean_csv: Path) -> Optional[Path]:
    """
    Try to infer a matching raw Ward file from the cleaned filename.
    """
    stem = clean_csv.stem
    match = re.search(r"(Biological_\d{4}-\d{2}-\d{2})", stem, flags=re.IGNORECASE)
    if not match:
        return None

    raw_name = f"{match.group(1)}.csv"

    direct_candidates = [
        clean_csv.parent / raw_name,
        clean_csv.parent.parent / raw_name,
        clean_csv.parent.parent / "data-raw" / raw_name,
        clean_csv.parent.parent / "csv" / raw_name,
    ]

    for candidate in direct_candidates:
        if candidate.exists():
            return candidate

    for parent in [clean_csv.parent, *clean_csv.parents]:
        try:
            hits = list(parent.rglob(raw_name))
        except Exception:
            hits = []
        if hits:
            return hits[0]

    return None


def _convert_raw_bio_to_clean_shape(raw_csv: Path, clean_columns: Iterable[str]) -> pd.DataFrame:
    """
    Convert a raw Ward Biological_*.csv file into the cleaned machine-readable schema.
    """
    raw_df = pd.read_csv(raw_csv)
    raw_lookup = _build_raw_header_lookup(raw_df.columns)

    rows: List[Dict[str, Any]] = []

    for _, row in raw_df.iterrows():
        strip_value = ""
        for source_col in ("Sample ID 2", "Sample ID 1", "Sample ID 3", "Results For", "strip"):
            strip_value = _normalize_strip(row.get(source_col))
            if strip_value:
                break

        date_rec = _parse_date_iso(row.get("Date Reported") or row.get("Date Received"))
        date_rept = _parse_date_iso(row.get("Date Reported"))

        record: Dict[str, Any] = {
            "strip": strip_value,
            "date_rec": date_rec,
            "date_rept": date_rept,
        }

        for clean_col, aliases in CLEAN_TO_RAW_ALIASES.items():
            raw_value = _first_matching_raw_value(
                row=row,
                raw_lookup=raw_lookup,
                aliases=aliases,
            )
            record[clean_col] = _coerce_numeric(raw_value)

        if record.get("rhizobia_biomass") == 0.0 and record.get("rhizobia_pct") is None:
            record["rhizobia_pct"] = 0.0

        rows.append(record)

    out = pd.DataFrame(rows)

    for col in clean_columns:
        if col not in out.columns:
            out[col] = None

    out = out[list(clean_columns)].copy()

    if "strip" in out.columns:
        out["strip"] = out["strip"].map(_normalize_strip)

    return out


def _prepare_soilbio_csv(
    clean_csv: Path,
    output_csv: Path,
    supplemental_raw_csv: Optional[Path] = None,
) -> Path:
    """
    Create a prepared soil-bio CSV for table rendering.
    """
    clean_df = pd.read_csv(clean_csv)
    clean_df = _backfill_blank_strips(clean_df)

    for col in ("strip", "date_rec"):
        if col in clean_df.columns:
            clean_df[col] = clean_df[col].fillna("").astype(str).str.strip()

    if "strip" in clean_df.columns:
        clean_df["strip"] = clean_df["strip"].map(_normalize_strip)

    raw_path = supplemental_raw_csv or _infer_supplemental_raw_path(clean_csv)
    if raw_path is not None and raw_path.exists():
        raw_clean_df = _convert_raw_bio_to_clean_shape(raw_path, clean_df.columns)

        if {"strip", "date_rec", "rhizobia_biomass", "rhizobia_pct"}.issubset(raw_clean_df.columns):
            print(raw_clean_df[["strip", "date_rec", "rhizobia_biomass", "rhizobia_pct"]].to_string(index=False))

        all_na_cols = [c for c in raw_clean_df.columns if raw_clean_df[c].isna().all()]
        if all_na_cols:
            print("\nColumns that are entirely NA in raw_clean_df:")
            for c in all_na_cols:
                print("  ", c)
        else:
            print("\nNo all-NA columns detected in raw_clean_df")

        raw_non_all_na = raw_clean_df.dropna(axis=1, how="all").copy()
        shared_cols = [c for c in clean_df.columns if c in raw_non_all_na.columns]
        raw_non_all_na = raw_non_all_na[shared_cols].copy()

        merged_df = pd.concat([clean_df, raw_non_all_na], ignore_index=True, sort=False)
        merged_df = merged_df.reindex(columns=clean_df.columns)

        if {"strip", "date_rec"}.issubset(merged_df.columns):
            merged_df = merged_df.drop_duplicates(subset=["strip", "date_rec"], keep="last")
    else:
        merged_df = clean_df

    sort_cols = [c for c in ("date_rec", "strip") if c in merged_df.columns]
    if sort_cols:
        merged_df = merged_df.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    merged_df.to_csv(output_csv, index=False)
    return output_csv


# -----------------------------------------------------------------------------
# Public builder
# -----------------------------------------------------------------------------
def build_soilbio_table(
    clean_csv: Path,
    min_year: int = 2023,
    supplemental_raw_csv: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Build the soil biology tab payload.
    """
    with tempfile.TemporaryDirectory(prefix="soilbio_tables_") as tmpdir:
        prepared_csv = Path(tmpdir) / clean_csv.name
        prepared_csv = _prepare_soilbio_csv(
            clean_csv=clean_csv,
            output_csv=prepared_csv,
            supplemental_raw_csv=supplemental_raw_csv,
        )

        def _builder(grp: Dict[str, Any]) -> Dict[str, Any]:
            return build_soil_table_payload(
                clean_csv=prepared_csv,
                variables=grp["variables"],
                min_year=min_year,
                include_ratio_rows=True,
            )

        return build_grouped_tab_payload(
            title="Soil Biological Health",
            top_note=SOIL_TABLE_TOP_NOTE,
            groups=SOILBIO_VARIABLE_GROUPS,
            build_payload_for_group=_builder,
            include_display_labels=False,
        )