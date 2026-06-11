#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]  # biochar_app/scripts → biochar_app → project root

# ----------------------------
# User paths (edit if needed)
# ----------------------------
MASTER_TEST_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-raw"
    / "lab-tests"
    / "hay-tests"
    / "csv-files"
    / "Master_test.csv"
)

# This MUST be the NIR file whose values are already in Master_test
REFERENCE_OLD_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-raw"
    / "lab-tests"
    / "hay-tests"
    / "csv-files"
    / "NIR_2024-08-06.csv"
)

OUT_JSON = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-raw"
    / "lab-tests"
    / "hay-tests"
    / "csv-files"
    / "nir_old_to_new_column_map.json"
)


def _as_str_cell(x) -> str:
    """
    Exact-ish string conversion without numeric normalization.
    We DO normalize NaN/None to empty string so that blank cells compare cleanly.
    """
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)


def signature_from_col(df: pd.DataFrame, col_idx: int) -> str:
    """
    Build the signature string for a column from the 4 data rows,
    preserving exact string forms (except NaN->"").
    """
    vals = [_as_str_cell(v) for v in df.iloc[:, col_idx].tolist()]
    return "||".join(vals)


def load_master_test(master_test_csv: Path) -> Tuple[List[str], pd.DataFrame]:
    """
    Master_test format:
      row 0 = human headers
      row 1 = machine headers  <-- we want these as mapping targets
      rows 2-5 = 4 data rows
    """
    raw = pd.read_csv(master_test_csv, header=None, dtype=str, keep_default_na=False)
    if raw.shape[0] < 6:
        raise ValueError(
            f"Master_test must have at least 6 rows (2 header + 4 data). Got {raw.shape[0]}"
        )

    machine_headers = [str(x) if x is not None else "" for x in raw.iloc[1, :].tolist()]
    data = raw.iloc[2:6, :].copy()  # 4 rows
    return machine_headers, data


def is_unnamed_col(col: object) -> bool:
    s = "" if col is None else str(col).strip()
    return (s == "") or (s.lower().startswith("unnamed:"))


def load_old_reference(old_csv: Path) -> Tuple[List[str], pd.DataFrame]:
    """
    Old NIR reference file:
      row 0 = headers
      rows 1-4 = 4 data rows

    BUT the file may contain thousands of trailing comma-only lines, so:
      read only first 5 physical lines (header + 4 data rows).
    """
    df = pd.read_csv(
        old_csv,
        dtype=str,
        keep_default_na=False,  # keep empty strings as ""
        na_filter=False,
        nrows=5,                # header + 4 rows only
        engine="python",        # more tolerant of odd CSVs
    )

    # Drop unnamed/blank columns up front (these cause huge ambiguity).
    df = df.loc[:, [c for c in df.columns if not is_unnamed_col(c)]].copy()

    if df.shape[0] < 4:
        raise ValueError(f"Old file needs at least 4 data rows. Got {df.shape[0]}")

    headers = list(df.columns)
    data = df.iloc[0:4, :].copy()  # first 4 data rows
    return headers, data


def build_mapping(
    master_machine_headers: List[str],
    master_data: pd.DataFrame,
    old_headers: List[str],
    old_data: pd.DataFrame,
) -> Tuple[Dict[str, str], Dict[str, List[str]], List[str]]:
    """
    Returns:
      mapping: old_name -> master_machine_name
      ambiguous: old_name -> list of possible master columns (if signature matches >1)
      unmapped: list of old columns with no matches
    """
    # Build signature -> master column indices (allow duplicates)
    sig_to_master_idxs: Dict[str, List[int]] = {}
    for j in range(master_data.shape[1]):
        sig = signature_from_col(master_data, j)
        sig_to_master_idxs.setdefault(sig, []).append(j)

    mapping: Dict[str, str] = {}
    ambiguous: Dict[str, List[str]] = {}
    unmapped: List[str] = []

    for j, old_name in enumerate(old_headers):
        if is_unnamed_col(old_name):
            # Extra safety: if anything unnamed sneaks through, ignore it
            continue

        sig_old = signature_from_col(old_data, j)
        hits = sig_to_master_idxs.get(sig_old, [])

        if not hits:
            unmapped.append(old_name)
            continue

        if len(hits) == 1:
            master_idx = hits[0]
            target = master_machine_headers[master_idx]
            mapping[old_name] = target
        else:
            candidates = [master_machine_headers[idx] for idx in hits]
            ambiguous[old_name] = candidates

    return mapping, ambiguous, unmapped


def main() -> None:
    print(f"📥 Loading MASTER_TEST (2 header rows): {MASTER_TEST_CSV}")
    master_machine_headers, master_data = load_master_test(MASTER_TEST_CSV)
    print(f"Master_test: cols={len(master_machine_headers)}, data_rows={master_data.shape[0]}")

    print(f"📥 Loading OLD reference (to map): {REFERENCE_OLD_CSV}")
    old_headers, old_data = load_old_reference(REFERENCE_OLD_CSV)
    print(
        f"Old ref: rows={old_data.shape[0]}, cols={len(old_headers)} "
        f"(read nrows=5; dropped unnamed columns)"
    )

    mapping, ambiguous, unmapped = build_mapping(master_machine_headers, master_data, old_headers, old_data)

    print(f"\n✅ Learned mappings: {len(mapping)}")
    for k, v in list(mapping.items())[:50]:
        print(f"  ✔ {k}  →  {v}")

    if ambiguous:
        print(f"\n⚠️ Ambiguous matches: {len(ambiguous)}")
        for k, cands in list(ambiguous.items())[:25]:
            print(f"  ? {k}: {cands}")

    print(f"\n⚠️ Unmapped OLD columns: {len(unmapped)}")
    for c in unmapped[:40]:
        print(f"  - {c}")
    if len(unmapped) > 40:
        print(f"  ... (+{len(unmapped) - 40} more)")

    payload = {
        "mapping": mapping,
        "ambiguous": ambiguous,
        "unmapped": unmapped,
        "source_files": {
            "master_test": str(MASTER_TEST_CSV),
            "old_reference": str(REFERENCE_OLD_CSV),
        },
        "notes": [
            "Mapping is exact string match on the 4 data rows only (no numeric normalization).",
            "Old reference file read uses nrows=5 to avoid trailing comma-only lines inflating shape.",
            "Unnamed/blank old columns (e.g., 'Unnamed: 69') are ignored to reduce ambiguous matches.",
            "Repeated-constant columns are allowed; if their 4-row signature matches multiple master columns, they will remain ambiguous.",
        ],
    }

    print(f"\n💾 Saved mapping JSON → {OUT_JSON}")
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()