from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Dict

import pandas as pd

from biochar_app.config.paths import (
    WARD_MASTER_SOILCHEM_CSV,
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_NIR_CSV,
)


# -----------------------------------------------------------------------------
# Registry spec
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class BulkSheetSpec:
    dataset_key: str          # stable identifier used by UI/API
    label: str                # human label shown in UI
    sheet_name: Optional[str] # Excel tab name (spaces matter!), None for file-backed datasets
    year: Optional[int]       # if set, inject Year column when missing
    filename: str             # CSV filename inside the zip
    csv_path: Optional[str] = None  # if set, load from disk CSV instead of Excel


# -----------------------------------------------------------------------------
# Loaders
# -----------------------------------------------------------------------------

def _inject_year_if_missing(df: pd.DataFrame, year: Optional[int]) -> pd.DataFrame:
    if year is None:
        return df
    if "Year" not in df.columns:
        df = df.copy()
        df["Year"] = int(year)
    return df


def load_sheet_as_dataframe(xlsx_path: str | Path, spec: BulkSheetSpec) -> pd.DataFrame:
    if not spec.sheet_name:
        raise ValueError(f"Spec {spec.dataset_key} has no sheet_name (file-backed?)")
    df = pd.read_excel(xlsx_path, sheet_name=spec.sheet_name, engine="openpyxl")
    df = _inject_year_if_missing(df, spec.year)
    return df


def load_csv_as_dataframe(csv_path: str | Path, spec: BulkSheetSpec) -> pd.DataFrame:
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"CSV path not found for {spec.dataset_key}: {p}")
    df = pd.read_csv(p)
    df = _inject_year_if_missing(df, spec.year)
    return df


def load_spec_as_dataframe(xlsx_path: str | Path, spec: BulkSheetSpec) -> pd.DataFrame:
    if spec.csv_path:
        return load_csv_as_dataframe(spec.csv_path, spec)
    return load_sheet_as_dataframe(xlsx_path, spec)


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


# -----------------------------------------------------------------------------
# Public zip builder
# -----------------------------------------------------------------------------

def build_zip_for_selection(
    xlsx_path: str | Path,
    selected_keys: List[str],
    registry: Optional[List[BulkSheetSpec]] = None,
) -> bytes:
    reg = registry or default_bulk_registry()
    lookup = {s.dataset_key: s for s in reg}

    print("\n🧾 build_zip_for_selection() called")
    print("   requested keys:", selected_keys)
    print("   registry keys (expected):", sorted(lookup.keys()))

    missing = [k for k in selected_keys if k not in lookup]
    if missing:
        print("❌ missing keys:", missing)
        raise ValueError(f"Unknown dataset keys: {missing}")

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for key in selected_keys:
            spec = lookup[key]

            print(f"✅ key matched: {key}")
            print(f"   sheet_name expected: {spec.sheet_name!r}")
            print(f"   csv_path:            {spec.csv_path!r}")
            print(f"   zip filename:        {spec.filename}")

            df = load_spec_as_dataframe(xlsx_path, spec)

            csv_bytes = dataframe_to_csv_bytes(df)
            zf.writestr(spec.filename, csv_bytes)

    return out.getvalue()


# -----------------------------------------------------------------------------
# Registry
# -----------------------------------------------------------------------------

def default_bulk_registry() -> List[BulkSheetSpec]:
    """
    Add new datasets by appending new BulkSheetSpec entries.
    Keep sheet_name EXACT (including trailing spaces).

    For file-backed datasets, set:
      sheet_name=None
      csv_path="path/to/file.csv"
    """
    soil_chem_csv = str(WARD_MASTER_SOILCHEM_CSV)
    soil_bio_csv = str(WARD_MASTER_SOILBIO_CSV)
    hay_nir_csv = str(WARD_MASTER_NIR_CSV)

    return [
        # Workbook-backed datasets
        BulkSheetSpec("biomass_2023", "Biomass (2023)", "2023 BIOMASS", 2023, "biomass_2023.csv"),
        BulkSheetSpec("biomass_2024", "Biomass (2024)", "2024 BIOMASS", 2024, "biomass_2024.csv"),
        BulkSheetSpec("biomass_2025", "Biomass (2025)", "2025 BIOMASS", 2025, "biomass_2025.csv"),

        # File-backed datasets
        BulkSheetSpec(
            dataset_key="soil_chem_all",
            label="Soil Chemistry (all years)",
            sheet_name=None,
            year=None,
            filename="soil_chem_all.csv",
            csv_path=soil_chem_csv,
        ),
        BulkSheetSpec(
            dataset_key="soil_bio_all",
            label="Soil Biology (all years)",
            sheet_name=None,
            year=None,
            filename="soil_bio_all.csv",
            csv_path=soil_bio_csv,
        ),
        BulkSheetSpec(
            dataset_key="hay_all",
            label="Biomass / Hay NIR (all years)",
            sheet_name=None,
            year=None,
            filename="hay_all.csv",
            csv_path=hay_nir_csv,
        ),
    ]


def build_manifest(xlsx_path: str | Path) -> List[Dict[str, Any]]:
    """
    Compatibility wrapper for routes.py (if still used somewhere).
    """
    from biochar_app.scripts.bulk_downloads import bulk_download_manifest
    manifest = bulk_download_manifest()
    entries = manifest.get("entries") if isinstance(manifest, dict) else manifest
    return entries if isinstance(entries, list) else []