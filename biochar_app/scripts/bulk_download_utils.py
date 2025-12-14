import io
import zipfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd


# -----------------------------
# Dataset registry (edit here)
# -----------------------------

@dataclass(frozen=True)
class BulkSheetSpec:
    dataset_key: str          # stable identifier used by UI/API
    label: str                # human label shown in UI
    sheet_name: str           # exact Excel tab name (spaces matter!)
    year: Optional[int]       # if set, inject Year column when missing
    filename: str             # CSV filename inside the zip


def default_bulk_registry() -> List[BulkSheetSpec]:
    """
    Add new datasets by appending new BulkSheetSpec entries.
    Keep sheet_name EXACT (including trailing spaces).
    """
    return [
        # Irrigation (already used elsewhere)
        BulkSheetSpec("irrigation_2023", "Irrigation (2023)", "2023 IRRIGATION ", 2023, "irrigation_2023.csv"),
        BulkSheetSpec("irrigation_2024", "Irrigation (2024)", "2024 IRRIGATION",  2024, "irrigation_2024.csv"),
        BulkSheetSpec("irrigation_2025", "Irrigation (2025)", "2025 IRRIGATION",  2025, "irrigation_2025.csv"),

        # Fertilizer
        BulkSheetSpec("fertilizing_2023", "Fertilizing (2023)", "2023 FERTILIZING", 2023, "fertilizing_2023.csv"),
        BulkSheetSpec("fertilizing_2024", "Fertilizing (2024)", "2024 FERTILIZING", 2024, "fertilizing_2024.csv"),
        BulkSheetSpec("fertilizing_2025", "Fertilizing (2025)", "2025 FERTILIZING", 2025, "fertilizing_2025.csv"),

        # Biomass
        BulkSheetSpec("biomass_2023", "Biomass (2023)", "2023 BIOMASS", 2023, "biomass_2023.csv"),
        BulkSheetSpec("biomass_2024", "Biomass (2024)", "2024 BIOMASS", 2024, "biomass_2024.csv"),
        BulkSheetSpec("biomass_2025", "Biomass (2025)", "2025 BIOMASS", 2025, "biomass_2025.csv"),
    ]


# -----------------------------
# Loading / cleaning rules
# -----------------------------

def _drop_fully_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep original column names, but drop columns that are completely empty.
    This is the safe way to handle hand-built sheets with merged headers
    that become Unnamed:* padding columns.
    """
    if df.empty:
        return df
    return df.dropna(axis=1, how="all")


def _ensure_year_column(df: pd.DataFrame, year: Optional[int]) -> pd.DataFrame:
    if year is None or df.empty:
        return df

    # If any common year-like column exists, leave it alone
    existing = {str(c).strip().lower(): c for c in df.columns}
    for k in ("year", "yr"):
        if k in existing:
            return df

    df = df.copy()
    df.insert(0, "Year", int(year))
    return df


def load_sheet_as_dataframe(xlsx_path: str, spec: BulkSheetSpec) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=spec.sheet_name)
    df = _drop_fully_empty_columns(df)
    df = _ensure_year_column(df, spec.year)
    return df


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    bio = io.StringIO()
    df.to_csv(bio, index=False)
    return bio.getvalue().encode("utf-8")


def human_bytes(n: int) -> str:
    # simple binary units
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n/1024:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} GiB"


def build_manifest(xlsx_path: str, registry: Optional[List[BulkSheetSpec]] = None) -> List[Dict]:
    """
    Returns rows the UI can display next to checkboxes:
      key, label, filename, rows, cols, size_bytes, size_human
    """
    reg = registry or default_bulk_registry()
    manifest: List[Dict] = []

    for spec in reg:
        df = load_sheet_as_dataframe(xlsx_path, spec)
        csv_bytes = dataframe_to_csv_bytes(df)

        manifest.append({
            "key": spec.dataset_key,
            "label": spec.label,
            "filename": spec.filename,
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "size_bytes": int(len(csv_bytes)),
            "size_human": human_bytes(len(csv_bytes)),
        })

    return manifest


def build_zip_for_selection(
    xlsx_path: str,
    selected_keys: List[str],
    registry: Optional[List[BulkSheetSpec]] = None,
) -> bytes:
    reg = registry or default_bulk_registry()
    lookup = {s.dataset_key: s for s in reg}

    missing = [k for k in selected_keys if k not in lookup]
    if missing:
        raise ValueError(f"Unknown dataset keys: {missing}")

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for key in selected_keys:
            spec = lookup[key]
            df = load_sheet_as_dataframe(xlsx_path, spec)
            csv_bytes = dataframe_to_csv_bytes(df)
            zf.writestr(spec.filename, csv_bytes)

    return out.getvalue()