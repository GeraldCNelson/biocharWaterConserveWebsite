# biochar_app/scripts/bulk_downloads.py

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from biochar_app.config.paths import (
    PARQUET_DIR,
    PARQUET_SUMMARY_DIR,
    PARQUET_SUMMARY_WEATHER_15MIN_DIR,
    PARQUET_SUMMARY_WEATHER_HOURLY_DIR,
    PARQUET_SUMMARY_WEATHER_DAILY_DIR,
    PARQUET_SUMMARY_WEATHER_MONTHLY_DIR,
)

# --------------------------------------------------------------------------------------
# Router
# --------------------------------------------------------------------------------------

bulk_router = APIRouter()

# --------------------------------------------------------------------------------------
# Paths / conventions
# --------------------------------------------------------------------------------------

# Resolve paths relative to this file so uvicorn CWD never matters.
REPO_ROOT = Path(__file__).resolve().parents[2]

IRRIGATION_WORKBOOK_PATH = REPO_ROOT / "biochar_app" / "data-raw" / "biochar-data-master.xlsx"

# What the UI uses
ALLOWED_RESOLUTIONS = ["15min", "hourly", "daily", "monthly", "gseason"]


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------

def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None


def _list_years_on_disk() -> list[int]:
    """
    Years visible for parquet datasets.
    We look in PARQUET_SUMMARY_DIR first (preferred), then fall back to PARQUET_DIR children.
    """
    years: set[int] = set()

    if PARQUET_SUMMARY_DIR.exists():
        # summary/<res>/<year>_<res>.parquet
        for res_dir in PARQUET_SUMMARY_DIR.iterdir():
            if not res_dir.is_dir():
                continue
            for p in res_dir.glob("*.parquet"):
                stem = p.stem
                y = _safe_int(stem.split("_", 1)[0])
                if y is not None and 1900 <= y <= 2100:
                    years.add(y)

    # Fallback: PARQUET_DIR/<year> folders
    if PARQUET_DIR.exists():
        for p in PARQUET_DIR.iterdir():
            if p.is_dir():
                y = _safe_int(p.name)
                if y is not None:
                    years.add(y)

    return sorted(years)


def _list_resolutions_on_disk(year: int) -> list[str]:
    """
    Determine which resolutions exist for a given year, using the current layout:
      parquet/summary/<res>/<year>_<res>.parquet
    """
    found: list[str] = []

    for res in ALLOWED_RESOLUTIONS:
        if _summary_logger_parquet_path(year, res).exists():
            found.append(res)

    return [r for r in ALLOWED_RESOLUTIONS if r in found]


# --------------------------------------------------------------------------------------
# Parquet path resolution (robust to multiple layouts)
# --------------------------------------------------------------------------------------

def _summary_logger_parquet_path(year: int, resolution: str) -> Path:
    return PARQUET_SUMMARY_DIR / resolution / f"{year}_{resolution}.parquet"


def _summary_logger_ratios_parquet_path(year: int, resolution: str) -> Path:
    return PARQUET_SUMMARY_DIR / resolution / f"{year}_{resolution}_ratios.parquet"


def _summary_weather_base_dir(resolution: str) -> Path:
    """
    Canonical weather summary directory for supported weather resolutions.
    Falls back to the generic layout for anything unexpected.
    """
    mapping: dict[str, Path] = {
        "15min": PARQUET_SUMMARY_WEATHER_15MIN_DIR,
        "hourly": PARQUET_SUMMARY_WEATHER_HOURLY_DIR,
        "daily": PARQUET_SUMMARY_WEATHER_DAILY_DIR,
        "monthly": PARQUET_SUMMARY_WEATHER_MONTHLY_DIR,
    }
    return mapping.get(resolution, PARQUET_SUMMARY_DIR / "weather" / resolution)


def _summary_weather_parquet_candidates(year: int, resolution: str) -> list[Path]:
    """
    Actual layout:
      parquet/summary/weather/<res>/<year>_<res>.parquet

    Also try a couple legacy/alternate names, just in case.
    """
    base = _summary_weather_base_dir(resolution)
    return [
        base / f"{year}_{resolution}.parquet",
        base / f"{year}_{resolution}_weather.parquet",
        base / f"weather_{year}_{resolution}.parquet",
    ]


def _logger_parquet_path(year: int, resolution: str) -> Path:
    """
    Preferred: summary layout
    Fallback: old layout (per-year/per-resolution)
    """
    preferred = _summary_logger_parquet_path(year, resolution)
    if preferred.exists():
        return preferred

    old = PARQUET_DIR / str(year) / resolution / f"{year}_{resolution}.parquet"
    return old


def _logger_ratios_parquet_path(year: int, resolution: str) -> Optional[Path]:
    """
    Preferred: summary layout ratios
    Fallback: old layout ratios
    """
    preferred = _summary_logger_ratios_parquet_path(year, resolution)
    if preferred.exists():
        return preferred

    old = PARQUET_DIR / str(year) / resolution / f"{year}_{resolution}_ratios.parquet"
    if old.exists():
        return old

    return None


def _weather_parquet_path(year: int, resolution: str) -> Optional[Path]:
    """
    Return the first weather parquet that exists, else None.
    """
    for c in _summary_weather_parquet_candidates(year, resolution):
        if c.exists():
            return c

    old_base = PARQUET_DIR / str(year) / resolution / "weather"
    old_candidates = [
        old_base / f"{year}_{resolution}_weather.parquet",
        old_base / f"{year}_{resolution}.parquet",
        old_base / f"weather_{year}_{resolution}.parquet",
    ]
    for c in old_candidates:
        if c.exists():
            return c

    return None


# --------------------------------------------------------------------------------------
# File reading helpers
# --------------------------------------------------------------------------------------

def _read_parquet_to_csv_bytes(path: Path) -> bytes:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Parquet not found: {path}")
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read parquet {path}: {e}")
    return df.to_csv(index=False).encode("utf-8")


def _read_workbook_sheet_to_csv_bytes(sheet_name: str) -> bytes:
    if not IRRIGATION_WORKBOOK_PATH.exists():
        raise HTTPException(status_code=404, detail=f"Workbook not found: {IRRIGATION_WORKBOOK_PATH}")
    try:
        df = pd.read_excel(IRRIGATION_WORKBOOK_PATH, sheet_name=sheet_name, engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read sheet {sheet_name}: {e}")
    return df.to_csv(index=False).encode("utf-8")


def _zip_bytes(files: list[tuple[str, bytes]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files:
            zf.writestr(name, content)
    buf.seek(0)
    return buf.read()


def _readme_text(dataset: str, year: Optional[int], resolution: Optional[str], notes: str = "") -> str:
    ytxt = str(year) if year is not None else "ALL/NA"
    rtxt = resolution if resolution is not None else "ALL/NA"
    base = f"""Biochar Fruita CSU – Bulk Download

Dataset: {dataset}
Year: {ytxt}
Resolution: {rtxt}

Notes
-----
{notes or "See website Technical Details for variable definitions and units."}

Units
-----
Stored data are in US customary units in the backend (display may be metric if toggled).

Generated by the Biochar dashboard bulk download endpoint.
"""
    return base


# --------------------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------------------

@bulk_router.get("/bulk_download_manifest")
def bulk_download_manifest() -> list[dict[str, Any]]:
    """
    Returns a manifest of what can be downloaded.

    Logger/weather availability:
      - primary: biochar_app/data-processed/parquet/summary/<res>/<year>_<res>.parquet
      - fallback: old per-year/per-res layout (if present)
    """
    items: list[dict[str, Any]] = []

    years = _list_years_on_disk()
    for y in years:
        res_list = _list_resolutions_on_disk(y)
        for res in res_list:
            lp = _logger_parquet_path(y, res)
            if lp.exists():
                items.append(
                    {
                        "key": f"loggers_{y}_{res}",
                        "dataset": "loggers",
                        "year": y,
                        "resolution": res,
                        "label": f"Logger data ({y}, {res})",
                    }
                )

            wp = _weather_parquet_path(y, res)
            if wp is not None and wp.exists():
                items.append(
                    {
                        "key": f"weather_{y}_{res}",
                        "dataset": "weather",
                        "year": y,
                        "resolution": res,
                        "label": f"Weather data ({y}, {res})",
                    }
                )

    # Workbook-derived datasets
    if IRRIGATION_WORKBOOK_PATH.exists():
        try:
            xls = pd.ExcelFile(IRRIGATION_WORKBOOK_PATH, engine="openpyxl")
            sheets = [str(s).strip() for s in xls.sheet_names]
        except Exception:
            sheets = []

        workbook_rules = [
            ("irrigation", "IRRIGATION"),
            ("fertilizing", "FERTIL"),
            ("biomass", "BIOMASS"),
        ]

        for base, token in workbook_rules:
            for s in sheets:
                if token in s.upper():
                    year = None
                    for part in s.replace("-", " ").replace("_", " ").split():
                        if len(part) == 4 and part.isdigit():
                            year = int(part)
                            break
                    if year is None:
                        continue
                    items.append(
                        {
                            "key": f"{base}_{year}",
                            "dataset": base,
                            "year": year,
                            "resolution": None,
                            "label": f"{base.title()} ({year})",
                            "workbook_sheet": s,
                        }
                    )

    # File-backed “all years” datasets
    file_rules = [
        (
            "soil_chem_all",
            "Soil Chemistry (all years)",
            REPO_ROOT / "biochar_app/data-processed/lab-tests/soil-tests-chem/csv-files/ward_master_soilchem_clean.csv",
        ),
        (
            "soil_bio_all",
            "Soil Biology (all years)",
            REPO_ROOT / "biochar_app/data-processed/lab-tests/soil-tests-bio/csv-files/ward_master_soilbio_clean.csv",
        ),
        (
            "hay_all",
            "Biomass / Hay NIR (all years)",
            REPO_ROOT / "biochar_app/data-processed/lab-tests/hay-tests/csv-files/ward_master_nir_clean.csv",
        ),
    ]
    for key, label, p in file_rules:
        if p.exists():
            items.append(
                {
                    "key": key,
                    "dataset": "file",
                    "year": None,
                    "resolution": None,
                    "label": label,
                    "file_path": str(p),
                }
            )

    return items


# --------------------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------------------

@bulk_router.post("/bulk_download")
async def bulk_download(payload: dict[str, Any]):
    """
    Payload supports:
      - {"key": "loggers_2025_daily"}
      - {"keys": ["loggers_2025_daily"]}
    """
    keys = payload.get("keys")
    if isinstance(keys, list) and keys:
        key = str(keys[0])
    else:
        key = str(payload.get("key") or "").strip()

    if not key:
        raise HTTPException(status_code=400, detail="Missing key (or keys[0])")

    parts = key.split("_")
    dataset = parts[0].lower()

    files: list[tuple[str, bytes]] = []
    zip_name = f"biochar_{key}.zip"

    # Logger / Weather
    if dataset in {"loggers", "weather"}:
        if len(parts) < 3:
            raise HTTPException(status_code=400, detail=f"Expected {dataset}_YYYY_resolution key, got: {key}")

        year = _safe_int(parts[1])
        resolution = str(parts[2]).strip()

        if year is None:
            raise HTTPException(status_code=400, detail=f"Invalid year in key: {key}")
        if resolution not in ALLOWED_RESOLUTIONS:
            raise HTTPException(status_code=400, detail=f"Invalid resolution '{resolution}' in key: {key}")

        if dataset == "loggers":
            pq = _logger_parquet_path(year, resolution)
            csv_bytes = _read_parquet_to_csv_bytes(pq)
            files.append((f"biochar_loggers_{year}_{resolution}.csv", csv_bytes))

            ratios_pq = _logger_ratios_parquet_path(year, resolution)
            if ratios_pq is not None and ratios_pq.exists():
                ratios_bytes = _read_parquet_to_csv_bytes(ratios_pq)
                files.append((f"biochar_loggers_{year}_{resolution}_ratios.csv", ratios_bytes))

            readme = _readme_text(
                "logger",
                year,
                resolution,
                notes=f"Source parquet: {pq}\nRatios included: {'yes' if ratios_pq and ratios_pq.exists() else 'no'}",
            )

        else:
            wp = _weather_parquet_path(year, resolution)
            if wp is None or not wp.exists():
                raise HTTPException(status_code=404, detail=f"No weather parquet found for {year} {resolution}")
            csv_bytes = _read_parquet_to_csv_bytes(wp)
            files.append((f"biochar_weather_{year}_{resolution}.csv", csv_bytes))
            readme = _readme_text("weather", year, resolution, notes=f"Source parquet: {wp}")

        files.append(("README.txt", readme.encode("utf-8")))

    # Workbook-based
    elif dataset in {"irrigation", "fertilizing", "biomass"}:
        year = _safe_int(parts[1]) if len(parts) >= 2 else None
        if year is None:
            raise HTTPException(status_code=400, detail=f"Invalid year in key: {key}")

        try:
            xls = pd.ExcelFile(IRRIGATION_WORKBOOK_PATH, engine="openpyxl")
            sheets = [str(s).strip() for s in xls.sheet_names]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Cannot read workbook sheets: {e}")

        token = {
            "irrigation": "IRRIGATION",
            "fertilizing": "FERTIL",
            "biomass": "BIOMASS",
        }[dataset]

        sheet = None
        for s in sheets:
            s_up = s.upper()
            if token in s_up and str(year) in s_up:
                sheet = s
                break

        if not sheet:
            raise HTTPException(status_code=404, detail=f"No workbook sheet found for {dataset} {year}")

        csv_bytes = _read_workbook_sheet_to_csv_bytes(sheet)
        files.append((f"biochar_{dataset}_{year}.csv", csv_bytes))
        readme = _readme_text(
            dataset,
            year,
            None,
            notes=f"Source workbook: {IRRIGATION_WORKBOOK_PATH}\nSheet: {sheet}",
        )
        files.append(("README.txt", readme.encode("utf-8")))

    # File-backed “all years” datasets
    elif dataset == "soil":
        raise HTTPException(
            status_code=400,
            detail=f"Unknown dataset in key: {key} (did you mean soil_chem_all / soil_bio_all?)",
        )

    elif dataset == "hay" or dataset == "file" or key in {"soil_chem_all", "soil_bio_all", "hay_all"}:
        file_map = {
            "soil_chem_all": REPO_ROOT / "biochar_app/data-processed/lab-tests/soil-tests-chem/csv-files/ward_master_soilchem_clean.csv",
            "soil_bio_all": REPO_ROOT / "biochar_app/data-processed/lab-tests/soil-tests-bio/csv-files/ward_master_soilbio_clean.csv",
            "hay_all": REPO_ROOT / "biochar_app/data-processed/lab-tests/hay-tests/csv-files/ward_master_nir_clean.csv",
        }
        p = file_map.get(key)
        if p is None or not p.exists():
            raise HTTPException(status_code=404, detail=f"File dataset not found for key: {key}")
        csv_bytes = p.read_bytes()
        files.append((p.name, csv_bytes))
        readme = _readme_text("file", None, None, notes=f"Source file: {p}")
        files.append(("README.txt", readme.encode("utf-8")))

    else:
        raise HTTPException(status_code=400, detail=f"Unknown dataset in key: {key}")

    zip_bytes = _zip_bytes(files)

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_name}"'},
    )