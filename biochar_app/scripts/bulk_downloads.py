#!/usr/bin/env python3
"""
biochar_app/scripts/bulk_downloads.py

Bulk download API routes for the Biochar Fruita CSU dashboard.

Responsibilities
----------------
- Build the bulk-download manifest used by the frontend.
- Serve ZIP downloads for logger, weather, irrigation, fertilizer, soil chemistry,
  soil biology, and biomass/NIR datasets.
- Include a README.txt file with each ZIP archive.
- Pass unit-system state through to README builders where applicable.

Notes
-----
This module serves already-processed/standardized datasets. It should not perform
heavy ETL work.
"""
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
    WARD_MASTER_SOILCHEM_CSV,
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_NIR_CSV,
    IRRIGATION_CSV,
    FERTILIZER_CSV_OUT,
)
from biochar_app.scripts.data_loading import load_logger_data, load_weather_data
from biochar_app.scripts.readme_builders import (
    build_file_dataset_readme,
    build_management_readme,
    build_timeseries_yearly_readme,
)

bulk_router = APIRouter()

REPO_ROOT = Path(__file__).resolve().parents[2]
IRRIGATION_WORKBOOK_PATH = REPO_ROOT / "biochar_app" / "data-raw" / "biochar-data-master.xlsx"

ALLOWED_RESOLUTIONS = ["15min", "hourly", "daily", "monthly", "gseason"]

FILE_BACKED_DOWNLOADS: dict[str, tuple[str, Path, str]] = {
    "soil_chem_all": ("Soil Chemistry (all years)", WARD_MASTER_SOILCHEM_CSV, "biochar_soil_chemistry_all_years.csv"),
    "soil_bio_all": ("Soil Biology (all years)", WARD_MASTER_SOILBIO_CSV, "biochar_soil_biology_all_years.csv"),
    "hay_all": ("Biomass / Hay NIR (all years)", WARD_MASTER_NIR_CSV, "biochar_biomass_hay_all_years.csv"),
}

MANAGEMENT_DATASETS: dict[str, tuple[Path, str, str]] = {
    "irrigation": (IRRIGATION_CSV, "Irrigation", "biochar_irrigation_all_years.csv"),
    "fertilizer": (FERTILIZER_CSV_OUT, "Fertilizer use", "biochar_fertilizer_all_years.csv"),
}

WORKBOOK_TOKENS: dict[str, str] = {
    "biomass": "BIOMASS",
}


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None




def _list_years_on_disk() -> list[int]:
    years: set[int] = set()

    if PARQUET_SUMMARY_DIR.exists():
        for res_dir in PARQUET_SUMMARY_DIR.iterdir():
            if not res_dir.is_dir():
                continue
            for p in res_dir.glob("*.parquet"):
                y = _safe_int(p.stem.split("_", 1)[0])
                if y is not None and 1900 <= y <= 2100:
                    years.add(y)

    if PARQUET_DIR.exists():
        for p in PARQUET_DIR.iterdir():
            if p.is_dir():
                y = _safe_int(p.name)
                if y is not None:
                    years.add(y)

    return sorted(years)


def _list_resolutions_on_disk(year: int) -> list[str]:
    found: list[str] = []

    for res in ALLOWED_RESOLUTIONS:
        if _summary_logger_parquet_path(year, res).exists():
            found.append(res)

    return [r for r in ALLOWED_RESOLUTIONS if r in found]


def _summary_logger_parquet_path(year: int, resolution: str) -> Path:
    return PARQUET_SUMMARY_DIR / resolution / f"{year}_{resolution}.parquet"


def _summary_logger_ratios_parquet_path(year: int, resolution: str) -> Path:
    return PARQUET_SUMMARY_DIR / resolution / f"{year}_{resolution}_ratios.parquet"


def _summary_weather_base_dir(resolution: str) -> Path:
    mapping: dict[str, Path] = {
        "15min": PARQUET_SUMMARY_WEATHER_15MIN_DIR,
        "hourly": PARQUET_SUMMARY_WEATHER_HOURLY_DIR,
        "daily": PARQUET_SUMMARY_WEATHER_DAILY_DIR,
        "monthly": PARQUET_SUMMARY_WEATHER_MONTHLY_DIR,
    }
    return mapping.get(resolution, PARQUET_SUMMARY_DIR / "weather" / resolution)


def _summary_weather_parquet_candidates(year: int, resolution: str) -> list[Path]:
    base = _summary_weather_base_dir(resolution)
    return [
        base / f"{year}_{resolution}.parquet",
        base / f"{year}_{resolution}_weather.parquet",
        base / f"weather_{year}_{resolution}.parquet",
    ]


def _logger_parquet_path(year: int, resolution: str) -> Path:
    preferred = _summary_logger_parquet_path(year, resolution)
    if preferred.exists():
        return preferred
    return PARQUET_DIR / str(year) / resolution / f"{year}_{resolution}.parquet"


def _logger_ratios_parquet_path(year: int, resolution: str) -> Optional[Path]:
    preferred = _summary_logger_ratios_parquet_path(year, resolution)
    if preferred.exists():
        return preferred

    old = PARQUET_DIR / str(year) / resolution / f"{year}_{resolution}_ratios.parquet"
    if old.exists():
        return old

    return None


def _weather_parquet_path(year: int, resolution: str) -> Optional[Path]:
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


def _read_parquet_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Parquet not found: {path}")
    try:
        return pd.read_parquet(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read parquet {path}: {e}")


def _read_workbook_sheet_df(sheet_name: str) -> pd.DataFrame:
    if not IRRIGATION_WORKBOOK_PATH.exists():
        raise HTTPException(status_code=404, detail=f"Workbook not found: {IRRIGATION_WORKBOOK_PATH}")
    try:
        return pd.read_excel(IRRIGATION_WORKBOOK_PATH, sheet_name=sheet_name, engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read sheet {sheet_name}: {e}")


def _load_logger_download_df(year: int, resolution: str) -> pd.DataFrame:
    try:
        return load_logger_data(year=year, granularity=resolution)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except OSError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load logger data for year={year}, resolution={resolution}: {e}",
        )


def _load_weather_download_df(year: int, resolution: str) -> pd.DataFrame:
    try:
        df = load_weather_data(year=year, granularity=resolution)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except OSError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load weather data for year={year}, resolution={resolution}: {e}",
        )

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No weather data found for {year} {resolution}")

    return df


def _zip_bytes(files: list[tuple[str, bytes]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files:
            zf.writestr(name, content)
    buf.seek(0)
    return buf.read()


@bulk_router.get("/bulk_download_manifest")
def bulk_download_manifest() -> dict[str, Any]:
    items: list[dict[str, Any]] = []

    years = _list_years_on_disk()
    for y in years:
        for res in _list_resolutions_on_disk(y):
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

    for dataset_key, (csv_path, label_base, _) in MANAGEMENT_DATASETS.items():
         if csv_path.exists():
            items.append(
                {
                    "key": f"{dataset_key}_all",
                    "dataset": dataset_key,
                    "year": None,
                    "resolution": None,
                    "label": f"{label_base} (all years)",
                    "file_path": str(csv_path),
                }
            )

    if IRRIGATION_WORKBOOK_PATH.exists():
        try:
            xls = pd.ExcelFile(IRRIGATION_WORKBOOK_PATH, engine="openpyxl")
            sheets = [str(s) for s in xls.sheet_names]
        except Exception:
            sheets = []

        seen_workbook_keys: set[str] = set()
        for base, token in WORKBOOK_TOKENS.items():
            for s in sheets:
                s_match = s.strip().upper()
                if token not in s_match:
                    continue

                year = None
                for part in s_match.replace("-", " ").replace("_", " ").split():
                    if len(part) == 4 and part.isdigit():
                        year = int(part)
                        break

                if year is None:
                    continue

                workbook_key = f"{base}_{year}"
                if workbook_key in seen_workbook_keys:
                    continue
                seen_workbook_keys.add(workbook_key)

                items.append(
                    {
                        "key": workbook_key,
                        "dataset": base,
                        "year": year,
                        "resolution": None,
                        "label": f"{base.title()} ({year})",
                        "workbook_sheet": s,
                    }
                )

    for key, (label, p, _) in FILE_BACKED_DOWNLOADS.items():
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

    manifest_years = sorted({int(item["year"]) for item in items if item.get("year") is not None})
    manifest_granularities = sorted({str(item["resolution"]) for item in items if item.get("resolution")})

    return {
        "entries": items,
        "years": manifest_years,
        "granularities": manifest_granularities,
    }


@bulk_router.post("/bulk_download")
async def bulk_download(payload: dict[str, Any]):
    unit_system = str(payload.get("unitSystem") or payload.get("unit_system") or "us").lower()
    keys = payload.get("keys")
    if isinstance(keys, list) and keys:
        key = str(keys[0])
    else:
        key = str(payload.get("key") or "").strip()

    if not key:
        raise HTTPException(status_code=400, detail="Missing key (or keys[0])")

    if key in FILE_BACKED_DOWNLOADS:
        dataset_label, p, zip_filename = FILE_BACKED_DOWNLOADS[key]

        if not p.exists():
            raise HTTPException(status_code=404, detail=f"File dataset not found on disk for key: {key}")

        try:
            df = pd.read_csv(p)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read CSV for key {key}: {e}")

        csv_bytes = df.to_csv(index=False).encode("utf-8")
        readme = build_file_dataset_readme(
            dataset_key=key,
            dataset_label=dataset_label,
            df=df,
        )

        zip_bytes = _zip_bytes(
            [
                (zip_filename, csv_bytes),
                ("README.txt", readme.encode("utf-8")),
            ]
        )

        return StreamingResponse(
            io.BytesIO(zip_bytes),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="biochar_{key}.zip"'},
        )

    parts = key.split("_")
    dataset = parts[0].lower()

    files: list[tuple[str, bytes]] = []
    zip_name = f"biochar_{key}.zip"

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
            logger_df = _load_logger_download_df(year=year, resolution=resolution)
            csv_bytes = logger_df.to_csv(index=False).encode("utf-8")
            files.append((f"biochar_loggers_{year}_{resolution}.csv", csv_bytes))

            ratios_pq = _logger_ratios_parquet_path(year, resolution)
            ratios_included = False
            if ratios_pq is not None and ratios_pq.exists():
                ratios_df = _read_parquet_df(ratios_pq)
                ratios_bytes = ratios_df.to_csv(index=False).encode("utf-8")
                files.append((f"biochar_loggers_{year}_{resolution}_ratios.csv", ratios_bytes))
                ratios_included = True

            readme = build_timeseries_yearly_readme(
                dataset="logger",
                year=year,
                resolution=resolution,
                notes=(
                    "Source loader: biochar_app.scripts.data_loading.load_logger_data\n"
                    f"Ratios included as separate file: {'yes' if ratios_included else 'no'}"
                ),
                df=logger_df,
                unit_system=unit_system,
            )

        else:
            weather_df = _load_weather_download_df(year=year, resolution=resolution)
            csv_bytes = weather_df.to_csv(index=False).encode("utf-8")
            files.append((f"biochar_weather_{year}_{resolution}.csv", csv_bytes))

            readme = build_timeseries_yearly_readme(
                dataset="weather",
                year=year,
                resolution=resolution,
                notes="Source loader: biochar_app.scripts.data_loading.load_weather_data",
                df=weather_df,
                unit_system=unit_system,
            )

        files.append(("README.txt", readme.encode("utf-8")))

    elif dataset in MANAGEMENT_DATASETS:
        csv_path, dataset_label, filename_pattern = MANAGEMENT_DATASETS[dataset]

        if not csv_path.exists():
            raise HTTPException(status_code=404, detail=f"Management CSV not found: {csv_path}")

        try:
            management_df = pd.read_csv(csv_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read management CSV {csv_path}: {e}")

        csv_bytes = management_df.to_csv(index=False).encode("utf-8")

        filename = (
            filename_pattern.format(year="all")
            if "{year}" in filename_pattern
            else filename_pattern
        )

        files.append((filename, csv_bytes))

        readme = build_management_readme(
            dataset=dataset,
            dataset_label=dataset_label,
            df=management_df,
            unit_system=unit_system,
        )
        files.append(("README.txt", readme.encode("utf-8")))

    elif dataset in WORKBOOK_TOKENS:
        year = _safe_int(parts[1]) if len(parts) >= 2 else None
        if year is None:
            raise HTTPException(status_code=400, detail=f"Invalid year in key: {key}")

        try:
            xls = pd.ExcelFile(IRRIGATION_WORKBOOK_PATH, engine="openpyxl")
            sheets = [str(s) for s in xls.sheet_names]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Cannot read workbook sheets: {e}")

        token = WORKBOOK_TOKENS[dataset]
        sheet = None
        for s in sheets:
            s_match = s.strip().upper()
            if token in s_match and str(year) in s_match:
                sheet = s
                break

        if not sheet:
            raise HTTPException(status_code=404, detail=f"No workbook sheet found for {dataset} {year}")

        workbook_df = _read_workbook_sheet_df(sheet)
        csv_bytes = workbook_df.to_csv(index=False).encode("utf-8")
        files.append((f"biochar_{dataset}_{year}.csv", csv_bytes))

        readme = build_timeseries_yearly_readme(
            dataset=dataset,
            year=year,
            resolution="workbook sheet",
            notes=f"Workbook sheet: {sheet}",
            df=workbook_df,
        )
        files.append(("README.txt", readme.encode("utf-8")))

    else:
        raise HTTPException(status_code=400, detail=f"Unknown dataset in key: {key}")

    zip_bytes = _zip_bytes(files)

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_name}"'},
    )