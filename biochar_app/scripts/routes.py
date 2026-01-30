"""
================================================================================
routes.py — API Endpoints & Orchestration for Biochar Dashboard
================================================================================
"""
from __future__ import annotations

import io
import os
import re
import math
import json
import logging
import textwrap
from io import BytesIO
import zipfile
from pathlib import Path
from typing import Dict, Any, Optional, List, Literal
from biochar_app.scripts.bulk_downloads import bulk_router
from biochar_app.scripts.biomass_field_tables import get_biomass_field_table_payload
from bulk_download_utils import default_bulk_registry

logger = logging.getLogger(__name__)

import pandas as pd
from fastapi import APIRouter, Request, HTTPException, Query, Body
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)

from starlette.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from biochar_app.scripts.bulk_download_utils import build_manifest, build_zip_for_selection
from biochar_app.scripts.routes_utils import (
    load_gseason_df,
    load_logger_year,
    periods_to_list_of_dicts,
)
from biochar_app.scripts.gseason_utils import (
    compute_summary_statistics,
    get_flat_gseason_summary,
)
from biochar_app.scripts.plot_utils import (
    make_raw_figure,
    make_ratio_figure,
    make_raw_gseason_figure,
    make_ratio_gseason_figure,
    make_temperature_delta_figure,
)
from biochar_app.scripts import state
from biochar_app.scripts.config import (
    DEFAULT_YEAR,
    DEFAULT_START_DATE,
    DEFAULT_END_DATE,
    DEFAULT_VARIABLE,
    DEFAULT_DEPTH,
    DEFAULT_STRIP,
    DEFAULT_LOGGER_LOCATION,
    DEFAULT_GRANULARITY,
    PARQUET_DIR,
    YEARS,
    PLOT_BASED_ON_OPTIONS,
    TRACE_OPTION_MAP,
    DEFAULT_GSEASON_PERIODS,
    label_name_mapping,
    sensor_depth_mapping,
    logger_location_mapping,
    variable_name_mapping,
    granularity_name_mapping,
    strip_name_mapping,
    variable_name_abbrev,
)
from biochar_app.scripts.markdown_config import build_markdown_mapping

# Soil tables (compiled clean Ward-style masters)
from biochar_app.scripts.soil_tables import (
    build_soilbio_table,
    build_soilchem_table,
)

# NIR tables (Sets 1–4)
from biochar_app.scripts.nir_tables import (
    build_nir_set1_table,
    build_nir_set2_table,
    build_nir_set3_table,
    build_nir_set4_table,
)

# -----------------------------------------------------------------------------
# Project root + canonical cleaned master paths
# -----------------------------------------------------------------------------
# routes.py lives at: biochar_app/scripts/routes.py
# so:
#   parents[0] = .../biochar_app/scripts
#   parents[1] = .../biochar_app
#   parents[2] = .../biocharWaterConserveWebsite   <-- repo root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# --- Ward “clean masters” (authoritative) ---

WARD_MASTER_NIR_CLEAN_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "hay-tests"
    / "csv-files"
    / "ward_master_nir_clean.csv"
)

WARD_MASTER_SOILBIO_CLEAN_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "soil-tests-bio"
    / "csv-files"
    / "ward_master_soilbio_clean_plus_Biological_2025-11-03_v2.csv"
)

WARD_MASTER_SOILCHEM_CLEAN_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "soil-tests-chem"
    / "csv-files"
    / "ward_master_soilchem_clean_plus_Soil_2025-11-03_v1.csv"
)

WARD_MASTER_BIOMASS_FIELD_CLEAN_CSV = (
    PROJECT_ROOT
    / "biochar_app"
    / "data-processed"
    / "lab-tests"
    / "biomass-field"
    / "csv-files"
    / "field_biomass_dry_g_wide_clean.csv"
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _normalize_sheet_name(s: str) -> str:
    # Your workbook has at least one sheet with trailing spaces ("2023 IRRIGATION ")
    return (s or "").strip()


def load_irrigation_for_year(
    year: int,
    xlsx_path: str | Path,
    logger=None,
) -> pd.DataFrame:
    """
    Return irrigation events for a given year.

    If the year sheet doesn't exist, return an EMPTY dataframe with the expected columns.
    This avoids warnings/noise and makes downstream plotting logic simple.
    """
    xlsx_path = Path(xlsx_path)

    # Always return these columns (even when empty) so plotting code is consistent.
    empty = pd.DataFrame(columns=["time_on", "time_off", "gallons", "side"])

    if not xlsx_path.exists():
        if logger:
            logger.info(f"ℹ️ Irrigation workbook not found at {xlsx_path}; skipping irrigation overlay.")
        return empty

    try:
        xl = pd.ExcelFile(xlsx_path)
    except Exception as e:
        if logger:
            logger.warning(f"⚠️ Failed to open irrigation workbook {xlsx_path}: {e}")
        return empty

    # Find "YYYY IRRIGATION" sheet (robust to trailing spaces)
    target = f"{int(year)} IRRIGATION"
    sheets_norm = {_normalize_sheet_name(s): s for s in xl.sheet_names}
    sheet_name = sheets_norm.get(target)

    if not sheet_name:
        if logger:
            # Use INFO (not WARNING) — this is expected early season / future years.
            logger.info(
                f"ℹ️ No irrigation sheet for year {year}. "
                f"Expected '{target}'. Available: {list(sheets_norm.keys())}"
            )
        return empty

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)

    # ---- Normalize columns to your canonical names ----
    rename_map = {
        "Time On": "time_on",
        "Time Off": "time_off",
        "time on": "time_on",
        "time off": "time_off",
        "gallons": "gallons",
        "Gallons": "gallons",
        "Side": "side",
        "side": "side",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    # If there are duplicate columns, keep the first occurrence
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Keep only columns we care about (if present)
    keep = [c for c in ["time_on", "time_off", "gallons", "side"] if c in df.columns]
    df = df[keep].copy()

    # Parse datetimes safely
    for c in ["time_on", "time_off"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "gallons" in df.columns:
        df["gallons"] = pd.to_numeric(df["gallons"], errors="coerce")

    # Drop rows without a valid interval
    if "time_on" in df.columns and "time_off" in df.columns:
        df = df.dropna(subset=["time_on", "time_off"])

    if df.empty:
        return empty

    # Ensure all expected columns exist
    for col in empty.columns:
        if col not in df.columns:
            df[col] = pd.NA

    return df[empty.columns].copy()


def _clean_for_json(obj):
    """
    Recursively walk dicts/lists and replace NaN/inf floats with None
    so JSONResponse can serialize the payload.
    """
    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean_for_json(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


# ---- Paths ----
main_router = APIRouter()
api_router = APIRouter(prefix="/api")
api_router.include_router(bulk_router)

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)

# ---------------------------------------------------------------------
# Bulk download directories (must match etl.py)
# ---------------------------------------------------------------------
DOWNLOADS_BASE_DIR = Path(PARQUET_DIR).parent / "downloads"
LOGGER_DOWNLOADS_DIR = DOWNLOADS_BASE_DIR / "loggers"
WEATHER_DOWNLOADS_DIR = DOWNLOADS_BASE_DIR / "weather"

# User confirmed: biochar-data-master.xlsx is in data-raw
BIOCHAR_MASTER_XLSX = "biochar_app/data-raw/biochar-data-master.xlsx"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ensure_year_allowed(year: int) -> None:
    """Raise 404 if year is not in configured YEARS."""
    if year not in YEARS:
        raise HTTPException(status_code=404, detail=f"Year {year} is not available.")


def _round_ratio_columns(df: pd.DataFrame, decimals: int = 6) -> pd.DataFrame:
    """Rounds all ratio columns (containing '_ratio_') to a fixed number of decimals."""
    df_out = df.copy()
    ratio_cols = [c for c in df_out.columns if "_ratio_" in c]
    if ratio_cols:
        df_out[ratio_cols] = df_out[ratio_cols].round(decimals)
    return df_out


def _select_trace_columns(
    df: pd.DataFrame,
    variable: str,
    strip: str,
    depth: str,
    logger_location: str,
    trace_option: str,
    kind: str,  # "raw" | "ratio" | "all"
) -> pd.DataFrame:
    """
    Column selector for download endpoints.
    """
    cols: List[str] = []

    if "timestamp" in df.columns:
        cols.append("timestamp")

    source_var = "VWC" if variable == "SWC" else variable

    raw_expected: set[str] = set()

    if trace_option == "depths":
        raw_expected = {
            f"{source_var}_{d}_raw_{strip}_{logger_location}"
            for d in sensor_depth_mapping
        }
    else:
        raw_expected = {
            f"{source_var}_{depth}_raw_{strip}_{loc}"
            for loc in logger_location_mapping
        }

    def is_ratio_col(col: str) -> bool:
        return (
            "_ratio_" in col
            and col.startswith(f"{source_var}_")
            and f"_{strip}" in col
        )

    AUX_PREFIXES = ("precip", "rain", "irrig", "gallon")
    AIR_TEMP_PREFIXES = ("temp_air_degF", "temp_air_degC")

    for col in df.columns:
        if col == "timestamp":
            continue

        if any(col.startswith(prefix) for prefix in AUX_PREFIXES):
            cols.append(col)
            continue

        if variable == "T" and any(col.startswith(p) for p in AIR_TEMP_PREFIXES):
            cols.append(col)
            continue

        if kind in ("raw", "all") and col in raw_expected:
            cols.append(col)
            continue

        if kind in ("ratio", "all") and is_ratio_col(col):
            cols.append(col)
            continue

    if len(cols) <= (1 if "timestamp" in cols else 0):
        logger.warning(
            (
                "Download selector found no columns for variable=%s, strip=%s, "
                "depth=%s, logger=%s, trace_option=%s, kind=%s. Returning full DataFrame."
            ),
            variable,
            strip,
            depth,
            logger_location,
            trace_option,
            kind,
        )
        return df

    return df[cols]


def _add_unit_suffixes_for_download(df: pd.DataFrame, variable: str) -> pd.DataFrame:
    """Cosmetic helper for CSV downloads only."""
    df_out = df.copy()
    rename_map: Dict[str, str] = {}

    var_upper = (variable or "").upper()

    for col in df_out.columns:
        if var_upper in {"VWC", "SWC"} and col.startswith("VWC_") and "_raw_" in col:
            if not col.endswith("_pct"):
                rename_map[col] = f"{col}_pct"
            continue

        if var_upper == "T" and col.startswith("T_") and "_raw_" in col:
            if not col.endswith("_degF"):
                rename_map[col] = f"{col}_degF"
            continue

        if var_upper == "EC" and col.startswith("EC_") and "_raw_" in col:
            if not col.endswith("_dS_per_m"):
                rename_map[col] = f"{col}_dS_per_m"
            continue

    if rename_map:
        df_out = df_out.rename(columns=rename_map)

    return df_out


# ---------------------------------------------------------------------------
# Bulk download endpoints (loggers + weather)  [NON-API routes]
# ---------------------------------------------------------------------------

@main_router.get("/bulk_download/options")
async def get_bulk_download_options():
    """Report which bulk-download ZIPs exist for each year."""
    logger.info("📦 get_bulk_download_options() called")

    available: Dict[str, Dict[str, bool]] = {}

    for year in YEARS:
        logger.info("📅 Checking bulk-download availability for year=%s", year)

        loggers_zip = LOGGER_DOWNLOADS_DIR / f"Biochar_Loggers_15min_{year}_USunits.zip"
        weather_zip = WEATHER_DOWNLOADS_DIR / f"Biochar_Weather_15min_{year}_USunits.zip"

        logger.info(
            "   🗂 Logger ZIP: %s → exists=%s",
            loggers_zip,
            loggers_zip.exists(),
        )
        logger.info(
            "   🌦 Weather ZIP: %s → exists=%s",
            weather_zip,
            weather_zip.exists(),
        )

        ancillary = {}
        for key in ANCILLARY_DATASETS.keys():
            try:
                exists = _ancillary_available_for_year(
                    BIOCHAR_MASTER_XLSX,
                    key,
                    int(year),
                )
                ancillary[key] = exists
                logger.info(
                    "   🧪 Ancillary %-12s year=%s → exists=%s",
                    key,
                    year,
                    exists,
                )
            except Exception as e:
                ancillary[key] = False
                logger.exception(
                    "   ❌ Ancillary check failed for key=%s year=%s",
                    key,
                    year,
                )

        available[str(year)] = {
            "loggers": loggers_zip.exists(),
            "weather": weather_zip.exists(),
            **ancillary,
        }

    logger.info("✅ Bulk download availability computed successfully")
    logger.debug("📤 Availability payload: %s", available)

    return JSONResponse({"available": available})


@main_router.get("/bulk_download/loggers/{year}")
async def download_loggers_zip(year: int):
    _ensure_year_allowed(year)

    zip_path = LOGGER_DOWNLOADS_DIR / f"Biochar_Loggers_15min_{year}_USunits.zip"
    if not zip_path.exists():
        raise HTTPException(status_code=404, detail=f"Logger download ZIP not found for {year}.")

    return FileResponse(path=str(zip_path), filename=zip_path.name, media_type="application/zip")


@main_router.get("/bulk_download/weather/{year}")
async def download_weather_zip(year: int):
    _ensure_year_allowed(year)

    zip_path = WEATHER_DOWNLOADS_DIR / f"Biochar_Weather_15min_{year}_USunits.zip"
    if not zip_path.exists():
        raise HTTPException(status_code=404, detail=f"Weather download ZIP not found for {year}.")

    return FileResponse(path=str(zip_path), filename=zip_path.name, media_type="application/zip")


@main_router.get("/bulk_download/irrigation/{year}")
async def download_irrigation_zip(year: int):
    _ensure_year_allowed(year)
    try:
        zip_bytes = _build_ancillary_zip_bytes(BIOCHAR_MASTER_XLSX, "irrigation", year)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Irrigation data not found for {year}.")
    headers = {"Content-Disposition": f'attachment; filename="Biochar_Irrigation_{year}.zip"'}
    return Response(content=zip_bytes, media_type="application/zip", headers=headers)


@main_router.get("/bulk_download/soil_chem/{year}")
async def download_soil_chem_zip(year: int):
    _ensure_year_allowed(year)
    try:
        zip_bytes = _build_ancillary_zip_bytes(BIOCHAR_MASTER_XLSX, "soil_chem", year)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Soil chemistry data not found for {year}.")
    headers = {"Content-Disposition": f'attachment; filename="Biochar_SoilChem_{year}.zip"'}
    return Response(content=zip_bytes, media_type="application/zip", headers=headers)


@main_router.get("/bulk_download/soil_bio/{year}")
async def download_soil_bio_zip(year: int):
    _ensure_year_allowed(year)
    try:
        zip_bytes = _build_ancillary_zip_bytes(BIOCHAR_MASTER_XLSX, "soil_bio", year)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Soil biology data not found for {year}.")
    headers = {"Content-Disposition": f'attachment; filename="Biochar_SoilBio_{year}.zip"'}
    return Response(content=zip_bytes, media_type="application/zip", headers=headers)


@main_router.get("/bulk_download/biomass/{year}")
async def download_biomass_zip(year: int):
    _ensure_year_allowed(year)
    try:
        zip_bytes = _build_ancillary_zip_bytes(BIOCHAR_MASTER_XLSX, "biomass", year)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Biomass/hay data not found for {year}.")
    headers = {"Content-Disposition": f'attachment; filename="Biochar_BiomassHay_{year}.zip"'}
    return Response(content=zip_bytes, media_type="application/zip", headers=headers)


ANCILLARY_DATASETS: Dict[str, Dict[str, str]] = {
    "irrigation": {"sheet": "IRRIGATION", "csv": "irrigation.csv"},
    "soil_chem":  {"sheet": "SOIL CHEM",  "csv": "soil_chem.csv"},
    "soil_bio":   {"sheet": "SOIL BIO",   "csv": "soil_bio.csv"},
    "biomass":    {"sheet": "Hay Data All", "csv": "biomass_hay.csv"},
}


def _find_sheet_for_year(xlsx_path: str, base_sheet: str, year: int) -> Optional[str]:
    """
    Supports either:
      - year-specific tabs like "2023 IRRIGATION" / "2023 IRRIGATION " (trailing spaces)
      - or a single tab like "Hay Data All" with a Year column we can filter.
    """
    try:
        xl = pd.ExcelFile(xlsx_path)
    except FileNotFoundError:
        return None

    if base_sheet in xl.sheet_names:
        return base_sheet

    target_prefix = f"{year} {base_sheet}".strip().lower()
    for name in xl.sheet_names:
        if name.strip().lower() == target_prefix:
            return name

    for name in xl.sheet_names:
        if name.strip().lower().startswith(f"{year} ") and base_sheet.lower() in name.strip().lower():
            return name

    return None


def _load_ancillary_df_for_year(xlsx_path: str, dataset_key: str, year: int) -> pd.DataFrame:
    """
    Load an ancillary dataset from biochar-data-master.xlsx.
    Handles:
      - per-year sheets, OR
      - single sheet filtered by Year.
    """
    if dataset_key not in ANCILLARY_DATASETS:
        raise HTTPException(status_code=400, detail=f"Unknown dataset: {dataset_key}")

    base_sheet = ANCILLARY_DATASETS[dataset_key]["sheet"]
    sheet_name = _find_sheet_for_year(xlsx_path, base_sheet, year)

    if sheet_name is None:
        raise FileNotFoundError(f"No sheet found for dataset={dataset_key} year={year}")

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)

    df = df.dropna(axis=1, how="all")

    year_cols = [c for c in df.columns if str(c).strip().lower() in ("year", "yr")]
    if year_cols:
        yc = year_cols[0]
        df_year = df[pd.to_numeric(df[yc], errors="coerce") == int(year)].copy()
        if not df_year.empty:
            df = df_year

    if not year_cols:
        df.insert(0, "Year", int(year))

    return df


def _build_ancillary_zip_bytes(
    xlsx_path: str,
    dataset_key: str,
    year: int,
) -> bytes:
    """
    Build a ZIP with:
      - one CSV file (dataset)
      - README.txt
    """
    df = _load_ancillary_df_for_year(xlsx_path, dataset_key, year)

    csv_name = ANCILLARY_DATASETS[dataset_key]["csv"]
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    readme = (
        f"Biochar Project — Bulk Download\n"
        f"Dataset: {dataset_key}\n"
        f"Year: {year}\n\n"
        f"Source: {Path(xlsx_path).name}\n"
        f"Notes:\n"
        f" - Irrigation is periodic during the irrigation season.\n"
        f" - Soil chemistry/biology are typically sampled ~2× per year.\n"
        f" - Biomass/hay includes yield and chemical analysis (tab: Hay Data All).\n"
    ).encode("utf-8")

    out = BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_bytes)
        zf.writestr("README.txt", readme)
    out.seek(0)
    return out.getvalue()


def _ancillary_available_for_year(xlsx_path: str, dataset_key: str, year: int) -> bool:
    """
    Used by /bulk_download/options to enable/disable buttons.
    We try to locate a sheet and (if Year-filtered) check for at least 1 row.
    """
    try:
        df = _load_ancillary_df_for_year(xlsx_path, dataset_key, year)
        return not df.empty
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Defaults / options endpoint
# ---------------------------------------------------------------------------
@api_router.get("/markdown_files")
async def get_markdown_files():
    mapping = build_markdown_mapping()
    return JSONResponse(mapping)


@api_router.get("/get_defaults_and_options")
async def get_defaults_and_options():
    years = YEARS
    strips = [{"value": k, "label": strip_name_mapping[k]} for k in strip_name_mapping]
    variables = [{"value": k, "label": variable_name_mapping[k]} for k in variable_name_mapping]
    depths = [{"value": str(d), "label": sensor_depth_mapping[d]["us"]} for d in sensor_depth_mapping]
    logger_locations = [{"value": k, "label": logger_location_mapping[k]} for k in logger_location_mapping]
    granularities = [{"value": g, "label": granularity_name_mapping[g]} for g in granularity_name_mapping]

    r = state.DATE_RANGES.get(int(DEFAULT_YEAR), {}).get(str(DEFAULT_GRANULARITY))

    if r:
        start_date = r["min"]
        end_date = r["max"]
    else:
        start_date = DEFAULT_START_DATE
        end_date = DEFAULT_END_DATE

    response_data = {
        "defaults": {
            "year": DEFAULT_YEAR,
            "startDate": start_date,
            "endDate": end_date,
            "variable": DEFAULT_VARIABLE,
            "depth": str(DEFAULT_DEPTH),
            "strip": DEFAULT_STRIP,
            "loggerLocation": DEFAULT_LOGGER_LOCATION,
            "granularity": DEFAULT_GRANULARITY,
            "traceOption": PLOT_BASED_ON_OPTIONS[0]["value"],
            "unitSystem": "us",
        },
        "years": years,
        "strips": strips,
        "variables": variables,
        "depths": depths,
        "loggerLocations": logger_locations,
        "granularities": granularities,
        "traceOptions": PLOT_BASED_ON_OPTIONS,
        "depthMapping": sensor_depth_mapping,
        "gseasonPeriods": DEFAULT_GSEASON_PERIODS,
        "label_name_mapping": label_name_mapping,
        "dateRanges": state.DATE_RANGES,
    }

    return JSONResponse(response_data)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class PeriodSpec(BaseModel):
    code: str
    label: str
    start: str
    end: str


class PlotRequest(BaseModel):
    year: int
    variable: str
    strip: str
    granularity: str
    startDate: str
    endDate: str
    loggerLocation: str
    depth: str
    traceOption: str
    unitSystem: str
    periods: Optional[List[PeriodSpec]] = Field(default=None)


# ---------------------------------------------------------------------------
# Plot routes
# ---------------------------------------------------------------------------
@api_router.post("/plot_raw")
async def api_plot_raw(req: PlotRequest):
    year = req.year
    gran = req.granularity.lower()
    var = req.variable
    strip = req.strip
    logger_loc = req.loggerLocation
    depth = req.depth
    unit = req.unitSystem
    trace_option = TRACE_OPTION_MAP[req.traceOption]
    start = req.startDate
    end = req.endDate

    source_var = "VWC" if var == "SWC" else var

    if gran == "gseason":
        periods_raw = req.periods or []
        periods_list = periods_to_list_of_dicts(periods_raw)

        df_gseason = load_gseason_df(
            year=year,
            periods=periods_list,
            unit_system=unit,
        )

        fig = make_raw_gseason_figure(
            df=df_gseason,
            periods=periods_list,
            variable=var,
            strip=strip,
            logger_location=logger_loc,
            depth=int(depth),
            unit_system=unit,
            year=year,
            trace_option=trace_option,
        )
        return JSONResponse(fig)

    df = load_logger_year(year, gran)
    if "timestamp" not in df.columns:
        raise HTTPException(400, "No timestamp column in data")

    df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    if trace_option == "depths":
        expected = [f"{source_var}_{d}_raw_{strip}_{logger_loc}" for d in sensor_depth_mapping]
    else:
        expected = [f"{source_var}_{depth}_raw_{strip}_{lkey}" for lkey in logger_location_mapping]

    present = [c for c in expected if c in df.columns]
    non_empty = [c for c in present if df[c].notna().any()]

    if not non_empty:
        raise HTTPException(
            400,
            detail=(
                f"No valid data to plot for {var!r} @ strip={strip!r}, "
                f"loc={logger_loc!r}, depth={depth!r} between {start} and {end}. "
                f"Found columns: {present}"
            ),
        )

    fig = make_raw_figure(
        df=df,
        year=year,
        variable=var,
        strip=strip,
        granularity=gran,
        logger_location=logger_loc,
        depth=depth,
        trace_option=trace_option,
        unit_system=unit,
        start=start,
        end=end,
    )

    start_ts = pd.to_datetime(start)
    end_ts = pd.to_datetime(end)

    layout = fig.setdefault("layout", {})
    xaxis = layout.setdefault("xaxis", {})

    xaxis["range"] = [start_ts.isoformat(), end_ts.isoformat()]
    xaxis["autorange"] = False
    return JSONResponse(fig)


@api_router.post("/plot_ratio")
async def api_plot_ratio(req: PlotRequest):
    year = req.year
    gran = req.granularity.lower()
    var = req.variable
    strip = req.strip
    logger_loc = req.loggerLocation
    depth = int(req.depth)
    unit = req.unitSystem
    start, end = req.startDate, req.endDate

    if gran == "gseason":
        periods = req.periods or []
        df_gs = load_gseason_df(
            year=year,
            periods=periods,
            unit_system=unit,
            use_ratios=True,
        )
        fig = make_ratio_gseason_figure(
            df=df_gs,
            periods=periods,
            variable=var,
            strip=strip,
            logger_location=logger_loc,
            depth=depth,
            unit_system=unit,
            year=year,
        )
        return JSONResponse(fig)

    df = load_logger_year(year, gran)
    if "timestamp" not in df.columns:
        raise HTTPException(400, "No timestamp column in data")

    df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    if var == "T":
        fig = make_temperature_delta_figure(
            df=df,
            depth=depth,
            logger_location=logger_loc,
            unit_system=unit,
            granularity=gran,
            year=year,
            start=start,
            end=end,
        )
    else:
        fig = make_ratio_figure(
            df=df,
            variable=var,
            strip=strip,
            logger_location=logger_loc,
            unit_system=unit,
            granularity=gran,
            year=year,
            start=start,
            end=end,
            depth=str(depth),
        )

    return JSONResponse(fig)


# ---------------------------------------------------------------------------
# Lab table routes (NIR + Soil) — NEW STANDARD
#   Payload shape:
#     { "title": str, "sets": [ { "key", "label", "periods", "variables", "rows",
#                                "rowLabels", "data" } ] }
# ---------------------------------------------------------------------------

# Soil tables (compiled clean Ward-style masters)
from biochar_app.scripts.soil_tables import (
    build_soilbio_table,
    build_soilchem_table,
)

# NIR tables (recommended: single builder returning {title, sets})
# NOTE: You should update nir_tables.py to expose build_nir_table() that wraps Sets 1–4.
# Until then, keep the set builders and wrap them here.
from biochar_app.scripts.nir_tables import (
    build_nir_set1_table,
    build_nir_set2_table,
    build_nir_set3_table,
    build_nir_set4_table,
)


@api_router.get("/get_soilbio_table")
async def api_get_soilbio_table():
    """
    Soil Biological Health (Ward PLFA) — NEW STANDARD payload:
      { "title": "...", "sets": [ ... ] }
    """
    payload = build_soilbio_table(WARD_MASTER_SOILBIO_CLEAN_CSV, min_year=2023)
    return JSONResponse(payload)


@api_router.get("/get_soilchem_table")
async def api_get_soilchem_table():
    """
    Soil Chemistry (Ward) — NEW STANDARD payload:
      { "title": "...", "sets": [ ... ] }
    """
    payload = build_soilchem_table(WARD_MASTER_SOILCHEM_CLEAN_CSV, min_year=2023)
    return JSONResponse(payload)


@api_router.get("/get_nir_table")
async def api_get_nir_table():
    """
    NIR (Ward) — NEW STANDARD payload:
      { "title": "...", "sets": [ ... ] }

    If/when you add build_nir_table() in nir_tables.py, replace this wrapper with:
        payload = build_nir_table(WARD_MASTER_NIR_CLEAN_CSV, min_year=2023)
    """
    set1 = build_nir_set1_table(WARD_MASTER_NIR_CLEAN_CSV)
    set2 = build_nir_set2_table(WARD_MASTER_NIR_CLEAN_CSV)
    set3 = build_nir_set3_table(WARD_MASTER_NIR_CLEAN_CSV)
    set4 = build_nir_set4_table(WARD_MASTER_NIR_CLEAN_CSV)

    # Defensive: allow either:
    #   - set payloads already shaped like a set: {key,label,periods,variables,rows,rowLabels,data}
    #   - or older "single-set" payloads: {title,periods,variables,rows,rowLabels,data}
    def _coerce_to_set(obj: Dict[str, Any], fallback_key: str, fallback_label: str) -> Dict[str, Any]:
        if not isinstance(obj, dict):
            return {
                "key": fallback_key,
                "label": fallback_label,
                "periods": [],
                "variables": [],
                "rows": [],
                "rowLabels": {},
                "data": {},
            }

        if "periods" in obj and "variables" in obj and "rows" in obj and "data" in obj:
            # If it already looks like a set, ensure key/label exist
            if "key" not in obj:
                obj = {**obj, "key": fallback_key}
            if "label" not in obj:
                obj = {**obj, "label": fallback_label}
            # Drop accidental embedded "title" if present
            obj.pop("title", None)
            return obj

        # Unknown shape — return empty set
        return {
            "key": fallback_key,
            "label": fallback_label,
            "periods": [],
            "variables": [],
            "rows": [],
            "rowLabels": {},
            "data": {},
        }

    sets = [
        _coerce_to_set(set1, "nir_set1", "Set 1: Pasture Quality Metrics"),
        _coerce_to_set(set2, "nir_set2", "Set 2: Carbohydrates & Energy Partitioning"),
        _coerce_to_set(set3, "nir_set3", "Set 3: Minerals & Ash"),
        _coerce_to_set(set4, "nir_set4", "Set 4: Digestibility Metrics"),
    ]

    return JSONResponse(
        {
            "title": "Pasture Qualitative Metrics (Ward NIR)",
            "sets": sets,
        }
    )

# ---------------------------------------------------------------------------
# Summary statistics + downloads
# ---------------------------------------------------------------------------
class SummaryStatsRequest(BaseModel):
    year: int
    variable: str
    strip: str
    granularity: str
    downloadType: Literal["raw", "ratio", "all", "zip"] = "zip"
    summaryStats: Dict[str, Any]


@api_router.post("/download_summary_data")
async def download_summary_data(req: SummaryStatsRequest):
    """
    Download summary statistics in one of four formats:
      - raw   -> single CSV (raw rows only)
      - ratio -> single CSV (ratio rows only)
      - all   -> single CSV (raw + ratio rows combined)
      - zip   -> ZIP bundle (raw.csv + ratio.csv + README.txt)
    """
    download_type = (getattr(req, "downloadType", None) or "zip").lower().strip()

    # These should already exist on your request model (based on your current code)
    year: int = int(req.year)
    variable: str = str(req.variable)
    granularity: str = str(req.granularity)
    strip: str = str(req.strip)

    stats: Dict[str, Any] = req.summaryStats or {}
    if not isinstance(stats, dict):
        raise HTTPException(status_code=400, detail="summaryStats must be a JSON object")

    # ---------------------------------------------------------------------
    # Build raw_df / ratio_df using your EXISTING logic (gseason vs non-gseason)
    # ---------------------------------------------------------------------

    raw_rows: List[Dict[str, Any]] = []
    ratio_rows: List[Dict[str, Any]] = []

    if granularity == "gseason":
        # stats structure (your existing): { season: { variable: { strip_depth: {raw_statistics, ratio_statistics}}}}
        # We flatten to rows with Season + Variable + StripDepth + stats columns
        for season, season_block in stats.items():
            if not isinstance(season_block, dict):
                continue

            var_block = season_block.get(variable)
            if not isinstance(var_block, dict):
                continue

            for strip_depth, sd_block in var_block.items():
                if not isinstance(sd_block, dict):
                    continue

                raw_stats = sd_block.get("raw_statistics") or {}
                ratio_stats = sd_block.get("ratio_statistics") or {}

                if isinstance(raw_stats, dict) and raw_stats:
                    row = {"Season": season, "StripDepth": strip_depth, "Type": "raw"}
                    # include StartDate/EndDate if present in your seasonal structure
                    if "StartDate" in raw_stats:
                        row["StartDate"] = raw_stats.get("StartDate", "")
                    if "EndDate" in raw_stats:
                        row["EndDate"] = raw_stats.get("EndDate", "")
                    for k in ("min", "mean", "max", "std"):
                        if k in raw_stats:
                            row[k] = raw_stats.get(k)
                    raw_rows.append(row)

                if isinstance(ratio_stats, dict) and ratio_stats:
                    row = {"Season": season, "StripDepth": strip_depth, "Type": "ratio"}
                    if "StartDate" in ratio_stats:
                        row["StartDate"] = ratio_stats.get("StartDate", "")
                    if "EndDate" in ratio_stats:
                        row["EndDate"] = ratio_stats.get("EndDate", "")
                    for k in ("min", "mean", "max", "std"):
                        if k in ratio_stats:
                            row[k] = ratio_stats.get(k)
                    ratio_rows.append(row)

    else:
        # Non-gseason structure: you already have this working today in routes.py.
        # Most commonly you have something like:
        #   stats = {"raw_statistics": {...}, "ratio_statistics": {...}}
        # where each inner dict is keyed by a row label like "S1_D1" or "S1/S2_T" etc.

        raw_stats_block = stats.get("raw_statistics") or {}
        ratio_stats_block = stats.get("ratio_statistics") or {}

        if isinstance(raw_stats_block, dict):
            for row_key, vals in raw_stats_block.items():
                if not isinstance(vals, dict):
                    continue
                raw_rows.append(
                    {
                        "Row": row_key,
                        "Type": "raw",
                        "min": vals.get("min"),
                        "mean": vals.get("mean"),
                        "max": vals.get("max"),
                        "std": vals.get("std"),
                    }
                )

        if isinstance(ratio_stats_block, dict):
            for row_key, vals in ratio_stats_block.items():
                if not isinstance(vals, dict):
                    continue
                ratio_rows.append(
                    {
                        "Row": row_key,
                        "Type": "ratio",
                        "min": vals.get("min"),
                        "mean": vals.get("mean"),
                        "max": vals.get("max"),
                        "std": vals.get("std"),
                    }
                )

    raw_df = pd.DataFrame(raw_rows)
    ratio_df = pd.DataFrame(ratio_rows)

    # Optional: enforce a stable column order
    def _reorder(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        preferred = []
        for c in ("Season", "StartDate", "EndDate", "Row", "StripDepth", "Type", "min", "mean", "max", "std"):
            if c in df.columns:
                preferred.append(c)
        rest = [c for c in df.columns if c not in preferred]
        return df[preferred + rest]

    raw_df = _reorder(raw_df)
    ratio_df = _reorder(ratio_df)

    # ---------------------------------------------------------------------
    # Helpers to return real CSV (bytes) so browsers don’t “helpfully” zip it.
    # ---------------------------------------------------------------------
    def _csv_response(df: pd.DataFrame, filename: str) -> StreamingResponse:
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        return StreamingResponse(
            io.BytesIO(csv_bytes),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # ---------------------------------------------------------------------
    # Return requested type
    # ---------------------------------------------------------------------
    safe_var = re.sub(r"[^A-Za-z0-9_\\-]+", "_", variable).strip("_")
    safe_gran = re.sub(r"[^A-Za-z0-9_\\-]+", "_", granularity).strip("_")
    safe_strip = re.sub(r"[^A-Za-z0-9_\\-]+", "_", strip).strip("_")

    if download_type == "raw":
        fname = f"summary_raw_{safe_gran}_{safe_var}_{year}_{safe_strip}.csv"
        return _csv_response(raw_df, fname)

    if download_type == "ratio":
        fname = f"summary_ratio_{safe_gran}_{safe_var}_{year}_{safe_strip}.csv"
        return _csv_response(ratio_df, fname)

    if download_type == "all":
        combined_df = pd.concat(
            [raw_df, ratio_df],
            ignore_index=True,
        )
        fname = f"summary_all_{safe_gran}_{safe_var}_{year}_{safe_strip}.csv"
        return _csv_response(combined_df, fname)

    if download_type != "zip":
        raise HTTPException(status_code=400, detail=f"Unknown downloadType={download_type!r}")

    # ZIP bundle: raw.csv + ratio.csv + README.txt
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            f"summary_raw_{safe_gran}_{safe_var}_{year}_{safe_strip}.csv",
            raw_df.to_csv(index=False),
        )
        zf.writestr(
            f"summary_ratio_{safe_gran}_{safe_var}_{year}_{safe_strip}.csv",
            ratio_df.to_csv(index=False),
        )

        readme = (
            f"Biochar Fruita CSU – Summary Statistics Download\n"
            f"\n"
            f"Year: {year}\n"
            f"Granularity: {granularity}\n"
            f"Variable: {variable}\n"
            f"Selection (strip dropdown): {strip}\n"
            f"\n"
            f"Files:\n"
            f"  - summary_raw_*.csv   : raw statistics rows\n"
            f"  - summary_ratio_*.csv : ratio statistics rows (unitless)\n"
        )
        zf.writestr("README.txt", readme)

    zip_buf.seek(0)
    zip_fname = f"summary_bundle_{safe_gran}_{safe_var}_{year}_{safe_strip}.zip"
    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_fname}"'},
    )


@api_router.post("/get_summary_stats")
async def get_summary_stats(data: Dict[str, Any] = Body(...)):
    """
    (Unchanged from your version)
    """
    required = ["year", "variable", "strip", "granularity", "depth"]
    missing = [k for k in required if data.get(k) is None]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing parameter(s): {', '.join(missing)}")

    try:
        year = int(data["year"])
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid year: {data.get('year')}")

    variable = data["variable"]
    strip = data["strip"]
    granularity = data["granularity"]

    try:
        depth = int(data["depth"])
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid depth: {data.get('depth')}")

    unit_system = data.get("unitSystem", "us")
    start = data.get("startDate")
    end = data.get("endDate")

    label_block = label_name_mapping.get(variable, {})
    if isinstance(label_block, dict):
        human_var = label_block.get(unit_system) or label_block.get("us") or str(variable)
    else:
        human_var = str(variable)

    abbr = variable_name_abbrev.get(variable, variable)

    def _clean_numbers(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: _clean_numbers(v) for k, v in obj.items()}
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    if granularity == "gseason":
        try:
            df = load_logger_year(year, "15min")
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))

        if df.empty:
            return JSONResponse(
                {
                    "year": year,
                    "variable": variable,
                    "strip": strip,
                    "granularity": granularity,
                    "depth": str(depth),
                    "unitSystem": unit_system,
                    "raw_statistics": {},
                    "ratio_statistics": {},
                    "gseason_stats": {},
                    "display_label_raw": human_var,
                    "display_label_ratio": abbr,
                }
            )

        if "timestamp" not in df.columns:
            raise HTTPException(status_code=500, detail="Expected 'timestamp' column in logger data.")

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        gseason_stats: Dict[str, Dict[str, Any]] = {}

        for code, spec in DEFAULT_GSEASON_PERIODS.items():
            sm, sd = map(int, spec["start"].split("-"))
            em, ed = map(int, spec["end"].split("-"))

            start_year = year - 1 if sm > em else year
            end_year = year

            start_ts = pd.Timestamp(f"{start_year}-{spec['start']}")
            end_ts = pd.Timestamp(f"{end_year}-{spec['end']}") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

            slice_df = df[(df["timestamp"] >= start_ts) & (df["timestamp"] <= end_ts)]

            if slice_df.empty:
                raw_stats: Dict[str, Dict[str, float]] = {}
                ratio_stats: Dict[str, Dict[str, float]] = {}
            else:
                raw_stats, ratio_stats = compute_summary_statistics(slice_df, variable, strip, str(depth))

            gseason_stats[code] = {
                "raw_statistics": _clean_numbers(raw_stats),
                "ratio_statistics": _clean_numbers(ratio_stats),
            }

        return JSONResponse(
            {
                "year": year,
                "variable": variable,
                "strip": strip,
                "granularity": granularity,
                "depth": str(depth),
                "unitSystem": unit_system,
                "raw_statistics": {},
                "ratio_statistics": {},
                "gseason_stats": gseason_stats,
                "display_label_raw": human_var,
                "display_label_ratio": abbr,
            }
        )

    try:
        df = load_logger_year(year, granularity)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df.empty:
        return JSONResponse(
            {
                "year": year,
                "variable": variable,
                "strip": strip,
                "granularity": granularity,
                "depth": str(depth),
                "unitSystem": unit_system,
                "raw_statistics": {},
                "ratio_statistics": {},
                "gseason_stats": {},
                "display_label_raw": human_var,
                "display_label_ratio": abbr,
            }
        )

    if start and end and "timestamp" in df.columns:
        df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    if variable == "SWC":
        stats_raw: Dict[str, Dict[str, float]] = {}
        stats_ratio: Dict[str, Dict[str, float]] = {}

        vol_prefix = "SWC_vol_L" if unit_system == "metric" else "SWC_vol_gal"

        for loc_key in logger_location_mapping.keys():
            vol_col = f"{vol_prefix}_{strip}_{loc_key}_{depth}"
            if vol_col not in df.columns:
                continue

            series = pd.to_numeric(df[vol_col], errors="coerce").dropna()
            if series.empty:
                continue

            label_key = f"SWC_{depth}_raw_{strip}_{loc_key}"
            stats_raw[label_key] = {
                "min": float(series.min()),
                "mean": float(series.mean()),
                "max": float(series.max()),
                "std": float(series.std(ddof=1)) if len(series) > 1 else 0.0,
            }

        return JSONResponse(
            {
                "year": year,
                "variable": variable,
                "strip": strip,
                "granularity": granularity,
                "depth": str(depth),
                "unitSystem": unit_system,
                "raw_statistics": _clean_numbers(stats_raw),
                "ratio_statistics": _clean_numbers(stats_ratio),
                "gseason_stats": {},
                "display_label_raw": human_var,
                "display_label_ratio": abbr,
            }
        )

    stats_raw, stats_ratio = compute_summary_statistics(df, variable, strip, str(depth))

    if variable in ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"]:
        stats_ratio = {}

    return JSONResponse(
        {
            "year": year,
            "variable": variable,
            "strip": strip,
            "granularity": granularity,
            "depth": str(depth),
            "unitSystem": unit_system,
            "raw_statistics": _clean_numbers(stats_raw),
            "ratio_statistics": _clean_numbers(stats_ratio),
            "gseason_stats": {},
            "display_label_raw": human_var,
            "display_label_ratio": abbr,
        }
    )


# ---------------------------------------------------------------------------
# Markdown + custom gseason pages
# ---------------------------------------------------------------------------
@main_router.get("/markdown/{filename}")
async def serve_markdown(filename: str):
    md_dir = os.path.join(os.path.dirname(__file__), "..", "markdown")
    fullpath = os.path.abspath(os.path.join(md_dir, filename))

    if not os.path.exists(fullpath):
        raise HTTPException(status_code=404, detail=f"Markdown file '{filename}' not found")

    return FileResponse(fullpath, media_type="text/markdown")


@main_router.get("/custom-gseason")
async def custom_gseason(request: Request):
    return templates.TemplateResponse("_custom_gseason.html", {"request": request})


# ---------------------------------------------------------------------------
# Trace download routes (raw / ratio / all)
# ---------------------------------------------------------------------------
class DownloadDataRequest(BaseModel):
    year: int
    variable: str
    strip: str
    granularity: str
    depth: str
    unitSystem: str = "us"
    downloadType: str = "all"  # "raw" | "ratio" | "all"
    startDate: Optional[str] = None
    endDate: Optional[str] = None


@api_router.post("/download_data")
async def download_data(req: DownloadDataRequest):
    """
    (Unchanged from your version)
    """
    year = req.year
    variable = req.variable
    strip = req.strip
    granularity = req.granularity.lower()
    depth = req.depth
    unit_system = req.unitSystem
    download_ty = req.downloadType or "all"
    start = req.startDate
    end = req.endDate

    _ensure_year_allowed(year)

    try:
        df = load_logger_year(year, granularity)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data available for year={year}, granularity={granularity}")

    if start and end and "timestamp" in df.columns:
        df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data in the requested date range for year={year}, granularity={granularity}")

    source_var = "VWC" if variable == "SWC" else variable

    AUX_PREFIXES = ("precip", "rain", "irrig", "gallon")
    AIR_TEMP_PREFIXES = ("temp_air_degF", "temp_air_degC")

    raw_cols: List[str] = []
    ratio_cols: List[str] = []
    aux_cols: List[str] = []

    for col in df.columns:
        if col == "timestamp":
            continue

        if any(col.startswith(p) for p in AUX_PREFIXES):
            aux_cols.append(col)
            continue

        if variable == "T" and any(col.startswith(p) for p in AIR_TEMP_PREFIXES):
            aux_cols.append(col)
            continue

        if col.startswith(f"{source_var}_{depth}_raw_{strip}_") and f"_{strip}_" in col:
            raw_cols.append(col)
            continue

        if col.startswith(f"{source_var}_{depth}_ratio_") and f"_{strip}" in col:
            ratio_cols.append(col)
            continue

    has_raw = bool(raw_cols) and download_ty in ("raw", "all")
    has_ratio = bool(ratio_cols) and download_ty in ("ratio", "all")

    if not has_raw and not has_ratio:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No matching columns found for variable={variable}, "
                f"strip={strip}, depth={depth}, granularity={granularity} "
                f"for downloadType={download_ty}"
            ),
        )

    combined_cols: List[str] = []
    if "timestamp" in df.columns:
        combined_cols.append("timestamp")

    combined_cols.extend(sorted(aux_cols))
    if has_raw:
        combined_cols.extend(sorted(raw_cols))
    if has_ratio:
        combined_cols.extend(sorted(ratio_cols))

    combined_df = df[combined_cols].copy()
    combined_df = _add_unit_suffixes_for_download(combined_df, variable)
    combined_df = _round_ratio_columns(combined_df, decimals=6)

    bioio = BytesIO()
    with zipfile.ZipFile(bioio, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.txt", "See server-generated README in your version.\n")
        zf.writestr("plot_data.csv", combined_df.to_csv(index=False))

    bioio.seek(0)

    fname = f"data_{download_ty}_{variable}_{strip}_D{depth}_{granularity}_{year}.zip"
    return StreamingResponse(
        bioio,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@main_router.get("/download_raw_data")
async def download_raw_data(
    year: int = Query(...),
    granularity: str = Query(...),
    variable: str = Query(...),
    strip: str = Query(...),
    depth: str = Query(...),
    loggerLocation: str = Query(..., alias="loggerLocation"),
    traceOption: str = Query("depths", alias="traceOption"),
):
    gran = granularity.lower()
    df = load_logger_year(year, gran)

    df_sel = _select_trace_columns(
        df=df,
        variable=variable,
        strip=strip,
        depth=depth,
        logger_location=loggerLocation,
        trace_option=traceOption,
        kind="raw",
    )

    df_sel = _round_ratio_columns(df_sel, decimals=6)
    df_sel = _add_unit_suffixes_for_download(df_sel, variable)
    csv = df_sel.to_csv(index=False)
    fname = f"raw_{year}_{gran}_{variable}_{strip}_{loggerLocation}_D{depth}.csv"

    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@main_router.get("/download_ratio_data")
async def download_ratio_data(
    year: int = Query(...),
    granularity: str = Query(...),
    variable: str = Query(...),
    strip: str = Query(...),
    depth: str = Query(...),
    loggerLocation: str = Query(..., alias="loggerLocation"),
    traceOption: str = Query("depths", alias="traceOption"),
):
    gran = granularity.lower()
    df = load_logger_year(year, gran)

    df_sel = _select_trace_columns(
        df=df,
        variable=variable,
        strip=strip,
        depth=depth,
        logger_location=loggerLocation,
        trace_option=traceOption,
        kind="ratio",
    )

    df_sel = _round_ratio_columns(df_sel, decimals=6)
    df_sel = _add_unit_suffixes_for_download(df_sel, variable)
    csv = df_sel.to_csv(index=False)
    fname = f"ratio_{year}_{gran}_{variable}_{strip}_{loggerLocation}_D{depth}.csv"

    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ---------------------------------------------------------------------------
# Registry-based bulk download (checkbox UI) [API routes]
# ---------------------------------------------------------------------------
class BulkDownloadRequest(BaseModel):
    keys: List[str]



async def api_bulk_download_manifest():
    manifest = build_manifest(BIOCHAR_MASTER_XLSX)
    return JSONResponse(manifest)


@api_router.post("/bulk_download")
async def api_bulk_download(req: BulkDownloadRequest):
    logger.info("📦 /api/bulk_download called with keys=%s", req.keys)

    try:
        # Show what the backend considers valid keys
        reg = default_bulk_registry()
        valid = sorted([s.dataset_key for s in reg])
        logger.info("✅ Valid registry keys (%d): %s", len(valid), valid)

        missing = [k for k in req.keys if k not in valid]
        if missing:
            logger.error("❌ Missing/unknown keys from request: %s", missing)
        else:
            logger.info("✅ All requested keys recognized.")

        zip_bytes = build_zip_for_selection(BIOCHAR_MASTER_XLSX, req.keys, registry=reg)

    except Exception as e:
        logger.exception("❌ build_zip_for_selection failed")
        raise HTTPException(status_code=400, detail=str(e))

@api_router.get("/get_biomass_field_table")
async def api_get_biomass_field_table():
    payload = get_biomass_field_table_payload(WARD_MASTER_BIOMASS_FIELD_CLEAN_CSV, min_year=2023)
    return JSONResponse(payload)