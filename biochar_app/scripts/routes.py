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

import pandas as pd
from fastapi import APIRouter, Request, HTTPException, Query, Body
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from biochar_app.scripts.bulk_downloads import bulk_router
from biochar_app.scripts.biomass_field_tables import get_biomass_field_table_payload

# ✅ Fix: package-qualified import
from biochar_app.scripts.bulk_download_utils import default_bulk_registry
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
    WARD_MASTER_NIR_CSV,
    WARD_MASTER_SOILBIO_CSV,
    WARD_MASTER_SOILCHEM_CSV,
    BIOMASS_FIELD_CSV,
)
from biochar_app.scripts.markdown_config import build_markdown_mapping

# ✅ NEW soil table modules
from biochar_app.scripts.tables_soil_bio import build_soilbio_table
from biochar_app.scripts.tables_soil_chem import build_soilchem_table

# NIR tables (Sets 1–4)
from biochar_app.scripts.tables_nir import (
    build_nir_set1_table,
    build_nir_set2_table,
    build_nir_set3_table,
    build_nir_set4_table,
)

logger = logging.getLogger(__name__)

# ---- Paths ----
main_router = APIRouter()
api_router = APIRouter(prefix="/api")
api_router.include_router(bulk_router)

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)

# ---------------------------------------------------------------------------
# Bulk download directories (must match etl.py)
# ---------------------------------------------------------------------------
DOWNLOADS_BASE_DIR = Path(PARQUET_DIR).parent / "downloads"
LOGGER_DOWNLOADS_DIR = DOWNLOADS_BASE_DIR / "loggers"
WEATHER_DOWNLOADS_DIR = DOWNLOADS_BASE_DIR / "weather"

# User confirmed: biochar-data-master.xlsx is in data-raw
BIOCHAR_MASTER_XLSX = "biochar_app/data-raw/biochar-data-master.xlsx"
_LOADED_LOGGER_CACHE: dict[tuple[int, str], Any] = {}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _normalize_sheet_name(s: str) -> str:
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

    target = f"{int(year)} IRRIGATION"
    sheets_norm = {_normalize_sheet_name(s): s for s in xl.sheet_names}
    sheet_name = sheets_norm.get(target)

    if not sheet_name:
        if logger:
            logger.info(
                f"ℹ️ No irrigation sheet for year {year}. "
                f"Expected '{target}'. Available: {list(sheets_norm.keys())}"
            )
        return empty

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)

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

    df = df.loc[:, ~df.columns.duplicated()].copy()

    keep = [c for c in ["time_on", "time_off", "gallons", "side"] if c in df.columns]
    df = df[keep].copy()

    for c in ["time_on", "time_off"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "gallons" in df.columns:
        df["gallons"] = pd.to_numeric(df["gallons"], errors="coerce")

    if "time_on" in df.columns and "time_off" in df.columns:
        df = df.dropna(subset=["time_on", "time_off"])

    if df.empty:
        return empty

    for col in empty.columns:
        if col not in df.columns:
            df[col] = pd.NA

    return df[empty.columns].copy()


def _clean_for_json(obj):
    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean_for_json(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


def _ensure_year_allowed(year: int) -> None:
    if year not in YEARS:
        raise HTTPException(status_code=404, detail=f"Year {year} is not available.")


def _round_ratio_columns(df: pd.DataFrame, decimals: int = 6) -> pd.DataFrame:
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

    AUX_PREFIXES = ("precip", "rain", "irrig", "gallon", "liter")
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
    logger.info("📦 get_bulk_download_options() called")

    available: Dict[str, Dict[str, bool]] = {}

    for year in YEARS:
        loggers_zip = LOGGER_DOWNLOADS_DIR / f"Biochar_Loggers_15min_{year}_USunits.zip"
        weather_zip = WEATHER_DOWNLOADS_DIR / f"Biochar_Weather_15min_{year}_USunits.zip"

        ancillary = {}
        for key in ANCILLARY_DATASETS.keys():
            try:
                exists = _ancillary_available_for_year(BIOCHAR_MASTER_XLSX, key, int(year))
                ancillary[key] = exists
            except Exception:
                ancillary[key] = False

        available[str(year)] = {
            "loggers": loggers_zip.exists(),
            "weather": weather_zip.exists(),
            **ancillary,
        }

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


def _build_ancillary_zip_bytes(xlsx_path: str, dataset_key: str, year: int) -> bytes:
    df = _load_ancillary_df_for_year(xlsx_path, dataset_key, year)

    csv_name = ANCILLARY_DATASETS[dataset_key]["csv"]
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    readme = (
        f"Biochar Project — Bulk Download\n"
        f"Dataset: {dataset_key}\n"
        f"Year: {year}\n\n"
        f"Source: {Path(xlsx_path).name}\n"
    ).encode("utf-8")

    out = BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_bytes)
        zf.writestr("README.txt", readme)
    out.seek(0)
    return out.getvalue()


def _ancillary_available_for_year(xlsx_path: str, dataset_key: str, year: int) -> bool:
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

@api_router.post("/get_summary_stats")
async def api_get_summary_stats(payload: Dict[str, Any] = Body(...)):
    """
    Summary stats endpoint used by the Summary Statistics tab.

    Expected keys:
      year, variable, strip, granularity, depth
      startDate, endDate (optional but normally provided)
      unitSystem (optional)
      periods (optional when granularity == 'gseason')
    """
    # --- Validate required keys ---
    required = ["year", "variable", "strip", "granularity", "depth"]
    missing = [k for k in required if payload.get(k) is None]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing: {', '.join(missing)}")

    year = int(payload["year"])
    variable = str(payload["variable"])
    strip = str(payload["strip"])
    granularity = str(payload["granularity"]).lower()
    depth = int(payload["depth"])
    unit_system = str(payload.get("unitSystem", "us"))

    start = payload.get("startDate")
    end = payload.get("endDate")

    def _clean(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    # ------------------------------------------------------------------
    # SPEEDUP: cache the loaded logger dataframe per (year, granularity)
    # NOTE: _LOADED_LOGGER_CACHE must be defined at module scope in routes.py:
    #   _LOADED_LOGGER_CACHE: dict[tuple[int, str], Any] = {}
    # ------------------------------------------------------------------
    cache_key = (year, "15min")

    df_15min = _LOADED_LOGGER_CACHE.get(cache_key)
    if df_15min is None:
        df_15min = load_logger_year(year, "15min")
        # Only cache successful loads (avoid caching None)
        if df_15min is not None:
            _LOADED_LOGGER_CACHE[cache_key] = df_15min

    if df_15min is None or getattr(df_15min, "empty", True):
        return JSONResponse(
            {
                "year": year,
                "variable": variable,
                "strip": strip,
                "granularity": granularity,
                "depth": str(depth),
                "title": "",
                "raw_statistics": {},
                "ratio_statistics": {},
                "gseason_stats": {},
            }
        )

    # ------------------------------------------------------------------
    # IMPORTANT: never mutate the cached dataframe in-place.
    # If we need a filtered version for this request, create a copy/slice.
    # ------------------------------------------------------------------
    df_req = df_15min

    # Filter by date range if provided
    if "timestamp" in df_req.columns and start and end:
        # (Optional) ensure comparable types; keep your current string-based filtering
        df_req = df_req[(df_req["timestamp"] >= start) & (df_req["timestamp"] <= end)]

    # ---- gseason special case ----
    if granularity == "gseason":
        # Your current codebase already has gseason helpers imported:
        # - load_gseason_df
        # - periods_to_list_of_dicts
        periods_raw = payload.get("periods") or []
        periods_list = periods_to_list_of_dicts(periods_raw)

        df_gs = load_gseason_df(
            year=year,
            periods=periods_list,
            unit_system=unit_system,
            use_ratios=False,
        )

        flat = get_flat_gseason_summary(df_gs, variable=variable, strip=strip, depth=depth)

        title = (
            f"{granularity_name_mapping.get('gseason','Seasonal')} Summary for "
            f"{label_name_mapping.get(variable, variable)}, "
            f"{strip_name_mapping.get(strip, strip)}, "
            f"{sensor_depth_mapping.get(str(depth), {}).get('us', f'{depth} in')}, "
            f"{year}"
        )

        return JSONResponse(
            {
                "year": year,
                "variable": variable,
                "strip": strip,
                "granularity": granularity,
                "depth": str(depth),
                "title": title,
                "gseason_stats": _clean(flat),
            }
        )

    # ---- non-gseason: compute stats from (filtered) 15min ----
    stats_raw, stats_ratio = compute_summary_statistics(df_req, variable, strip, str(depth))

    # match your old behavior: no ratios for temperature-ish variables
    if variable in ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"]:
        stats_ratio = {}

    title = (
        f"{granularity_name_mapping.get(granularity, granularity)} Summary for "
        f"{label_name_mapping.get(variable, variable)}, "
        f"{strip_name_mapping.get(strip, strip)}, "
        f"{sensor_depth_mapping.get(str(depth), {}).get('us', f'{depth} in')}, "
        f"{year}"
    )

    return JSONResponse(
        {
            "year": year,
            "variable": variable,
            "strip": strip,
            "granularity": granularity,
            "depth": str(depth),
            "title": title,
            "raw_statistics": _clean(stats_raw),
            "ratio_statistics": _clean(stats_ratio),
        }
    )

# ---------------------------------------------------------------------------
# Lab table routes (NIR + Soil) — STANDARD
# ---------------------------------------------------------------------------
@api_router.get("/get_soilbio_table")
async def api_get_soilbio_table():
    payload = build_soilbio_table(WARD_MASTER_SOILBIO_CSV, min_year=2023)
    sets = [

    ]
    return JSONResponse(payload)


@api_router.get("/get_soilchem_table")
async def api_get_soilchem_table():
    payload = build_soilchem_table(WARD_MASTER_SOILCHEM_CSV, min_year=2023)
    return JSONResponse(payload)


@api_router.get("/get_nir_table")
async def api_get_nir_table():
    set1 = build_nir_set1_table(WARD_MASTER_NIR_CSV)
    set2 = build_nir_set2_table(WARD_MASTER_NIR_CSV)
    set3 = build_nir_set3_table(WARD_MASTER_NIR_CSV)
    set4 = build_nir_set4_table(WARD_MASTER_NIR_CSV)

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
            if "key" not in obj:
                obj = {**obj, "key": fallback_key}
            if "label" not in obj:
                obj = {**obj, "label": fallback_label}
            obj.pop("title", None)
            return obj

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
# Registry-based bulk download (checkbox UI) [API routes]
# ---------------------------------------------------------------------------
class BulkDownloadRequest(BaseModel):
    keys: List[str]


@api_router.get("/bulk_download_manifest")
async def api_bulk_download_manifest():
    manifest = build_manifest(BIOCHAR_MASTER_XLSX)
    return JSONResponse(manifest)


@api_router.post("/bulk_download")
async def api_bulk_download(req: BulkDownloadRequest):
    logger.info("📦 /api/bulk_download called with keys=%s", req.keys)

    try:
        reg = default_bulk_registry()
        valid = sorted([s.dataset_key for s in reg])

        missing = [k for k in req.keys if k not in valid]
        if missing:
            raise HTTPException(status_code=400, detail=f"Unknown dataset keys: {missing}")

        zip_bytes = build_zip_for_selection(BIOCHAR_MASTER_XLSX, req.keys, registry=reg)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("❌ build_zip_for_selection failed")
        raise HTTPException(status_code=400, detail=str(e))

    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="biochar_bulk_download.zip"'},
    )


@api_router.get("/get_biomass_field_table")
async def api_get_biomass_field_table():
    payload = get_biomass_field_table_payload(BIOMASS_FIELD_CSV, min_year=2023)
    return JSONResponse(payload)