"""
================================================================================
routes.py — API Endpoints & Orchestration for Biochar Dashboard
================================================================================
"""
import os
import re
import math
import json
import logging
import textwrap
from io import BytesIO
import zipfile
from zipfile import ZipFile
from pathlib import Path
from typing import Dict, Any, Optional, List

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
)
from biochar_app.scripts.markdown_config import build_markdown_mapping


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


logger = logging.getLogger(__name__)

main_router = APIRouter()
api_router = APIRouter(prefix="/api")

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
# (You can later add ANCILLARY_DOWNLOADS_DIR here as well.)

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
    available: Dict[str, Dict[str, bool]] = {}

    for year in YEARS:
        loggers_zip = LOGGER_DOWNLOADS_DIR / f"Biochar_Loggers_15min_{year}_USunits.zip"
        weather_zip = WEATHER_DOWNLOADS_DIR / f"Biochar_Weather_15min_{year}_USunits.zip"

        # Ancillary datasets: computed from biochar-data-master.xlsx
        ancillary = {
            k: _ancillary_available_for_year(BIOCHAR_MASTER_XLSX, k, int(year))
            for k in ANCILLARY_DATASETS.keys()
        }

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
    # key used by buttons / options -> Excel sheet name + default csv name inside zip
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

    # 1) direct match (exact)
    if base_sheet in xl.sheet_names:
        return base_sheet

    # 2) year-prefixed match (ignore trailing spaces in sheet names)
    target_prefix = f"{year} {base_sheet}".strip().lower()
    for name in xl.sheet_names:
        if name.strip().lower() == target_prefix:
            return name

    # 3) fallback: some of your irrigation sheets have slightly different names (e.g. "2023 IRRIGATION ")
    # try contains match
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

    # Drop completely empty columns (common with hand-built sheets)
    df = df.dropna(axis=1, how="all")

    # If this is not a year-specific sheet, try filtering by Year column.
    # (For Hay Data All, this is the expected case.)
    # We'll be lenient about column spelling/case.
    year_cols = [c for c in df.columns if str(c).strip().lower() in ("year", "yr")]
    if year_cols:
        yc = year_cols[0]
        df_year = df[pd.to_numeric(df[yc], errors="coerce") == int(year)].copy()
        # If filtering would remove everything, fall back to full sheet
        if not df_year.empty:
            df = df_year

    # If still no Year column, add it (makes downstream consistent)
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

    response_data = {
        "defaults": {
            "year": DEFAULT_YEAR,
            "startDate": DEFAULT_START_DATE,
            "endDate": DEFAULT_END_DATE,
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
        "label_name_mapping": label_name_mapping,  # or similar
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
# Summary statistics + downloads
# ---------------------------------------------------------------------------

class SummaryStatsRequest(BaseModel):
    year: int
    variable: str
    strip: str
    granularity: str
    summaryStats: Dict[str, Any]


@api_router.post("/download_summary_data")
async def download_summary_data(req: SummaryStatsRequest):
    year, variable, strip, granularity, stats = (
        req.year,
        req.variable,
        req.strip,
        req.granularity,
        req.summaryStats,
    )

    # 🔍 Top-level debug of the request
    print(
        f"[download_summary_data] year={year}, variable={variable}, "
        f"strip={strip}, granularity={granularity}, "
        f"has_stats={bool(stats)}"
    )

    # Figure out depth from the summaryStats payload if present
    depth_val = None
    if isinstance(stats, dict):
        depth_val = stats.get("depth")
    if depth_val is None:
        depth_val = getattr(req, "depth", None)
    depth_str = str(depth_val) if depth_val is not None else ""

    # ------------------------------------------------------
    # 🌱 Growing-season (gseason) path
    # ------------------------------------------------------
    if granularity == "gseason":
        raw_rows: List[Dict[str, Any]] = []
        ratio_rows: List[Dict[str, Any]] = []

        if not isinstance(stats, dict):
            raise HTTPException(
                status_code=400,
                detail="summaryStats for gseason must be a JSON object",
            )

        # Prefer the nested gseason_stats block if present
        season_data = stats.get("gseason_stats")
        if isinstance(season_data, dict):
            print(
                "[download_summary_data:gseason] Using stats['gseason_stats'] "
                f"with seasons={list(season_data.keys())}"
            )
        else:
            print(
                "[download_summary_data:gseason] No 'gseason_stats' key; "
                "treating stats as the seasonal map directly."
            )
            season_data = stats

        if not isinstance(season_data, dict):
            raise HTTPException(
                status_code=400,
                detail="Could not interpret seasonal summary structure",
            )

        for season_code, season_block in season_data.items():
            if not isinstance(season_block, dict):
                print(
                    "[download_summary_data:gseason] "
                    f"Non-dict season_block for season={season_code}: "
                    f"{season_block!r} (type={type(season_block).__name__})"
                )
                continue

            raw_map = season_block.get("raw_statistics", {}) or {}
            ratio_map = season_block.get("ratio_statistics", {}) or {}

            # Derive season start/end dates in YYYY-MM-DD form
            spec = DEFAULT_GSEASON_PERIODS.get(season_code)
            if spec:
                sm, sd = map(int, spec["start"].split("-"))
                em, ed = map(int, spec["end"].split("-"))
                # Same wrap logic you use in get_summary_stats
                start_year = year - 1 if sm > em else year
                end_year = year
                start_date_str = f"{start_year}-{spec['start']}"
                end_date_str = f"{end_year}-{spec['end']}"
            else:
                start_date_str = ""
                end_date_str = ""

            # ---------- RAW rows ----------
            for trace, vals in raw_map.items():
                if not isinstance(vals, dict):
                    print(
                        "[download_summary_data:gseason:raw] "
                        f"Non-dict vals for season={season_code}, trace={trace}: "
                        f"{vals!r} (type={type(vals).__name__})"
                    )
                    continue

                # e.g. VWC_1_raw_S1_T → logger_id = "T"
                logger_id = trace.split("_")[-1]
                raw_rows.append(
                    {
                        "Year": year,
                        "Depth": depth_str,
                        "Season": season_code,
                        "StartDate": start_date_str,
                        "EndDate": end_date_str,
                        "Variable": variable,
                        "Strip": strip,
                        "Logger": logger_id,
                        **{
                            k: round(vals.get(k, 0), 4)
                            for k in ("min", "mean", "max", "std")
                        },
                    }
                )

            # ---------- RATIO rows ----------
            for trace, vals in ratio_map.items():
                if not isinstance(vals, dict):
                    print(
                        "[download_summary_data:gseason:ratio] "
                        f"Non-dict vals for season={season_code}, trace={trace}: "
                        f"{vals!r} (type={type(vals).__name__})"
                    )
                    continue

                # e.g. VWC_1_ratio_S1_S2_T → strips + logger
                strips_match = re.search(r"_(S\d(?:_S\d)*)_", trace)
                strips_ = strips_match.group(1) if strips_match else ""
                logger_id = trace.split("_")[-1]

                ratio_rows.append(
                    {
                        "Year": year,
                        "Depth": depth_str,
                        "Season": season_code,
                        "StartDate": start_date_str,
                        "EndDate": end_date_str,
                        "Variable": variable,
                        "Strips": strips_,
                        "Logger": logger_id,
                        **{
                            k: round(vals.get(k, 0), 4)
                            for k in ("min", "mean", "max", "std")
                        },
                    }
                )

        # Build ZIP payload
        bioio = BytesIO()
        with zipfile.ZipFile(bioio, "w") as zf:
            if raw_rows:
                df_r = pd.DataFrame(raw_rows)
                zf.writestr("gseason_raw_summary.csv", df_r.to_csv(index=False))
            if ratio_rows:
                df_ra = pd.DataFrame(ratio_rows)
                zf.writestr("gseason_ratio_summary.csv", df_ra.to_csv(index=False))
        bioio.seek(0)

        fname = f"summary_gseason_{year}_{variable}_{strip}.zip"
        return StreamingResponse(
            bioio,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={fname}"},
        )

    # ------------------------------------------------------
    # 📅 Non-seasonal (standard) path
    # ------------------------------------------------------
    if not stats:
        raise HTTPException(status_code=400, detail="No summary statistics provided")

    if not isinstance(stats, dict):
        raise HTTPException(status_code=400, detail="summaryStats must be a JSON object")

    rows: List[Dict[str, Any]] = []
    for trace, vals in stats.items():
        # 🧪 Debug: catch any fields that are not the stat dicts
        if not isinstance(vals, dict):
            print(
                "[download_summary_data:standard] Non-dict vals encountered: "
                f"trace={trace}, vals={vals!r}, type={type(vals).__name__}"
            )
            continue

        parts = trace.split("_")
        typ = "raw" if "_raw_" in trace else "ratio" if "_ratio_" in trace else ""
        strips_match = re.search(r"_(S\d(?:_S\d)*)_", trace)
        strips_ = strips_match.group(1) if strips_match else ""
        logger_id = parts[-1] if parts else ""

        rows.append(
            {
                "Year": year,
                "Depth": depth_str,
                "Variable": parts[0] if parts else "",
                "Type": typ,
                "Strips": strips_,
                "Logger": logger_id,
                **{
                    k: round(vals.get(k, 0), 4)
                    for k in ("min", "mean", "max", "std")
                },
            }
        )

    df_out = pd.DataFrame(rows)
    csv = df_out.to_csv(index=False)
    fname = f"summary_data_{year}_{variable}_{strip}_{granularity}.csv"
    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@api_router.post("/get_summary_stats")
async def get_summary_stats(data: Dict[str, Any] = Body(...)):
    """
    Return summary statistics for the Summary Statistics tab.

    For granularity != "gseason":
        - raw_statistics / ratio_statistics summarize the full filtered DataFrame.

    For granularity == "gseason":
        - raw_statistics / ratio_statistics are left empty at the top level.
        - gseason_stats is a dict:
            {
              "Q1_Winter": {
                  "raw_statistics":   { col -> {min, mean, max, std} },
                  "ratio_statistics": { col -> {min, mean, max, std} },
              },
              "Q2_Early_Growing": { ... },
              "Q3_Peak_Harvest":  { ... },
            }
    """
    required = ["year", "variable", "strip", "granularity", "depth"]
    missing = [k for k in required if data.get(k) is None]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing parameter(s): {', '.join(missing)}",
        )

    # --- basic parsing / validation -----------------------------------------
    try:
        year = int(data["year"])
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid year: {data.get('year')}",
        )

    variable    = data["variable"]
    strip       = data["strip"]
    granularity = data["granularity"]

    try:
        depth = int(data["depth"])
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid depth: {data.get('depth')}",
        )

    unit_system = data.get("unitSystem", "us")
    start       = data.get("startDate")
    end         = data.get("endDate")

    # --- helper: JSON-safe stats (no NaN / Inf) -----------------------------
    def _clean_numbers(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: _clean_numbers(v) for k, v in obj.items()}
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    # ========================================================================
    # 1) GROWING-SEASON BRANCH
    #    Use 15-minute data and slice into the three periods.
    # ========================================================================
    if granularity == "gseason":
        # Use 15-minute data as the base for growing-season summaries.
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
                }
            )

        if "timestamp" not in df.columns:
            raise HTTPException(
                status_code=500,
                detail="Expected 'timestamp' column in logger data.",
            )

        # Make sure timestamp is datetime
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        gseason_stats: Dict[str, Dict[str, Any]] = {}

        for code, spec in DEFAULT_GSEASON_PERIODS.items():
            # spec["start"] / spec["end"] are "MM-DD" strings
            sm, sd = map(int, spec["start"].split("-"))
            em, ed = map(int, spec["end"].split("-"))

            # Handle wrap windows (e.g. Winter 11-01 → 04-30, crossing year)
            start_year = year - 1 if sm > em else year
            end_year   = year

            start_ts = pd.Timestamp(f"{start_year}-{spec['start']}")
            # inclusive end-of-day
            end_ts   = (
                pd.Timestamp(f"{end_year}-{spec['end']}")
                + pd.Timedelta(days=1)
                - pd.Timedelta(seconds=1)
            )

            slice_df = df[
                (df["timestamp"] >= start_ts) & (df["timestamp"] <= end_ts)
            ]

            if slice_df.empty:
                raw_stats: Dict[str, Dict[str, float]] = {}
                ratio_stats: Dict[str, Dict[str, float]] = {}
            else:
                raw_stats, ratio_stats = compute_summary_statistics(
                    slice_df, variable, strip, str(depth)
                )

            gseason_stats[code] = {
                "raw_statistics":   _clean_numbers(raw_stats),
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
                "raw_statistics": {},      # intentionally empty at top level
                "ratio_statistics": {},
                "gseason_stats": gseason_stats,
            }
        )

    # ========================================================================
    # 2) STANDARD (NON-GSEASON) BRANCH
    # ========================================================================

    # Load the data for the requested granularity (15min/daily/monthly/…)
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
            }
        )

    if start and end and "timestamp" in df.columns:
        df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    # --- SWC special case ----------------------------------------------------
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
                "min":  float(series.min()),
                "mean": float(series.mean()),
                "max":  float(series.max()),
                "std":  float(series.std(ddof=1)) if len(series) > 1 else 0.0,
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
            }
        )

    # --- all other variables -------------------------------------------------
    stats_raw, stats_ratio = compute_summary_statistics(
        df, variable, strip, str(depth)
    )

    # Temperature variables: ratios are not meaningful
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
    Combined trace download for the Main Data Display tab.

    Always returns a ZIP bundle containing:
      - raw_data.csv  (if downloadType in {"raw","all"} and available)
      - ratio_data.csv (if downloadType in {"ratio","all"} and available)
      - README.txt    (describing the selection and contents)
    """
    year        = req.year
    variable    = req.variable
    strip       = req.strip
    granularity = req.granularity.lower()
    depth       = req.depth
    unit_system = req.unitSystem
    download_ty = req.downloadType or "all"
    start       = req.startDate
    end         = req.endDate

    _ensure_year_allowed(year)

    try:
        df = load_logger_year(year, granularity)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for year={year}, granularity={granularity}",
        )

    # Optional date filtering if we have timestamps + explicit range
    if start and end and "timestamp" in df.columns:
        df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No data in the requested date range for year={year}, granularity={granularity}",
        )

    source_var = "VWC" if variable == "SWC" else variable

    AUX_PREFIXES = ("precip", "rain", "irrig", "gallon")
    AIR_TEMP_PREFIXES = ("temp_air_degF", "temp_air_degC")

    raw_cols: List[str] = []
    ratio_cols: List[str] = []
    aux_cols: List[str] = []

    for col in df.columns:
        if col == "timestamp":
            continue

        # Always keep aux/overlay columns
        if any(col.startswith(p) for p in AUX_PREFIXES):
            aux_cols.append(col)
            continue

        if variable == "T" and any(col.startswith(p) for p in AIR_TEMP_PREFIXES):
            aux_cols.append(col)
            continue

        # Raw columns for this variable/strip/depth
        if (
            col.startswith(f"{source_var}_{depth}_raw_{strip}_")
            and f"_{strip}_" in col
        ):
            raw_cols.append(col)
            continue

        # Ratio columns for this variable/strip/depth
        if (
            col.startswith(f"{source_var}_{depth}_ratio_")
            and f"_{strip}" in col
        ):
            ratio_cols.append(col)
            continue

    # Build raw and ratio DataFrames (if any)
    raw_df = None
    if raw_cols and download_ty in ("raw", "all"):
        cols = []
        if "timestamp" in df.columns:
            cols.append("timestamp")
        cols.extend(aux_cols)
        cols.extend(sorted(raw_cols))
        raw_df = df[cols].copy()
        raw_df = _add_unit_suffixes_for_download(raw_df, variable)

    ratio_df = None
    if ratio_cols and download_ty in ("ratio", "all"):
        cols = []
        if "timestamp" in df.columns:
            cols.append("timestamp")
        cols.extend(aux_cols)
        cols.extend(sorted(ratio_cols))
        ratio_df = df[cols].copy()
        ratio_df = _round_ratio_columns(ratio_df, decimals=6)

    if raw_df is None and ratio_df is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No matching columns found for variable={variable}, "
                f"strip={strip}, depth={depth}, granularity={granularity}"
            ),
        )

    # ------------------------------------------------------------------
    # Build README
    # ------------------------------------------------------------------
    included_files: List[str] = []
    if raw_df is not None:
        included_files.append("raw_data.csv")
    if ratio_df is not None:
        included_files.append("ratio_data.csv")

    readme_text = textwrap.dedent(
        f"""
        Biochar Fruita CSU – Downloaded Data Bundle
        ===========================================

        This ZIP file was generated from the Interactive Plots tab.

        Selection summary
        -----------------
        • Year:         {year}
        • Variable:     {variable}
        • Strip:        {strip}
        • Depth index:  {depth}
        • Granularity:  {granularity}
        • Unit system:  {unit_system}
        • Download type:{download_ty}

        Files in this bundle
        --------------------
        {os.linesep.join(f"- {name}" for name in included_files) or "- (none)"}

        Notes
        -----
        • Timestamps are stored in naive local time (America/Denver).
        • VWC is stored as percent (0–100) when labelled with '_pct'.
        • SWC volumes are in gallons if unit system = 'us', litres if unit system = 'metric'.
        • Ratio columns (with '_ratio_') are unitless ratios between strip groups.
        """
    ).strip() + "\n"

    # ------------------------------------------------------------------
    # Build ZIP
    # ------------------------------------------------------------------
    bioio = BytesIO()
    with zipfile.ZipFile(bioio, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.txt", readme_text)

        if raw_df is not None:
            zf.writestr("raw_data.csv", raw_df.to_csv(index=False))

        if ratio_df is not None:
            zf.writestr("ratio_data.csv", ratio_df.to_csv(index=False))

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


@api_router.get("/bulk_download_manifest")
async def api_bulk_download_manifest():
    # IMPORTANT: api_router already has prefix="/api"
    manifest = build_manifest(BIOCHAR_MASTER_XLSX)
    return JSONResponse(manifest)


@api_router.post("/bulk_download")
async def api_bulk_download(req: BulkDownloadRequest):
    # IMPORTANT: api_router already has prefix="/api"
    zip_bytes = build_zip_for_selection(BIOCHAR_MASTER_XLSX, req.keys)
    headers = {"Content-Disposition": 'attachment; filename="biochar_bulk_download.zip"'}
    return Response(content=zip_bytes, media_type="application/zip", headers=headers)