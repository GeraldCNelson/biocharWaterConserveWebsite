"""
================================================================================
routes.py — API Endpoints & Orchestration for Biochar Dashboard
================================================================================

This module defines the HTTP API used by the web front end (Main Data Display,
Custom Season, Summary Statistics, and markdown-based tabs).

Responsibilities:
    • Accept JSON payloads from the UI (year, variable, strip, depth, logger
      location, granularity, unitSystem, custom-season periods, etc.).
    • Load and cache logger + weather data from Parquet (via helper utilities).
    • Filter by year, date range, variable, strip, depth, and granularity.
    • Call plot_utils.py to build Plotly figures and return them as JSON.
    • Call summary/aggregation utilities to build summary tables.
    • Handle CSV/ZIP downloads for raw, ratio, and summary data.
    • Serve markdown/HTML content for Introduction, Experiment Design,
      Technical Details, and modal “Directions” content.
------------------------------------------------------------------------------
"""

import os
import re
import logging
from io import BytesIO
import zipfile
from typing import Dict, Any, Optional, List

import pandas as pd
from fastapi import APIRouter, Request, HTTPException, Query, Body
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from biochar_app.scripts.routes_utils import (
    load_gseason_df,
    load_logger_year,
    periods_to_list_of_dicts,
)
from biochar_app.scripts.gseason_utils import (
    compute_summary_statistics,
    get_flat_gseason_summary,
    build_gseason_frame_for_strip_depth,
)
from biochar_app.scripts.plot_utils import (
    make_raw_figure,
    make_ratio_figure,
    make_raw_gseason_figure,
    make_ratio_gseason_figure,
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
    YEARS,
    PLOT_BASED_ON_OPTIONS,
    TRACE_OPTION_MAP,
    sensor_depth_mapping,
    logger_location_mapping,
    variable_name_mapping,
    granularity_name_mapping,
    strip_name_mapping,
)

from biochar_app.scripts.markdown_config import build_markdown_mapping

logger = logging.getLogger(__name__)
main_router = APIRouter()
api_router = APIRouter(prefix="/api")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)

# ---------------------------------------------------------------------------
# Defaults / options endpoint
# ---------------------------------------------------------------------------

@api_router.get("/markdown_files")
async def get_markdown_files():
    """
    Expose the Markdown ID → URL mapping to the frontend.

    Source of truth is markdown_config.build_markdown_mapping().
    """
    mapping = build_markdown_mapping()
    return JSONResponse(mapping)

@api_router.get("/get_defaults_and_options")
async def get_defaults_and_options():
    # 1) Build each of the arrays the UI expects:
    years = YEARS
    strips = [
        {"value": k, "label": strip_name_mapping[k]}
        for k in strip_name_mapping
    ]
    variables = [
        {"value": k, "label": variable_name_mapping[k]}
        for k in variable_name_mapping
    ]
    depths = [
        {"value": str(d), "label": sensor_depth_mapping[d]["us"]}
        for d in sensor_depth_mapping
    ]
    logger_locations = [
        {"value": k, "label": logger_location_mapping[k]}
        for k in logger_location_mapping
    ]
    granularities = [
        {"value": g, "label": granularity_name_mapping[g]}
        for g in granularity_name_mapping
    ]

    # 2) Assemble the payload exactly as your UI code expects:
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
    }

    return JSONResponse(response_data)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _round_ratio_columns(df: pd.DataFrame, decimals: int = 6) -> pd.DataFrame:
    """
    Rounds all ratio columns (containing '_ratio_') to a fixed number of decimals.
    Used only for CSV downloads.
    """
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

    - Always keeps 'timestamp' if present.
    - RAW:
        * trace_option == "depths":
              all depths at a single logger_location
        * trace_option == "loggerLocation":
              single depth across all logger locations
    - RATIO:
        * any *_ratio_* columns that involve the chosen strip.
    - "all":
        * union of raw + ratio, plus precip/irrigation helpers,
          and (for soil-temperature variable) air-temperature helpers.

    Downloads are in US units (inches / gallons / °F) regardless of the
    on-screen unit toggle.
    """

    cols: List[str] = []

    if "timestamp" in df.columns:
        cols.append("timestamp")

    # SWC uses VWC columns under the hood
    source_var = "VWC" if variable == "SWC" else variable

    # --- expected RAW columns based on traceOption ------------------------
    raw_expected: set[str] = set()

    if trace_option == "depths":
        # one loggerLocation, all depths
        raw_expected = {
            f"{source_var}_{d}_raw_{strip}_{logger_location}"
            for d in sensor_depth_mapping
        }
    else:
        # fixed depth, all logger locations
        raw_expected = {
            f"{source_var}_{depth}_raw_{strip}_{loc}"
            for loc in logger_location_mapping
        }

    # --- helper to recognise ratio columns for this variable/strip --------
    def is_ratio_col(col: str) -> bool:
        return (
            "_ratio_" in col
            and col.startswith(f"{source_var}_")
            and f"_{strip}" in col
        )

    # --- precip / irrigation / air-temp helper columns --------------------
    AUX_PREFIXES = ("precip", "rain", "irrig", "gallon")
    AIR_TEMP_PREFIXES = ("temp_air_degF", "temp_air_degC")

    for col in df.columns:
        if col == "timestamp":
            continue

        # include precip / irrigation context if present
        if any(col.startswith(prefix) for prefix in AUX_PREFIXES):
            cols.append(col)
            continue

        # for temperature variable, also include air temperature overlays
        if variable == "T" and any(col.startswith(p) for p in AIR_TEMP_PREFIXES):
            cols.append(col)
            continue

        if kind in ("raw", "all") and col in raw_expected:
            cols.append(col)
            continue

        if kind in ("ratio", "all") and is_ratio_col(col):
            cols.append(col)
            continue

    # If we ended up with only timestamp (or nothing), fall back to the full df
    if len(cols) <= (1 if "timestamp" in cols else 0):
        logger.warning(
            (
                "Download selector found no columns for variable=%s, strip=%s, "
                "depth=%s, logger=%s, trace_option=%s, kind=%s. "
                "Returning full DataFrame."
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
    """
    Cosmetic helper for CSV downloads only.

    Returns a copy of df with unit suffixes appended to *raw* logger
    columns for the main variables:

        - VWC_*_raw_*  ->  VWC_*_raw_*_pct
        - T_*_raw_*    ->  T_*_raw_*_degF
        - EC_*_raw_*   ->  EC_*_raw_*_dS_per_m

    Weather columns (temp_air_degF, precip_in, precip_mm, etc.) are left
    unchanged, since they already encode units in their names.

    This does NOT mutate the original DataFrame and has no effect on any
    internal processing; it is used only right before to_csv().
    """
    df_out = df.copy()
    rename_map: Dict[str, str] = {}

    var_upper = (variable or "").upper()

    for col in df_out.columns:
        # VWC (including when selected via SWC, since underlying values are %)
        if var_upper in {"VWC", "SWC"} and col.startswith("VWC_") and "_raw_" in col:
            if not col.endswith("_pct"):
                rename_map[col] = f"{col}_pct"
            continue

        # Soil temperature
        if var_upper == "T" and col.startswith("T_") and "_raw_" in col:
            if not col.endswith("_degF"):
                rename_map[col] = f"{col}_degF"
            continue

        # Electrical conductivity
        if var_upper == "EC" and col.startswith("EC_") and "_raw_" in col:
            if not col.endswith("_dS_per_m"):
                rename_map[col] = f"{col}_dS_per_m"
            continue

    if rename_map:
        df_out = df_out.rename(columns=rename_map)

    return df_out


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class PeriodSpec(BaseModel):
    code: str
    label: str
    start: str  # e.g. "2024-03-01"
    end: str  # e.g. "2024-05-31"


class GSeasonParams(BaseModel):
    periods: Optional[List[PeriodSpec]] = Field(
        default=None,
        description="Custom G-season periods",
        examples=[
            {
                "code": "Q1",
                "label": "Winter",
                "start": "2023-11-01",
                "end": "2024-02-28",
            }
        ],
    )

    class Config:
        validate_by_name = True


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
    periods: Optional[List[PeriodSpec]] = Field(
        default=None,
        description="Custom G-season periods",
        json_schema_extra={
            "examples": [
                {
                    "code": "Q1",
                    "label": "Winter",
                    "start": "2023-11-01",
                    "end": "2024-02-28",
                }
            ]
        },
    )


# ---------------------------------------------------------------------------
# Plot routes
# ---------------------------------------------------------------------------


@api_router.post("/plot_raw")
async def api_plot_raw(req: PlotRequest):
    year = req.year
    gran = req.granularity.lower()
    var = req.variable  # "VWC","T","EC","SWC"
    strip = req.strip
    logger_loc = req.loggerLocation
    depth = req.depth
    unit = req.unitSystem
    trace_option = TRACE_OPTION_MAP[req.traceOption]
    start = req.startDate
    end = req.endDate

    # For SWC, the underlying numeric columns are still VWC_*_raw_*.
    source_var = "VWC" if var == "SWC" else var

    # ---- growing-season / custom ----
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

    # ---- standard time-series ----
    df = load_logger_year(year, gran)
    if "timestamp" not in df.columns:
        raise HTTPException(400, "No timestamp column in data")

    df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    if trace_option == "depths":
        expected = [
            f"{source_var}_{d}_raw_{strip}_{logger_loc}"
            for d in sensor_depth_mapping
        ]
    else:
        expected = [
            f"{source_var}_{depth}_raw_{strip}_{lkey}"
            for lkey in logger_location_mapping
        ]

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
    year, gran = req.year, req.granularity.lower()
    var, strip, logger_loc = req.variable, req.strip, req.loggerLocation
    depth, unit = int(req.depth), req.unitSystem
    start, end = req.startDate, req.endDate

    # ---- growing-season ratios ----
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

    # ---- time-series ratios ----
    df = load_logger_year(year, gran)
    if "timestamp" not in df.columns:
        raise HTTPException(400, "No timestamp column in data")
    df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

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

    # Growing-season: use pre-flattened JSON
    if granularity == "gseason":
        gstats = get_flat_gseason_summary(year)
        raw_rows: List[Dict[str, Any]] = []
        ratio_rows: List[Dict[str, Any]] = []

        for season_code, season_block in gstats.items():
            for var_key, strip_blocks in season_block.items():
                for strip_key, stat_obj in strip_blocks.items():
                    for trace, vals in stat_obj.get(
                        "raw_statistics", {}
                    ).items():
                        logger_id = trace.split("_")[-1]
                        raw_rows.append(
                            {
                                "Season": season_code,
                                "Variable": var_key,
                                "Strip": strip_key,
                                "Logger": logger_id,
                                **{
                                    k: round(vals.get(k, 0), 4)
                                    for k in ("min", "mean", "max", "std")
                                },
                            }
                        )
                    for trace, vals in stat_obj.get(
                        "ratio_statistics", {}
                    ).items():
                        parts = trace.split("_")
                        strips_ = parts[3] if len(parts) > 4 else ""
                        logger_id = parts[-1]
                        ratio_rows.append(
                            {
                                "Season": season_code,
                                "Variable": var_key,
                                "Strips": strips_,
                                "Logger": logger_id,
                                **{
                                    k: round(vals.get(k, 0), 4)
                                    for k in ("min", "mean", "max", "std")
                                },
                            }
                        )

        bioio = BytesIO()
        with zipfile.ZipFile(bioio, "w") as zf:
            if raw_rows:
                df_r = pd.DataFrame(raw_rows)
                zf.writestr("gseason_raw_summary.csv", df_r.to_csv(index=False))
            if ratio_rows:
                df_ra = pd.DataFrame(ratio_rows)
                zf.writestr(
                    "gseason_ratio_summary.csv", df_ra.to_csv(index=False)
                )
        bioio.seek(0)
        fname = f"summary_gseason_{year}_{variable}_{strip}.zip"
        return StreamingResponse(
            bioio,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={fname}"},
        )

    # Non-seasonal:
    if not stats:
        raise HTTPException(
            status_code=400, detail="No summary statistics provided"
        )

    rows: List[Dict[str, Any]] = []
    for trace, vals in stats.items():
        parts = trace.split("_")
        typ = "raw" if "_raw_" in trace else "ratio" if "_ratio_" in trace else ""
        strips_match = re.search(r"_(S\d(?:_S\d)*)_", trace)
        strips_ = strips_match.group(1) if strips_match else ""
        logger_id = parts[-1] if parts else ""
        row = {
            "Variable": parts[0] if parts else "",
            "Type": typ,
            "Strips": strips_,
            "Logger": logger_id,
            **{
                k: round(vals.get(k, 0), 4)
                for k in ("min", "mean", "max", "std")
            },
        }
        rows.append(row)

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
    print("🔍 POST /api/get_summary_stats payload:", data)
    """
    Expects JSON with keys:
      year, variable, strip, granularity, depth,
      startDate (optional), endDate (optional), unitSystem (optional)
    Returns raw & ratio summary statistics.
    """
    required = ["year", "variable", "strip", "granularity", "depth"]
    missing = [k for k in required if data.get(k) is None]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing parameter(s): {', '.join(missing)}",
        )

    try:
        year = int(data["year"])
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400, detail=f"Invalid year: {data.get('year')}"
        )
    variable = data["variable"]
    strip = data["strip"]
    granularity = data["granularity"]
    try:
        depth = int(data["depth"])
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400, detail=f"Invalid depth: {data.get('depth')}"
        )

    start = data.get("startDate")
    end = data.get("endDate")

    try:
        df = load_logger_year(year, granularity)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if start and end:
        df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    stats_raw, stats_ratio = compute_summary_statistics(
        df, variable, strip, str(depth)
    )

    if variable in ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"]:
        stats_ratio = {}

    return JSONResponse(
        {
            "year": year,
            "variable": variable,
            "strip": strip,
            "granularity": granularity,
            "depth": str(depth),
            "raw_statistics": stats_raw,
            "ratio_statistics": stats_ratio,
        }
    )


# ---------------------------------------------------------------------------
# Markdown + custom gseason pages
# ---------------------------------------------------------------------------


@main_router.get("/markdown/{filename}")
async def serve_markdown(filename: str):
    """
    Serves markdown from: biochar_app/templates/markdown/<filename>
    """
    md_dir = os.path.join(os.path.dirname(__file__), "..", "markdown")
    fullpath = os.path.abspath(os.path.join(md_dir, filename))

    if not os.path.exists(fullpath):
        raise HTTPException(
            status_code=404,
            detail=f"Markdown file '{filename}' not found",
        )
    return FileResponse(fullpath, media_type="text/markdown")


@main_router.get("/custom-gseason")
async def custom_gseason(request: Request):
    return templates.TemplateResponse(
        "_custom_gseason.html",
        {"request": request},
    )


# ---------------------------------------------------------------------------
# Trace download routes (raw / ratio / all)
# ---------------------------------------------------------------------------


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

    # Cosmetic: add unit suffixes to logger raw columns for download only
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

    # Mostly ratios (dimensionless), but if any raw columns slipped through,
    # we still give them unit suffixes for clarity.
    df_sel = _round_ratio_columns(df_sel, decimals=6)
    df_sel = _add_unit_suffixes_for_download(df_sel, variable)
    csv = df_sel.to_csv(index=False)
    fname = f"ratio_{year}_{gran}_{variable}_{strip}_{loggerLocation}_D{depth}.csv"

    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@main_router.get("/download_all_data")
async def download_all_data(
    year: int,
    granularity: str,
    variable: str,
    strip: str,
    depth: int,
    loggerLocation: str,
    unitSystem: str = "us",
):
    """
    Download all data for the Main Data Display selection.

    - For 15min/hourly/daily/monthly: returns time-series CSV from Parquet.
    - For gseason: returns *seasonal summary* rows based on gseason_summary_YEAR.json.
    """

    gran = granularity.lower()

    # ------------------------------------------------------------------
    # Special handling for growing-season: use seasonal summary table
    # ------------------------------------------------------------------
    if gran == "gseason":
        try:
            df_gs = get_flat_gseason_summary(year)
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No growing-season summary available for {year}. "
                    f"Re-run the seasonal summary job first."
                ),
            ) from exc

        if df_gs.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No growing-season data available for {year}.",
            )

        # df_gs columns (per get_flat_gseason_summary docstring):
        #   period_code, variable, strip, depth, logger_location,
        #   raw_min, raw_mean, raw_max, raw_std,
        #   ratio_min, ratio_mean, ratio_max, ratio_std

        df_sel = df_gs.copy()

        # Filter by the user’s selection (Main Data Display controls)
        if variable:
            df_sel = df_sel[df_sel["variable"] == variable]

        if strip:
            df_sel = df_sel[df_sel["strip"] == strip]

        if depth is not None:
            df_sel = df_sel[df_sel["depth"].astype(str) == str(depth)]

        if loggerLocation:
            df_sel = df_sel[df_sel["logger_location"] == loggerLocation]

        if df_sel.empty:
            raise HTTPException(
                status_code=404,
                detail=(
                    "No growing-season summary rows match the requested filters "
                    f"(year={year}, variable={variable}, strip={strip}, "
                    f"depth={depth}, loggerLocation={loggerLocation})."
                ),
            )

        csv_bytes = df_sel.to_csv(index=False).encode("utf-8")

        filename = (
            f"gseason_{year}_{variable}_"
            f"{strip}_D{depth}_{loggerLocation}_{unitSystem}.csv"
        )

        return Response(
            content=csv_bytes,
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    # ------------------------------------------------------------------
    # Normal granularities: keep your existing time-series behavior
    # ------------------------------------------------------------------
    try:
        df = load_logger_year(year, gran)
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No logger data available for year={year}, "
                f"granularity={granularity!r}."
            ),
        ) from exc

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No data rows available for year={year}, "
                f"granularity={granularity!r}."
            ),
        )

    df_sel = df.copy()

    if variable:
        cols = [c for c in df_sel.columns if c.startswith(f"{variable}_")]
        base_cols = ["timestamp"] + cols if "timestamp" in df_sel.columns else cols
        df_sel = df_sel[base_cols]

    # Cosmetic unit suffixes for exported CSV only
    df_sel = _add_unit_suffixes_for_download(df_sel, variable)
    df_sel = _round_ratio_columns(df_sel, decimals=6)
    csv_bytes = df_sel.to_csv(index=False).encode("utf-8")
    filename = f"{year}_{granularity}_{variable}_{strip}_D{depth}_{loggerLocation}_{unitSystem}.csv"

    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )