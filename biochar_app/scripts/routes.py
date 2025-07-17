import os
import re
import glob
import logging
from io import BytesIO
import zipfile
from typing import Dict, Any

import pandas as pd
import numpy as np
from fastapi import APIRouter, Request, HTTPException, Query, Body
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from biochar_app.scripts.utils import parse_filenames
from biochar_app.scripts.routes_utils import load_logger_year, load_summary_df
from biochar_app.scripts.gseason import compute_summary_statistics, get_gseason_summary
from biochar_app.scripts.plot_helpers import sanitize_json, convert_units_for_download
from biochar_app.scripts.plot_utils import (
    make_raw_figure,
    make_ratio_figure,
    make_raw_gseason_figure,
    make_ratio_gseason_figure,
)
from biochar_app.scripts.config import (
    BASE_DIR,DEFAULT_YEAR,
    DEFAULT_START_DATE,
    DEFAULT_END_DATE,
    DEFAULT_VARIABLE,
    DEFAULT_DEPTH,
    DEFAULT_STRIP,
    DEFAULT_LOGGER_LOCATION,
    DEFAULT_GRANULARITY,
    YEARS,
    GSEASON_PERIODS,
    sensor_depth_mapping,
    logger_location_mapping,
    variable_name_mapping,
    granularity_name_mapping,
    strip_name_mapping,
    label_name_mapping,
)

logger = logging.getLogger(__name__)
main_router = APIRouter()
api_router = APIRouter(prefix="/api")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))

# --- Default and options endpoint ---
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
    trace_options = [
        {"value": "depth",          "label": "Depth"},
        {"value": "loggerLocation", "label": "Logger Location"},
    ]

    # 2) Assemble the payload exactly as your UI code expects:
    response_data = {
        "defaults": {
            "year":           DEFAULT_YEAR,
            "startDate":      DEFAULT_START_DATE,
            "endDate":        DEFAULT_END_DATE,
            "variable":       DEFAULT_VARIABLE,
            "depth":          str(DEFAULT_DEPTH),
            "strip":          DEFAULT_STRIP,
            "loggerLocation": DEFAULT_LOGGER_LOCATION,
            "granularity":    DEFAULT_GRANULARITY,
            "traceOption":    "depth",
            "unitSystem":     "us",
        },
        "years":           years,
        "strips":          strips,
        "variables":       variables,
        "depths":          depths,
        "loggerLocations": logger_locations,
        "granularities":   granularities,
        "traceOptions":    trace_options,
        "depthMapping":    sensor_depth_mapping,
    }

    # 3) Return it!
    return JSONResponse(response_data)


# --- Page routes (render index.html) ---

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

@api_router.post("/plot_raw")
async def api_plot_raw(req: PlotRequest):
    year           = req.year
    granularity    = req.granularity
    start_date     = req.startDate
    end_date       = req.endDate
    variable       = req.variable
    strip          = req.strip
    logger_loc     = req.loggerLocation
    depth          = req.depth
    trace_option   = "depths" if req.traceOption == "depth" else "locations" if req.traceOption=="loggerLocation" else req.traceOption
    unit_system    = req.unitSystem

    # 2) load (15 min or pre-agg) by granularity
    try:
        df = load_logger_year(year, granularity)
    except FileNotFoundError:
        raise HTTPException(404, f"No data found for {year}/{granularity}")

    # 3) sanity-check
    if "timestamp" not in df.columns:
        raise HTTPException(400, "No timestamp column in data")

    # 4) filter to window
    df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)]

    # 5) build figure
    try:
        fig_json = make_raw_figure(
            df               = df,
            year             = year,
            variable         = variable,
            strip            = strip,
            granularity      = granularity,
            logger_location  = logger_loc,
            depth            = depth,
            trace_option     = trace_option,
            unit_system      = unit_system,
            start            = start_date,
            end              = end_date,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))

    return JSONResponse(fig_json)


@api_router.post("/plot_ratio")
async def api_plot_ratio(req: PlotRequest):
    year           = req.year
    granularity    = req.granularity
    start_date     = req.startDate
    end_date       = req.endDate
    variable       = req.variable
    strip          = req.strip
    logger_loc     = req.loggerLocation
    depth          = req.depth
 #   trace_option   = "depths" if req.traceOption == "depth" else "locations" if req.traceOption=="loggerLocation" else req.traceOption
    unit_system    = req.unitSystem

    # 2) load
    try:
        df = load_logger_year(year, granularity)
    except FileNotFoundError:
        raise HTTPException(404, f"No data found for {year}/{granularity}")

    # 3) sanity-check
    if "timestamp" not in df.columns:
        raise HTTPException(400, "No timestamp column in data")

    # 4) filter
    df = df[(df["timestamp"] >= start_date) & (df["timestamp"] <= end_date)]

    # 5) build ratio figure
    try:
        fig_json = make_ratio_figure(
            df               = df,
            year             = year,
            variable         = variable,
            strip            = strip,
            granularity      = granularity,
            logger_location  = logger_loc,
            depth            = depth,
      #      trace_option     = trace_option,
            start            = start_date,
            end              = end_date,
            unit_system      = unit_system,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))

    return JSONResponse(fig_json)


# --- Download summary endpoint ---
class SummaryStatsRequest(BaseModel):
    year: int
    variable: str
    strip: str
    granularity: str
    summaryStats: Dict[str, Any]

@api_router.post("/download_summary_data")
async def download_summary_data(req: SummaryStatsRequest):
    # existing implementation unchanged...
    year, variable, strip, granularity, stats = (
        req.year, req.variable, req.strip, req.granularity, req.summaryStats
    )
    if granularity == "gseason":
        gstats = get_gseason_summary(year, GSEASON_PERIODS)
        raw_rows, ratio_rows = [], []
        for season_code, season_block in gstats.items():
            for var_key, strip_blocks in season_block.items():
                for strip_key, stat_obj in strip_blocks.items():
                    for trace, vals in stat_obj.get("raw_statistics", {}).items():
                        logger_id = trace.split("_")[-1]
                        raw_rows.append({
                            "Season": season_code,
                            "Variable": var_key,
                            "Strip": strip_key,
                            "Logger": logger_id,
                            **{k: round(vals.get(k, 0), 4) for k in ("min","mean","max","std")}
                        })
                    for trace, vals in stat_obj.get("ratio_statistics", {}).items():
                        parts = trace.split("_")
                        strips_ = parts[3] if len(parts) > 4 else ""
                        logger_id = parts[-1]
                        ratio_rows.append({
                            "Season": season_code,
                            "Variable": var_key,
                            "Strips": strips_,
                            "Logger": logger_id,
                            **{k: round(vals.get(k, 0), 4) for k in ("min","mean","max","std")}
                        })
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
        return StreamingResponse(bioio, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename={fname}"})

    if not stats:
        raise HTTPException(status_code=400, detail="No summary statistics provided")
    # ... non-seasonal summary logic ...
    rows = []
    for trace, vals in stats.items():
        parts = trace.split("_")
        typ = "raw" if "_raw_" in trace else "ratio" if "_ratio_" in trace else ""
        strips_match = re.search(r"_(S\d(?:_S\d)*)_", trace)
        strips_ = strips_match.group(1) if strips_match else ""
        logger_id = parts[-1] if parts else ""
        row = {"Variable": parts[0] if parts else "",
               "Type": typ,
               "Strips": strips_,
               "Logger": logger_id,
               **{k: round(vals.get(k,0),4) for k in ("min","mean","max","std")}}
        rows.append(row)
    df_out = pd.DataFrame(rows)
    csv = df_out.to_csv(index=False)
    fname = f"summary_data_{year}_{variable}_{strip}_{granularity}.csv"
    return Response(content=csv, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={fname}"})

# ――― Summary Statistics API endpoint ―――
@api_router.post("/get_summary_stats")
async def get_summary_stats(data: Dict[str, Any] = Body(...)):
    print("🔍 POST /api/get_summary_stats payload:", data)
    """
    Expects JSON with keys:
      year, variable, strip, granularity, depth,
      startDate (optional), endDate (optional), unitSystem (optional)
    Returns raw & ratio summary statistics.
    """
    # 1) Check required parameters
    required = ["year", "variable", "strip", "granularity", "depth"]
    missing = [k for k in required if data.get(k) is None]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing parameter(s): {', '.join(missing)}")

    # 2) Parse + validate types
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

    start = data.get("startDate")
    end = data.get("endDate")
    unit_system = data.get("unitSystem", "us")

    # 3) Load the DataFrame
    try:
        df = load_logger_year(year, granularity)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # 4) Filter by date if provided
    if start and end:
        df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]

    # 5) Compute summary statistics
    stats_raw, stats_ratio = compute_summary_statistics(df, variable, strip, str(depth))
    # Omit ratio for temperature variables
    if variable in ["T", "temp_air", "temp_soil_5cm", "temp_soil_15cm"]:
        stats_ratio = {}

    return JSONResponse({
        "year":             year,
        "variable":         variable,
        "strip":            strip,
        "granularity":      granularity,
        "depth":            str(depth),
        "raw_statistics":   stats_raw,
        "ratio_statistics": stats_ratio,
    })


# ――― Serve Markdown files ―――
@main_router.get("/markdown/{filename}")
async def serve_markdown(filename: str):
    """
    Serves markdown from: biochar_app/templates/markdown/<filename>
    """
    # Calculate the absolute path to your markdown folder
    md_dir = os.path.join(os.path.dirname(__file__), "..", "markdown")
    fullpath = os.path.abspath(os.path.join(md_dir, filename))

    if not os.path.exists(fullpath):
        raise HTTPException(status_code=404, detail=f"Markdown file '{filename}' not found")
    return FileResponse(fullpath, media_type="text/markdown")