#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import subprocess
import logging
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from biochar_app.scripts.config import (
    BASE_DIR,
    DEFAULT_GRANULARITY,
    DEFAULT_YEAR,
    YEARS,
    DEFAULT_GSEASON_PERIODS,
)

from biochar_app.config.core import MONTH_ABBR
from biochar_app.config.paths import PARQUET_DIR
from biochar_app.scripts.data_loading import load_logger_data
from biochar_app.scripts.routes import main_router, api_router
from biochar_app.scripts.date_ranges import build_date_ranges
from biochar_app.scripts import state

from biochar_app.scripts.management.management_routes import management_router

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ============================= App setup ============================= #

load_dotenv()

app = FastAPI(
    title="Biochar Water Conserve API",
    description="Plots & data endpoints for biocharresearch.org",
)

base_dir = Path(BASE_DIR)
static_dir = base_dir / "static"
templates_dir = base_dir / "templates"

# 1) Serve static assets
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# 2) Include routers
app.include_router(main_router)
app.include_router(api_router)

app.include_router(management_router)

# 3) SPA entrypoint via Jinja2
templates = Jinja2Templates(directory=str(templates_dir))

@app.get("/", response_class=HTMLResponse)
async def serve_spa(request: Request) -> HTMLResponse:
    periods_list = [
        {
            "code": code,
            "label": info["label"],
            "start": info["start"],
            "end": info["end"],
        }
        for code, info in DEFAULT_GSEASON_PERIODS.items()
    ]

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "DEFAULT_YEAR": DEFAULT_YEAR,
            "YEARS": YEARS,
            "DEFAULT_GSEASON_PERIODS": periods_list,
            "MONTH_ABBR": dict(MONTH_ABBR),
        },
    )

# 4) Ensure processed data exists
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

has_any_parquet = any(PARQUET_DIR.rglob("*.parquet"))
if not has_any_parquet:
    logger.info("⚙️ No parquet files found under %s; running ETL", PARQUET_DIR)
    try:
        subprocess.run([sys.executable, "-m", "biochar_app.scripts.etl"], check=True)
        logger.info("✅ ETL completed successfully")
    except subprocess.CalledProcessError as exc:
        logger.error("❌ ETL failed: %s", exc)

# 5) Build DATE_RANGES once at import time
logger.info("⏳ Preloading parquet date ranges...")
try:
    state.DATE_RANGES = build_date_ranges(
        base_dir=PARQUET_DIR,
        years=YEARS,
        granularities=["raw", "15min", "daily", "monthly", "gseason"],
    )

    for year, ranges in state.DATE_RANGES.items():
        if "raw_logger" in ranges:
            logger.info(
                "📅 %s raw_logger: %s → %s",
                year,
                ranges["raw_logger"]["min"],
                ranges["raw_logger"]["max"],
            )
        if "daily" in ranges:
            logger.info(
                "📅 %s daily     : %s → %s",
                year,
                ranges["daily"]["min"],
                ranges["daily"]["max"],
            )
except Exception as exc:
    logger.exception("❌ Failed to build DATE_RANGES: %s", exc)
    state.DATE_RANGES = {}

logger.info("✅ Date range preload complete")

# 6) Warm the interactive-plot caches at boot. Daily is the initial view;
# 15-minute is the largest commonly selected view and is expensive enough to
# cause a noticeable delay when first loaded on demand.
preload_granularities = dict.fromkeys((DEFAULT_GRANULARITY, "15min"))
for granularity in preload_granularities:
    try:
        df0 = load_logger_data(DEFAULT_YEAR, granularity)
        logger.info(
            "✅ Preloaded logger data (%s, %s) rows=%d",
            DEFAULT_YEAR,
            granularity,
            len(df0),
        )
    except FileNotFoundError:
        logger.warning(
            "⚠️ No parquet found for logger preload %s/%s",
            DEFAULT_YEAR,
            granularity,
        )
    except Exception as exc:
        logger.exception(
            "❌ Failed to preload logger data %s/%s: %s",
            DEFAULT_YEAR,
            granularity,
            exc,
        )

# 7) Run with Uvicorn when invoked directly
if __name__ == "__main__":
    uvicorn.run(
        "biochar_app.scripts.app:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        log_level="info",
        reload=True,
    )
