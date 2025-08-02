#!/usr/bin/env python3
from __future__ import annotations
import os
import sys
import glob
import subprocess
import logging

import pandas as pd
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

# ─── Your imports ──────────────────────────────────────────────────────────────
from biochar_app.scripts.config        import BASE_DIR, DEFAULT_GRANULARITY, DEFAULT_YEAR, YEARS, DEFAULT_GSEASON_PERIODS
from biochar_app.scripts.routes_utils  import load_logger_year as _orig_load_logger_year
from biochar_app.scripts.routes        import main_router, api_router

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── App setup ─────────────────────────────────────────────────────────────────
load_dotenv()
app = FastAPI(
    title="Biochar Water Conserve API",
    description="Plots & data endpoints for biocharresearch.org",
)

# ─── 1) Serve static assets ──────────────────────────────────────────────────
static_dir = os.path.join(BASE_DIR, "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# ─── 2) Include your routers ─────────────────────────────────────────────────
#    main_router: /plot_raw, /plot_ratio, etc.
#    api_router  : /api/get_defaults_and_options, /api/download_summary_data, etc.
app.include_router(main_router)
app.include_router(api_router)

# ─── 3) SPA entrypoint via Jinja2 ─────────────────────────────────────────────
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.get("/", response_class=HTMLResponse)
async def serve_spa(request: Request):
    # convert the PERIODS dict into a list suitable for JSON:
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
        "index.html",
        {
            "request": request,
            "DEFAULT_YEAR": DEFAULT_YEAR,
            "YEARS": YEARS,
            # now this is a *list* in JS, not an object
            "DEFAULT_GSEASON_PERIODS": periods_list,
        },
    )

# ─── 4) Ensure processed data exists ───────────────────────────────────────────
PARQUET_ROOT = os.path.join(BASE_DIR, "data-processed", "parquet")
os.makedirs(PARQUET_ROOT, exist_ok=True)
if not glob.glob(os.path.join(PARQUET_ROOT, "*", "*.parquet")):
    logger.info("⚙️  No parquet files found; running ETL")
    try:
        subprocess.run([sys.executable, "-m", "biochar_app.scripts.etl"], check=True)
        logger.info("✅ ETL completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ ETL failed: {e}")

# ─── 5) Lazy, in-memory cache of (year,granularity) DataFrames ───────────────
_cache: dict[tuple[int, str], pd.DataFrame] = {}

def _cached_load_logger_year(year: int, granularity: str) -> pd.DataFrame:
    key = (year, granularity)
    if key not in _cache:
        df = _orig_load_logger_year(year, granularity)
        _cache[key] = df
        logger.info(f"📥 Cached slice {year!r}×{granularity!r} (rows={len(df)})")
    return _cache[key]

# Monkey-patch the loader so all routes use caching “for free”
import biochar_app.scripts.routes_utils as _ru
_ru.load_logger_year = _cached_load_logger_year  # type: ignore

# Preload only the default slice at boot
try:
    df0 = _cached_load_logger_year(DEFAULT_YEAR, DEFAULT_GRANULARITY)
    logger.info(f"✅ Preloaded default slice ({DEFAULT_YEAR}, {DEFAULT_GRANULARITY}) rows={len(df0)}")
except FileNotFoundError:
    logger.warning(f"No parquet found for default slice {DEFAULT_YEAR}/{DEFAULT_GRANULARITY}")

# ─── 6) Run with Uvicorn when invoked directly ────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "biochar_app.scripts.app:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        log_level="info",
        reload=True,
    )