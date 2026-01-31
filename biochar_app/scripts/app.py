#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import glob
import subprocess
import logging
from pathlib import Path

import pandas as pd
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

# ─── Your imports ──────────────────────────────────────────────────────────────
from biochar_app.scripts.config import (
    BASE_DIR,
    DEFAULT_GRANULARITY,
    DEFAULT_YEAR,
    YEARS,
    DEFAULT_GSEASON_PERIODS,
)
from biochar_app.scripts.routes_utils import load_logger_year as _orig_load_logger_year
from biochar_app.scripts.routes import main_router, api_router

from biochar_app.scripts import state

from biochar_app.scripts.date_ranges import build_date_ranges
PARQUET_ROOT = Path(BASE_DIR) / "data-processed" / "parquet"

state.DATE_RANGES = build_date_ranges(
    base_dir=PARQUET_ROOT,
    years=YEARS,
    granularities=["raw", "15min", "daily", "monthly", "gseason"],
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# logger.info("📅 Date ranges loaded:")
# for y, gmap in state.DATE_RANGES.items():
#     for g, r in gmap.items():
#         logger.info(f"  {y} {g}: {r['min']} → {r['max']}")

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Global caches ─────────────────────────────────────────────────────────────
_cache: dict[tuple[int, str], pd.DataFrame] = {}  # (year, granularity) -> df
DATE_RANGES: dict[int, dict[str, dict[str, str]]] = {}  # year -> key -> {"min": "...", "max": "..."}


# ============================= Date range helpers ============================= #

def parquet_timestamp_range(path: Path) -> dict[str, str] | None:
    """
    Return {"min": "YYYY-MM-DD", "max": "YYYY-MM-DD"} from a parquet file,
    reading only the timestamp column.
    """
    try:
        s = pd.read_parquet(path, columns=["timestamp"])["timestamp"]
    except Exception:
        return None

    s = pd.to_datetime(s, errors="coerce").dropna()
    if s.empty:
        return None

    return {
        "min": s.min().date().isoformat(),
        "max": s.max().date().isoformat(),
    }

# ============================= Caching hook ============================= #

def _cached_load_logger_year(year: int, granularity: str) -> pd.DataFrame:
    """
    Thin wrapper around routes_utils.load_logger_year to cache (year, granularity)
    in memory so all routes benefit.
    """
    key = (int(year), str(granularity))
    if key not in _cache:
        df = _orig_load_logger_year(int(year), str(granularity))
        _cache[key] = df
        logger.info(f"📥 Cached slice {key[0]}×{key[1]} (rows={len(df)})")
    return _cache[key]


# ============================= App setup ============================= #

load_dotenv()
app = FastAPI(
    title="Biochar Water Conserve API",
    description="Plots & data endpoints for biocharresearch.org",
)

# 1) Serve static assets
static_dir = os.path.join(BASE_DIR, "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# 2) Include routers
app.include_router(main_router)
app.include_router(api_router)

# 3) SPA entrypoint via Jinja2
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@app.get("/", response_class=HTMLResponse)
async def serve_spa(request: Request):
    # Convert the PERIODS dict into a list suitable for JSON
    periods_list = [
        {"code": code, "label": info["label"], "start": info["start"], "end": info["end"]}
        for code, info in DEFAULT_GSEASON_PERIODS.items()
    ]

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "DEFAULT_YEAR": DEFAULT_YEAR,
            "YEARS": YEARS,
            "DEFAULT_GSEASON_PERIODS": periods_list,
        },
    )


# 4) Ensure processed data exists
PARQUET_ROOT = os.path.join(BASE_DIR, "data-processed", "parquet")
os.makedirs(PARQUET_ROOT, exist_ok=True)
if not glob.glob(os.path.join(PARQUET_ROOT, "*", "*.parquet")):
    logger.info("⚙️  No parquet files found; running ETL")
    try:
        subprocess.run([sys.executable, "-m", "biochar_app.scripts.etl"], check=True)
        logger.info("✅ ETL completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ ETL failed: {e}")


# 5) Monkey-patch loader so all routes use caching “for free”
import biochar_app.scripts.routes_utils as _ru
_ru.load_logger_year = _cached_load_logger_year  # type: ignore


# 6) Build DATE_RANGES once at import time (timestamp-only scan)
logger.info("⏳ Preloading parquet date ranges (timestamp-only scan)...")
try:
    # log a short summary
    for year, d in DATE_RANGES.items():
        if "raw_logger" in d:
            logger.info(f"📅 {year} raw_logger: {d['raw_logger']['min']} → {d['raw_logger']['max']}")
        if "daily" in d:
            logger.info(f"📅 {year} daily     : {d['daily']['min']} → {d['daily']['max']}")
except Exception as e:
    logger.exception(f"❌ Failed to build DATE_RANGES: {e}")
    DATE_RANGES = {}

logger.info("✅ Date range preload complete")


# 7) Preload only the default slice at boot (optional, but nice)
try:
    df0 = _cached_load_logger_year(DEFAULT_YEAR, DEFAULT_GRANULARITY)
    logger.info(f"✅ Preloaded default slice ({DEFAULT_YEAR}, {DEFAULT_GRANULARITY}) rows={len(df0)}")
except FileNotFoundError:
    logger.warning(f"⚠️ No parquet found for default slice {DEFAULT_YEAR}/{DEFAULT_GRANULARITY}")
except Exception as e:
    logger.exception(f"❌ Failed to preload default slice: {e}")


# 8) Run with Uvicorn when invoked directly
if __name__ == "__main__":
    uvicorn.run(
        "biochar_app.scripts.app:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        log_level="info",
        reload=True,
    )