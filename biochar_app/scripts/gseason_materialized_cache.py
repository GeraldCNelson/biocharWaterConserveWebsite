"""Persistent cache for versioned seasonal summary rows."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from biochar_app.config.paths import (
    DATA_PROCESSED_DIR,
    PARQUET_SUMMARY_15MIN_DIR,
    PARQUET_SUMMARY_WEATHER_15MIN_DIR,
)


CACHE_FORMAT_VERSION = 2
DEFAULT_CACHE_DIR = DATA_PROCESSED_DIR / "seasonal-summary-cache"


def gseason_source_paths(year: int) -> list[Path]:
    """Return files whose metadata determines whether a cached year is current."""
    candidates = [
        PARQUET_SUMMARY_15MIN_DIR / f"{year}_15min.parquet",
        PARQUET_SUMMARY_15MIN_DIR / f"{year}_15min_ratios.parquet",
    ]
    if PARQUET_SUMMARY_WEATHER_15MIN_DIR.exists():
        candidates.extend(PARQUET_SUMMARY_WEATHER_15MIN_DIR.glob(f"*{year}*"))
    return sorted({path for path in candidates if path.exists()}, key=str)


def source_fingerprint(paths: Iterable[Path]) -> str:
    """Hash path, size, and nanosecond modification time without reading data."""
    records = []
    for path in sorted((Path(item) for item in paths), key=str):
        stat = path.stat()
        records.append((str(path.resolve()), stat.st_size, stat.st_mtime_ns))
    encoded = json.dumps(records, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _configuration(
    *,
    year: int,
    variable: str,
    strip: str,
    depth: str,
    unit_system: str,
    periods: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "year": int(year),
        "variable": str(variable),
        "strip": str(strip),
        "depth": str(depth),
        "unit_system": str(unit_system),
        "periods": [
            {
                "code": str(period.get("code", "")),
                "label": str(period.get("label", "")),
                "start": str(period.get("start", "")),
                "end": str(period.get("end", "")),
            }
            for period in periods
        ],
    }


def _cache_path(cache_dir: Path, configuration: dict[str, Any]) -> Path:
    encoded = json.dumps(
        configuration, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()[:24]
    return Path(cache_dir) / str(configuration["year"]) / f"{digest}.json"


def load_materialized_gseason_summary(
    *,
    year: int,
    variable: str,
    strip: str,
    depth: str,
    unit_system: str,
    periods: list[dict[str, Any]],
    cache_dir: Path = DEFAULT_CACHE_DIR,
    source_paths: Iterable[Path] | None = None,
) -> list[dict[str, Any]] | None:
    """Return cached rows only when format, configuration, and sources match."""
    configuration = _configuration(
        year=year,
        variable=variable,
        strip=strip,
        depth=depth,
        unit_system=unit_system,
        periods=periods,
    )
    path = _cache_path(cache_dir, configuration)
    if not path.exists():
        return None

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        current_fingerprint = source_fingerprint(
            source_paths if source_paths is not None else gseason_source_paths(year)
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None

    if payload.get("format_version") != CACHE_FORMAT_VERSION:
        return None
    if payload.get("configuration") != configuration:
        return None
    if payload.get("source_fingerprint") != current_fingerprint:
        return None
    rows = payload.get("gseason_stats")
    return rows if isinstance(rows, list) else None


def save_materialized_gseason_summary(
    *,
    year: int,
    variable: str,
    strip: str,
    depth: str,
    unit_system: str,
    periods: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    cache_dir: Path = DEFAULT_CACHE_DIR,
    source_paths: Iterable[Path] | None = None,
) -> Path:
    """Atomically persist one year/configuration of seasonal summary rows."""
    configuration = _configuration(
        year=year,
        variable=variable,
        strip=strip,
        depth=depth,
        unit_system=unit_system,
        periods=periods,
    )
    path = _cache_path(cache_dir, configuration)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "format_version": CACHE_FORMAT_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "configuration": configuration,
        "source_fingerprint": source_fingerprint(
            source_paths if source_paths is not None else gseason_source_paths(year)
        ),
        "gseason_stats": rows,
    }
    temporary = path.with_suffix(
        f".tmp-{os.getpid()}-{threading.get_ident()}.json"
    )
    temporary.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    temporary.replace(path)
    return path
