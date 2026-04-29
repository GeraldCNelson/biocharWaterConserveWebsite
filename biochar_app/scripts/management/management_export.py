from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from biochar_app.config.paths import DATA_PROCESSED_DIR
from biochar_app.scripts.management.management_db import get_connection


MANAGEMENT_DIR = DATA_PROCESSED_DIR / "management"
IRRIGATION_CLEAN_CSV = MANAGEMENT_DIR / "irrigation_clean.csv"


EXPORT_COLUMNS = [
    "year",
    "date",
    "start_timestamp",
    "end_timestamp",
    "strip_group",
    "location",
    "gallons",
    "notes",
    "total_meter_gallons",
    "flow_allocation_fraction",
    "start_totalizer_gal_x100",
    "end_totalizer_gal_x100",
    "start_flow_gpm",
    "end_flow_gpm",
    "avg_flow_gpm",
    "entered_by",
    "event_id",
    "start_photo",
    "end_photo",
]


def irrigation_year_csv_path(year: int) -> Path:
    return MANAGEMENT_DIR / f"irrigation_{year}.csv"


def _backup_existing_file(path: Path) -> Path | None:
    if not path.exists():
        return None

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = path.with_name(f"{path.stem}_backup_{stamp}{path.suffix}")
    path.rename(backup_path)
    return backup_path


def _load_complete_irrigation_events() -> pd.DataFrame:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM irrigation_events
            WHERE status = 'complete'
            ORDER BY date, start_timestamp, strip_group
            """
        ).fetchall()

    if not rows:
        return pd.DataFrame(columns=EXPORT_COLUMNS)

    raw = pd.DataFrame([dict(r) for r in rows])

    out = pd.DataFrame()

    out["year"] = raw["year"]
    out["date"] = raw["date"]
    out["start_timestamp"] = raw["start_timestamp"]
    out["end_timestamp"] = raw["end_timestamp"]
    out["strip_group"] = raw["strip_group"]
    out["location"] = raw["location"]

    # For year files and clean master, `gallons` means gallons assigned
    # to this strip group. Split-flow events use allocated_gallons.
    out["gallons"] = raw["allocated_gallons"].where(
        raw["allocated_gallons"].notna(),
        raw["gallons"],
    )

    out["notes"] = raw["notes"].fillna("")
    out["start_photo"] = raw["start_photo"].fillna("")
    out["end_photo"] = raw["end_photo"].fillna("")

    # Extra audit/debug columns
    out["total_meter_gallons"] = raw["gallons"]
    out["flow_allocation_fraction"] = raw["flow_allocation_fraction"]
    out["start_totalizer_gal_x100"] = raw["start_totalizer_gal_x100"]
    out["end_totalizer_gal_x100"] = raw["end_totalizer_gal_x100"]
    out["start_flow_gpm"] = raw["start_flow_gpm"]
    out["end_flow_gpm"] = raw["end_flow_gpm"]
    out["avg_flow_gpm"] = raw["avg_flow_gpm"]
    out["entered_by"] = raw["entered_by"].fillna("")
    out["event_id"] = raw["event_id"]
    # Ensure photo columns exist even if missing
    for col in ["start_photo", "end_photo"]:
        if col not in out.columns:
            out[col] = ""

    out = out[EXPORT_COLUMNS]
    out = out.drop_duplicates(subset=["event_id"], keep="last")

    return out.sort_values(["year", "start_timestamp", "strip_group"]).reset_index(drop=True)


def export_irrigation_year_csvs() -> dict[str, Any]:
    """
    Export complete SQLite irrigation events into year-specific files:
      irrigation_2026.csv, etc.

    This does NOT rebuild irrigation_clean.csv.
    """
    MANAGEMENT_DIR.mkdir(parents=True, exist_ok=True)

    df = _load_complete_irrigation_events()
    if df.empty:
        return {
            "ok": True,
            "rows": 0,
            "years_written": [],
        }

    outputs: list[dict[str, Any]] = []

    for year, df_year in df.groupby("year", sort=True):
        year_int = int(year)
        out_path = irrigation_year_csv_path(year_int)
        backup_path = _backup_existing_file(out_path)

        df_year = df_year.sort_values(["start_timestamp", "end_timestamp", "strip_group"])
        df_year.to_csv(out_path, index=False)

        outputs.append(
            {
                "year": year_int,
                "rows": int(len(df_year)),
                "output_csv": str(out_path),
                "backup_csv": str(backup_path) if backup_path else None,
            }
        )

    return {
        "ok": True,
        "rows": int(len(df)),
        "years_written": outputs,
    }


def rebuild_irrigation_clean_csv() -> dict[str, Any]:
    """
    Rebuild irrigation_clean.csv from all year-specific irrigation_YYYY.csv files.
    """
    MANAGEMENT_DIR.mkdir(parents=True, exist_ok=True)

    year_files = sorted(
        p for p in MANAGEMENT_DIR.glob("irrigation_*.csv")
        if p.stem.removeprefix("irrigation_").isdigit()
    )

    if not year_files:
        backup_path = _backup_existing_file(IRRIGATION_CLEAN_CSV)
        pd.DataFrame(columns=EXPORT_COLUMNS).to_csv(IRRIGATION_CLEAN_CSV, index=False)
        return {
            "ok": True,
            "rows": 0,
            "files_used": [],
            "output_csv": str(IRRIGATION_CLEAN_CSV),
            "backup_csv": str(backup_path) if backup_path else None,
        }

    frames: list[pd.DataFrame] = []
    for path in year_files:
        df = pd.read_csv(path)

        # Make older 2023-2025 files compatible with the newer audit columns.
        for col in EXPORT_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[EXPORT_COLUMNS].copy()
        frames.append(df)

    df["start_timestamp"] = pd.to_datetime(df["start_timestamp"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M")
    df["end_timestamp"] = pd.to_datetime(df["end_timestamp"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M")
    combined = pd.concat(frames, ignore_index=True)

    combined["start_timestamp_sort"] = pd.to_datetime(
        combined["start_timestamp"],
        errors="coerce",
        format="mixed",
    )

    combined = (
        combined
        .sort_values(["year", "start_timestamp_sort", "strip_group"], kind="mergesort")
        .drop(columns=["start_timestamp_sort"])
        .reset_index(drop=True)
    )

    backup_path = _backup_existing_file(IRRIGATION_CLEAN_CSV)
    combined.to_csv(IRRIGATION_CLEAN_CSV, index=False)

    return {
        "ok": True,
        "rows": int(len(combined)),
        "files_used": [str(p) for p in year_files],
        "output_csv": str(IRRIGATION_CLEAN_CSV),
        "backup_csv": str(backup_path) if backup_path else None,
    }


# Backward-compatible name for the current route.
# You can keep the old endpoint while changing its behavior to year-file export.
def export_irrigation_clean_csv() -> dict[str, Any]:
    return export_irrigation_year_csvs()