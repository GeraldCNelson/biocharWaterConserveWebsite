from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import uuid4

from fastapi import UploadFile, File
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from biochar_app.scripts.management.management_db import (
    initialize_management_db,
    insert_irrigation_event,
    update_irrigation_event,
    get_irrigation_event,
    list_irrigation_events,
)

from biochar_app.scripts.management.management_export import (
    export_irrigation_clean_csv,
    rebuild_irrigation_clean_csv,
)

from biochar_app.config.paths import DATA_PROCESSED_DIR

from PIL import Image
from io import BytesIO

management_router = APIRouter(prefix="/api/management", tags=["management"])


class StartIrrigationRequest(BaseModel):
    strip_group: str = Field(..., examples=["S1_S2"])
    location: str = Field(..., examples=["west"])
    start_timestamp: str
    start_totalizer_gal_x100: Optional[float] = None
    start_flow_gpm: Optional[float] = None
    flow_allocation_fraction: float = 1.0
    entered_by: str = ""
    notes: str = ""


class FinishIrrigationRequest(BaseModel):
    end_timestamp: str
    end_totalizer_gal_x100: Optional[float] = None
    end_flow_gpm: Optional[float] = None
    notes: str = ""


def _year_from_timestamp(ts: str) -> int:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).year


def _date_from_timestamp(ts: str) -> str:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).date().isoformat()


def _normalize_strip_group(value: str) -> str:
    compact = value.strip().upper().replace(" ", "").replace("-", "").replace("_", "")
    if compact in {"S1S2", "1AND2", "1&2", "S1ANDS2", "S1&S2"}:
        return "S1_S2"
    if compact in {"S3S4", "3AND4", "3&4", "S3ANDS4", "S3&S4"}:
        return "S3_S4"
    raise HTTPException(status_code=400, detail=f"Invalid strip_group: {value}")


def _location_for_group(strip_group: str, fallback: str) -> str:
    if strip_group == "S1_S2":
        return "west"
    if strip_group == "S3_S4":
        return "east"
    return fallback.strip().lower()


def _compute_gallons(start_x100: Optional[float], end_x100: Optional[float]) -> Optional[float]:
    if start_x100 is None or end_x100 is None:
        return None
    gallons = (end_x100 - start_x100) * 100.0
    if gallons < 0:
        raise HTTPException(status_code=400, detail="End meter reading is less than start meter reading.")
    return gallons


def _compute_allocated_gallons(
    gallons: Optional[float],
    flow_allocation_fraction: object,
) -> Optional[float]:
    if gallons is None:
        return None

    try:
        fraction = float(flow_allocation_fraction)
    except (TypeError, ValueError):
        fraction = 1.0

    return gallons * fraction


def _compute_avg_flow(start_flow: Optional[float], end_flow: Optional[float]) -> Optional[float]:
    vals = [v for v in [start_flow, end_flow] if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


@management_router.on_event("startup")
async def startup_management_db() -> None:
    initialize_management_db()


@management_router.post("/irrigation/start")
async def start_irrigation_event(req: StartIrrigationRequest):
    strip_group = _normalize_strip_group(req.strip_group)
    location = _location_for_group(strip_group, req.location)

    event_id = f"{_date_from_timestamp(req.start_timestamp)}_{strip_group}_{uuid4().hex[:8]}"

    row = {
        "event_id": event_id,
        "year": _year_from_timestamp(req.start_timestamp),
        "date": _date_from_timestamp(req.start_timestamp),
        "strip_group": strip_group,
        "location": location,
        "start_timestamp": req.start_timestamp,
        "start_totalizer_gal_x100": req.start_totalizer_gal_x100,
        "start_flow_gpm": req.start_flow_gpm,
        "flow_allocation_fraction": req.flow_allocation_fraction,
        "entered_by": req.entered_by.strip(),
        "notes": req.notes.strip(),
        "status": "in_progress",
    }

    insert_irrigation_event(row)
    return {"ok": True, "event_id": event_id, "event": get_irrigation_event(event_id)}


@management_router.post("/irrigation/{event_id}/finish")
async def finish_irrigation_event(event_id: str, req: FinishIrrigationRequest):
    event = get_irrigation_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail=f"Irrigation event not found: {event_id}")

    gallons = _compute_gallons(
        event.get("start_totalizer_gal_x100"),
        req.end_totalizer_gal_x100,
    )

    allocated_gallons = _compute_allocated_gallons(
        gallons,
        event.get("flow_allocation_fraction", 1.0),
    )

    avg_flow = _compute_avg_flow(
        event.get("start_flow_gpm"),
        req.end_flow_gpm,
    )

    existing_notes = str(event.get("notes") or "").strip()
    new_notes = req.notes.strip()
    notes = existing_notes
    if new_notes:
        notes = f"{existing_notes}\n{new_notes}".strip() if existing_notes else new_notes

    updates = {
        "end_timestamp": req.end_timestamp,
        "end_totalizer_gal_x100": req.end_totalizer_gal_x100,
        "end_flow_gpm": req.end_flow_gpm,
        "gallons": gallons,
        "allocated_gallons": allocated_gallons,
        "avg_flow_gpm": avg_flow,
        "notes": notes,
        "status": "complete",
    }

    update_irrigation_event(event_id, updates)
    return {"ok": True, "event_id": event_id, "event": get_irrigation_event(event_id)}


@management_router.get("/irrigation/events")
async def api_list_irrigation_events(limit: int = 100):
    return {"events": list_irrigation_events(limit=limit)}


@management_router.get("/irrigation/{event_id}")
async def api_get_irrigation_event(event_id: str):
    event = get_irrigation_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail=f"Irrigation event not found: {event_id}")
    return {"event": event}

@management_router.post("/irrigation/export-clean-csv")
async def api_export_irrigation_clean_csv():
    return export_irrigation_clean_csv()

@management_router.post("/irrigation/rebuild-clean-csv")
async def api_rebuild_irrigation_clean_csv():
    return rebuild_irrigation_clean_csv()


PHOTO_DIR = DATA_PROCESSED_DIR / "management" / "photos" / "irrigation"


def _photo_path(event_id: str, photo_type: str, original_filename: str) -> Path:
    event = get_irrigation_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    event_date = str(event["date"])
    flow_fraction = float(event.get("flow_allocation_fraction") or 1.0)
    suffix = event_id.split("_")[-1]

    if flow_fraction < 1.0:
        label = "combined"
    else:
        label = str(event["strip_group"])

    filename = f"{event_date}_{label}_{suffix}_{photo_type}.jpg"
    return PHOTO_DIR / filename


@management_router.post("/irrigation/{event_id}/photo/{photo_type}")
async def upload_irrigation_photo(
    event_id: str,
    photo_type: str,
    file: UploadFile = File(...),
):
    if photo_type not in {"start", "end"}:
        raise HTTPException(status_code=400, detail="photo_type must be 'start' or 'end'")

    event = get_irrigation_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    PHOTO_DIR.mkdir(parents=True, exist_ok=True)
    path = _photo_path(event_id, photo_type, file.filename or "photo.jpg")

    try:
        content = await file.read()

        img = Image.open(BytesIO(content))
        img = img.convert("RGB")
        img.thumbnail((1200, 1200))

        img.save(
            path,
            format="JPEG",
            quality=75,
            optimize=True,
        )

    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Could not process uploaded image: {exc}",
        ) from exc

    field_name = "start_photo" if photo_type == "start" else "end_photo"

    update_irrigation_event(
        event_id,
        {
            field_name: str(path),
        },
    )

    return {
        "ok": True,
        "event_id": event_id,
        "photo_type": photo_type,
        "photo_path": str(path),
    }

@management_router.post("/irrigation/export-and-rebuild")
async def api_export_and_rebuild_irrigation():
    export_result = export_irrigation_clean_csv()
    rebuild_result = rebuild_irrigation_clean_csv()

    return {
        "ok": True,
        "export": export_result,
        "rebuild": rebuild_result,
    }