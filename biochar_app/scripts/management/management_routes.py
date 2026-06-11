from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel, Field
from PIL import Image

from biochar_app.config.paths import DATA_PROCESSED_DIR
from biochar_app.scripts.management.management_db import (
    initialize_management_db,
    insert_irrigation_event,
    update_irrigation_event,
    get_irrigation_event,
    list_irrigation_events,
)

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


def _compute_total_meter_gallons(
    start_totalizer_gal_x100: Optional[float],
    end_totalizer_gal_x100: Optional[float],
) -> Optional[float]:
    if start_totalizer_gal_x100 is None or end_totalizer_gal_x100 is None:
        return None

    total_meter_gallons = (
        end_totalizer_gal_x100 - start_totalizer_gal_x100
    ) * 100.0

    if total_meter_gallons < 0:
        raise HTTPException(
            status_code=400,
            detail="End meter reading is less than start meter reading.",
        )

    return total_meter_gallons


def _compute_gallons_group(
    total_meter_gallons: Optional[float],
    flow_allocation_fraction: float | int | str | None,
) -> Optional[float]:
    if total_meter_gallons is None:
        return None

    if flow_allocation_fraction is None:
        fraction = 1.0
    else:
        try:
            fraction = float(flow_allocation_fraction)
        except (TypeError, ValueError):
            fraction = 1.0

    return total_meter_gallons * fraction


def _compute_avg_flow_gpm_group(
    start_flow_gpm: Optional[float],
    end_flow_gpm: Optional[float],
    flow_allocation_fraction: float | int | str | None,
) -> Optional[float]:
    vals = [v for v in [start_flow_gpm, end_flow_gpm] if v is not None]

    if not vals:
        return None

    if flow_allocation_fraction is None:
        fraction = 1.0
    else:
        try:
            fraction = float(flow_allocation_fraction)
        except (TypeError, ValueError):
            fraction = 1.0

    avg_meter_flow_gpm = sum(vals) / len(vals)
    return avg_meter_flow_gpm * fraction


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

    return {
        "ok": True,
        "event_id": event_id,
        "event": get_irrigation_event(event_id),
    }


@management_router.post("/irrigation/{event_id}/finish")
async def finish_irrigation_event(event_id: str, req: FinishIrrigationRequest):
    event = get_irrigation_event(event_id)

    if event is None:
        raise HTTPException(
            status_code=404,
            detail=f"Irrigation event not found: {event_id}",
        )

    total_meter_gallons = _compute_total_meter_gallons(
        event.get("start_totalizer_gal_x100"),
        req.end_totalizer_gal_x100,
    )

    gallons_group = _compute_gallons_group(
        total_meter_gallons,
        event.get("flow_allocation_fraction", 1.0),
    )

    avg_flow_gpm_group = _compute_avg_flow_gpm_group(
        event.get("start_flow_gpm"),
        req.end_flow_gpm,
        event.get("flow_allocation_fraction", 1.0),
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
        "total_meter_gallons": total_meter_gallons,
        "gallons_group": gallons_group,
        "avg_flow_gpm_group": avg_flow_gpm_group,
        "notes": notes,
        "status": "complete",
    }

    update_irrigation_event(event_id, updates)

    return {
        "ok": True,
        "event_id": event_id,
        "event": get_irrigation_event(event_id),
    }


@management_router.get("/irrigation/events")
async def api_list_irrigation_events(limit: int = 100):
    return {"events": list_irrigation_events(limit=limit)}


@management_router.get("/irrigation/{event_id}")
async def api_get_irrigation_event(event_id: str):
    event = get_irrigation_event(event_id)

    if event is None:
        raise HTTPException(
            status_code=404,
            detail=f"Irrigation event not found: {event_id}",
        )

    return {"event": event}


PHOTO_DIR = DATA_PROCESSED_DIR / "management" / "photos" / "irrigation"


def _photo_path(event_id: str, photo_type: str, original_filename: str) -> Path:
    event = get_irrigation_event(event_id)

    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    event_date = str(event["date"])
    flow_fraction = float(event.get("flow_allocation_fraction") or 1.0)
    suffix = event_id.split("_")[-1]

    label = "combined" if flow_fraction < 1.0 else str(event["strip_group"])
    filename = f"{event_date}_{label}_{suffix}_{photo_type}.jpg"

    return PHOTO_DIR / filename


@management_router.post("/irrigation/{event_id}/photo/{photo_type}")
async def upload_irrigation_photo(
    event_id: str,
    photo_type: str,
    file: UploadFile = File(...),
):
    if photo_type not in {"start", "end"}:
        raise HTTPException(
            status_code=400,
            detail="photo_type must be 'start' or 'end'",
        )

    event = get_irrigation_event(event_id)

    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    PHOTO_DIR.mkdir(parents=True, exist_ok=True)
    path = _photo_path(event_id, photo_type, file.filename or "photo.jpg")

    try:
        content = await file.read()

        with Image.open(BytesIO(content)) as opened_img:
            img = opened_img.convert("RGB")

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