"""Public collection and explicitly tracked document-download routes."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, field_validator

from biochar_app.analytics.collector import PendingUsageEvent, mark_usage_event
from biochar_app.config.paths import STATIC_DIR

usage_router = APIRouter()

DRAFT_REPORT_VERSION = "2026-11-13"
DRAFT_REPORT_FILES: dict[str, tuple[Path, str]] = {
    "pdf": (
        STATIC_DIR
        / "documents"
        / "VPI_Biochar_Report_US_Units_2026-11-13.pdf",
        "application/pdf",
    ),
    "docx": (
        STATIC_DIR
        / "documents"
        / "VPI_Biochar_Report_US_Units_2026-11-13.docx",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ),
}

ALLOWED_FEATURE_EVENTS = frozenset(
    {
        "tab:intro",
        "tab:draft-report",
        "tab:what-is-biochar",
        "tab:experiment",
        "tab:interactive-plots",
        "tab:summary",
        "tab:growing-season",
        "tab:biochar-analysis",
        "tab:nir",
        "tab:biomass-field",
        "tab:soil-biology",
        "tab:soil-chemistry",
        "tab:technical-details",
        "tab:bulk-downloads",
        "tab:glossary",
        "tab:acknowledgements",
    }
)


class FeatureEventRequest(BaseModel):
    event_name: str

    @field_validator("event_name")
    @classmethod
    def event_name_must_be_allowlisted(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in ALLOWED_FEATURE_EVENTS:
            raise ValueError("Unsupported feature event")
        return normalized


@usage_router.post("/api/usage/event", status_code=204)
async def collect_feature_event(
    request: Request,
    payload: FeatureEventRequest,
) -> Response:
    """Accept one small, allowlisted browser feature event."""
    mark_usage_event(
        request,
        PendingUsageEvent(event_type="feature", event_name=payload.event_name),
    )
    return Response(status_code=204)


@usage_router.get(
    "/downloads/draft-paper/{file_format}",
    name="download_draft_paper",
)
async def download_draft_paper(
    request: Request,
    file_format: str,
) -> FileResponse:
    """Serve and count an explicit draft-paper download."""
    normalized_format = file_format.strip().lower()
    report = DRAFT_REPORT_FILES.get(normalized_format)
    if report is None:
        raise HTTPException(status_code=404, detail="Draft format not found")

    path, media_type = report
    mark_usage_event(
        request,
        PendingUsageEvent(
            event_type="download",
            event_name="draft-paper",
            document_version=DRAFT_REPORT_VERSION,
            file_format=normalized_format,
        ),
    )
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Draft report file not found")

    return FileResponse(
        path=str(path),
        filename=path.name,
        media_type=media_type,
    )
