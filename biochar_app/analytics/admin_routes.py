"""Authenticated private dashboard, summary API, and CSV export."""

from __future__ import annotations

import csv
import os
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from biochar_app.analytics.auth import require_usage_admin
from biochar_app.analytics.collector import (
    INTERNAL_COOKIE_NAME,
    INTERNAL_SECRET_ENV,
    cookie_is_secure,
    is_internal_browser,
    make_internal_cookie_value,
    tracking_enabled,
    usage_db_path,
)
from biochar_app.analytics.database import MOUNTAIN_TIME
from biochar_app.analytics.reporting import (
    cumulative_totals,
    daily_totals,
    download_breakdown,
    exclusion_breakdown,
    feature_breakdown,
    tracking_period,
)

admin_usage_router = APIRouter()
TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
INTERNAL_COOKIE_MAX_AGE_SECONDS = 5 * 365 * 24 * 60 * 60
ADMIN_ACTION_HEADER = "X-Biochar-Usage-Admin"


def _private_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store",
        "X-Robots-Tag": "noindex, nofollow",
    }


def _validated_range(start: date, end: date) -> None:
    if end < start:
        raise HTTPException(status_code=400, detail="End date precedes start date")
    if (end - start).days > 366:
        raise HTTPException(status_code=400, detail="Date range exceeds 367 days")


def _require_admin_action_header(request: Request) -> None:
    """Reject cross-site attempts to change the internal-browser marker."""
    if request.headers.get(ADMIN_ACTION_HEADER) != "1":
        raise HTTPException(status_code=403, detail="Admin action header is required")


@admin_usage_router.get(
    "/admin/usage",
    response_class=HTMLResponse,
    name="usage_dashboard",
)
async def usage_dashboard(
    request: Request,
    _admin: Annotated[str, Depends(require_usage_admin)],
) -> HTMLResponse:
    """Render the separate private dashboard shell."""
    today = datetime.now(MOUNTAIN_TIME).date().isoformat()
    return templates.TemplateResponse(
        request=request,
        name="usage_dashboard.html",
        context={"request": request, "today": today},
        headers=_private_headers(),
    )


@admin_usage_router.get("/api/admin/usage/summary")
async def usage_summary(
    request: Request,
    _admin: Annotated[str, Depends(require_usage_admin)],
    end_date: date | None = None,
    days: int = Query(default=30, ge=1, le=367),
) -> JSONResponse:
    """Return dashboard totals and breakdowns for one Mountain Time range."""
    selected_end = end_date or datetime.now(MOUNTAIN_TIME).date()
    selected_start = selected_end - timedelta(days=days - 1)
    previous_date = selected_end - timedelta(days=1)
    db_path = usage_db_path()
    comparison = daily_totals(previous_date, selected_end, db_path=db_path)
    payload = {
        "reporting_timezone": MOUNTAIN_TIME.key,
        "start_date": selected_start.isoformat(),
        "end_date": selected_end.isoformat(),
        "days": days,
        "selected_day": comparison[-1],
        "previous_day": comparison[0],
        "daily": daily_totals(selected_start, selected_end, db_path=db_path),
        "cumulative": cumulative_totals(db_path=db_path),
        "features": feature_breakdown(
            selected_start,
            selected_end,
            db_path=db_path,
        ),
        "downloads": download_breakdown(
            selected_start,
            selected_end,
            db_path=db_path,
        ),
        "exclusions": exclusion_breakdown(
            selected_start,
            selected_end,
            db_path=db_path,
        ),
        "tracking_period": tracking_period(db_path=db_path),
        "tracking_enabled": tracking_enabled(),
        "internal_browser": is_internal_browser(request),
        "internal_exclusion_ready": bool(
            os.getenv(INTERNAL_SECRET_ENV, "").strip()
        ),
    }
    return JSONResponse(payload, headers=_private_headers())


@admin_usage_router.get("/api/admin/usage/export.csv")
async def export_usage_csv(
    _admin: Annotated[str, Depends(require_usage_admin)],
    start_date: date,
    end_date: date,
) -> Response:
    """Export daily public totals using Mountain Time reporting dates."""
    _validated_range(start_date, end_date)
    rows = daily_totals(start_date, end_date, db_path=usage_db_path())
    output = StringIO(newline="")
    fieldnames = [
        "reporting_timezone",
        "date",
        "public_visits",
        "page_views",
        "feature_uses",
        "paper_downloads",
        "data_downloads",
        "failed_downloads",
        "excluded_events",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {"reporting_timezone": MOUNTAIN_TIME.key, **row}
        )

    filename = f"biochar_usage_{start_date.isoformat()}_to_{end_date.isoformat()}.csv"
    return Response(
        content=output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={
            **_private_headers(),
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


@admin_usage_router.get("/api/admin/usage/internal-browser")
async def internal_browser_status(
    request: Request,
    _admin: Annotated[str, Depends(require_usage_admin)],
) -> JSONResponse:
    """Report whether the current browser is excluded as an internal browser."""
    return JSONResponse(
        {
            "internal_browser": is_internal_browser(request),
            "configured": bool(os.getenv(INTERNAL_SECRET_ENV, "").strip()),
        },
        headers=_private_headers(),
    )


@admin_usage_router.post("/api/admin/usage/internal-browser")
async def mark_internal_browser(
    request: Request,
    _admin: Annotated[str, Depends(require_usage_admin)],
) -> JSONResponse:
    """Set a signed, identity-free internal-browser marker."""
    _require_admin_action_header(request)
    secret = os.getenv(INTERNAL_SECRET_ENV, "").strip()
    if not secret:
        raise HTTPException(
            status_code=503,
            detail="Internal-browser signing is not configured",
        )
    response = JSONResponse(
        {"internal_browser": True},
        headers=_private_headers(),
    )
    response.set_cookie(
        INTERNAL_COOKIE_NAME,
        make_internal_cookie_value(secret),
        max_age=INTERNAL_COOKIE_MAX_AGE_SECONDS,
        httponly=True,
        secure=cookie_is_secure(request),
        # Keep the marker on ordinary top-level navigations from email,
        # search results, and other sites so an internal user's landing page
        # is not briefly counted as public activity.
        samesite="lax",
        path="/",
    )
    return response


@admin_usage_router.delete("/api/admin/usage/internal-browser")
async def unmark_internal_browser(
    request: Request,
    _admin: Annotated[str, Depends(require_usage_admin)],
) -> JSONResponse:
    """Remove the internal-browser marker from the current browser."""
    _require_admin_action_header(request)
    response = JSONResponse(
        {"internal_browser": False},
        headers=_private_headers(),
    )
    response.delete_cookie(INTERNAL_COOKIE_NAME, path="/")
    return response
