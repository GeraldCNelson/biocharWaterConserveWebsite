"""Response-aware tracking middleware for the public FastAPI application."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from biochar_app.analytics.collector import (
    PendingUsageEvent,
    mark_usage_event,
    pending_usage_event,
    record_request_event,
    refresh_session_cookie,
)

AUTO_FEATURE_ROUTES = {
    ("POST", "/api/plot_raw"): "plot:raw",
    ("POST", "/api/plot_ratio"): "plot:ratio",
    ("POST", "/api/get_summary_stats"): "summary:calculate",
    ("POST", "/api/download_plot_data"): "export:plot-data",
    ("POST", "/api/download_summary_data"): "export:summary-data",
}


def _automatic_event(request: Request) -> PendingUsageEvent | None:
    method_path = (request.method.upper(), request.url.path)
    if method_path == ("GET", "/"):
        return PendingUsageEvent(event_type="page_view", event_name="home")

    feature_name = AUTO_FEATURE_ROUTES.get(method_path)
    if feature_name:
        return PendingUsageEvent(event_type="feature", event_name=feature_name)

    if request.method.upper() == "GET" and request.url.path.startswith(
        "/bulk_download/"
    ):
        parts = request.url.path.strip("/").split("/")
        if len(parts) == 3:
            dataset, year_text = parts[1], parts[2]
            dataset_year = int(year_text) if year_text.isdigit() else None
            return PendingUsageEvent(
                event_type="download",
                event_name="bulk-data",
                dataset=dataset,
                dataset_year=dataset_year,
                granularity=(
                    "15min" if dataset in {"loggers", "weather"} else None
                ),
            )
    return None


class UsageTrackingMiddleware(BaseHTTPMiddleware):
    """Record selected requests after their response status is known."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        automatic = _automatic_event(request)
        if automatic is not None:
            mark_usage_event(request, automatic)

        try:
            response = await call_next(request)
        except Exception:
            event = pending_usage_event(request)
            if event is not None:
                record_request_event(request, event, status_code=500)
            raise

        event = pending_usage_event(request)
        context = None
        if event is not None:
            context = record_request_event(
                request,
                event,
                status_code=response.status_code,
            )
        refresh_session_cookie(response, request, context)
        return response
