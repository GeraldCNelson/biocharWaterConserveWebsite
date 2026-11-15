"""Route and middleware tests for the first public usage integrations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from http.cookies import SimpleCookie
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi import HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from biochar_app.analytics.collector import (
    DB_PATH_ENV,
    INTERNAL_COOKIE_NAME,
    INTERNAL_SECRET_ENV,
    SESSION_COOKIE_NAME,
    SESSION_SECRET_ENV,
    make_internal_cookie_value,
)
from biochar_app.analytics.database import read_usage_events
from biochar_app.analytics.middleware import UsageTrackingMiddleware
from biochar_app.analytics.routes import (
    FeatureEventRequest,
    collect_feature_event,
    download_draft_paper,
)
from biochar_app.scripts.bulk_downloads import bulk_download

Endpoint = Callable[[Request], Awaitable[Response]]


@pytest.fixture
def usage_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    db_path = tmp_path / "usage.sqlite3"
    monkeypatch.setenv(SESSION_SECRET_ENV, "test-session-secret")
    monkeypatch.setenv(INTERNAL_SECRET_ENV, "test-internal-secret")
    monkeypatch.setenv(DB_PATH_ENV, str(db_path))
    return db_path


def _request(
    path: str,
    *,
    method: str = "GET",
    cookies: dict[str, str] | None = None,
    user_agent: str = "Mozilla/5.0 Safari/605.1.15",
) -> Request:
    headers = [(b"user-agent", user_agent.encode("latin-1"))]
    if cookies:
        cookie_text = "; ".join(f"{key}={value}" for key, value in cookies.items())
        headers.append((b"cookie", cookie_text.encode("latin-1")))
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("ascii"),
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
    )


def _dispatch(request: Request, endpoint: Endpoint) -> Response:
    middleware = UsageTrackingMiddleware(app=lambda scope, receive, send: None)
    return asyncio.run(middleware.dispatch(request, endpoint))


async def _ok_response(_request: Request) -> Response:
    return JSONResponse({"ok": True})


def _session_cookie(response: Response) -> str:
    parsed = SimpleCookie()
    parsed.load(response.headers["set-cookie"])
    return parsed[SESSION_COOKIE_NAME].value


def test_page_views_share_one_short_lived_anonymous_visit(
    usage_environment: Path,
) -> None:
    first = _dispatch(_request("/"), _ok_response)
    session_id = _session_cookie(first)
    second = _dispatch(
        _request("/", cookies={SESSION_COOKIE_NAME: session_id}),
        _ok_response,
    )

    assert first.status_code == 200
    assert second.status_code == 200
    events = read_usage_events(usage_environment)
    assert len(events) == 2
    assert events[0]["session_hash"] == events[1]["session_hash"]
    assert all(event["event_type"] == "page_view" for event in events)


def test_allowlisted_tab_event_is_recorded(usage_environment: Path) -> None:
    request = _request("/api/usage/event", method="POST")
    payload = FeatureEventRequest(event_name="tab:draft-report")

    async def endpoint(active_request: Request) -> Response:
        return await collect_feature_event(active_request, payload)

    response = _dispatch(request, endpoint)
    assert response.status_code == 204
    events = read_usage_events(usage_environment)
    assert [event["event_name"] for event in events] == ["tab:draft-report"]

    with pytest.raises(ValidationError, match="Unsupported feature event"):
        FeatureEventRequest(event_name="arbitrary:user-supplied-value")


def test_internal_and_bot_requests_are_retained_but_excluded(
    usage_environment: Path,
) -> None:
    internal_cookie = make_internal_cookie_value("test-internal-secret")
    _dispatch(
        _request("/", cookies={INTERNAL_COOKIE_NAME: internal_cookie}),
        _ok_response,
    )
    _dispatch(
        _request("/", user_agent="Googlebot/2.1"),
        _ok_response,
    )

    events = read_usage_events(usage_environment)
    assert [(event["excluded"], event["exclusion_reason"]) for event in events] == [
        (1, "internal"),
        (1, "bot"),
    ]


@pytest.mark.parametrize("file_format", ["pdf", "docx"])
def test_explicit_draft_download_records_version_and_format(
    usage_environment: Path,
    file_format: str,
) -> None:
    request = _request(f"/downloads/draft-paper/{file_format}")

    async def endpoint(active_request: Request) -> Response:
        return await download_draft_paper(active_request, file_format)

    response = _dispatch(request, endpoint)
    assert response.status_code == 200
    assert "attachment" in response.headers["content-disposition"]
    event = read_usage_events(usage_environment)[0]
    assert event["event_name"] == "draft-paper"
    assert event["document_version"] == "2026-11-13"
    assert event["file_format"] == file_format
    assert event["success"] == 1


def test_active_bulk_route_records_manifest_dimensions(
    usage_environment: Path,
) -> None:
    logger_frame = pd.DataFrame(
        {"timestamp": ["2026-01-01 00:00:00"], "VWC_1_raw_S1_T": [25.0]}
    )
    request = _request("/api/bulk_download", method="POST")

    async def endpoint(active_request: Request) -> Response:
        return await bulk_download(
            active_request,
            {"keys": ["loggers_2026_daily"], "unitSystem": "us"},
        )

    with (
        patch(
            "biochar_app.scripts.bulk_downloads._load_logger_download_df",
            return_value=logger_frame,
        ),
        patch(
            "biochar_app.scripts.bulk_downloads._logger_ratios_parquet_path",
            return_value=None,
        ),
        patch(
            "biochar_app.scripts.bulk_downloads.build_timeseries_yearly_readme",
            return_value="README",
        ),
    ):
        response = _dispatch(request, endpoint)

    assert response.status_code == 200
    event = read_usage_events(usage_environment)[0]
    assert event["event_name"] == "bulk-data"
    assert event["dataset"] == "loggers"
    assert event["dataset_year"] == 2026
    assert event["granularity"] == "daily"
    assert event["success"] == 1


def test_failed_bulk_request_is_recorded_separately(
    usage_environment: Path,
) -> None:
    request = _request("/api/bulk_download", method="POST")

    async def endpoint(active_request: Request) -> Response:
        try:
            return await bulk_download(
                active_request,
                {"keys": ["unknown_2026_daily"]},
            )
        except HTTPException as exc:
            return JSONResponse(
                {"detail": exc.detail},
                status_code=exc.status_code,
            )

    response = _dispatch(request, endpoint)
    assert response.status_code == 400
    event = read_usage_events(usage_environment)[0]
    assert event["event_name"] == "bulk-data"
    assert event["success"] == 0
    assert event["status_code"] == 400


def test_retained_direct_download_path_preserves_underscore_dataset(
    usage_environment: Path,
) -> None:
    _dispatch(_request("/bulk_download/soil_chem/2026"), _ok_response)

    event = read_usage_events(usage_environment)[0]
    assert event["dataset"] == "soil_chem"
    assert event["dataset_year"] == 2026
    assert event["granularity"] is None
