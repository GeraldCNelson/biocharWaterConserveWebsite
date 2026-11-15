"""Authentication, reporting, CSV, and internal-browser dashboard tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from http.cookies import SimpleCookie
from pathlib import Path

import pytest
from fastapi import HTTPException, Request
from fastapi.security import HTTPBasicCredentials

from biochar_app.analytics.admin_routes import (
    export_usage_csv,
    mark_internal_browser,
    usage_summary,
)
from biochar_app.analytics.auth import (
    ADMIN_PASSWORD_ENV,
    ADMIN_USERNAME_ENV,
    require_usage_admin,
)
from biochar_app.analytics.collector import (
    DB_PATH_ENV,
    INTERNAL_COOKIE_NAME,
    INTERNAL_SECRET_ENV,
    SESSION_SECRET_ENV,
    is_internal_browser,
)
from biochar_app.analytics.database import UsageEvent, record_usage_event


def _request(
    *,
    cookies: dict[str, str] | None = None,
    admin_action: bool = False,
) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if cookies:
        value = "; ".join(f"{key}={item}" for key, item in cookies.items())
        headers.append((b"cookie", value.encode("latin-1")))
    if admin_action:
        headers.append((b"x-biochar-usage-admin", b"1"))
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/admin/usage",
            "raw_path": b"/admin/usage",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
    )


@pytest.fixture
def admin_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    db_path = tmp_path / "usage.sqlite3"
    monkeypatch.setenv(DB_PATH_ENV, str(db_path))
    monkeypatch.setenv(SESSION_SECRET_ENV, "session-secret")
    monkeypatch.setenv(INTERNAL_SECRET_ENV, "internal-secret")
    monkeypatch.setenv(ADMIN_USERNAME_ENV, "reviewer")
    monkeypatch.setenv(ADMIN_PASSWORD_ENV, "review-password")
    return db_path


def test_dashboard_authentication_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ADMIN_USERNAME_ENV, raising=False)
    monkeypatch.delenv(ADMIN_PASSWORD_ENV, raising=False)
    with pytest.raises(HTTPException) as unconfigured:
        require_usage_admin(None)
    assert unconfigured.value.status_code == 503

    monkeypatch.setenv(ADMIN_USERNAME_ENV, "reviewer")
    monkeypatch.setenv(ADMIN_PASSWORD_ENV, "correct-password")
    with pytest.raises(HTTPException) as rejected:
        require_usage_admin(
            HTTPBasicCredentials(username="reviewer", password="wrong")
        )
    assert rejected.value.status_code == 401
    assert rejected.value.headers == {
        "WWW-Authenticate": 'Basic realm="Biochar Usage"'
    }
    assert require_usage_admin(
        HTTPBasicCredentials(username="reviewer", password="correct-password")
    ) == "reviewer"


def test_summary_and_csv_use_mountain_dates(admin_environment: Path) -> None:
    occurred_at = datetime(2026, 11, 15, 16, 0, tzinfo=UTC)
    record_usage_event(
        UsageEvent(
            event_type="page_view",
            event_name="home",
            occurred_at=occurred_at,
            session_hash="public-session",
        ),
        db_path=admin_environment,
    )
    record_usage_event(
        UsageEvent(
            event_type="download",
            event_name="bulk-data",
            occurred_at=occurred_at,
            session_hash="public-session",
            dataset="weather",
            dataset_year=2026,
            granularity="daily",
        ),
        db_path=admin_environment,
    )

    summary = asyncio.run(
        usage_summary(
            _request(),
            "reviewer",
            end_date=date(2026, 11, 15),
            days=7,
        )
    )
    payload = bytes(summary.body).decode("utf-8")
    assert '"reporting_timezone":"America/Denver"' in payload
    assert '"public_visits":1' in payload
    assert '"dataset":"weather"' in payload
    assert summary.headers["cache-control"] == "no-store"

    exported = asyncio.run(
        export_usage_csv(
            "reviewer",
            start_date=date(2026, 11, 15),
            end_date=date(2026, 11, 15),
        )
    )
    csv_text = bytes(exported.body).decode("utf-8")
    assert csv_text.startswith("reporting_timezone,date,public_visits")
    assert "America/Denver,2026-11-15,1,1,0,0,1,0,0" in csv_text
    assert "attachment;" in exported.headers["content-disposition"]


def test_internal_browser_registration_sets_signed_cookie(
    admin_environment: Path,
) -> None:
    del admin_environment
    with pytest.raises(HTTPException) as cross_site:
        asyncio.run(mark_internal_browser(_request(), "reviewer"))
    assert cross_site.value.status_code == 403

    response = asyncio.run(
        mark_internal_browser(_request(admin_action=True), "reviewer")
    )
    parsed = SimpleCookie()
    parsed.load(response.headers["set-cookie"])
    marker = parsed[INTERNAL_COOKIE_NAME].value

    assert marker.startswith("v1.")
    assert parsed[INTERNAL_COOKIE_NAME]["httponly"]
    assert parsed[INTERNAL_COOKIE_NAME]["samesite"] == "lax"
    assert is_internal_browser(
        _request(cookies={INTERNAL_COOKIE_NAME: marker})
    )


def test_dashboard_template_contains_required_review_sections() -> None:
    template = (
        Path(__file__).resolve().parents[1]
        / "templates"
        / "usage_dashboard.html"
    ).read_text(encoding="utf-8")
    assert "Selected and previous day" in template
    assert "Cumulative public totals" in template
    assert 'id="cumulative-period-label"' in template
    assert "Draft-paper downloads" in template
    assert "Bulk research-data downloads" in template
    assert "Mark this browser internal" in template
    assert "Export daily CSV" in template

    script = (
        Path(__file__).resolve().parents[1]
        / "static"
        / "js"
        / "usage_dashboard.js"
    ).read_text(encoding="utf-8")
    assert "Since tracking began · ${period.first_date}" in script
