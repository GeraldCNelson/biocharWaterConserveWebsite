"""Request-scoped anonymous usage collection for FastAPI."""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from fastapi import Request, Response

from biochar_app.analytics.database import UsageEvent, record_usage_event
from biochar_app.analytics.exclusions import classify_exclusion
from biochar_app.analytics.identifiers import hash_session_id, new_session_id
from biochar_app.config.paths import USAGE_DB_PATH

logger = logging.getLogger(__name__)

SESSION_COOKIE_NAME = "biochar_usage_session"
INTERNAL_COOKIE_NAME = "biochar_usage_internal"
SESSION_MAX_AGE_SECONDS = 30 * 60
SESSION_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{24,128}$")

SESSION_SECRET_ENV = "BIOCHAR_USAGE_SESSION_SECRET"
INTERNAL_SECRET_ENV = "BIOCHAR_USAGE_INTERNAL_SECRET"
DB_PATH_ENV = "BIOCHAR_USAGE_DB_PATH"
COOKIE_SECURE_ENV = "BIOCHAR_USAGE_COOKIE_SECURE"


@dataclass(frozen=True, slots=True)
class PendingUsageEvent:
    event_type: str
    event_name: str
    document_version: str | None = None
    file_format: str | None = None
    dataset: str | None = None
    dataset_year: int | None = None
    granularity: str | None = None


@dataclass(frozen=True, slots=True)
class RequestUsageContext:
    raw_session_id: str
    session_hash: str
    exclusion_reason: str | None


def usage_db_path() -> Path:
    """Return the runtime database path, allowing a test/deployment override."""
    configured = os.getenv(DB_PATH_ENV, "").strip()
    return Path(configured) if configured else USAGE_DB_PATH


def tracking_enabled() -> bool:
    """Collection is dormant until a deployment-specific secret is configured."""
    return bool(os.getenv(SESSION_SECRET_ENV, "").strip())


def make_internal_cookie_value(secret: str) -> str:
    """Create the signed marker later set by the private dashboard."""
    if not secret:
        raise ValueError("Internal-browser signing secret must not be blank")
    signature = hmac.new(
        secret.encode("utf-8"),
        b"biochar-internal-browser-v1",
        hashlib.sha256,
    ).hexdigest()
    return f"v1.{signature}"


def is_internal_browser(request: Request) -> bool:
    """Validate the internal-browser marker without retaining an identity."""
    secret = os.getenv(INTERNAL_SECRET_ENV, "").strip()
    supplied = request.cookies.get(INTERNAL_COOKIE_NAME, "")
    if not secret or not supplied:
        return False
    expected = make_internal_cookie_value(secret)
    return hmac.compare_digest(supplied, expected)


def request_usage_context(request: Request) -> RequestUsageContext | None:
    """Build or reuse the anonymous context for one tracked request."""
    cached = getattr(request.state, "usage_context", None)
    if isinstance(cached, RequestUsageContext):
        return cached

    secret = os.getenv(SESSION_SECRET_ENV, "").strip()
    if not secret:
        return None

    supplied = request.cookies.get(SESSION_COOKIE_NAME, "")
    raw_session_id = (
        supplied
        if SESSION_TOKEN_PATTERN.fullmatch(supplied)
        else new_session_id()
    )
    exclusion_reason = classify_exclusion(
        user_agent=request.headers.get("user-agent"),
        is_internal_browser=is_internal_browser(request),
    )
    context = RequestUsageContext(
        raw_session_id=raw_session_id,
        session_hash=hash_session_id(raw_session_id, secret=secret),
        exclusion_reason=exclusion_reason,
    )
    request.state.usage_context = context
    return context


def mark_usage_event(request: Request, event: PendingUsageEvent) -> None:
    """Attach event metadata for recording after the response is prepared."""
    request.state.pending_usage_event = event


def pending_usage_event(request: Request) -> PendingUsageEvent | None:
    event = getattr(request.state, "pending_usage_event", None)
    return event if isinstance(event, PendingUsageEvent) else None


def record_request_event(
    request: Request,
    event: PendingUsageEvent,
    *,
    status_code: int,
) -> RequestUsageContext | None:
    """Persist one response outcome without allowing analytics to break the site."""
    context = request_usage_context(request)
    if context is None:
        return None

    try:
        record_usage_event(
            UsageEvent(
                event_type=event.event_type,
                event_name=event.event_name,
                session_hash=context.session_hash,
                excluded=context.exclusion_reason is not None,
                exclusion_reason=context.exclusion_reason,
                success=200 <= status_code < 400,
                status_code=status_code,
                route=request.url.path,
                document_version=event.document_version,
                file_format=event.file_format,
                dataset=event.dataset,
                dataset_year=event.dataset_year,
                granularity=event.granularity,
            ),
            db_path=usage_db_path(),
        )
    except (OSError, ValueError, sqlite3.Error) as exc:
        logger.warning("Usage event could not be stored: %s", exc)
    return context


def cookie_is_secure(request: Request) -> bool:
    """Return whether analytics cookies should carry the Secure attribute."""
    configured = os.getenv(COOKIE_SECURE_ENV, "").strip().lower()
    if configured:
        return configured in {"1", "true", "yes", "on"}
    forwarded = request.headers.get("x-forwarded-proto", "").split(",", 1)[0]
    return request.url.scheme == "https" or forwarded.strip().lower() == "https"


def refresh_session_cookie(
    response: Response,
    request: Request,
    context: RequestUsageContext | None,
) -> None:
    """Refresh the short-lived anonymous session after a tracked response."""
    if context is None:
        return
    response.set_cookie(
        SESSION_COOKIE_NAME,
        context.raw_session_id,
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        secure=cookie_is_secure(request),
        samesite="lax",
        path="/",
    )
