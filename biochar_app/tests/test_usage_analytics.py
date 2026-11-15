"""Focused tests for anonymous usage storage and Mountain Time reporting."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from biochar_app.analytics.database import (
    UsageEvent,
    initialize_usage_db,
    read_usage_events,
    record_usage_event,
)
from biochar_app.analytics.exclusions import classify_exclusion, is_known_bot
from biochar_app.analytics.identifiers import hash_session_id
from biochar_app.analytics.reporting import (
    cumulative_totals,
    daily_totals,
    download_breakdown,
    feature_breakdown,
)


def _record(db_path: Path, **values: object) -> None:
    record_usage_event(UsageEvent(**values), db_path=db_path)  # type: ignore[arg-type]


def test_mountain_date_handles_dst_boundary(tmp_path: Path) -> None:
    db_path = tmp_path / "usage.sqlite3"
    initialize_usage_db(db_path)

    _record(
        db_path,
        event_type="page_view",
        event_name="home",
        session_hash="before-midnight",
        occurred_at=datetime(2026, 11, 1, 5, 30, tzinfo=UTC),
    )
    _record(
        db_path,
        event_type="page_view",
        event_name="home",
        session_hash="after-midnight",
        occurred_at=datetime(2026, 11, 1, 7, 30, tzinfo=UTC),
    )

    events = read_usage_events(db_path)
    assert [event["local_date_mt"] for event in events] == [
        "2026-10-31",
        "2026-11-01",
    ]


def test_daily_and_cumulative_totals_exclude_internal_and_bots(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "usage.sqlite3"
    timestamp = datetime(2026, 11, 15, 16, 0, tzinfo=UTC)
    public = {"occurred_at": timestamp, "session_hash": "public-session"}

    _record(
        db_path,
        event_type="page_view",
        event_name="home",
        **public,
    )
    _record(
        db_path,
        event_type="page_view",
        event_name="home",
        **public,
    )
    _record(
        db_path,
        event_type="feature",
        event_name="tab:summary",
        **public,
    )
    _record(
        db_path,
        event_type="page_view",
        event_name="home",
        occurred_at=timestamp,
        session_hash="internal-session",
        excluded=True,
        exclusion_reason="internal",
    )
    _record(
        db_path,
        event_type="feature",
        event_name="tab:bulk-downloads",
        occurred_at=timestamp,
        session_hash="bot-session",
        excluded=True,
        exclusion_reason="bot",
    )
    _record(
        db_path,
        event_type="download",
        event_name="draft-paper",
        document_version="2026-11-13",
        file_format="pdf",
        **public,
    )
    _record(
        db_path,
        event_type="download",
        event_name="bulk-data",
        dataset="loggers",
        dataset_year=2026,
        granularity="daily",
        **public,
    )
    _record(
        db_path,
        event_type="download",
        event_name="bulk-data",
        dataset="weather",
        dataset_year=2026,
        granularity="hourly",
        success=False,
        status_code=500,
        **public,
    )

    rows = daily_totals("2026-11-14", "2026-11-16", db_path=db_path)
    assert rows[0]["public_visits"] == 0
    day = rows[1]
    assert day == {
        "date": "2026-11-15",
        "public_visits": 1,
        "page_views": 2,
        "feature_uses": 1,
        "paper_downloads": 1,
        "data_downloads": 1,
        "failed_downloads": 1,
        "excluded_events": 2,
    }
    assert cumulative_totals(db_path=db_path) == {
        "public_visits": 1,
        "page_views": 2,
        "feature_uses": 1,
        "paper_downloads": 1,
        "data_downloads": 1,
        "failed_downloads": 1,
    }


def test_dimension_breakdowns_preserve_download_details(tmp_path: Path) -> None:
    db_path = tmp_path / "usage.sqlite3"
    timestamp = datetime(2026, 11, 15, 18, 0, tzinfo=UTC)

    _record(
        db_path,
        event_type="download",
        event_name="draft-paper",
        document_version="2026-11-13",
        file_format="docx",
        occurred_at=timestamp,
        session_hash="session-1",
    )
    _record(
        db_path,
        event_type="download",
        event_name="bulk-data",
        dataset="weather",
        dataset_year=2025,
        granularity="monthly",
        occurred_at=timestamp,
        session_hash="session-1",
    )
    _record(
        db_path,
        event_type="feature",
        event_name="tab:draft-report",
        occurred_at=timestamp,
        session_hash="session-1",
    )

    downloads = download_breakdown(
        "2026-11-15", "2026-11-15", db_path=db_path
    )
    assert downloads == [
        {
            "category": "data",
            "document_version": None,
            "file_format": None,
            "dataset": "weather",
            "dataset_year": 2025,
            "granularity": "monthly",
            "downloads": 1,
        },
        {
            "category": "paper",
            "document_version": "2026-11-13",
            "file_format": "docx",
            "dataset": None,
            "dataset_year": None,
            "granularity": None,
            "downloads": 1,
        },
    ]
    assert feature_breakdown(
        "2026-11-15", "2026-11-15", db_path=db_path
    ) == [{"event_name": "tab:draft-report", "uses": 1}]


def test_exclusion_rules_are_explicit_and_internal_takes_precedence() -> None:
    assert is_known_bot("Mozilla/5.0 (compatible; Googlebot/2.1)")
    assert is_known_bot("curl/8.7.1")
    assert not is_known_bot("Mozilla/5.0 Safari/605.1.15")
    assert classify_exclusion(
        user_agent="Googlebot/2.1",
        is_internal_browser=True,
    ) == "internal"
    assert classify_exclusion(
        user_agent="Googlebot/2.1",
        is_internal_browser=False,
    ) == "bot"


def test_session_hash_is_stable_without_storing_the_raw_token() -> None:
    first = hash_session_id("browser-token", secret="deployment-secret")
    second = hash_session_id("browser-token", secret="deployment-secret")
    other = hash_session_id("browser-token", secret="different-secret")

    assert first == second
    assert first != other
    assert "browser-token" not in first


def test_invalid_events_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timestamps must include a timezone"):
        _record(
            tmp_path / "usage.sqlite3",
            event_type="page_view",
            event_name="home",
            occurred_at=datetime(2026, 11, 15, 12, 0),
        )

    with pytest.raises(ValueError, match="require an exclusion reason"):
        _record(
            tmp_path / "usage.sqlite3",
            event_type="feature",
            event_name="tab:summary",
            excluded=True,
        )
