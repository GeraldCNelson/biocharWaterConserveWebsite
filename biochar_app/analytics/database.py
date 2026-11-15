"""SQLite storage for anonymous website-usage events.

The schema intentionally excludes IP addresses, raw user-agent strings, email
addresses, and authenticated identities. A session hash is the only visitor-
level value retained, and it is used solely to count visits without counting
every page refresh as a new visitor session.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from biochar_app.config.paths import USAGE_DB_PATH

MOUNTAIN_TIME = ZoneInfo("America/Denver")
VALID_EVENT_TYPES = frozenset({"page_view", "feature", "download"})


@dataclass(frozen=True, slots=True)
class UsageEvent:
    """One anonymous usage event ready for durable storage."""

    event_type: str
    event_name: str
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    session_hash: str | None = None
    excluded: bool = False
    exclusion_reason: str | None = None
    success: bool = True
    status_code: int | None = None
    route: str | None = None
    document_version: str | None = None
    file_format: str | None = None
    dataset: str | None = None
    dataset_year: int | None = None
    granularity: str | None = None


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA journal_mode = WAL")
    return connection


def initialize_usage_db(db_path: Path = USAGE_DB_PATH) -> None:
    """Create the usage-event schema and reporting indexes when absent."""
    with _connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS usage_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                occurred_at_utc TEXT NOT NULL,
                local_date_mt TEXT NOT NULL,
                event_type TEXT NOT NULL
                    CHECK (event_type IN ('page_view', 'feature', 'download')),
                event_name TEXT NOT NULL,
                session_hash TEXT,
                excluded INTEGER NOT NULL DEFAULT 0
                    CHECK (excluded IN (0, 1)),
                exclusion_reason TEXT,
                success INTEGER NOT NULL DEFAULT 1
                    CHECK (success IN (0, 1)),
                status_code INTEGER,
                route TEXT,
                document_version TEXT,
                file_format TEXT,
                dataset TEXT,
                dataset_year INTEGER,
                granularity TEXT,
                CHECK (
                    (excluded = 0 AND exclusion_reason IS NULL)
                    OR (excluded = 1 AND exclusion_reason IS NOT NULL)
                )
            );

            CREATE INDEX IF NOT EXISTS idx_usage_events_local_date
                ON usage_events (local_date_mt);
            CREATE INDEX IF NOT EXISTS idx_usage_events_public_type_date
                ON usage_events (excluded, event_type, local_date_mt);
            CREATE INDEX IF NOT EXISTS idx_usage_events_download_dimensions
                ON usage_events (
                    event_type,
                    dataset,
                    dataset_year,
                    granularity,
                    document_version,
                    file_format
                );
            """
        )


def _validate_event(event: UsageEvent) -> None:
    if event.event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"Unsupported usage event type: {event.event_type!r}")
    if not event.event_name.strip():
        raise ValueError("Usage event name must not be blank")
    if event.occurred_at.tzinfo is None or event.occurred_at.utcoffset() is None:
        raise ValueError("Usage event timestamps must include a timezone")
    if event.excluded and not event.exclusion_reason:
        raise ValueError("Excluded usage events require an exclusion reason")
    if not event.excluded and event.exclusion_reason is not None:
        raise ValueError("Public usage events cannot have an exclusion reason")


def record_usage_event(
    event: UsageEvent,
    db_path: Path = USAGE_DB_PATH,
) -> int:
    """Insert one event and return its database identifier."""
    _validate_event(event)
    initialize_usage_db(db_path)

    occurred_at_utc = event.occurred_at.astimezone(UTC)
    local_date_mt = event.occurred_at.astimezone(MOUNTAIN_TIME).date().isoformat()
    timestamp_text = occurred_at_utc.isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )

    with _connect(db_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO usage_events (
                occurred_at_utc,
                local_date_mt,
                event_type,
                event_name,
                session_hash,
                excluded,
                exclusion_reason,
                success,
                status_code,
                route,
                document_version,
                file_format,
                dataset,
                dataset_year,
                granularity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp_text,
                local_date_mt,
                event.event_type,
                event.event_name.strip(),
                event.session_hash,
                int(event.excluded),
                event.exclusion_reason,
                int(event.success),
                event.status_code,
                event.route,
                event.document_version,
                event.file_format,
                event.dataset,
                event.dataset_year,
                event.granularity,
            ),
        )
        return int(cursor.lastrowid)


def read_usage_events(db_path: Path = USAGE_DB_PATH) -> list[dict[str, object]]:
    """Return stored events for diagnostics and focused tests."""
    initialize_usage_db(db_path)
    with _connect(db_path) as connection:
        rows = connection.execute(
            "SELECT * FROM usage_events ORDER BY id"
        ).fetchall()
    return [dict(row) for row in rows]
