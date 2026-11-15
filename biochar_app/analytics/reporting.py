"""Mountain Time summaries for the private usage dashboard."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

from biochar_app.analytics.database import initialize_usage_db
from biochar_app.config.paths import USAGE_DB_PATH


def _date_value(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _connection(db_path: Path) -> sqlite3.Connection:
    initialize_usage_db(db_path)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _zero_daily_row(day: date) -> dict[str, int | str]:
    return {
        "date": day.isoformat(),
        "public_visits": 0,
        "page_views": 0,
        "feature_uses": 0,
        "paper_downloads": 0,
        "data_downloads": 0,
        "failed_downloads": 0,
        "excluded_events": 0,
    }


def daily_totals(
    start_date: date | str,
    end_date: date | str,
    *,
    db_path: Path = USAGE_DB_PATH,
) -> list[dict[str, int | str]]:
    """Return one row per Mountain Time date, including zero-activity dates."""
    start = _date_value(start_date)
    end = _date_value(end_date)
    if end < start:
        raise ValueError("End date must not precede start date")

    with _connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
                local_date_mt AS date,
                COUNT(DISTINCT CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'page_view'
                    THEN session_hash END
                ) AS public_visits,
                SUM(CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'page_view' THEN 1 ELSE 0 END
                ) AS page_views,
                SUM(CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'feature' THEN 1 ELSE 0 END
                ) AS feature_uses,
                SUM(CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'download'
                         AND document_version IS NOT NULL THEN 1 ELSE 0 END
                ) AS paper_downloads,
                SUM(CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'download'
                         AND dataset IS NOT NULL THEN 1 ELSE 0 END
                ) AS data_downloads,
                SUM(CASE
                    WHEN excluded = 0 AND success = 0
                         AND event_type = 'download' THEN 1 ELSE 0 END
                ) AS failed_downloads,
                SUM(CASE WHEN excluded = 1 THEN 1 ELSE 0 END) AS excluded_events
            FROM usage_events
            WHERE local_date_mt BETWEEN ? AND ?
            GROUP BY local_date_mt
            ORDER BY local_date_mt
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()

    by_date = {str(row["date"]): dict(row) for row in rows}
    result: list[dict[str, int | str]] = []
    current = start
    while current <= end:
        row = _zero_daily_row(current)
        stored = by_date.get(current.isoformat())
        if stored:
            row.update({key: int(value or 0) for key, value in stored.items() if key != "date"})
        result.append(row)
        current += timedelta(days=1)
    return result


def cumulative_totals(
    *,
    db_path: Path = USAGE_DB_PATH,
) -> dict[str, int]:
    """Return public cumulative counts since collection began."""
    with _connection(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(DISTINCT CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'page_view'
                    THEN session_hash END
                ) AS public_visits,
                SUM(CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'page_view' THEN 1 ELSE 0 END
                ) AS page_views,
                SUM(CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'feature' THEN 1 ELSE 0 END
                ) AS feature_uses,
                SUM(CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'download'
                         AND document_version IS NOT NULL THEN 1 ELSE 0 END
                ) AS paper_downloads,
                SUM(CASE
                    WHEN excluded = 0 AND success = 1
                         AND event_type = 'download'
                         AND dataset IS NOT NULL THEN 1 ELSE 0 END
                ) AS data_downloads,
                SUM(CASE
                    WHEN excluded = 0 AND success = 0
                         AND event_type = 'download' THEN 1 ELSE 0 END
                ) AS failed_downloads
            FROM usage_events
            """
        ).fetchone()
    assert row is not None
    return {key: int(value or 0) for key, value in dict(row).items()}


def download_breakdown(
    start_date: date | str,
    end_date: date | str,
    *,
    db_path: Path = USAGE_DB_PATH,
) -> list[dict[str, object]]:
    """Return successful public downloads by paper or dataset dimensions."""
    start = _date_value(start_date)
    end = _date_value(end_date)
    if end < start:
        raise ValueError("End date must not precede start date")

    with _connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
                CASE
                    WHEN document_version IS NOT NULL THEN 'paper'
                    ELSE 'data'
                END AS category,
                document_version,
                file_format,
                dataset,
                dataset_year,
                granularity,
                COUNT(*) AS downloads
            FROM usage_events
            WHERE local_date_mt BETWEEN ? AND ?
              AND excluded = 0
              AND success = 1
              AND event_type = 'download'
            GROUP BY
                category,
                document_version,
                file_format,
                dataset,
                dataset_year,
                granularity
            ORDER BY category, downloads DESC
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()
    return [dict(row) for row in rows]


def feature_breakdown(
    start_date: date | str,
    end_date: date | str,
    *,
    db_path: Path = USAGE_DB_PATH,
) -> list[dict[str, object]]:
    """Return successful public feature interactions by stable event name."""
    start = _date_value(start_date)
    end = _date_value(end_date)
    if end < start:
        raise ValueError("End date must not precede start date")

    with _connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT event_name, COUNT(*) AS uses
            FROM usage_events
            WHERE local_date_mt BETWEEN ? AND ?
              AND excluded = 0
              AND success = 1
              AND event_type = 'feature'
            GROUP BY event_name
            ORDER BY uses DESC, event_name
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()
    return [dict(row) for row in rows]


def exclusion_breakdown(
    start_date: date | str,
    end_date: date | str,
    *,
    db_path: Path = USAGE_DB_PATH,
) -> list[dict[str, object]]:
    """Return excluded event counts by transparent exclusion reason."""
    start = _date_value(start_date)
    end = _date_value(end_date)
    if end < start:
        raise ValueError("End date must not precede start date")

    with _connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT exclusion_reason, COUNT(*) AS events
            FROM usage_events
            WHERE local_date_mt BETWEEN ? AND ?
              AND excluded = 1
            GROUP BY exclusion_reason
            ORDER BY events DESC, exclusion_reason
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()
    return [dict(row) for row in rows]


def tracking_period(
    *,
    db_path: Path = USAGE_DB_PATH,
) -> dict[str, str | None]:
    """Return the first and latest Mountain Time dates stored."""
    with _connection(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                MIN(local_date_mt) AS first_date,
                MAX(local_date_mt) AS latest_date
            FROM usage_events
            """
        ).fetchone()
    assert row is not None
    return {
        "first_date": row["first_date"],
        "latest_date": row["latest_date"],
    }
