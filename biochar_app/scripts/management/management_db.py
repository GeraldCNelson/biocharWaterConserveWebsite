from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Any, List, Dict

# Adjust this import if your paths module differs
from biochar_app.config.paths import DATA_PROCESSED_DIR

# ---------------------------------------------------------------------
# Database location
# ---------------------------------------------------------------------

DB_PATH = DATA_PROCESSED_DIR / "management" / "management_entries.sqlite"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def utc_now_iso() -> str:
    """Return current UTC time in ISO format."""
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_connection() -> sqlite3.Connection:
    """Get SQLite connection (creates directory if needed)."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------

def initialize_management_db() -> None:
    """Create tables if they do not exist."""
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS irrigation_events (
                event_id TEXT PRIMARY KEY,
                year INTEGER NOT NULL,
                date TEXT NOT NULL,
                strip_group TEXT NOT NULL,
                location TEXT NOT NULL,

                start_timestamp TEXT,
                end_timestamp TEXT,

                start_totalizer_gal_x100 REAL,
                end_totalizer_gal_x100 REAL,

                gallons REAL,  -- total meter gallons
                flow_allocation_fraction REAL DEFAULT 1.0,
                allocated_gallons REAL,  -- gallons assigned to this strip group

                start_flow_gpm REAL,
                end_flow_gpm REAL,
                avg_flow_gpm REAL,

                start_photo TEXT,
                end_photo TEXT,

                entered_by TEXT,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'in_progress',

                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


# ---------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------

def insert_irrigation_event(row: Dict[str, Any]) -> None:
    """Insert a new irrigation event."""
    now = utc_now_iso()

    row = {
        **row,
        "created_at": now,
        "updated_at": now,
    }

    cols = list(row.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_sql = ", ".join(cols)

    with get_connection() as conn:
        conn.execute(
            f"INSERT INTO irrigation_events ({col_sql}) VALUES ({placeholders})",
            [row[c] for c in cols],
        )
        conn.commit()


def update_irrigation_event(event_id: str, updates: Dict[str, Any]) -> None:
    """Update an existing irrigation event."""
    if not updates:
        return

    updates = {
        **updates,
        "updated_at": utc_now_iso(),
    }

    set_sql = ", ".join([f"{k} = ?" for k in updates.keys()])

    with get_connection() as conn:
        conn.execute(
            f"UPDATE irrigation_events SET {set_sql} WHERE event_id = ?",
            [*updates.values(), event_id],
        )
        conn.commit()


def get_irrigation_event(event_id: str) -> Dict[str, Any] | None:
    """Fetch a single irrigation event."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM irrigation_events WHERE event_id = ?",
            [event_id],
        ).fetchone()

    return dict(row) if row else None


def list_irrigation_events(limit: int = 100) -> List[Dict[str, Any]]:
    """List recent irrigation events."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM irrigation_events
            ORDER BY COALESCE(start_timestamp, created_at) DESC LIMIT ?
            """,
            [limit],
        ).fetchall()

    return [dict(r) for r in rows]


def delete_irrigation_event(event_id: str) -> None:
    """Delete an irrigation event."""
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM irrigation_events WHERE event_id = ?",
            [event_id],
        )
        conn.commit()
