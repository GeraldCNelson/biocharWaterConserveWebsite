"""
biochar_app.config.pakbus

All PakBus-related configuration in one place.
"""

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List
from zoneinfo import ZoneInfo

DEFAULT_SETTINGS_PATH = Path(__file__).with_name("pakbus_settings.json")
SETTINGS_PATH = Path(os.getenv("BIOCHAR_PAKBUS_CONFIG", DEFAULT_SETTINGS_PATH))
SETTINGS = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
DOWNLOAD_SETTINGS = SETTINGS["download"]
DAILY_SETTINGS = SETTINGS["daily"]
ARCHIVE_SETTINGS = SETTINGS["archive"]

DEFAULT_TABLE = str(DOWNLOAD_SETTINGS["table"])
DEFAULT_HOURS = int(DOWNLOAD_SETTINGS["hours"])
DEFAULT_TIMEZONE = ZoneInfo(os.getenv("DEFAULT_TIMEZONE", str(DOWNLOAD_SETTINGS["timezone"])))
DEFAULT_LAG_MINUTES = int(DOWNLOAD_SETTINGS["lag_minutes"])
DEFAULT_STATION_ATTEMPTS = int(DOWNLOAD_SETTINGS["attempts"])
DEFAULT_RETRY_DELAY_SECONDS = float(DOWNLOAD_SETTINGS["retry_delay_seconds"])
DEFAULT_STATION_PAUSE_SECONDS = float(DOWNLOAD_SETTINGS["station_pause_seconds"])

# Map PakBus numeric IDs to station names and back
STATION_BY_ID: Dict[int, str] = {
    1: "CR800",
    2: "S1T",
    3: "S1M",
    4: "S1B",
    5: "S2T",
    6: "S2M",
    7: "S2B",
    8: "S3T",
    9: "S3M",
    10: "S3B",
    11: "S4T",
    12: "S4M",
    13: "S4B",
}

ID_BY_STATION: Dict[str, int] = {v: k for k, v in STATION_BY_ID.items()}

def parse_ids(s: str) -> List[int]:
    """
    Supports '2-13' or '2,3,5-7'. Defaults to an empty list on bad input.
    """
    out: list[int] = []
    try:
        for part in s.replace(" ", "").split(","):
            if not part:
                continue
            if "-" in part:
                a, b = part.split("-", 1)
                out.extend(range(int(a), int(b) + 1))
            else:
                out.append(int(part))
    except Exception:
        return []
    return out

@dataclass(frozen=True)
class PakbusConfig:
    host: str
    port: int
    router_id: int
    base_id: int
    logger_ids: List[int]
    response_timeout_seconds: float

connection = SETTINGS["connection"]
PAKBUS = PakbusConfig(
    host=os.getenv("PAKBUS_HOST", str(connection["host"])),
    port=int(os.getenv("PAKBUS_PORT", connection["port"])),
    router_id=int(os.getenv("PAKBUS_ROUTER_ID", connection["router_id"])),
    # PC400 captures from this installation use 0xFFD (4093) as the
    # client/source PakBus address.
    base_id=int(os.getenv("PAKBUS_BASE_ID", connection["base_id"])),
    logger_ids=[int(value) for value in connection["logger_ids"]],
    response_timeout_seconds=float(
        os.getenv("PAKBUS_RESPONSE_TIMEOUT_SECONDS", connection["response_timeout_seconds"])
    ),
)
