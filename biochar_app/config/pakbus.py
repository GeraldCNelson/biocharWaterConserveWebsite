"""
biochar_app.config.pakbus

All PakBus-related configuration in one place.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List
from zoneinfo import ZoneInfo

DEFAULT_TABLE = "Table1"
DEFAULT_HOURS = 1
DEFAULT_TIMEZONE = ZoneInfo(os.getenv("DEFAULT_TIMEZONE", "America/Denver"))
DEFAULT_LAG_MINUTES = 30  # delay before "now" to ensure data availability

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
    base_id: int
    logger_ids: List[int]

PAKBUS = PakbusConfig(
    host=os.getenv("PAKBUS_HOST", "2605:59ca:2202:7700:2d0:2cff:fe02:1ddd"),
    port=int(os.getenv("PAKBUS_PORT", 6785)),
    base_id=int(os.getenv("PAKBUS_BASE_ID", 4094)),
    logger_ids=list(range(2, 14)),
)
