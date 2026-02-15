# biochar_app/utils/errors.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class UserFacingError(Exception):
    """
    An exception meant to be shown to the user (safe message), with an HTTP status.
    Keep this pure-Python (no Flask import) so utils/plotting code stays reusable.
    """
    message: str
    status_code: int = 400
    details: Optional[dict[str, Any]] = None

    def __str__(self) -> str:
        return self.message