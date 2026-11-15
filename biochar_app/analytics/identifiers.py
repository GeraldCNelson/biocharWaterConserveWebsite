"""Anonymous session identifiers used by the usage collector."""

from __future__ import annotations

import hashlib
import hmac
import secrets


def new_session_id() -> str:
    """Return a cryptographically random browser-session token."""
    return secrets.token_urlsafe(24)


def hash_session_id(session_id: str, *, secret: str) -> str:
    """Return a one-way, deployment-specific session identifier."""
    if not session_id:
        raise ValueError("Session identifier must not be blank")
    if not secret:
        raise ValueError("Session hash secret must not be blank")
    return hmac.new(
        secret.encode("utf-8"),
        session_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
