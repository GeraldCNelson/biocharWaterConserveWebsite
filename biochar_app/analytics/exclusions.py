"""Transparent exclusion rules for internal users and obvious automated traffic."""

from __future__ import annotations

import re

BOT_USER_AGENT_PATTERN = re.compile(
    r"(?:bot\b|crawler|spider|slurp|headlesschrome|lighthouse|pagespeed|"
    r"uptime|monitoring|healthcheck|curl/|wget/|python-requests|httpx/)",
    flags=re.IGNORECASE,
)


def is_known_bot(user_agent: str | None) -> bool:
    """Return whether a user agent is clearly automated."""
    return bool(user_agent and BOT_USER_AGENT_PATTERN.search(user_agent))


def classify_exclusion(
    *,
    user_agent: str | None,
    is_internal_browser: bool,
) -> str | None:
    """Return the exclusion reason used by public reporting, if any."""
    if is_internal_browser:
        return "internal"
    if is_known_bot(user_agent):
        return "bot"
    return None
