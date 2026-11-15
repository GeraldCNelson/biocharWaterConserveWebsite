"""Fail-closed HTTP Basic authentication for the private usage dashboard."""

from __future__ import annotations

import os
import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

ADMIN_USERNAME_ENV = "BIOCHAR_USAGE_ADMIN_USERNAME"
ADMIN_PASSWORD_ENV = "BIOCHAR_USAGE_ADMIN_PASSWORD"

basic_security = HTTPBasic(auto_error=False)


def dashboard_credentials_configured() -> bool:
    """Return whether both private-dashboard credentials are configured."""
    return bool(
        os.getenv(ADMIN_USERNAME_ENV, "").strip()
        and os.getenv(ADMIN_PASSWORD_ENV, "")
    )


def require_usage_admin(
    credentials: Annotated[
        HTTPBasicCredentials | None,
        Depends(basic_security),
    ] = None,
) -> str:
    """Authenticate one dashboard request, failing closed when unconfigured."""
    configured_username = os.getenv(ADMIN_USERNAME_ENV, "").strip()
    configured_password = os.getenv(ADMIN_PASSWORD_ENV, "")
    if not configured_username or not configured_password:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Usage dashboard authentication is not configured",
        )

    supplied_username = credentials.username if credentials else ""
    supplied_password = credentials.password if credentials else ""
    valid_username = secrets.compare_digest(
        supplied_username.encode("utf-8"),
        configured_username.encode("utf-8"),
    )
    valid_password = secrets.compare_digest(
        supplied_password.encode("utf-8"),
        configured_password.encode("utf-8"),
    )
    if not (valid_username and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usage dashboard credentials are required",
            headers={"WWW-Authenticate": 'Basic realm="Biochar Usage"'},
        )
    return configured_username
