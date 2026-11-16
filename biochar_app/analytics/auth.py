"""Fail-closed HTTP Basic authentication for the private usage dashboard."""

from __future__ import annotations

import os
import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

ADMIN_USERNAME_ENV = "BIOCHAR_USAGE_ADMIN_USERNAME"
ADMIN_PASSWORD_ENV = "BIOCHAR_USAGE_ADMIN_PASSWORD"

basic_security = HTTPBasic(auto_error=False)


class UsageAdminAuthError(HTTPException):
    """Authentication failure handled specially for the dashboard page."""


async def usage_admin_auth_error_response(
    request: Request,
    exc: UsageAdminAuthError,
) -> HTMLResponse | JSONResponse:
    """Show browser users a retry action while preserving JSON API errors."""
    headers = exc.headers or {}
    if request.url.path == "/admin/usage" and exc.status_code == 401:
        retry_token = secrets.token_urlsafe(8)
        return HTMLResponse(
            content=f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex,nofollow">
  <title>Biochar Research — Sign in</title>
  <style>
    body {{ margin: 0; background: #edf2ef; color: #17332b;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    main {{ width: min(560px, calc(100% - 32px)); margin: 12vh auto; padding: 28px;
      box-sizing: border-box; border: 1px solid #dce4df; border-radius: 14px;
      background: #fff; box-shadow: 0 4px 18px rgba(27, 56, 46, .06); }}
    h1 {{ margin-top: 0; font-size: 1.6rem; }}
    p {{ color: #617069; line-height: 1.5; }}
    a {{ display: inline-block; margin-top: 8px; padding: 10px 16px;
      border-radius: 8px; background: #246b54; color: #fff; font-weight: 650;
      text-decoration: none; }}
  </style>
</head>
<body>
<main>
  <h1>Private usage dashboard</h1>
  <p>The sign-in prompt was cancelled or the credentials were incomplete.</p>
  <a href="/admin/usage?retry={retry_token}">Try sign in again</a>
</main>
</body>
</html>""",
            status_code=exc.status_code,
            headers=headers,
        )
    return JSONResponse(
        {"detail": exc.detail},
        status_code=exc.status_code,
        headers=headers,
    )


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
        raise UsageAdminAuthError(
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
        raise UsageAdminAuthError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usage dashboard credentials are required",
            headers={"WWW-Authenticate": 'Basic realm="Biochar Usage"'},
        )
    return configured_username
