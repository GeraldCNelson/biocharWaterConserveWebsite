"""Compatibility entry point used by the production Gunicorn service."""

from biochar_app.scripts.app import app

__all__ = ["app"]
