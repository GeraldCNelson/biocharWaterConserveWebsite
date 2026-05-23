from __future__ import annotations

from biochar_app.config.lab_variable_metadata import (
    get_display_label,
    get_lab_variable_metadata,
)


def metadata_label(key: str, fallback: str | None = None) -> str:
    label = get_display_label(key)
    return label if label != key else (fallback or key)


def metadata_note(key: str, fallback: str | None = None) -> str:
    meta = get_lab_variable_metadata(key)
    return str(
        meta.get("interpretation_note")
        or meta.get("definition")
        or fallback
        or ""
    )