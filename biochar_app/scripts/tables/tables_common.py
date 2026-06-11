#!/usr/bin/env python3
"""
tables_common.py

Shared helpers for constructing dashboard table payloads.

This module is intentionally UI/payload focused:
- It does NOT read CSVs.
- It does NOT normalize or clean data.
- It just standardizes the payload envelope and per-set metadata.

This keeps table "shape" consistent across tabs and prevents drift.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence
from biochar_app.scripts.lab.reference_helpers import get_reference_for_varspec
from biochar_app.scripts.lab.serializers import serialize_reference_bundle

# A build function that returns the standard set payload dict
SetPayloadBuilder = Callable[[], Dict[str, Any]]


def _dedupe_note(group_note: str, top_note: str) -> str:
    """
    Return group_note unless it is empty or matches top_note (after strip).
    """
    gn = (group_note or "").strip()
    tn = (top_note or "").strip()
    if not gn:
        return ""
    if gn == tn:
        return ""
    return gn


def make_set(
    *,
    key: str,
    label: str,
    payload: Dict[str, Any],
    top_note: str = "",
    group_note: str = "",
    display_label: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Standardize a single set object.

    - Adds key/label
    - Adds display_label if provided
    - Adds note/notes only if group_note is non-empty and differs from top_note
    - Merges in payload last (payload wins if there’s overlap, but avoid overlap)
    """
    out: Dict[str, Any] = {
        "key": key,
        "label": label,
    }
    if display_label:
        out["display_label"] = display_label

    gn = _dedupe_note(group_note=group_note, top_note=top_note)
    if gn:
        # Some existing frontend code checks either `note` or `notes`.
        out["note"] = gn
        out["notes"] = gn

    out.update(payload)
    return out


def build_grouped_tab_payload(
    *,
    title: str,
    top_note: str,
    groups: Sequence[Dict[str, Any]],
    build_payload_for_group: Callable[[Dict[str, Any]], Dict[str, Any]],
    include_display_labels: bool = False,
) -> Dict[str, Any]:
    """
    Build a tab payload with a shared top-level note and grouped sets.

    `groups` must contain:
      - group_key
      - group_label
      - optional notes
      - whatever else build_payload_for_group needs (e.g. variables)

    `build_payload_for_group(group)` should return the standard payload dict for that group,
    e.g. build_soil_table_payload(...).

    Returns:
      {"title": ..., "note": ..., "sets": [...]}
    """
    sets: List[Dict[str, Any]] = []

    for i, grp in enumerate(groups, start=1):
        payload = build_payload_for_group(grp)
        label = str(grp["group_label"])
        display = f"Set {i}: {label}" if include_display_labels else None

        sets.append(
            make_set(
                key=str(grp["group_key"]),
                label=label,
                display_label=display,
                group_note=str(grp.get("notes", "") or ""),
                top_note=top_note,
                payload=payload,
            )
        )

    return {
        "title": title,
        "note": top_note,
        "sets": sets,
    }


def build_variable_meta(var_spec: Any) -> Dict[str, Any]:
    """
    Build a standard variable metadata payload, including optional Ward reference info.

    This does not include any values. It only describes the variable for the frontend:
    - key
    - label
    - short note
    - reference key
    - whether a reference exists
    - serialized reference bundle (if present)

    `var_spec` is intentionally typed broadly because this helper is used by
    multiple table builders whose variable-spec classes are structurally similar
    but not always the same concrete type.
    """
    bundle = get_reference_for_varspec(var_spec)

    return {
        "key": var_spec.key,
        "label": var_spec.label,
        "note": bundle.short_note if bundle else "",
        "reference_key": getattr(var_spec, "reference_key", None),
        "has_reference": bundle is not None,
        "reference": serialize_reference_bundle(bundle),
    }