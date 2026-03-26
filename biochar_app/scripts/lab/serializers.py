from __future__ import annotations

from dataclasses import asdict
from typing import Any, Optional

from biochar_app.config.lab_reference_models import VariableReferenceBundle


def serialize_reference_bundle(bundle: Optional[VariableReferenceBundle]) -> Optional[dict[str, Any]]:
    if bundle is None:
        return None
    return asdict(bundle)
