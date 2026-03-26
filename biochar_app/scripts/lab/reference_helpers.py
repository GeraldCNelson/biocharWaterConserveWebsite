from __future__ import annotations

from typing import Optional

from biochar_app.config.lab_reference_data import LAB_REFERENCES
from biochar_app.config.lab_reference_models import (
    InterpretationBand,
    InterpretationInfo,
    VariableReferenceBundle,
)
from biochar_app.config.lab_specs import LabVarSpec


# ---------------------------------------------------------------------
# Core lookup
# ---------------------------------------------------------------------

def get_reference_bundle(reference_key: Optional[str]) -> Optional[VariableReferenceBundle]:
    if not reference_key:
        return None
    return LAB_REFERENCES.get(reference_key)


def get_reference_for_varspec(var_spec: LabVarSpec) -> Optional[VariableReferenceBundle]:
    return get_reference_bundle(var_spec.reference_key)


def has_reference(var_spec: LabVarSpec) -> bool:
    return var_spec.reference_key in LAB_REFERENCES if var_spec.reference_key else False


# ---------------------------------------------------------------------
# Threshold logic
# ---------------------------------------------------------------------

def get_matching_band(
    value: float,
    interpretation: Optional[InterpretationInfo],
) -> Optional[InterpretationBand]:
    if interpretation is None:
        return None

    for band in interpretation.bands:
        if band.matches(value):
            return band

    return None


def get_band_label_for_value(
    value: Optional[float],
    bundle: Optional[VariableReferenceBundle],
) -> Optional[str]:
    if value is None or bundle is None or bundle.thresholds is None:
        return None

    band = get_matching_band(value, bundle.thresholds)
    if band is None:
        return None

    return band.label