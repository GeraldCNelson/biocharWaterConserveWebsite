from dataclasses import dataclass, field
from typing import Optional, Sequence


@dataclass(frozen=True)
class ReferenceInfo:
    guide_key: str
    guide_label: str
    section_title: Optional[str] = None
    anchor: Optional[str] = None
    table_number: Optional[str] = None
    table_title: Optional[str] = None
    page_hint: Optional[int] = None
    source_url: Optional[str] = None


@dataclass(frozen=True)
class InterpretationBand:
    label: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    include_min: bool = True
    include_max: bool = False

    def matches(self, value: float) -> bool:
        if self.min_value is not None:
            if self.include_min:
                if value < self.min_value:
                    return False
            else:
                if value <= self.min_value:
                    return False

        if self.max_value is not None:
            if self.include_max:
                if value > self.max_value:
                    return False
            else:
                if value >= self.max_value:
                    return False

        return True


@dataclass(frozen=True)
class InterpretationInfo:
    unit_label: Optional[str] = None
    method_note: Optional[str] = None
    bands: Sequence[InterpretationBand] = field(default_factory=tuple)


@dataclass(frozen=True)
class VariableReferenceBundle:
    short_note: str
    detail: Optional[str] = None
    interpretation: Optional[str] = None
    caveat: Optional[str] = None
    references: Sequence[ReferenceInfo] = field(default_factory=tuple)
    thresholds: Optional[InterpretationInfo] = None