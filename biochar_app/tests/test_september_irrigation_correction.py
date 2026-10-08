"""Photo-supported date correction survives the upstream workbook correction."""
import pandas as pd
import pytest
from biochar_app.scripts.management import build_irrigation_qc_candidate as qc


@pytest.mark.parametrize("date", ["2026-09-30", "2026-09-29"])
def test_september_correction_preserves_volume_and_source(date, monkeypatch):
    corrections = tuple(c for c in qc.IRRIGATION_QC_CORRECTIONS
                        if c.source_date == "2026-09-30")
    monkeypatch.setattr(qc, "IRRIGATION_QC_CORRECTIONS", corrections)
    rows = []
    for strip in ("S1", "S2", "S3", "S4"):
        rows.append(dict(date=date, year=2026, strip=strip,
                         strip_group="S1_S2" if strip in {"S1", "S2"} else "S3_S4",
                         start_timestamp=pd.Timestamp(f"{date} 10:14"),
                         end_timestamp=pd.Timestamp(f"{date} 15:33"),
                         start_totalizer_gal_x100=240194., end_totalizer_gal_x100=241488.,
                         gallons_strip=32350., event_id=strip))
    frame = pd.DataFrame(rows)
    frame["original_start_timestamp"] = frame.start_timestamp
    corrected, audit = qc.apply_timestamp_corrections(frame)
    assert corrected.start_timestamp.eq(pd.Timestamp("2026-09-29 10:14")).all()
    assert corrected.end_timestamp.eq(pd.Timestamp("2026-09-29 15:33")).all()
    assert corrected.original_start_timestamp.eq(pd.Timestamp(f"{date} 10:14")).all()
    assert corrected.gallons_strip.eq(32350.).all()
    assert len(audit) == 2


def test_september_correction_rejects_different_meter_readings():
    correction = next(c for c in qc.IRRIGATION_QC_CORRECTIONS if c.source_date == "2026-09-30")
    frame = pd.DataFrame([dict(date="2026-09-30", strip_group="S1_S2",
                               start_totalizer_gal_x100=1., end_totalizer_gal_x100=241488.)])
    assert not qc.match_correction_rows(frame, correction).any()
