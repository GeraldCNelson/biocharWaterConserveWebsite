from __future__ import annotations

import json

import pandas as pd

from biochar_app.diagnostics.plausible_range_audit import audit_frame, run_audit, write_audit


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=4, freq="15min"),
            "VWC_1_raw_S1_T": [49.0, 50.0, 50.1, None],
            "VWC_2_raw_S2_M": [55.0, 45.0, 40.0, 35.0],
            "T_1_raw_S1_T": [-10.0, -10.1, 20.0, None],
            "T_3_raw_S4_B": [-20.0, -11.0, -10.0, 30.0],
            "VWC_1_ratio_S1_S2_T": [1.0, 1.0, 1.0, 1.0],
        }
    )


def test_audit_frame_counts_strict_threshold_exceedances() -> None:
    rows = audit_frame(_frame(), year=2026)
    by_column = {row["column"]: row for row in rows}

    vwc = by_column["VWC_1_raw_S1_T"]
    assert vwc["flagged_observations"] == 1
    assert vwc["valid_observations"] == 3
    assert vwc["strip"] == "S1"
    assert vwc["depth_code"] == "1"
    assert vwc["sensor_location"] == "T"
    assert vwc["first_flagged_timestamp"] == "2026-01-01T00:30:00"

    temperature = by_column["T_1_raw_S1_T"]
    assert temperature["flagged_observations"] == 1
    assert temperature["observed_min"] == -10.1
    assert "VWC_1_ratio_S1_S2_T" not in by_column


def test_run_audit_summarizes_variables_across_years() -> None:
    details, summary = run_audit([2025, 2026], loader=lambda year, granularity: _frame())

    assert len(details) == 8
    assert summary["years"] == [2025, 2026]
    assert summary["variables"]["VWC"]["flagged_observations"] == 4
    assert summary["variables"]["T"]["flagged_observations"] == 6
    assert summary["variables"]["VWC"]["affected_sensors"] == 4


def test_write_audit_creates_csv_and_json(tmp_path) -> None:
    details, summary = run_audit([2026], loader=lambda year, granularity: _frame())
    csv_path, json_path = write_audit(details, summary, tmp_path)

    assert len(pd.read_csv(csv_path)) == 4
    assert json.loads(json_path.read_text())["status"] == "audited"
