"""Seasonal VWC outputs share matched ratios of means, not mean ratios."""
import shutil
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from biochar_app.scripts.gseason import compute_seasons, matched_ratio_of_means
from biochar_app.scripts.gseason_utils import compute_period_summary_rows
from biochar_app.scripts.plot_builder import make_ratio_gseason_figure


@pytest.mark.parametrize("num,den", [("S1", "S2"), ("S3", "S4")])
def test_seasonal_bars_and_export_rows_match_comparison(num, den):
    frame = pd.DataFrame({
        "timestamp": pd.date_range("2025-04-01", periods=5, freq="15min"),
        f"VWC_1_raw_{num}_T": [30, 30, 99, None, float("inf")],
        f"VWC_1_raw_{den}_T": [10, 30, None, 10, 10],
        f"VWC_1_ratio_{num}_{den}_T": [3, 1, None, None, None],
        "EC_1_ratio_S1_S2_T": [3, 1, None, None, None],
    })
    original = frame.copy(deep=True)
    periods = [{"code": "GROWING", "label": "Growing", "start": "04-01", "end": "04-01"}]
    result = compute_seasons(frame, 2025, {"GROWING": periods[0]})
    ratio_col = f"VWC_1_ratio_{num}_{den}_T"
    assert result.iloc[0][ratio_col] == 1.5
    assert result.iloc[0]["EC_1_ratio_S1_S2_T"] == 2
    rows = compute_period_summary_rows(frame, year=2025, periods=periods,
                                       variable="VWC", strip=num, depth="1")
    summary = next(row for row in rows if row.get("ratio_group") == f"{num}/{den}")
    assert summary["ratio_of_means"] == result.iloc[0][ratio_col]
    assert summary["ratio_mean"] == 2  # Explicit individual-ratio statistic retained.
    figure = make_ratio_gseason_figure(df=result, periods=periods, variable="VWC",
        strip=num, logger_location="T", depth=1, unit_system="us", year=2025)
    trace = next(trace for trace in figure["data"] if trace["name"] == f"{num}/{den}")
    assert trace["y"][0] == 1.5
    pd.testing.assert_frame_equal(frame, original)  # Instantaneous data never mutated.


@pytest.mark.parametrize("values", [[None, None], [0, 0], [-1, -2]])
def test_unavailable_seasonal_ratio_does_not_fall_back_to_mean_ratios(values):
    frame = pd.DataFrame({"timestamp": pd.date_range("2025-04-01", periods=2, freq="15min"),
                          "VWC_1_raw_S1_T": [30, 30], "VWC_1_raw_S2_T": values,
                          "VWC_1_ratio_S1_S2_T": [3, 1]})
    result = compute_seasons(frame, 2025, {"G": {"start": "04-01", "end": "04-01"}})
    assert pd.isna(result.iloc[0]["VWC_1_ratio_S1_S2_T"])
    assert matched_ratio_of_means(frame, "missing", "VWC_1_raw_S2_T") == (None, 0)


def test_detailed_browser_summary_uses_matched_ratio_and_coverage():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required")
    source = Path(__file__).resolve().parents[1] / "static/js/tab_summary.js"
    script = r'''
import fs from "node:fs";
import vm from "node:vm";
const source = fs.readFileSync(process.argv[1], "utf8");
const start = source.indexOf("function buildGseasonSummaryTableHTML(");
const end = source.indexOf("\nfunction escapeSummaryHTML", start);
const context = vm.createContext({formatNumber: x => x == null ? "—" : String(x),
  getIncompletePeriodNotice: () => "", summaryWindow: {}});
vm.runInContext(source.slice(start, end), context);
context.rows = [{period_code: "G", ratio_group: "S1/S2", logger_location: "T", depth: "1",
 ratio_min: 1, ratio_mean: 2, ratio_max: 3, ratio_std: 1.414,
 ratio_n: 4, ratio_expected_n: 96, ratio_coverage_pct: 4.2,
 ratio_of_means: 1.5, ratio_of_means_n: 2, ratio_of_means_coverage_pct: 2.1}];
const html = vm.runInContext('buildGseasonSummaryTableHTML(rows, "VWC", "us", 2025, {G:{label:"Growing"}})', context);
if (!html.includes("<td>1.5</td>") || !html.includes("2.1%") || html.includes("4.2%")) throw Error("Wrong seasonal metric");
if (!html.includes("Min, Max and SD describe individual 15-minute ratios")) throw Error("Missing explanation");
'''
    subprocess.run([node, "--input-type=module", "-e", script, str(source)],
                   capture_output=True, text=True, check=True)
