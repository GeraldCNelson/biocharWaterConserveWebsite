from __future__ import annotations

import asyncio
from io import BytesIO
import zipfile

from biochar_app.scripts.routes import (
    DownloadSummaryDataRequest,
    api_download_summary_data,
)


def test_seasonal_zip_has_descriptive_separated_files() -> None:
    request = DownloadSummaryDataRequest(
        year=2025,
        variable="VWC",
        strip="S1",
        granularity="gseason",
        depth="1",
        mode="zip",
        summaryStats={
            "gseason_stats": [
                {
                    "period_code": "Q2_Growing",
                    "period_label": "Growing Season",
                    "strip": "S1",
                    "depth": "1",
                    "logger_location": "T",
                    "raw_mean": 24.5,
                    "raw_coverage_pct": 99.0,
                },
                {
                    "period_code": "Q2_Growing",
                    "period_label": "Growing Season",
                    "ratio_group": "S1/S2",
                    "depth": "1",
                    "logger_location": "T",
                    "ratio_mean": 1.25,
                    "ratio_coverage_pct": 98.5,
                },
            ]
        },
    )

    response = asyncio.run(api_download_summary_data(request))
    base = "summary_gseason_VWC_S1_depth_code_1_2025"

    with zipfile.ZipFile(BytesIO(response.body)) as archive:
        assert set(archive.namelist()) == {
            f"{base}_raw.csv",
            f"{base}_ratio.csv",
            f"{base}_README.txt",
        }
        raw_csv = archive.read(f"{base}_raw.csv").decode("utf-8")
        ratio_csv = archive.read(f"{base}_ratio.csv").decode("utf-8")

    assert "raw_mean" in raw_csv
    assert "ratio_mean" not in raw_csv
    assert "ratio_mean" in ratio_csv
    assert "raw_mean" not in ratio_csv
