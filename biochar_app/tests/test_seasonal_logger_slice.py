from pathlib import Path

import pandas as pd
import pytest

from biochar_app.scripts import data_loading
from biochar_app.scripts.gseason_utils import compute_period_summary_rows


@pytest.mark.parametrize("variable,depth", [("VWC", "1"), ("EC", "2"), ("SWC", "1")])
def test_projected_slice_matches_full_custom_summary(tmp_path, monkeypatch, variable, depth):
    monkeypatch.setattr(data_loading, "PARQUET_SUMMARY_DIR", tmp_path)
    base = tmp_path / "15min"
    base.mkdir()
    ts = pd.date_range("2025-04-01", periods=8, freq="15min")
    raw = pd.DataFrame({"timestamp": ts, "unrelated": range(8)})
    ratios = pd.DataFrame({"timestamp": ts})
    for strip, value in [("S1", 30), ("S2", 20), ("S3", 25), ("S4", 22)]:
        for location in ("T", "M", "B"):
            name = (f"SWC_vol_gal_{strip}_{location}_{depth}" if variable == "SWC"
                    else f"{variable}_{depth}_raw_{strip}_{location}")
            raw[name] = [value + n for n in range(8)]
    if variable != "SWC":
        ratios[f"{variable}_{depth}_ratio_S1_S2_T"] = [1.5] * 8
    raw.to_parquet(base / "2025_15min.parquet", index=False)
    ratios.to_parquet(base / "2025_15min_ratios.parquet", index=False)
    reads = []
    original_read = pd.read_parquet
    def projected(path, **kwargs):
        assert kwargs.get("columns") is not None
        assert "unrelated" not in kwargs["columns"]
        reads.append(path)
        return original_read(path, **kwargs)
    monkeypatch.setattr(pd, "read_parquet", projected)
    narrow = data_loading.load_seasonal_logger_slice(2025, variable, depth)
    assert len(reads) == 2
    full = raw.merge(ratios, on="timestamp", how="left")
    arguments = dict(year=2025, variable=variable, strip="S1", depth=depth,
                     periods=[{"code": "CUSTOM", "label": "Custom", "start": "2025-04-01", "end": "2025-04-02"}])
    assert compute_period_summary_rows(narrow, **arguments) == compute_period_summary_rows(full, **arguments)


def test_seasonal_route_does_not_retain_full_frames_or_parallelize_years():
    source = (Path(__file__).resolve().parents[1] / "scripts" / "routes.py").read_text()
    seasonal = source.split('if granularity == "gseason":', 1)[1].split("return JSONResponse(", 1)[0]
    assert "load_seasonal_logger_slice(summary_year, variable, depth_code)" in seasonal
    assert "_LOADED_LOGGER_CACHE" not in seasonal
    assert "asyncio.gather" not in seasonal
    assert "with _SEASONAL_COMPUTE_LOCK:" in seasonal
