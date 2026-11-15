"""Stable reporting dimensions derived from public download identifiers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BulkDownloadDimensions:
    dataset: str | None
    dataset_year: int | None = None
    granularity: str | None = None


ALL_YEAR_DATASETS = {
    "irrigation_all": "irrigation",
    "fertilizer_all": "fertilizer",
    "soil_chem_all": "soil_chem",
    "soil_bio_all": "soil_bio",
    "hay_all": "hay",
}


def parse_bulk_download_key(key: str) -> BulkDownloadDimensions:
    """Parse one manifest key without inspecting the generated archive."""
    normalized = key.strip().lower()
    if not normalized:
        return BulkDownloadDimensions(dataset=None)

    if normalized in ALL_YEAR_DATASETS:
        return BulkDownloadDimensions(dataset=ALL_YEAR_DATASETS[normalized])

    parts = normalized.split("_")
    dataset = parts[0] or None
    dataset_year: int | None = None
    granularity: str | None = None

    if len(parts) >= 2 and parts[1].isdigit():
        dataset_year = int(parts[1])
    if dataset in {"loggers", "weather"} and len(parts) >= 3:
        granularity = parts[2] or None

    return BulkDownloadDimensions(
        dataset=dataset,
        dataset_year=dataset_year,
        granularity=granularity,
    )
