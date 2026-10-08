"""External data-source definitions for the Biochar project."""

from dataclasses import dataclass
from pathlib import Path

from biochar_app.config.paths import (
    BIOCHAR_MASTER_WORKBOOK,
    WEATHER_DOWNLOADS_DIR,
)


@dataclass(frozen=True)
class ExternalDataSource:
    """Information shared by every external project data source."""

    key: str
    provider: str
    description: str
    source_url: str | None
    local_path: Path
    update_method: str


@dataclass(frozen=True)
class WorkbookDataSource(ExternalDataSource):
    """External Excel workbook with worksheets that must be present."""

    synced_source_path: Path
    required_sheets: tuple[str, ...]

@dataclass(frozen=True)
class WeatherDataSource(ExternalDataSource):
    station_id: str
    timezone: str


BIOCHAR_MASTER_SOURCE = WorkbookDataSource(
    key="biochar_master_workbook",
    provider="Project OneDrive",
    description="Authoritative field-management workbook",
    source_url=(
        "https://1drv.ms/f/c/4313d94c9016d33f/"
        "Ej_TFpBM2RMggEP0SAAAAAABtex3-yIuNGbovuvKAurUJg"
        "?e=5%3a2255Nx&sharingv2=true&fromShare=true&at=9"
    ),
    synced_source_path=(
        Path.home()
        / "Library"
        / "CloudStorage"
        / "OneDrive-Personal"
        / "Data"
        / "Biochar Injection Concept - Master.xlsx"
    ),
    local_path=BIOCHAR_MASTER_WORKBOOK,
    update_method="anonymous_shared_download",
    required_sheets=(
        "2023 IRRIGATION",
        "2024 IRRIGATION",
        "2025 IRRIGATION",
        "2026 IRRIGATION",
        "2023 BIOMASS",
        "2024 BIOMASS",
        "2025 BIOMASS",
        "2026 BIOMASS",
    ),
)

# Stable workbook identifier observed in OneDrive's workbook URL. Establish
# anonymous access with the shared-folder link before requesting this URL.
BIOCHAR_MASTER_DOWNLOAD_URL = (
    "https://onedrive.live.com/personal/4313d94c9016d33f/"
    "_layouts/15/download.aspx?UniqueId=9016D33F-D94C-2013-8043-325F00000000"
)

COAGMET_WEATHER_SOURCE = ExternalDataSource(
    key="coagmet_frt3",
    provider="Colorado Agricultural Meteorological Network",
    description="Weather observations from station FRT3",
    source_url="https://coagmet.colostate.edu/data/url-builder",
    local_path=WEATHER_DOWNLOADS_DIR,
    update_method="api_or_download",
)
