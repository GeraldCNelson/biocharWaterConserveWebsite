"""Refresh management products without risking the last validated irrigation data."""
from datetime import datetime, timezone
from pathlib import Path
import tempfile

from biochar_app.config.data_sources import BIOCHAR_MASTER_SOURCE
from biochar_app.scripts.management.build_irrigation_from_master import build_and_install_irrigation, DEFAULT_AUDIT_JSON
from biochar_app.scripts.management.update_master_workbook_snapshot import download_snapshot, update_snapshot, write_audit

REFRESH_STATUS_PATH = DEFAULT_AUDIT_JSON.with_name("irrigation_refresh_status.json")


def refresh_irrigation_products() -> dict:
    """Validate the incoming workbook and proposed irrigation before installing.

    Network/validation failures leave the prior snapshot and irrigation CSV
    untouched. Report failure explicitly while allowing logger/weather updates.
    """
    status = {"attempted_at_utc": datetime.now(timezone.utc).isoformat()}
    try:
        with tempfile.TemporaryDirectory(prefix="biochar-irrigation-") as directory:
            incoming = Path(directory) / BIOCHAR_MASTER_SOURCE.local_path.name
            downloaded = download_snapshot(
                destination=incoming, audit_path=Path(directory) / "snapshot.json")
            build_and_install_irrigation(workbook_path=incoming, dry_run=True)
            snapshot = update_snapshot(
                source=incoming, destination=BIOCHAR_MASTER_SOURCE.local_path,
                required_sheets=BIOCHAR_MASTER_SOURCE.required_sheets,
                audit_path=BIOCHAR_MASTER_SOURCE.local_path.with_suffix(".snapshot.json"),
                validate_only=False)
            snapshot["update_method"] = downloaded["update_method"]
            snapshot.pop("synced_source_path", None)
            write_audit(BIOCHAR_MASTER_SOURCE.local_path.with_suffix(".snapshot.json"), snapshot)
            audit = build_and_install_irrigation()
        status.update(status="refreshed", latest_irrigation_start=audit["latest_irrigation_start"],
                      production_strip_rows=audit["production_strip_rows"],
                      workbook_sha256=audit["workbook_sha256"])
    except Exception as exc:
        # Do not include request URLs (sharing tokens) in the persistent report.
        status.update(status="failed", detail=type(exc).__name__,
                      action="Retained last validated irrigation data; inspect operational log and retry refresh.")
    write_audit(REFRESH_STATUS_PATH, status)
    return status
