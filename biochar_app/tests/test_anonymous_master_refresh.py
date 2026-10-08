"""Anonymous download validation and management-refresh failure isolation."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from openpyxl import Workbook

from biochar_app.config.data_sources import BIOCHAR_MASTER_SOURCE
from biochar_app.scripts.management import update_master_workbook_snapshot as snapshots
from biochar_app.scripts.management import refresh_irrigation as refresh


def workbook_bytes(tmp_path):
    workbook = Workbook()
    workbook.remove(workbook.active)
    for name in BIOCHAR_MASTER_SOURCE.required_sheets:
        workbook.create_sheet(name)
    source = tmp_path / "source.xlsx"
    workbook.save(source)
    workbook.close()
    return source.read_bytes()


def session_mock(payload):
    session = MagicMock()
    download = MagicMock()
    download.iter_content.return_value = [payload]
    session.get.return_value.__enter__.return_value = download
    return session


def test_anonymous_download_redeems_share_before_downloading(tmp_path):
    payload = workbook_bytes(tmp_path)
    session = session_mock(payload)
    destination = tmp_path / "installed.xlsx"
    with patch.object(snapshots.requests, "Session") as factory:
        factory.return_value.__enter__.return_value = session
        result = snapshots.download_snapshot(destination=destination,
                                             audit_path=tmp_path / "audit.json")
    assert result["installed"]
    assert result["update_method"] == "anonymous_shared_download"
    assert session.get.call_args_list[0].args[0] == BIOCHAR_MASTER_SOURCE.source_url
    assert session.get.call_args_list[1].kwargs["stream"]
    assert destination.read_bytes() == payload


def test_http_failure_does_not_install_or_write_success_audit(tmp_path):
    destination = tmp_path / "installed.xlsx"
    destination.write_bytes(b"previous")
    session = session_mock(b"")
    session.get.return_value.__enter__.return_value.raise_for_status.side_effect = snapshots.requests.HTTPError("401")
    with patch.object(snapshots.requests, "Session") as factory:
        factory.return_value.__enter__.return_value = session
        with pytest.raises(snapshots.requests.HTTPError):
            snapshots.download_snapshot(destination=destination,
                                        audit_path=tmp_path / "audit.json")
    assert destination.read_bytes() == b"previous"
    assert not (tmp_path / "audit.json").exists()


def test_login_html_never_replaces_existing_snapshot(tmp_path):
    destination = tmp_path / "installed.xlsx"
    destination.write_bytes(b"previous snapshot")
    session = session_mock(b"<html>Please sign in</html>")
    with patch.object(snapshots.requests, "Session") as factory:
        factory.return_value.__enter__.return_value = session
        with pytest.raises(ValueError, match="XLSX"):
            snapshots.download_snapshot(destination=destination,
                                        audit_path=tmp_path / "audit.json")
    assert destination.read_bytes() == b"previous snapshot"


@pytest.mark.parametrize("failure_stage", ["download", "validation"])
def test_refresh_failure_preserves_previous_products(tmp_path, failure_stage):
    with patch.object(refresh, "REFRESH_STATUS_PATH", tmp_path / "status.json"), \
         patch.object(refresh, "download_snapshot") as download, \
         patch.object(refresh, "build_and_install_irrigation") as build, \
         patch.object(refresh, "update_snapshot") as install:
        if failure_stage == "download":
            download.side_effect = TimeoutError("unavailable")
        else:
            build.side_effect = ValueError("invalid events")
        result = refresh.refresh_irrigation_products()
    assert result["status"] == "failed"
    install.assert_not_called()
    assert all(call.kwargs.get("dry_run") for call in build.call_args_list)
    assert (tmp_path / "status.json").is_file()


def test_refresh_validates_irrigation_before_snapshot_install(tmp_path):
    order = []
    with patch.object(refresh, "REFRESH_STATUS_PATH", tmp_path / "status.json"), \
         patch.object(refresh, "download_snapshot", return_value={"update_method": "anonymous_shared_download"}), \
         patch.object(refresh, "write_audit"), \
         patch.object(refresh, "build_and_install_irrigation") as build, \
         patch.object(refresh, "update_snapshot") as install:
        def build_result(**kwargs):
            order.append("validate" if kwargs.get("dry_run") else "build")
            return dict(latest_irrigation_start="2026-09-30T10:14:00",
                        production_strip_rows=150, workbook_sha256="digest")
        build.side_effect = build_result
        install.side_effect = lambda **kwargs: order.append("install") or {}
        result = refresh.refresh_irrigation_products()
    assert result["status"] == "refreshed"
    assert order == ["validate", "install", "build"]
