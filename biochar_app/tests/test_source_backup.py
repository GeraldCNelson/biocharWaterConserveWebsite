"""Safety and configuration checks for the source-data backup tool."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from biochar_app.scripts.backup_source_data import (
    DEFAULT_MANIFEST,
    PROJECT_ROOT,
    SourceSpec,
    build_sync_command,
    inventory_source,
    is_excluded,
    load_config,
)


def test_source_backup_manifest_is_valid_and_scoped() -> None:
    config = load_config(DEFAULT_MANIFEST)

    assert config.bucket == "biochar-research-backup-gcn2026"
    assert config.region == "us-east-2"
    assert config.destination_prefix == "biocharWaterConserveWebsite"
    assert config.sources
    assert any(source.required_on_acquisition_server for source in config.sources)

    for source in config.sources:
        assert not source.path.is_absolute()
        assert ".." not in source.path.parts
        assert source.path.parts[0] == "biochar_app"


def test_source_backup_manifest_excludes_secrets_and_retired_sources() -> None:
    config = load_config(DEFAULT_MANIFEST)
    raw_source = next(
        source for source in config.sources if source.path == Path("biochar_app/data-raw")
    )
    patterns = config.global_excludes + raw_source.excludes

    for forbidden in (
        ".env",
        "nested/.env.production",
        "private.pem",
        "nested/private.key",
        "spm_sessions/logs/session.txt",
        "temp holder/scratch.csv",
        "datfiles_2026/COM3 Monitoring Session.spm",
        "datfiles_2026/Fruita router.txt",
        "datfiles_2026/IL vpn.txt",
        "datfiles_2026/netgearInfo.txt",
        "datfiles_2026/PC400.bkp",
        "datfiles_2026/PC400.zip.txt",
        "datfiles_2026/logs/io2IPPort_3.log",
        "datfiles_2026/manuals/PC400 manual.pdf",
        "datfiles_2026/python3/reqs.py",
        "datfiles_2026/sys/inifiles/PC400.ini",
        "nested/.DS_Store",
        "nested/__pycache__/module.pyc",
    ):
        assert is_excluded(PurePosixPath(forbidden), patterns), forbidden


def test_sync_command_is_deletion_free_and_preserves_relative_path() -> None:
    config = load_config(DEFAULT_MANIFEST)
    source = config.sources[0]
    command = build_sync_command(config, source, dry_run=True)

    assert "--delete" not in command
    assert "--dryrun" in command
    assert "--no-follow-symlinks" in command
    assert (
        f"s3://{config.bucket}/{config.destination_prefix}/{source.path.as_posix()}/"
        in command
    )


def test_inventory_skips_excluded_files_and_symlinks(tmp_path: Path) -> None:
    source_root = tmp_path / "biochar_app" / "data-raw"
    source_root.mkdir(parents=True)
    (source_root / "accepted.dat").write_bytes(b"accepted")
    (source_root / ".env").write_text("SECRET=value", encoding="utf-8")
    retired = source_root / "spm_sessions"
    retired.mkdir()
    (retired / "session.txt").write_text("retired", encoding="utf-8")
    (source_root / "linked.dat").symlink_to(source_root / "accepted.dat")

    source = SourceSpec(
        path=Path("biochar_app/data-raw"),
        required_on_acquisition_server=True,
        excludes=("spm_sessions/*",),
        description="test source",
    )
    record = inventory_source(
        tmp_path,
        source,
        (".env", ".env.*", "*/.env", "*/.env.*", "*.pem", "*.key"),
    )

    assert record.file_count == 1
    assert record.byte_count == len(b"accepted")


def test_default_manifest_project_root_matches_repository() -> None:
    assert DEFAULT_MANIFEST.is_file()
    assert (PROJECT_ROOT / "tools" / "backup-source-data").is_file()
