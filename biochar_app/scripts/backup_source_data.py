"""Inventory and copy approved source-evidence directories to object storage.

The configured destination is a private, versioned Lightsail object-storage
bucket. Uploads preserve repository-relative paths, never delete remote
objects, and skip credentials, caches, and explicitly retired source areas.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shutil
import socket
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = (
    PROJECT_ROOT / "biochar_app" / "config" / "source_backup_manifest.json"
)


@dataclass(frozen=True)
class SourceSpec:
    """One repository-relative directory approved for source-data backup."""

    path: Path
    required_on_acquisition_server: bool
    excludes: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class BackupConfig:
    """Validated destination and source definitions from the JSON manifest."""

    bucket: str
    region: str
    destination_prefix: str
    global_excludes: tuple[str, ...]
    sources: tuple[SourceSpec, ...]


@dataclass(frozen=True)
class InventoryRecord:
    """File count and byte total for one configured source."""

    path: str
    exists: bool
    required: bool
    file_count: int
    byte_count: int
    description: str


def _require_nonempty_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Manifest field {key!r} must be a non-empty string")
    return value.strip()


def _validate_relative_path(value: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute() or ".." in candidate.parts or candidate == Path("."):
        raise ValueError(f"Backup source must be a safe relative path: {value!r}")
    return candidate


def load_config(path: Path = DEFAULT_MANIFEST) -> BackupConfig:
    """Load and validate the source-backup manifest."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError("Unsupported source-backup manifest schema")

    global_excludes = payload.get("global_excludes")
    if not isinstance(global_excludes, list) or not all(
        isinstance(item, str) and item for item in global_excludes
    ):
        raise ValueError("global_excludes must be a list of non-empty strings")

    raw_sources = payload.get("sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ValueError("sources must be a non-empty list")

    sources: list[SourceSpec] = []
    seen_paths: set[Path] = set()
    for raw_source in raw_sources:
        if not isinstance(raw_source, dict):
            raise ValueError("Each backup source must be an object")
        source_path = _validate_relative_path(
            _require_nonempty_string(raw_source, "path")
        )
        if source_path in seen_paths:
            raise ValueError(f"Duplicate backup source: {source_path}")
        seen_paths.add(source_path)

        required = raw_source.get("required_on_acquisition_server")
        if not isinstance(required, bool):
            raise ValueError(
                f"required_on_acquisition_server must be true or false: {source_path}"
            )
        excludes = raw_source.get("excludes", [])
        if not isinstance(excludes, list) or not all(
            isinstance(item, str) and item for item in excludes
        ):
            raise ValueError(f"excludes must be a list of strings: {source_path}")

        sources.append(
            SourceSpec(
                path=source_path,
                required_on_acquisition_server=required,
                excludes=tuple(excludes),
                description=_require_nonempty_string(raw_source, "description"),
            )
        )

    config = BackupConfig(
        bucket=_require_nonempty_string(payload, "bucket"),
        region=_require_nonempty_string(payload, "region"),
        destination_prefix=_require_nonempty_string(
            payload, "destination_prefix"
        ).strip("/"),
        global_excludes=tuple(global_excludes),
        sources=tuple(sources),
    )
    _validate_secret_exclusions(config.global_excludes)
    return config


def _validate_secret_exclusions(patterns: Sequence[str]) -> None:
    required_patterns = {".env", ".env.*", "*/.env", "*/.env.*", "*.pem", "*.key"}
    missing = sorted(required_patterns.difference(patterns))
    if missing:
        raise ValueError(
            "Manifest must exclude credential-bearing files: " + ", ".join(missing)
        )


def is_excluded(relative_path: PurePosixPath, patterns: Sequence[str]) -> bool:
    """Return whether a source-relative path matches an exclusion pattern."""

    value = relative_path.as_posix()
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


def inventory_source(
    project_root: Path,
    source: SourceSpec,
    global_excludes: Sequence[str],
) -> InventoryRecord:
    """Count regular, non-excluded files without following symbolic links."""

    absolute_source = project_root / source.path
    if not absolute_source.exists():
        return InventoryRecord(
            path=source.path.as_posix(),
            exists=False,
            required=source.required_on_acquisition_server,
            file_count=0,
            byte_count=0,
            description=source.description,
        )
    if not absolute_source.is_dir() or absolute_source.is_symlink():
        raise ValueError(f"Backup source is not a normal directory: {source.path}")

    patterns = tuple(global_excludes) + source.excludes
    file_count = 0
    byte_count = 0
    for directory, directory_names, file_names in os.walk(
        absolute_source, followlinks=False
    ):
        directory_path = Path(directory)
        kept_directories: list[str] = []
        for name in directory_names:
            candidate = directory_path / name
            relative = PurePosixPath(candidate.relative_to(absolute_source).as_posix())
            if candidate.is_symlink() or is_excluded(relative, patterns) or is_excluded(
                PurePosixPath(f"{relative.as_posix()}/placeholder"), patterns
            ):
                continue
            kept_directories.append(name)
        directory_names[:] = kept_directories

        for name in file_names:
            candidate = directory_path / name
            if candidate.is_symlink() or not candidate.is_file():
                continue
            relative = PurePosixPath(candidate.relative_to(absolute_source).as_posix())
            if is_excluded(relative, patterns):
                continue
            file_count += 1
            byte_count += candidate.stat().st_size

    return InventoryRecord(
        path=source.path.as_posix(),
        exists=True,
        required=source.required_on_acquisition_server,
        file_count=file_count,
        byte_count=byte_count,
        description=source.description,
    )


def approved_source_files(
    project_root: Path,
    source: SourceSpec,
    global_excludes: Sequence[str],
) -> list[tuple[Path, PurePosixPath]]:
    """Return approved regular files and their source-relative paths."""

    absolute_source = project_root / source.path
    if not absolute_source.exists():
        return []
    if not absolute_source.is_dir() or absolute_source.is_symlink():
        raise ValueError(f"Backup source is not a normal directory: {source.path}")

    patterns = tuple(global_excludes) + source.excludes
    approved: list[tuple[Path, PurePosixPath]] = []
    for directory, directory_names, file_names in os.walk(
        absolute_source, followlinks=False
    ):
        directory_path = Path(directory)
        kept_directories: list[str] = []
        for name in directory_names:
            candidate = directory_path / name
            relative = PurePosixPath(candidate.relative_to(absolute_source).as_posix())
            if candidate.is_symlink() or is_excluded(relative, patterns) or is_excluded(
                PurePosixPath(f"{relative.as_posix()}/placeholder"), patterns
            ):
                continue
            kept_directories.append(name)
        directory_names[:] = kept_directories

        for name in file_names:
            candidate = directory_path / name
            if candidate.is_symlink() or not candidate.is_file():
                continue
            relative = PurePosixPath(candidate.relative_to(absolute_source).as_posix())
            if not is_excluded(relative, patterns):
                approved.append((candidate, relative))
    return approved


def object_key(
    config: BackupConfig,
    source: SourceSpec,
    relative_path: PurePosixPath,
) -> str:
    """Return the destination object key for one approved source file."""

    return (
        f"{config.destination_prefix}/{source.path.as_posix()}/"
        f"{relative_path.as_posix()}"
    )


def missing_source_files(
    config: BackupConfig,
    source: SourceSpec,
    remote_keys: set[str],
    *,
    project_root: Path = PROJECT_ROOT,
) -> list[tuple[Path, str]]:
    """Return approved local files whose exact object keys are absent."""

    return [
        (local_path, key)
        for local_path, relative_path in approved_source_files(
            project_root, source, config.global_excludes
        )
        if (key := object_key(config, source, relative_path)) not in remote_keys
    ]


def build_conditional_put_command(
    config: BackupConfig,
    local_path: Path,
    key: str,
) -> list[str]:
    """Build an upload command that cannot replace an existing object key."""

    return [
        "aws",
        "s3api",
        "put-object",
        "--bucket",
        config.bucket,
        "--key",
        key,
        "--body",
        str(local_path),
        "--if-none-match",
        "*",
        "--region",
        config.region,
        "--no-cli-pager",
    ]


def build_sync_command(
    config: BackupConfig,
    source: SourceSpec,
    *,
    project_root: Path = PROJECT_ROOT,
    dry_run: bool,
) -> list[str]:
    """Build a deletion-free AWS CLI sync command for one source."""

    source_path = project_root / source.path
    destination = (
        f"s3://{config.bucket}/{config.destination_prefix}/"
        f"{source.path.as_posix()}/"
    )
    command = [
        "aws",
        "s3",
        "sync",
        str(source_path),
        destination,
        "--region",
        config.region,
        "--no-follow-symlinks",
        "--no-progress",
    ]
    for pattern in config.global_excludes + source.excludes:
        command.extend(("--exclude", pattern))
    if dry_run:
        command.append("--dryrun")
    else:
        command.append("--only-show-errors")
    return command


def _format_bytes(byte_count: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(byte_count)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


def _collect_inventory(config: BackupConfig) -> list[InventoryRecord]:
    records = [
        inventory_source(PROJECT_ROOT, source, config.global_excludes)
        for source in config.sources
    ]
    for record in records:
        if record.exists:
            print(
                f"{record.path}: {record.file_count:,} files, "
                f"{_format_bytes(record.byte_count)}"
            )
        else:
            status = "required" if record.required else "optional"
            print(f"{record.path}: MISSING ({status})")

    total_files = sum(record.file_count for record in records)
    total_bytes = sum(record.byte_count for record in records)
    print(f"Total: {total_files:,} files, {_format_bytes(total_bytes)}")
    return records


def _run_aws(command: Sequence[str], *, quiet: bool = False) -> None:
    environment = os.environ.copy()
    environment["AWS_PAGER"] = ""
    subprocess.run(
        list(command),
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
        stdout=subprocess.DEVNULL if quiet else None,
    )


def _run_aws_json(command: Sequence[str]) -> Any:
    environment = os.environ.copy()
    environment["AWS_PAGER"] = ""
    completed = subprocess.run(
        list(command),
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def _list_remote_keys(config: BackupConfig) -> set[str]:
    payload = _run_aws_json(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            config.bucket,
            "--prefix",
            f"{config.destination_prefix}/",
            "--region",
            config.region,
            "--output",
            "json",
            "--no-cli-pager",
        ]
    )
    contents = payload.get("Contents", [])
    if not isinstance(contents, list):
        raise ValueError("Unexpected object-list response from S3")
    return {
        item["Key"]
        for item in contents
        if isinstance(item, dict) and isinstance(item.get("Key"), str)
    }


def _write_run_manifest(
    config: BackupConfig,
    records: Sequence[InventoryRecord],
) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    destination = Path("/tmp") / f"biochar-source-backup-{timestamp}.json"
    payload = {
        "schema_version": 1,
        "completed_utc": timestamp,
        "host": socket.gethostname(),
        "bucket": config.bucket,
        "region": config.region,
        "destination_prefix": config.destination_prefix,
        "sources": [record.__dict__ for record in records],
        "total_file_count": sum(record.file_count for record in records),
        "total_byte_count": sum(record.byte_count for record in records),
    }
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return destination


def run_backup(
    config: BackupConfig,
    *,
    dry_run: bool,
    missing_only: bool = False,
) -> int:
    """Inventory configured sources and run deletion-free sync operations."""

    records = _collect_inventory(config)
    missing_required = [
        record.path for record in records if record.required and not record.exists
    ]
    if missing_required:
        print(
            "Required backup sources are missing: " + ", ".join(missing_required),
            file=sys.stderr,
        )
        return 1

    if shutil.which("aws") is None:
        print("AWS CLI v2 is required but was not found.", file=sys.stderr)
        return 1

    _run_aws(
        [
            "aws",
            "s3api",
            "head-bucket",
            "--bucket",
            config.bucket,
            "--region",
            config.region,
        ]
    )

    if missing_only:
        remote_keys = _list_remote_keys(config)
        missing_by_source = {
            source.path.as_posix(): missing_source_files(config, source, remote_keys)
            for source in config.sources
        }
        missing_count = sum(len(files) for files in missing_by_source.values())
        missing_bytes = sum(
            local_path.stat().st_size
            for files in missing_by_source.values()
            for local_path, _key in files
        )
        print(
            f"Missing-only selection: {missing_count:,} files, "
            f"{_format_bytes(missing_bytes)}"
        )
        for source in config.sources:
            files = missing_by_source[source.path.as_posix()]
            print(
                f"{'Previewing' if dry_run else 'Uploading'} missing objects from "
                f"{source.path.as_posix()}: {len(files):,}"
            )
            for local_path, key in files:
                destination = f"s3://{config.bucket}/{key}"
                if dry_run:
                    print(f"(dryrun) upload: {local_path} to {destination}")
                else:
                    _run_aws(
                        build_conditional_put_command(config, local_path, key),
                        quiet=True,
                    )

        if dry_run:
            print("Missing-only dry run complete; the bucket was not changed.")
            return 0

        run_manifest = _write_run_manifest(config, records)
        manifest_key = (
            f"{config.destination_prefix}/backup-manifests/{run_manifest.name}"
        )
        _run_aws(
            [
                "aws",
                "s3",
                "cp",
                str(run_manifest),
                f"s3://{config.bucket}/{manifest_key}",
                "--region",
                config.region,
                "--only-show-errors",
            ]
        )
        print(
            "Missing-only backup complete; "
            f"{missing_count:,} objects added and run manifest uploaded as "
            f"{manifest_key}"
        )
        return 0

    records_by_path = {record.path: record for record in records}
    for source in config.sources:
        record = records_by_path[source.path.as_posix()]
        if not record.exists:
            continue
        print(f"{'Previewing' if dry_run else 'Uploading'} {record.path}...")
        _run_aws(build_sync_command(config, source, dry_run=dry_run))

    if dry_run:
        print("Dry run complete; the bucket was not changed.")
        return 0

    run_manifest = _write_run_manifest(config, records)
    manifest_key = (
        f"{config.destination_prefix}/backup-manifests/{run_manifest.name}"
    )
    _run_aws(
        [
            "aws",
            "s3",
            "cp",
            str(run_manifest),
            f"s3://{config.bucket}/{manifest_key}",
            "--region",
            config.region,
            "--only-show-errors",
        ]
    )
    print(f"Backup complete; run manifest uploaded as {manifest_key}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inventory or upload approved Biochar source-evidence files."
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        "--inventory", action="store_true", help="Count approved local source files"
    )
    action.add_argument(
        "--dry-run", action="store_true", help="Preview uploads without changing S3"
    )
    action.add_argument(
        "--upload", action="store_true", help="Upload new or changed approved files"
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Alternative JSON manifest (primarily for testing)",
    )
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Add absent object keys without replacing any existing object",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.inventory and args.missing_only:
        parser.error("--missing-only requires --dry-run or --upload")
    try:
        config = load_config(args.manifest)
        if args.inventory:
            records = _collect_inventory(config)
            return 1 if any(
                record.required and not record.exists for record in records
            ) else 0
        return run_backup(
            config,
            dry_run=args.dry_run,
            missing_only=args.missing_only,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Backup configuration error: {exc}", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as exc:
        print(f"Backup command failed with status {exc.returncode}.", file=sys.stderr)
        return exc.returncode or 1


if __name__ == "__main__":
    raise SystemExit(main())
