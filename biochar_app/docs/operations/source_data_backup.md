# Source-data backup to Lightsail object storage

**Last reviewed:** 2026-10-18

## Purpose

This process preserves source evidence that cannot be recreated from project
code alone. It copies approved files from the acquisition server to the
private, versioned `biochar-research-backup-gcn2026` Lightsail bucket. It does
not replace GitHub for code and documentation or the separate deployment of
rebuildable Parquet and download products.

The backup never uses remote deletion. If a source file is removed or renamed,
the previously uploaded object remains in the bucket for later review.

## Approved sources

The maintained list is
[`source_backup_manifest.json`](../../config/source_backup_manifest.json). It
currently includes:

- `biochar_app/data-raw/`, except retired `spm_sessions/` and temporary
  `temp holder/` material;
- original irrigation photographs and review records under
  `data-processed/management/irrigation/photos/`;
- drone source material; and
- manually developed QGIS project material.

Credential-bearing files, private keys, macOS metadata, Python caches, and
symbolic links are excluded. The raw-data directory also contains obsolete
PC400/SPM communication logs, application backups, manuals, helper scripts,
and network-setting notes; these are operational history rather than
scientific source evidence and are explicitly excluded. Logger `.dat` files,
original data archives, logger programs and table definitions, PakBus records,
and workbook evidence remain included. CoAgMET downloads, Parquet files,
public-download bundles, LiDAR-derived products, and Git-tracked application
files are not part of this source-evidence backup.

Optional sources can be absent from the acquisition server. Their absence is
reported in the inventory but does not fail the operational backup. The
`data-raw/` directory is required.

## Operator commands

Run an inventory without contacting object storage:

```bash
tools/backup-source-data --inventory
```

Preview the exact S3 changes without uploading files:

```bash
tools/backup-source-data --dry-run
```

Upload new or changed files:

```bash
tools/backup-source-data --upload
```

Each successful upload stores a small dated run manifest under
`biocharWaterConserveWebsite/backup-manifests/`. The manifest records the host,
source presence, file counts, and byte totals. It contains no credentials.

## Initial server setup

Prerequisites:

1. The private Lightsail bucket exists in `us-east-2` with versioning enabled.
2. The `biochar-test-fetch` instance is attached to the bucket through
   Lightsail Resource access.
3. AWS CLI v2 is installed on the instance.
4. A manual inventory, dry run, upload, and sample restore have succeeded.

Only after those checks, install the service definitions:

```bash
sudo install -m 0644 deployment/systemd/biochar-source-backup.service \
  /etc/systemd/system/biochar-source-backup.service
sudo install -m 0644 deployment/systemd/biochar-source-backup.timer \
  /etc/systemd/system/biochar-source-backup.timer
sudo systemctl daemon-reload
sudo systemctl enable --now biochar-source-backup.timer
systemctl list-timers biochar-source-backup.timer
```

The timer runs at 02:30 America/Denver, after the PakBus timer scheduled for
00:15. Its random delay reduces dependence on an exact start second. Review a
run with:

```bash
systemctl status biochar-source-backup.service
journalctl -u biochar-source-backup.service --since today
```

## Initial historical material

The acquisition server contains the active 2026 logger, PakBus, and workbook
sources but not all older laboratory, field-photo, field-mapping, and drone
material held on the Mac. Do not copy all of that material temporarily onto the
server: its system disk has limited free space. Inventory the differences and
seed Mac-only files separately before treating the bucket as complete.

## Verification and recovery

The first live upload is not complete until at least one small source file and
one representative binary file have been restored to a temporary directory
and their SHA-256 hashes match the originals. Repeat a sample restore at least
quarterly and after changing the manifest or backup tool.

Keep the current ETL local ZIP backup until this S3 upload and restore test has
passed. Review that local ZIP process separately afterward; a copy on the same
server is not an independent backup.
