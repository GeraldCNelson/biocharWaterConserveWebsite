# Irrigation Meter Photos

This directory contains the canonical photo archive and supporting files used to
extract, review, and maintain irrigation meter readings.

## Directory Structure

```
photos/
├── originals/              # Canonical deduplicated photo archive
├── photo_inventory.csv     # Master inventory of all photos
└── README.md               # This file
```

Temporary import folders and duplicate archives are intentionally excluded from
Git and should not be recreated here.

---

## Canonical Photo Archive

The `originals/` directory contains one copy of each irrigation meter photograph.

Rules:

- Each physical image appears only once.
- Exact duplicate files have been removed.
- Original filenames are preserved whenever possible.
- Photos should never be edited in place.
- If an image requires enhancement or annotation, create a derived copy elsewhere.

---

## Photo Inventory

`photo_inventory.csv` is the master catalog for every photo.

It contains:

- filename
- file path
- SHA-256 hash
- image dimensions
- EXIF timestamps
- GPS coordinates (when available)
- manually corrected timestamps
- effective timestamps
- irrigation year
- duplicate detection fields
- review fields

The inventory is regenerated from the contents of `originals/`.

Do **not** manually edit automatically generated columns.

Only the designated manual review fields should be edited.

---

## Typical Workflow

### 1. Add new photographs

Copy new photos into:

```
photos/originals/
```

Do not overwrite existing files.

---

### 2. Rebuild the inventory

```
python biochar_app/scripts/management/build_meter_photo_inventory.py
```

This will:

- scan all photos
- extract EXIF metadata
- compute SHA-256 hashes
- detect duplicate images
- preserve existing manual review information
- rebuild `photo_inventory.csv`

---

### 3. Review timestamps

Review and edit manual timestamp fields in `photo_inventory.csv` where necessary.

Rebuild the inventory after making timestamp corrections.

---

### 4. Remove duplicate files

Preview duplicate removal:

```
python biochar_app/scripts/management/apply_duplicate_actions.py
```

Apply duplicate removal:

```
python biochar_app/scripts/management/apply_duplicate_actions.py --apply
```

After cleanup, rebuild the inventory.

---

### 5. Verify cleanup

A successful cleanup should report:

- no rows marked DELETE
- each remaining image has `exact_duplicate_count = 1`

---

## Design Philosophy

The photo archive is intended to be reproducible.

Given only:

- `photos/originals/`

the inventory can always be regenerated.

`photo_inventory.csv` is considered a derived product and should always be
rebuildable from the photo archive.

---

## Related Scripts

Management scripts:

- build_meter_photo_inventory.py
- apply_duplicate_actions.py
- build_meter_review_workbook.py
- clean_meter_photo_readings.py
- compare_meter_photos_to_irrigation.py

Utility scripts:

- compare_photo_hashes.py
- compare_photo_directories.py

These scripts together provide the workflow for importing, validating,
deduplicating, reviewing, and maintaining irrigation meter photographs.

## Postscript
# Reproducible Irrigation Meter-Photo Review Pipeline

This package adds the missing reproducible step between the reviewed Excel
workbooks and `photo_inventory_unique.csv`.

## Why this exists

`build_meter_photo_inventory.py` creates photo metadata and preserves existing
manual fields. It does **not** import completed review workbooks. Historically,
the workbook merge and likely-duplicate selection were performed in a separate
Codex task, which made the final inventory difficult to reconstruct.

`finalize_meter_photo_inventory.py` makes that process explicit, repeatable,
auditable, and testable.

## Authoritative inputs and precedence

The script uses the following precedence:

1. **Explicit correction ledger.** A correction must identify a photo by
   SHA-256 and include a reason.
2. **Readable entry in the main review workbook.** This is the preferred
   reviewed value for the exact file.
3. **Readable main-workbook photo-family consensus.** A readable value from the
   full-resolution original is propagated to resized/exported renditions that
   share the same camera-photo identifier.
4. **Full-resolution follow-up workbook.** This fills readings that remain
   unresolved. It cannot silently replace a stronger main-workbook value.
5. **Existing inventory value.** Used only when no reviewed source supplies a
   value.

If readable rows in the main workbook disagree within the same filename
family, the script does not propagate a family value. It retains exact-file
readings and records the family in `ambiguous_main_workbook_families` for
review. Filename similarity alone is not treated as proof that two exports are
the same photograph.

## Inputs

- `photos/photo_inventory.csv`
- Main reviewed workbook, currently
  `meter_photo_review_updated_2026-07-23.xlsx`
- Full-resolution follow-up workbook, currently
  `meter_photo_unreadable_full_resolution_review_updated_2026-07-23.xlsx`
- `photos/meter_photo_reading_corrections.csv`

The Excel workbooks are user-reviewed source data. They should be supplied by
path at runtime. Do not hard-code a Downloads path in project code.

## Outputs

- `photo_inventory_unique.csv`: one preferred file per likely duplicate group.
- `photo_inventory_unique_manifest.csv`: every input row, whether it was kept
  or excluded, and the selected filename.
- `photo_inventory_unique_audit.json`: input paths, row counts, reading-source
  counts, suppressed follow-up conflicts, and SHA-256 of the output CSV.
- Optional `originals_unique/`: copied selected files when explicitly requested.

Inputs are never edited.

## Command

Run from the repository root:

```bash
python biochar_app/scripts/management/finalize_meter_photo_inventory.py \
  --inventory-csv biochar_app/data-processed/management/irrigation/photos/photo_inventory.csv \
  --main-workbook /path/to/meter_photo_review_updated_2026-07-23.xlsx \
  --full-resolution-workbook /path/to/meter_photo_unreadable_full_resolution_review_updated_2026-07-23.xlsx \
  --corrections-csv biochar_app/data-processed/management/irrigation/photos/meter_photo_reading_corrections.csv \
  --output-csv biochar_app/data-processed/management/irrigation/photos/photo_inventory_unique.csv \
  --manifest-csv biochar_app/data-processed/management/irrigation/photos/photo_inventory_unique_manifest.csv \
  --audit-json biochar_app/data-processed/management/irrigation/photos/photo_inventory_unique_audit.json
```

To also create a working photo directory, add:

```bash
  --originals-dir biochar_app/data-processed/management/irrigation/photos/originals \
  --copy-unique-dir biochar_app/data-processed/management/irrigation/photos/originals_unique
```

The destination must not already exist. This prevents accidental mixing of old
and new selections.

## Duplicate definition

Likely duplicates have both:

- the same `effective_datetime`; and
- the same six-digit `meter_reading`.

The retained copy is selected by:

1. highest pixel count;
2. largest file size;
3. filename as a stable final tie-break.

Rows without a usable timestamp or six-digit reading are retained for review
rather than deduplicated automatically.

## Required checks after each run

Inspect the audit JSON and verify:

- all expected review-workbook SHA-256 values matched inventory rows;
- `six_digit_unique_readings` is plausible;
- blank readings are limited to known non-reading or unresolved files;
- every suppressed follow-up conflict is understood;
- every ambiguous main-workbook filename family is understood;
- the output SHA-256 changed only when source data or logic changed.

For the 2026-07-23 reviewed data, the expected result is:

- 212 source inventory rows;
- 155 selected unique rows;
- 57 excluded likely duplicates;
- 37 likely-duplicate groups;
- 153 selected rows with six-digit readings;
- 2 selected rows without readings;
- no remaining `182023` reading;
- one retained `2023-05-23 19:19:29` reading of `162023`.

## Corrections ledger

Do not edit a generated CSV to fix a reading. Add a row to
`meter_photo_reading_corrections.csv`:

```text
sha256,filename,corrected_reading,reason
```

Then rerun the complete command. The correction remains visible and reviewable
in Git history.

## Testing

```bash
python -m unittest biochar_app/tests/test_finalize_meter_photo_inventory.py
```

The tests cover:

- main-workbook precedence over a conflicting follow-up rendition;
- explicit correction handling;
- propagation across photo renditions;
- selection of the highest-resolution duplicate;
- safe handling of ambiguous main-workbook filename families without guessing.

## Git and large files

Commit:

- the Python script;
- tests;
- this README material;
- the small corrections ledger;
- generated CSV/JSON products if the repository normally versions derived data.

Do not commit large reviewed `.xlsx` files through ordinary Git. Store them in
an approved data location or use Git LFS if workbook versioning is required.

## Downstream sequence

After finalizing the inventory:

1. Run `compare_meter_photos_to_irrigation.py` using
   `photo_inventory_unique.csv`.
2. Review boundary and volume discrepancies.
3. Build the camera-QC irrigation candidate.
4. Run the irrigation analysis with the `qc_candidate` variant.
5. Compare QC-candidate figures and reports with production outputs.

Never overwrite production analysis outputs while the QC candidate is under
review.
