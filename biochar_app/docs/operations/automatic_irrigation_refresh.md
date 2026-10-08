# Automatic irrigation refresh

The daily `--operational-update` now refreshes irrigation as well as logger and
weather products. No Microsoft account or OneDrive desktop application is needed.

Each run opens the shared-folder link in a fresh HTTP session and downloads the
master workbook by its stable OneDrive identifier. It validates required sheets
and performs a dry-run irrigation build before installing the workbook and
rebuilding the canonical irrigation CSV. Existing event IDs and established
photo-supported corrections remain in use. Blank future events are excluded.

If retrieval or validation fails, prior validated irrigation data remain in use.
Logger/weather publication continues, but the daily report records an irrigation
warning rather than claiming a successful management refresh. Inspect
`biochar_app/data-processed/management/irrigation/irrigation_refresh_status.json`.
This records refresh time, status, latest event, row count and workbook hash.
The existing build audit contains full build provenance.

`deploy.sh` transfers only the canonical irrigation CSV, build audit, and refresh
status to production, alongside the existing Parquet/download/cache products.
It does not copy research figures or management-photo files.

The shared-folder URL and workbook download URL live in
`biochar_app/config/data_sources.py`. Server-specific overrides are
`BIOCHAR_MASTER_SHARE_URL` and `BIOCHAR_MASTER_DOWNLOAD_URL` in the pipeline
environment. Change these if sharing is revoked or the workbook is replaced
with a new file identifier. Do not save browser cookies or Microsoft passwords.

To refresh irrigation alone, from the repository root using its Python environment:

```sh
python -c 'from biochar_app.scripts.management.refresh_irrigation import refresh_irrigation_products; print(refresh_irrigation_products())'
```

Check that the returned status is `refreshed` before deploying. A standalone
snapshot refresh is also available with
`python -m biochar_app.scripts.management.update_master_workbook_snapshot`.
Use `--source PATH` only when deliberately installing a local workbook.

After pulling on test, run the irrigation-only refresh above, restart the test
website, and check the daily plot across September 30, 2026. After approval,
merge/pull the code on production and publish from test with `deploy.sh`.
