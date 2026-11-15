# Private usage dashboard design

**Status:** phases 1–3 deployed to test and production on 2026-11-15

## Purpose

Add anonymous usage reporting to the existing FastAPI website while keeping the
dashboard separate and private. Reporting days use `America/Denver`. The public
website remains available without login.

The dashboard will distinguish:

- public visits and page views;
- feature interactions such as tab selections and plot requests;
- draft-paper downloads by version and file format;
- bulk research-data downloads by dataset, year, and granularity;
- successful and failed download requests; and
- internal or automated events excluded from public totals.

## Existing application routes

The production application is assembled in `biochar_app/scripts/app.py`, with
Nginx forwarding HTTPS traffic to Uvicorn at `127.0.0.1:8000`.

The repository review found these relevant paths:

| Current path | Role | Tracking implication |
|---|---|---|
| `/` | Single-page application entry | Record a page view and anonymous session visit. |
| `/static/documents/...pdf` | Embedded and directly linked draft PDF | Keep viewer fetches uncounted; move explicit downloads to a tracked route. |
| `/static/documents/...docx` | Direct draft Word download | Move the explicit download to the same tracked route family. |
| `/api/bulk_download` | Route used by the current bulk-download UI | Parse its manifest key after a successful response. |
| `/bulk_download/{dataset}/{year}` | Older direct ZIP routes | Instrument while retained so direct links are not missed. |
| `/api/download_plot_data` | Interactive plot-data download | Record as a feature/data export, separately from bulk research data. |
| `/api/download_summary_data` | Summary-data download | Record as a feature/data export, separately from bulk research data. |

There are two Python registrations for `/api/bulk_download`. Because the
router from `bulk_downloads.py` is included first, it is the route reached by
the current browser UI and is the first integration point. The duplicate route
should not be removed as part of analytics work without a separate route-cleanup
review.

The active bulk UI sends one manifest key per request. Examples include
`loggers_2026_daily`, `weather_2025_monthly`, `irrigation_all`, and
`soil_chem_all`. These keys provide the requested reporting dimensions without
examining ZIP contents.

## Privacy and counting rules

- Do not store IP addresses, raw user-agent strings, names, email addresses, or
  authenticated identities.
- Store a deployment-specific HMAC of a random browser-session token. The raw
  token remains only in the browser cookie.
- Count a visit once per session and count every successful `/` response as a
  page view.
- Store excluded events with `internal` or `bot` as the reason so exclusions can
  be audited, but omit them from public totals.
- Mark an internal browser through a signed cookie set only from the
  authenticated private dashboard.
- Classify only obvious automated user agents as bots. Uncertain traffic remains
  public rather than being silently discarded.
- Count a download when the application successfully prepares a download
  response. This does not prove that the visitor saved or opened the file.
- The embedded PDF reader and “Open PDF” action are document views, not draft
  downloads. Only the explicit PDF and Word download actions increment download
  totals.

## Storage

Runtime events are stored in a separate SQLite database at
`biochar_app/data-processed/usage/usage.sqlite3`. The directory is ignored by
Git and is separate from research, ETL, and irrigation-management data.

Each event contains:

- UTC timestamp and materialized Mountain Time date;
- event type and stable event name;
- anonymous session hash;
- success and response status;
- exclusion flag and reason; and
- optional paper or dataset dimensions.

The initial event volume is small enough to report directly from indexed event
rows. Daily rollup tables can be added later if retention or volume requires
them. Detailed events are privacy-minimized, so the first version can retain
them to preserve cumulative totals since tracking began.

## Private access

The proposed dashboard path is `/admin/usage`, with JSON and CSV endpoints
under `/api/admin/usage`. Initial authentication will use HTTP Basic credentials
read from environment variables and compared with constant-time checks.

The application must fail closed: if dashboard credentials are absent, private
endpoints return an unavailable response rather than becoming public. If the
separate signing secret is absent, internal-browser registration is unavailable
while authenticated read-only reporting remains accessible. Nginx Basic
authentication can be added as a second layer at deployment, but application
authentication remains required and testable.

No credential, cookie secret, or analytics database belongs in Git.

## Incremental implementation

### Phase 1 — isolated foundation

- SQLite event schema and indexes.
- Mountain Time date assignment and reporting queries.
- Anonymous session hashing.
- Transparent internal/bot exclusion classification.
- Tests for DST boundaries, public/excluded totals, download dimensions, and
  invalid records.

This phase is not imported by the FastAPI application and therefore does not
change public behavior.

### Phase 2 — collection and download instrumentation

- Add a short-lived anonymous session cookie at `/`.
- Add a small allowlisted public feature-event endpoint.
- Add tab-selection tracking and successful plot-request events.
- Serve explicit draft downloads through a tracked FastAPI route while leaving
  the embedded viewer uncounted.
- Instrument the active and retained bulk-download routes after response
  creation, recording failures separately.
- Add route-level tests proving viewer fetches are not downloads and dimensions
  are parsed correctly.

Implemented locally. Collection remains dormant unless a deployment-specific
session secret is configured. No production secret or database has been
created.

### Phase 3 — private dashboard and CSV

- Add fail-closed HTTP Basic authentication.
- Add today, yesterday, 7-day, 30-day, and cumulative summaries.
- Add paper and data-download breakdowns.
- Add CSV export using Mountain Time dates.
- Add an authenticated “mark this browser internal” action.
- Add tests for missing/wrong credentials, CSV contents, and exclusion-cookie
  behavior.

Implemented and deployed after browser review. Authentication remains
fail-closed, and test and production use separate credentials and signing
secrets supplied through protected environment files outside Git.

### Phase 4 — deployment review

Completed on 2026-11-15:

- reviewed the complete diff and local test results;
- deployed and verified the approved branch on `test.biocharresearch.org`;
- promoted commit `fd226a2` to `main` and deployed it to production;
- created separate protected test and production credentials and signing
  secrets outside Git;
- initialized owner-only SQLite storage on both servers; and
- verified authentication, public collection, draft-paper and bulk-data
  downloads, Mountain Time reporting, bot exclusion, and CSV export.
