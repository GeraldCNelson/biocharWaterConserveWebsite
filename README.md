# Biochar Water Conservation Website
Code to create a website that displays data from a research project to examine the water-holding capacity of biochar in an irrigated pasture field in Western Colorado.
## Biochar Water Conservation Dashboard

FastAPI-based dashboard + data pipeline for the Biochar Fruita CSU experiment.

## Repo layout

- `biochar_app/` — application code
  - `biochar_app/scripts/routes.py` — FastAPI routes (API + page endpoints)
  - `biochar_app/scripts/config.py` — constants (YEARS, PARQUET_DIR, etc.)
  - `biochar_app/templates/` — Jinja templates
- `biochar_app/data-processed/` — generated artifacts (NOT tracked in git)
  - `parquet/` — internal Parquet datasets used by the app
  - `downloads/` — prebuilt ZIPs (CSV) for bulk download endpoints

## Local dev

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip wheel
pip install -r requirements.txt
uvicorn biochar_app.scripts.app:app --reload --host 127.0.0.1 --port 8000

More details to come...