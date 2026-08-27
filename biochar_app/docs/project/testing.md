# Smoke Tests

## Purpose

Verify that major website functions still work after code changes.

These tests use Playwright to drive a real browser and confirm that key user workflows continue to function.

## Current Tests

- Home page load
- Browser console error check
- Soil biology bulk download
- Logger bulk download
- Raw plot data download

## Run Tests

From the project root:

```bash
python biochar_app/tests/playwright_smoke.py
```
Successful execution should end with:

✅ Playwright smoke test completed

## Project Inventory and Deployment Checks

Run the complete Python test suite from the project root:

```bash
python -m pytest biochar_app/tests -q
```

Before committing a deployment, confirm that required Git-delivered files are
present and tracked:

```bash
python biochar_app/scripts/dev-tools/check_deployment_requirements.py --git-only
```

On a test or production server, run the complete check after transferring the
external generated data directories:

```bash
python biochar_app/scripts/dev-tools/check_deployment_requirements.py
```

Regenerate the searchable documentation and function inventories after adding
documentation or substantially changing the code structure:

```bash
python biochar_app/scripts/dev-tools/build_documentation_catalog.py
python biochar_app/scripts/dev-tools/build_function_catalog.py
```

## Installation of required software

In main project terminal run

```bash
pip install playwright
playwright install
```
