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

## Installation of required software

In main project terminal run

```bash
pip install playwright
playwright install
```
