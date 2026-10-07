"""Native browser controls: apply snapshots and seasonal year selection."""
from pathlib import Path
import re

import pytest


def test_apply_seasons_and_change_plot_year_without_changing_resolution():
    playwright = pytest.importorskip("playwright.sync_api")
    root = Path(__file__).resolve().parents[1]
    with playwright.sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
        except playwright.Error as exc:
            pytest.skip(f"Local Chromium unavailable: {exc}")
        try:
            page = browser.new_page()
            page.set_content((root / "templates/_custom_gseason.html").read_text() + '''
              <select id="main-year"><option>2026</option><option>2025</option></select>
              <select id="main-granularity"><option value="gseason">Seasonal Periods</option></select>
              <input id="main-startDate" value="invalid"><input id="main-endDate" value="invalid">
            ''')
            for name in ("ui_controls.js", "custom_gseason.js"):
                source = (root / "static/js" / name).read_text()
                page.add_script_tag(content=re.sub(r"^export ", "", source, flags=re.M))
            page.evaluate('''() => initCustomGseason({defaultYear:2026,years:[2026,2025],defaultPeriods:[
              {code:"WINTER",label:"Winter",start:"11-01",end:"03-31"},
              {code:"GROWING",label:"Growing",start:"04-01",end:"10-31"}]})''')
            page.select_option("#anchor-year", "2025")
            page.locator(".period-start").first.fill("2024-10-01")
            page.locator("#apply-seasons").click()
            assert "Seasons applied" in page.locator("#apply-seasons-status").inner_text()
            snapshot = page.evaluate("getCustomSeasonPeriods()")
            assert snapshot[0]["start"] == "2024-10-01"
            page.locator(".period-start").first.fill("2024-10-02")
            assert page.evaluate("getCustomSeasonPeriods()")[0]["start"] == "2024-10-01"
            page.select_option("#main-year", "2025")
            filters = page.evaluate("getSelectedFilters('main')")
            assert filters["year"] == "2025"
            assert filters["granularity"] == "gseason"
            assert filters["periodsAnchorYear"] == 2025
            page.select_option("#main-year", "2026")
            assert page.evaluate("getSelectedFilters('main')")["year"] == "2026"
        finally:
            browser.close()
