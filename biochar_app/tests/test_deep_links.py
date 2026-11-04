from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DEEP_LINKS = REPO_ROOT / "biochar_app/static/js/deep_links.js"
MAIN_JS = REPO_ROOT / "biochar_app/static/js/main.js"
TEMPLATE = REPO_ROOT / "biochar_app/templates/index.html"


def test_every_deep_link_button_exists_in_page_template() -> None:
    source = DEEP_LINKS.read_text(encoding="utf-8")
    template = TEMPLATE.read_text(encoding="utf-8")

    button_ids = set(re.findall(r'buttonId:\s*"([^"]+)"', source))
    assert button_ids
    for button_id in button_ids:
        assert f'id="{button_id}"' in template


def test_deep_links_are_initialized_after_tab_renderers_are_wired() -> None:
    source = MAIN_JS.read_text(encoding="utf-8")

    assert 'from "./deep_links.js?v=20261104-deep-links-1"' in source
    assert source.index("wireTabRender({") < source.index("initializeDeepLinks();")


def test_soil_biology_report_anchor_is_stable_and_documented() -> None:
    source = DEEP_LINKS.read_text(encoding="utf-8")
    documentation = (
        REPO_ROOT / "biochar_app/docs/operations/website_deep_links.md"
    ).read_text(encoding="utf-8")

    assert 'paneId: "soilbio"' in source
    assert "#soilbio-total-living-microbial-biomass" in documentation
    assert "MutationObserver" in source
