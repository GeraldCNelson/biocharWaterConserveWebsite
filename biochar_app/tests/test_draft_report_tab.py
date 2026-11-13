from __future__ import annotations

import re
from pathlib import Path
from zipfile import ZipFile


REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = REPO_ROOT / "biochar_app/templates/index.html"
DEEP_LINKS = REPO_ROOT / "biochar_app/static/js/deep_links.js"
PDF_VIEWER = REPO_ROOT / "biochar_app/static/js/report_pdf_viewer.js"
PDFJS = REPO_ROOT / "biochar_app/static/vendor/pdfjs/pdf.min.mjs"
PDFJS_WORKER = REPO_ROOT / "biochar_app/static/vendor/pdfjs/pdf.worker.min.mjs"
DOCUMENTS = REPO_ROOT / "biochar_app/static/documents"
PDF_NAME = "VPI_Biochar_Report_US_Units_2026-11-13.pdf"
DOCX_NAME = "VPI_Biochar_Report_US_Units_2026-11-13.docx"


def test_introduction_is_first_and_draft_report_remains_second() -> None:
    source = TEMPLATE.read_text(encoding="utf-8")
    first_tab = re.search(r'<button id="([^"]+)" class="nav-link active"', source)

    assert first_tab is not None
    assert first_tab.group(1) == "intro-tab"
    assert source.index('id="intro-tab"') < source.index('id="draft-report-tab"')
    assert source.index('id="draft-report-tab"') < source.index(
        'id="what-is-biochar-tab"'
    )
    assert 'id="draft-report" role="tabpanel"' in source
    assert 'id="intro" role="tabpanel"' in source


def test_draft_report_has_stable_deep_link() -> None:
    source = DEEP_LINKS.read_text(encoding="utf-8")

    assert '"draft-report": {' in source
    assert 'buttonId: "draft-report-tab"' in source
    assert 'paneId: "draft-report"' in source


def test_report_assets_and_feedback_address_are_wired() -> None:
    source = TEMPLATE.read_text(encoding="utf-8")
    pdf_path = DOCUMENTS / PDF_NAME
    docx_path = DOCUMENTS / DOCX_NAME

    assert PDF_NAME in source
    assert DOCX_NAME in source
    assert 'class="draft-report-reader"' in source
    assert "data-pdf-url=" in source
    assert "data-pdf-pages" in source
    assert "report_pdf_viewer.js" in source
    assert "Current draft dated November 13, 2026" in source
    assert 'mailto:info@biocharresearch.org' in source
    assert pdf_path.read_bytes().startswith(b"%PDF-")
    with ZipFile(docx_path) as archive:
        assert "word/document.xml" in archive.namelist()


def test_embedded_pdf_reader_assets_are_available() -> None:
    viewer_source = PDF_VIEWER.read_text(encoding="utf-8")

    assert 'from "../vendor/pdfjs/pdf.min.mjs"' in viewer_source
    assert "pdf.worker.min.mjs" in viewer_source
    assert "getDocument({ url: reader.dataset.pdfUrl })" in viewer_source
    assert "new ResizeObserver" in viewer_source
    assert "new IntersectionObserver" in viewer_source
    assert "viewportElement.addEventListener(\"scroll\"" in viewer_source
    assert PDFJS.stat().st_size > 100_000
    assert PDFJS_WORKER.stat().st_size > 500_000
