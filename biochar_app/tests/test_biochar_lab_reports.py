from biochar_app.config.biochar_lab_reports import (
    BIOCHAR_LAB_REPORTS,
    biochar_lab_report_path,
)
from biochar_app.config.paths import BASE_DIR


def test_configured_biochar_lab_reports_exist_and_are_pdfs() -> None:
    assert set(BIOCHAR_LAB_REPORTS) == {
        "control-laboratories-2022",
        "wyoming-analytical-2020",
    }

    for report_key, report in BIOCHAR_LAB_REPORTS.items():
        path = biochar_lab_report_path(report_key)
        assert path.is_file()
        assert path.suffix.lower() == ".pdf"
        assert report["download_name"].endswith(".pdf")


def test_biochar_subtab_and_report_links_are_present() -> None:
    template = (BASE_DIR / "templates" / "index.html").read_text(encoding="utf-8")

    assert 'id="biochar-analysis-tab"' in template
    assert 'data-bs-target="#biochar-analysis"' in template
    assert 'id="biochar-analysis"' in template
    for report_key in BIOCHAR_LAB_REPORTS:
        assert f'/lab-reports/biochar/{report_key}"' in template
        assert f'/lab-reports/biochar/{report_key}?download=true' in template
