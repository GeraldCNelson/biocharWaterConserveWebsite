"""Application placement for Word-authored website content.

Word is the source of truth for document text, headings, hyperlinks, tables,
embedded images, and captions. ``convert_word_to_html.py`` extracts those
features directly, converts embedded raster images to WebP, and derives the
output filename from the Word filename unless an override is specified here.

This module retains only information Word cannot know: which application DOM
container receives a document and the few intentional output-name overrides.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, TypedDict


class DocumentSpec(TypedDict, total=False):
    source: str
    output: str


# Main page and tab content. Keys are DOM container IDs in index.html.
DOCUMENT_PAGES: dict[str, DocumentSpec] = {
    "intro-content": {"source": "intro.docx"},
    "experiment-content": {"source": "experimentDesign.docx"},
    "tech-content": {
        "source": "techDetails_updated.docx",
        "output": "techDetails.md",
    },
    "acknowledgements-content": {"source": "acknowledgements.docx"},
}


# Help documents displayed in application modals.
MODAL_DOCUMENTS: dict[str, DocumentSpec] = {
    "modal-main-help": {"source": "help_main.docx"},
    "modal-summary-help": {"source": "help_summary.docx"},
}


def output_name_for_spec(spec: DocumentSpec) -> str:
    """Return the configured output name or derive it from the DOCX stem."""
    explicit_output = spec.get("output")
    if explicit_output:
        return explicit_output
    return Path(spec["source"]).with_suffix(".md").name


def iter_document_specs() -> Iterable[DocumentSpec]:
    """Yield each configured source document once, in application order."""
    seen_sources: set[str] = set()
    for mapping in (DOCUMENT_PAGES, MODAL_DOCUMENTS):
        for spec in mapping.values():
            source = spec["source"]
            if source in seen_sources:
                continue
            seen_sources.add(source)
            yield spec


def build_markdown_mapping() -> dict[str, str]:
    """Map application container IDs to generated content URLs."""
    mapping: dict[str, str] = {}
    for document_mapping in (DOCUMENT_PAGES, MODAL_DOCUMENTS):
        for container_id, spec in document_mapping.items():
            mapping[container_id] = f"/markdown/{output_name_for_spec(spec)}"
    return mapping
