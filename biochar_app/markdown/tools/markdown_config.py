"""
===============================================================================
markdown_config.py  —  SINGLE SOURCE OF TRUTH for Markdown generation
===============================================================================

PURPOSE
-------
This file defines EVERYTHING the system needs to convert Word (.docx) documents
into Markdown (.md) and load that Markdown into index.html.

It is the authoritative configuration for:

  • Which .docx files exist and how they convert to .md
  • Output filenames for the generated Markdown
  • Optional embedded images, captions, and side-by-side layouts
  • Mapping DOM container IDs → Markdown URLs for frontend loading
  • Modal help documents (Main Data Display / Summary Statistics)
  • Keeping index.html, main.js, FastAPI, and create_markdown.py in sync

If Markdown isn't appearing in the browser, start troubleshooting HERE.


DIRECTORY STRUCTURE (as of 2025)
--------------------------------
Source Word documents (input):

    biochar_app/markdown/docx/

Generated Markdown (output):

    biochar_app/markdown/

Static images referenced inside Markdown:

    biochar_app/static/images/


HOW THE PIPELINE WORKS (Critical for Future Jerry)
---------------------------------------------------
1. YOU define the .docx → .md mapping here in:
       docx_markdown_config

2. create_markdown.py reads THIS FILE ONLY
   and converts the .docx to cleaned Markdown.

3. FastAPI exposes the Markdown paths to the frontend via something like:
       /api/markdown_files
   using DOM_MARKDOWN_MAP below.

4. main.js (or config.js) loads Markdown dynamically into index.html using:
       loadMarkdownContent()
       and the mapping returned by /api/markdown_files

5. index.html never references .docx directly.
   It ONLY knows about the .md URLs (e.g. "/markdown/intro.md").


CONFIG SECTIONS
---------------
1. docx_markdown_config
      • Lists every .docx → .md conversion.
      • Optional "images" entries insert <figure> blocks with captions.
      • Optional "side_by_side" entries insert 2-column comparison tables.
      • If a .docx isn’t listed here, it will NOT be converted.

2. modal_config
      • Maps modal .docx → modal .md.
      • These appear in pop-up “Directions” modals in the Main and Summary tabs.

3. build_markdown_mapping()
      • Maps DOM container IDs in index.html
        to the final Markdown URLs the site should load.
      • Example:
            "intro-content": "/markdown/intro.md"
        means:
            index.html → <div id="intro-content">
            will receive intro.md at runtime.

4. No other file should hard-code Markdown filenames.
   This module is the single source of truth for:
     - which .md files exist, and
     - which DOM container IDs they populate.


FUTURE JERRY — ADDING A NEW DOCX FILE
-------------------------------------
1. Place new_file.docx into:
       biochar_app/markdown/docx/

2. Add an entry in docx_markdown_config:
       "new_file.docx": { "output_md": "new_file.md" }

3. Run:
       python biochar_app/scripts/create_markdown.py

4. Add new mapping to build_markdown_mapping():
       "new-section-id": "/markdown/new_file.md"

5. Add matching <div id="new-section-id"> in index.html.

6. Reload website.

If all three pieces match (docx config → mapping → HTML ID), it will work.


SANITY CHECKS / SPECIAL NOTES
-----------------------------
• techDetails_updated.docx is the active Technical Details source.
  techDetails.docx is legacy and intentionally ignored.

• help_main.docx and help_summary.docx are the sources
  for the Directions modals in the Main and Summary tabs.

• All images referenced here must already exist in:
      biochar_app/static/images/

• If Markdown appears blank in the browser:
      1) Check build_markdown_mapping()
      2) Check output_md names in docx_markdown_config
      3) Check index.html container IDs

This file keeps ALL Markdown and modal dependencies synchronized.
===============================================================================
"""

from __future__ import annotations

from typing import Dict, List, TypedDict

# ---------------------------------------------------------------------------
# Typed config shapes (fixes mypy "object is not indexable")
# ---------------------------------------------------------------------------

class ImageSpec(TypedDict):
    file: str
    caption: str
    alt: str
    title: str


class DocxConfig(TypedDict, total=False):
    # required
    output_md: str
    # optional
    images: List[ImageSpec]
    side_by_side: List[List[str]]


DocxMarkdownConfig = Dict[str, DocxConfig]


class ModalSpec(TypedDict):
    source: str
    output: str


ModalConfig = Dict[str, ModalSpec]


# ---------------------------------------------------------------------------
# Main page Markdown sources (.docx → .md)
# ---------------------------------------------------------------------------

docx_markdown_config: DocxMarkdownConfig = {
    "intro.docx": {
        "output_md": "intro.md",
        "images": [
            {
                "file": "biocharMicro1.webp",
                "caption": "Figure 1. Scanning electron microscope image of biochar",
                "alt": "Scanning electron microscope image of biochar",
                "title": "Scanning electron microscope image of biochar",
            },
            {
                "file": "lignin_diagram.webp",
                "caption": "Figure 2. Lignin chemical structure",
                "alt": "Diagram showing lignin structure",
                "title": "Diagram showing lignin structure",
            },
            {
                "file": "biochar_diagram.webp",
                "caption": "Figure 3. Biochar chemical structure",
                "alt": "Diagram showing biochar chemical structure",
                "title": "Diagram showing biochar chemical structure",
            },
        ],
        "side_by_side": [
            ["lignin_diagram.webp", "biochar_diagram.webp"],
        ],
    },
    "experimentDesign.docx": {
        "output_md": "experimentDesign.md",
        "images": [
            {
                "file": "biocharExperimentalDesign.webp",
                "caption": "Figure 1: Field experimental layout",
                "alt": "Layout of biochar plots",
                "title": "Layout of biochar plots",
            },
            {
                "file": "biochar_closeup.webp",
                "caption": "Figure 2: Closeup image of biochar material",
                "alt": "Closeup of biochar material",
                "title": "Closeup of biochar material",
            },
        ],
    },
    # ACTIVE technical details file
    "techDetails_updated.docx": {
        "output_md": "techDetails.md",
        "images": [],
    },

    # acknowledgements file
    "acknowledgements.docx": {
        "output_md": "acknowledgements.md",
        "images": [],
    },
}


# ---------------------------------------------------------------------------
# Modal help Markdown sources (.docx → .md)
# ---------------------------------------------------------------------------

modal_config: ModalConfig = {
    "main": {
        "source": "help_main.docx",
        "output": "help_main.md",
    },
    "summary": {
        "source": "help_summary.docx",
        "output": "help_summary.md",
    },
}


# ---------------------------------------------------------------------------
# DOM container → Markdown URL mapping
# ---------------------------------------------------------------------------

def build_markdown_mapping() -> dict[str, str]:
    """
    Return a mapping from DOM container IDs (index.html) to Markdown URLs.

    This is the single source of truth used by:
      - the FastAPI route that exposes /api/markdown_files
      - frontend JS (main.js) which calls loadMarkdownContent(id, url)

    If you add a new .docx → .md pair in docx_markdown_config or modal_config,
    you only need to:
      1) wire it into this mapping, and
      2) ensure the HTML container ID exists in index.html.
    """
    return {
        # Main tabs
        "intro-content": f"/markdown/{docx_markdown_config['intro.docx']['output_md']}",
        "experiment-content": f"/markdown/{docx_markdown_config['experimentDesign.docx']['output_md']}",
        "tech-content": f"/markdown/{docx_markdown_config['techDetails_updated.docx']['output_md']}",
        "acknowledgements-content": f"/markdown/{docx_markdown_config['acknowledgements.docx']['output_md']}",
        # Modals
        "modal-main-help": f"/markdown/{modal_config['main']['output']}",
        "modal-summary-help": f"/markdown/{modal_config['summary']['output']}",
    }