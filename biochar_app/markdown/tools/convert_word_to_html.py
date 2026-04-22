#!/usr/bin/env python3
"""
convert_word_to_html.py

Convert Word (.docx) files in biochar_app/markdown/docx/ to HTML (via pandoc),
then post-process:
- inject CSS
- rewrite image src paths based on markdown_config.docx_markdown_config
- normalize figure/table captions with numbering
- write output HTML content into biochar_app/markdown/outputs_md/

Notes:
- This script writes HTML content (not Markdown) into .md files, matching the
  current workflow where serve_markdown() serves converted HTML.
- CRITICAL: Uses biochar_app.markdown.tools.markdown_config as the single
  source of truth.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Optional, cast

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from biochar_app.config.core import TAB_LINKS
from biochar_app.markdown.tools.markdown_config import (
    DocxConfig,
    docx_markdown_config,
    modal_config,
)


# ---------------------------------------------------------------------
# Paths (robust to script location inside biochar_app)
# ---------------------------------------------------------------------

HERE = Path(__file__).resolve()

try:
    BIOCHAR_APP = next(p for p in [HERE.parent, *HERE.parents] if p.name == "biochar_app")
except StopIteration as exc:
    raise RuntimeError(
        f"Could not locate 'biochar_app' in parents of: {HERE}"
    ) from exc

MARKDOWN_DIR = BIOCHAR_APP / "markdown"
DOCX_DIR = MARKDOWN_DIR / "docx"
OUTPUTS_MD_DIR = MARKDOWN_DIR / "outputs_md"
OUTPUTS_HTML_DIR = MARKDOWN_DIR / "outputs_html"

for d in [DOCX_DIR, OUTPUTS_MD_DIR, OUTPUTS_HTML_DIR]:
    d.mkdir(parents=True, exist_ok=True)

print(f"BIOCHAR_APP: {BIOCHAR_APP}")
print(f"MARKDOWN_DIR: {MARKDOWN_DIR}")
print(f"DOCX_DIR: {DOCX_DIR}")
print(f"OUTPUTS_MD_DIR: {OUTPUTS_MD_DIR}")
print(f"OUTPUTS_HTML_DIR: {OUTPUTS_HTML_DIR}")


# ---------------------------------------------------------------------
# Pandoc CSS
# ---------------------------------------------------------------------

pandoc_css: str = """
html {
  color: #1a1a1a;
  background-color: #fdfdfd;
}
body {
  margin: 0 auto;
  max-width: 1500px;
  padding-left: 50px;
  padding-right: 50px;
  padding-top: 50px;
  padding-bottom: 50px;
  hyphens: auto;
  overflow-wrap: break-word;
  text-rendering: optimizeLegibility;
  font-kerning: normal;
  font-family: Georgia, serif;
}
img { max-width: 100%; }
table {
  width: 100%;
  border-collapse: collapse;
  margin: 1em auto;
  display: table;
}
th, td {
  padding: 0.5em;
  text-align: center;
  border: 1px solid #ddd;
}
figcaption, caption {
  font-style: italic;
  text-align: center;
  margin-top: 0.5em;
}
.tab-link {
  color: #2c5aa0;
  text-decoration: none;
  font-weight: 500;
  cursor: pointer;
}
.tab-link:hover {
  color: #1f3f73;
  text-decoration: underline;
}
""".strip()


# ---------------------------------------------------------------------
# Soup helpers
# ---------------------------------------------------------------------

def _ensure_head(soup: BeautifulSoup) -> Tag:
    head = soup.head
    if head is None:
        new_head = soup.new_tag("head")
        soup.insert(0, new_head)
        head = soup.head
    return cast(Tag, head)


def _inject_css(soup: BeautifulSoup) -> None:
    head = _ensure_head(soup)
    style_tag = soup.new_tag("style")
    style_tag.string = pandoc_css
    head.append(style_tag)


def inject_tab_links(soup: BeautifulSoup) -> None:
    """Replace known tab labels with clickable links in <p> and <li> text."""
    for tag in soup.find_all(["p", "li"]):
        if not isinstance(tag, Tag):
            continue

        html = str(tag)
        original_html = html

        for label, tab_id in TAB_LINKS.items():
            if f'data-tab="{tab_id}"' in html:
                continue

            link_html = f'<a href="#" class="tab-link" data-tab="{tab_id}">{label}</a>'
            html = html.replace(label, link_html)

        if html != original_html:
            replacement = BeautifulSoup(html, "html.parser")
            if replacement.body:
                tag.replace_with(*replacement.body.contents)
            else:
                tag.replace_with(*replacement.contents)


def _rewrite_images(soup: BeautifulSoup, cfg: DocxConfig) -> None:
    """
    Rewrite <img src="..."> to /static/images/<file> using the ordered
    list from cfg["images"].
    """
    images = cfg.get("images") or []
    if not images:
        return

    img_tags = soup.find_all("img")
    for img_tag, spec in zip(img_tags, images):
        if not isinstance(img_tag, Tag):
            continue
        file_name = spec.get("file")
        if not file_name:
            continue
        img_tag.attrs["src"] = f"/static/images/{file_name}"


# ---------------------------------------------------------------------
# Caption normalization helpers
# ---------------------------------------------------------------------

_FIGURE_PREFIX_RE = re.compile(r"^\s*figure\b", flags=re.IGNORECASE)
_TABLE_PREFIX_RE = re.compile(r"^\s*table\b", flags=re.IGNORECASE)


def _clean_caption_remainder(text: str, label: str) -> str:
    """
    Normalize caption text so these all become a clean remainder:

    - "Figure 1. Caption"
    - "Figure 1 Caption"
    - "Figure. Caption"
    - ". Caption"
    - ": Caption"
    - "Caption"

    Returns only the caption body, without the leading Figure/Table label.
    """
    cleaned = " ".join(text.replace("\xa0", " ").split()).strip()
    if not cleaned:
        return ""

    prefix_re = _FIGURE_PREFIX_RE if label.lower() == "figure" else _TABLE_PREFIX_RE

    if prefix_re.match(cleaned):
        cleaned = prefix_re.sub("", cleaned, count=1).strip()

    cleaned = re.sub(r"^\d+\s*", "", cleaned).strip()
    cleaned = re.sub(r"^[\.\:\;\-\–\—\)\]]+\s*", "", cleaned).strip()
    cleaned = re.sub(r"^\d+\s*", "", cleaned).strip()
    cleaned = re.sub(r"^[\.\:\;\-\–\—\)\]]+\s*", "", cleaned).strip()

    return cleaned


def _format_numbered_caption(label: str, number: int, text: str) -> str:
    remainder = _clean_caption_remainder(text, label)
    if remainder:
        return f"{label} {number}. {remainder}"
    return f"{label} {number}."


def _tag_contains_only_image(tag: Tag) -> bool:
    """
    True for tags like <p><img ...></p> and false for mixed content.
    """
    has_img = False

    for child in tag.contents:
        if isinstance(child, NavigableString):
            if str(child).strip():
                return False
            continue

        if not isinstance(child, Tag):
            return False

        if child.name == "img":
            has_img = True
            continue

        return False

    return has_img


def _tag_contains_any_image(tag: Tag) -> bool:
    return tag.find("img") is not None


def _looks_like_figure_caption_text(text: str) -> bool:
    """
    Heuristic for paragraph captions associated with images.

    Accept things like:
    - "Figure 2. Lignin chemical structure"
    - ". Lignin chemical structure"
    - "Lignin chemical structure"

    Reject empty strings and likely normal prose.
    """
    cleaned = " ".join(text.replace("\xa0", " ").split()).strip()
    if not cleaned:
        return False

    if _FIGURE_PREFIX_RE.match(cleaned):
        return True

    if re.match(r"^[\.\:\;\-\–\—]+\s*\S", cleaned):
        return True

    word_count = len(cleaned.split())
    if 1 <= word_count <= 12 and cleaned[0].isupper():
        return True

    return False


def _normalize_figure_captions(soup: BeautifulSoup) -> int:
    """
    Normalize true <figure><figcaption>...</figcaption></figure> captions.

    Returns the next figure number after finishing.
    """
    figure_count = 1

    for fig in soup.find_all("figure"):
        if not isinstance(fig, Tag):
            continue

        cap = fig.find("figcaption")
        if not isinstance(cap, Tag):
            continue

        text = cap.get_text(" ", strip=True)
        if not text:
            continue

        cap.string = _format_numbered_caption("Figure", figure_count, text)
        figure_count += 1

    return figure_count


def _normalize_table_captions(soup: BeautifulSoup) -> None:
    table_count = 1

    for tbl in soup.find_all("table"):
        if not isinstance(tbl, Tag):
            continue

        cap = tbl.find("caption")
        if not isinstance(cap, Tag):
            continue

        text = cap.get_text(" ", strip=True)
        if not text:
            continue

        cap.string = _format_numbered_caption("Table", table_count, text)
        table_count += 1


def _normalize_paragraph_image_captions(
    soup: BeautifulSoup,
    starting_figure_count: int,
) -> int:
    """
    Normalize captions that Pandoc emitted as ordinary paragraphs instead of
    <figcaption>, including captions inside table cells.

    Pattern handled:
    - a paragraph/tag containing only an image
    - followed by a paragraph with caption-like text
    """
    figure_count = starting_figure_count

    candidate_containers = soup.find_all(["body", "td", "th", "div"])
    for container in candidate_containers:
        if not isinstance(container, Tag):
            continue

        children = [child for child in container.children if isinstance(child, Tag)]
        i = 0

        while i < len(children) - 1:
            current = children[i]
            nxt = children[i + 1]

            if (
                current.name == "p"
                and nxt.name == "p"
                and _tag_contains_only_image(current)
                and not _tag_contains_any_image(nxt)
            ):
                caption_text = nxt.get_text(" ", strip=True)
                if _looks_like_figure_caption_text(caption_text):
                    nxt.string = _format_numbered_caption(
                        "Figure",
                        figure_count,
                        caption_text,
                    )
                    figure_count += 1
                    i += 2
                    continue

            i += 1

    return figure_count


def _normalize_all_captions(soup: BeautifulSoup) -> None:
    next_figure_count = _normalize_figure_captions(soup)
    _normalize_paragraph_image_captions(
        soup,
        starting_figure_count=next_figure_count,
    )
    _normalize_table_captions(soup)


# ---------------------------------------------------------------------
# Config lookup
# ---------------------------------------------------------------------

def _output_name_for_docx(filename: str) -> str:
    """
    Decide output filename (.md) for a given docx.
    Priority:
      1) docx_markdown_config entry (output_md)
      2) modal_config entry (output)
      3) default: <stem>.md
    """
    if filename in docx_markdown_config:
        return docx_markdown_config[filename]["output_md"]

    for spec in modal_config.values():
        if spec.get("source") == filename:
            return spec["output"]

    return Path(filename).with_suffix(".md").name


def _cfg_for_docx(filename: str) -> Optional[DocxConfig]:
    return docx_markdown_config.get(filename)


# ---------------------------------------------------------------------
# Main conversion loop
# ---------------------------------------------------------------------

def main() -> int:
    docx_files = sorted(
        [
            p
            for p in DOCX_DIR.iterdir()
            if p.suffix.lower() == ".docx" and not p.name.startswith("~$")
        ]
    )

    if not docx_files:
        print(f"⚠️ No .docx files found in: {DOCX_DIR}")
        return 0

    for docx_path in docx_files:
        filename = docx_path.name
        output_name = _output_name_for_docx(filename)
        out_path = OUTPUTS_MD_DIR / output_name

        print(f"\n📄 Converting {filename} → {output_name}")

        try:
            result = subprocess.run(
                ["pandoc", str(docx_path), "-f", "docx", "-t", "html", "--wrap=none"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                text=True,
            )

            soup = BeautifulSoup(result.stdout, "html.parser")
            _inject_css(soup)

            cfg = _cfg_for_docx(filename)
            if cfg is not None:
                _rewrite_images(soup, cfg)

            _normalize_all_captions(soup)
            inject_tab_links(soup)

            html = str(soup)
            out_path.write_text(html, encoding="utf-8")
            print(f"✅ Saved cleaned HTML to: {out_path}")

        except subprocess.CalledProcessError as e:
            err = (e.stderr or "").strip()
            print(f"❌ Pandoc failed for {filename}: {err or e}")
        except Exception as e:
            print(f"❌ Failed to convert {filename}: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())