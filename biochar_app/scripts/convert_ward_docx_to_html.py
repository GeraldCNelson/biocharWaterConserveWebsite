#!/usr/bin/env python3
"""
biochar_app/scripts/convert_ward_docx_to_html.py

Convert Ward reference DOCX files into intermediate HTML files for the
website reference pipeline.

Workflow:
    data-processed/ward-docx/*.docx
        -> pandoc
        -> light HTML cleanup / anchor generation
        -> data-processed/ward-html/*.html

This script is intentionally separate from convert_word_to_html.py because
the Ward reference workflow has different goals:
- preserve document structure
- add stable heading anchors for source_url links
- add table anchors like #table-36
- produce intermediate HTML for later cleanup / publication

Recommended final publication flow:
    data-processed/ward-html/*.html
        -> manual cleanup if needed
        -> templates/lab_references/*.html
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Dict, Iterable

from bs4 import BeautifulSoup
from bs4.element import Tag


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

HERE = Path(__file__).resolve()
BIOCHAR_APP = HERE.parents[1]

WARD_DOCX_DIR = BIOCHAR_APP / "data-processed" / "ward-docx"
WARD_HTML_DIR = BIOCHAR_APP / "data-processed" / "ward-html"

WARD_DOCX_DIR.mkdir(parents=True, exist_ok=True)
WARD_HTML_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

WARD_HTML_CSS = """
body {
  max-width: 960px;
  margin: 0 auto;
  padding: 24px;
  line-height: 1.6;
}

img {
  max-width: 100%;
  height: auto;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin: 1em 0;
}

th, td {
  border: 1px solid #d0d0d0;
  padding: 0.5em 0.6em;
  vertical-align: top;
}

caption, figcaption {
  font-style: italic;
  margin-top: 0.4em;
}

.lab-reference-content h1,
.lab-reference-content h2,
.lab-reference-content h3,
.lab-reference-content h4 {
  scroll-margin-top: 90px;
}
""".strip()

# Optional explicit output names for known files
OUTPUT_NAME_MAP: Dict[str, str] = {
    "WardGuide-Master-20211118.docx": "ward_guide.html",
    "SHA-Guide-FINAL-May.docx": "soil_health_guide.html",
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def slugify(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s\-:]", "", text)
    text = text.replace(":", "")
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")


def unique_slug(base: str, seen: set[str]) -> str:
    slug = base or "section"
    if slug not in seen:
        seen.add(slug)
        return slug

    i = 2
    while f"{slug}-{i}" in seen:
        i += 1
    new_slug = f"{slug}-{i}"
    seen.add(new_slug)
    return new_slug


def ensure_head(soup: BeautifulSoup) -> Tag:
    head = soup.head
    if head is None:
        new_head = soup.new_tag("head")
        if soup.html:
            soup.html.insert(0, new_head)
        else:
            soup.insert(0, new_head)
        head = soup.head
    return head  # type: ignore[return-value]


def ensure_body(soup: BeautifulSoup) -> Tag:
    body = soup.body
    if body is None:
        new_body = soup.new_tag("body")
        if soup.html:
            soup.html.append(new_body)
        else:
            soup.append(new_body)
        body = soup.body
    return body  # type: ignore[return-value]


def inject_css(soup: BeautifulSoup) -> None:
    head = ensure_head(soup)
    style_tag = soup.new_tag("style")
    style_tag.string = WARD_HTML_CSS
    head.append(style_tag)


def wrap_body_content(soup: BeautifulSoup) -> None:
    body = ensure_body(soup)
    wrapper = soup.new_tag("div", attrs={"class": "lab-reference-content"})

    children = list(body.contents)
    for child in children:
        wrapper.append(child.extract())

    body.append(wrapper)


def add_heading_ids(soup: BeautifulSoup) -> None:
    seen: set[str] = set()

    for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
        if not isinstance(tag, Tag):
            continue

        text = tag.get_text(separator=" ", strip=True)
        if not text:
            continue

        if tag.has_attr("id") and str(tag["id"]).strip():
            seen.add(str(tag["id"]).strip())
            continue

        slug = slugify(text)
        tag["id"] = unique_slug(slug, seen)


def add_table_ids_from_captions(soup: BeautifulSoup) -> None:
    import re

    seen = set()

    for table in soup.find_all("table"):
        caption = table.find("caption")

        if caption:
            text = caption.get_text(strip=True)
        else:
            # fallback: look at preceding paragraph
            prev = table.find_previous("p")
            text = prev.get_text(strip=True) if prev else ""

        match = re.search(r"\btable\s+(\d+)\b", text, re.IGNORECASE)
        if not match:
            continue

        table_id = f"table-{match.group(1)}"

        if table_id in seen:
            continue

        table["id"] = table_id
        seen.add(table_id)


def remove_empty_paragraphs(soup: BeautifulSoup) -> None:
    for p in soup.find_all("p"):
        if not isinstance(p, Tag):
            continue
        if p.get_text(separator=" ", strip=True):
            continue
        # Keep paragraph if it contains an image or other meaningful child
        if p.find(["img", "svg", "table"]):
            continue
        p.decompose()


def cleanup_html(soup: BeautifulSoup) -> None:
    inject_css(soup)
    wrap_body_content(soup)
    add_heading_ids(soup)
    replace_word_bookmark_ids_with_table_ids(soup)
    remove_empty_paragraphs(soup)

def output_name_for_docx(docx_name: str) -> str:
    if docx_name in OUTPUT_NAME_MAP:
        return OUTPUT_NAME_MAP[docx_name]
    return Path(docx_name).with_suffix(".html").name


def convert_docx_to_html(docx_path: Path) -> str:
    result = subprocess.run(
        [
            "pandoc",
            str(docx_path),
            "-f",
            "docx",
            "-t",
            "html",
            "--standalone",
            "--wrap=none",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    return result.stdout


def iter_docx_files(directory: Path) -> Iterable[Path]:
    return sorted(
        p for p in directory.iterdir()
        if p.suffix.lower() == ".docx" and not p.name.startswith("~$")
    )


def replace_word_bookmark_ids_with_table_ids(soup: BeautifulSoup) -> None:
    """
    Convert Word bookmark targets used by TOC table links into stable public ids.

    Example:
      TOC link: <a href="#_bookmark74">Table 36: ...</a>
      target:   <span class="anchor" id="_bookmark74"></span>

    Result:
      - rewrites TOC href to #table-36
      - rewrites the bookmark id to table-36
      - if the bookmark is inside a table, also applies id="table-36" to the table
        so browser jumps land on the table, not a cell inside it
    """
    mapping: dict[str, str] = {}

    # Build mapping from TOC links
    for a in soup.find_all("a", href=True):
        if not isinstance(a, Tag):
            continue

        href = str(a.get("href", "")).strip()
        text = a.get_text(separator=" ", strip=True)

        if not href.startswith("#_bookmark"):
            continue

        match = re.match(r"^\s*Table\s+(\d+)\b", text, flags=re.IGNORECASE)
        if not match:
            continue

        old_id = href[1:]  # strip leading '#'
        new_id = f"table-{match.group(1)}"
        mapping[old_id] = new_id

    if not mapping:
        return

    # Rewrite bookmark targets and propagate table ids
    for tag in soup.find_all(True):
        if not isinstance(tag, Tag):
            continue

        old_id = tag.get("id")
        old_name = tag.get("name")

        if old_id in mapping:
            new_id = mapping[old_id]
            tag["id"] = new_id

            parent_table = tag.find_parent("table")
            if isinstance(parent_table, Tag) and not parent_table.has_attr("id"):
                parent_table["id"] = new_id

        if old_name in mapping:
            new_name = mapping[old_name]
            tag["name"] = new_name

            parent_table = tag.find_parent("table")
            if isinstance(parent_table, Tag) and not parent_table.has_attr("id"):
                parent_table["id"] = new_name

    # Rewrite TOC links to readable ids
    for a in soup.find_all("a", href=True):
        if not isinstance(a, Tag):
            continue

        href = str(a.get("href", "")).strip()
        if not href.startswith("#"):
            continue

        old_id = href[1:]
        if old_id in mapping:
            a["href"] = f"#{mapping[old_id]}"
# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> int:
    docx_files = list(iter_docx_files(WARD_DOCX_DIR))

    if not docx_files:
        print(f"⚠️ No .docx files found in: {WARD_DOCX_DIR}")
        return 0

    for docx_path in docx_files:
        out_name = output_name_for_docx(docx_path.name)
        out_path = WARD_HTML_DIR / out_name

        print(f"\n📄 Converting {docx_path.name} -> {out_path.name}")

        try:
            raw_html = convert_docx_to_html(docx_path)
            soup = BeautifulSoup(raw_html, "html.parser")
            cleanup_html(soup)
            out_path.write_text(str(soup), encoding="utf-8")
            print(f"✅ Saved HTML to: {out_path}")
        except subprocess.CalledProcessError as e:
            err = (e.stderr or "").strip()
            print(f"❌ Pandoc failed for {docx_path.name}: {err or e}")
        except Exception as e:
            print(f"❌ Failed to convert {docx_path.name}: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())