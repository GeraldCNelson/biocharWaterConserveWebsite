#!/usr/bin/env python3
"""Convert configured Word documents into website-ready HTML fragments.

Word is authoritative for text, headings, tables, hyperlinks, embedded images,
and captions. Pandoc extracts the document content and media; this script then
converts raster images to WebP, stores them under
``static/images/generated/<document>/``, rewrites their URLs, normalizes
captions, injects application tab links, and writes the resulting HTML into the
configured ``markdown/outputs_md`` file.

Convert every Word document in ``biochar_app/markdown/docx``::

    python -m biochar_app.markdown.tools.convert_word_to_html

Run only Experiment Design::

    python -m biochar_app.markdown.tools.convert_word_to_html \
        experimentDesign.docx

The output files retain the historical ``.md`` suffix for compatibility even
though their content is HTML.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable, cast

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from PIL import Image

from biochar_app.config.core import TAB_LINKS
from biochar_app.config.paths import (
    MARKDOWN_DOCX_DIR,
    MARKDOWN_GENERATED_IMAGES_DIR,
    MARKDOWN_OUTPUTS_DIR,
)
from biochar_app.markdown.tools.markdown_config import (
    DocumentSpec,
    iter_document_specs,
    output_name_for_spec,
)


PANDOC_CSS = """
html {
  color: #1a1a1a;
  background-color: #fdfdfd;
}
body {
  margin: 0 auto;
  max-width: 1500px;
  padding: 50px;
  hyphens: auto;
  overflow-wrap: break-word;
  text-rendering: optimizeLegibility;
  font-kerning: normal;
  font-family: Georgia, serif;
}
img { max-width: 100%; height: auto; }
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
  font-weight: normal;
  text-align: center;
  margin-top: 0.5em;
}
table.figure-grid {
  table-layout: fixed;
}
table.figure-grid th,
table.figure-grid td {
  vertical-align: top;
  text-align: center;
  font-weight: normal;
}
table.figure-grid figure {
  margin: 0;
}
table.figure-grid img {
  width: auto;
  max-width: 100%;
  height: auto;
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

_FIGURE_PREFIX_RE = re.compile(r"^\s*figure\b", flags=re.IGNORECASE)
_TABLE_PREFIX_RE = re.compile(r"^\s*table\b", flags=re.IGNORECASE)


def _safe_directory_name(stem: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_-]+", "-", stem).strip("-").lower()
    return name or "document"


def _ensure_head(soup: BeautifulSoup) -> Tag:
    head = soup.head
    if head is None:
        new_head = soup.new_tag("head")
        soup.insert(0, new_head)
        head = soup.head
    return cast(Tag, head)


def _inject_css(soup: BeautifulSoup) -> None:
    style_tag = soup.new_tag("style")
    style_tag.string = PANDOC_CSS
    _ensure_head(soup).append(style_tag)


def inject_tab_links(soup: BeautifulSoup) -> None:
    """Replace known application-tab labels in paragraph and list text."""
    for tag in soup.find_all(["p", "li"]):
        if not isinstance(tag, Tag):
            continue

        html = str(tag)
        original_html = html
        for label, tab_id in TAB_LINKS.items():
            if f'data-tab="{tab_id}"' in html:
                continue
            html = html.replace(
                label,
                f'<a href="#" class="tab-link" data-tab="{tab_id}">{label}</a>',
            )

        if html != original_html:
            replacement = BeautifulSoup(html, "html.parser")
            contents = replacement.body.contents if replacement.body else replacement.contents
            tag.replace_with(*contents)


def _clean_caption_remainder(text: str, label: str) -> str:
    cleaned = " ".join(text.replace("\xa0", " ").split()).strip()
    if not cleaned:
        return ""

    prefix_re = _FIGURE_PREFIX_RE if label.lower() == "figure" else _TABLE_PREFIX_RE
    if prefix_re.match(cleaned):
        cleaned = prefix_re.sub("", cleaned, count=1).strip()

    cleaned = re.sub(r"^\d+\s*", "", cleaned).strip()
    cleaned = re.sub(r"^[\.\:\;\-\–\—\)\]]+\s*", "", cleaned).strip()
    return cleaned


def _format_numbered_caption(label: str, number: int, text: str) -> str:
    remainder = _clean_caption_remainder(text, label)
    return f"{label} {number}. {remainder}" if remainder else f"{label} {number}."


def _number_caption_tag(caption: Tag, label: str, number: int) -> None:
    """Number a caption without discarding hyperlinks or inline formatting."""
    first_text = next(
        (
            node
            for node in caption.descendants
            if isinstance(node, NavigableString) and str(node).strip()
        ),
        None,
    )
    if first_text is None:
        caption.insert(0, f"{label} {number}.")
        return

    replacement = _format_numbered_caption(label, number, str(first_text))
    if str(first_text).endswith(" "):
        replacement += " "
    first_text.replace_with(replacement)


def _promote_table_cell_figures(soup: BeautifulSoup) -> None:
    """Turn Word table-cell image/caption pairs into semantic figures.

    Pandoc emits a real Word caption inside a table cell as an ordinary
    paragraph immediately following the image paragraph. Promoting that pair
    lets the normal document-wide figure numbering and accessibility handling
    work exactly as they do for figures outside tables.
    """
    for cell in soup.find_all(["th", "td"]):
        if not isinstance(cell, Tag):
            continue

        children = [child for child in cell.children if isinstance(child, Tag)]
        for image_paragraph, caption_paragraph in zip(children, children[1:]):
            if image_paragraph.name != "p" or caption_paragraph.name != "p":
                continue
            image_tags = image_paragraph.find_all("img")
            if len(image_tags) != 1 or image_paragraph.get_text(strip=True):
                continue
            if not caption_paragraph.get_text(" ", strip=True):
                continue

            figure = soup.new_tag("figure")
            image_paragraph.replace_with(figure)
            for child in list(image_paragraph.contents):
                figure.append(child.extract())

            caption = soup.new_tag("figcaption")
            for child in list(caption_paragraph.contents):
                caption.append(child.extract())
            figure.append(caption)
            caption_paragraph.decompose()

            table = figure.find_parent("table")
            if isinstance(table, Tag):
                existing_classes = list(table.get("class", []))
                if "figure-grid" not in existing_classes:
                    table["class"] = [*existing_classes, "figure-grid"]


def _normalize_captions(soup: BeautifulSoup) -> None:
    for number, figure in enumerate(soup.find_all("figure"), start=1):
        if not isinstance(figure, Tag):
            continue
        caption = figure.find("figcaption")
        if isinstance(caption, Tag):
            _number_caption_tag(caption, "Figure", number)

    for number, table in enumerate(soup.find_all("table"), start=1):
        if not isinstance(table, Tag):
            continue
        caption = table.find("caption")
        if isinstance(caption, Tag):
            _number_caption_tag(caption, "Table", number)


def _caption_for_image(image_tag: Tag) -> str:
    figure = image_tag.find_parent("figure")
    if not isinstance(figure, Tag):
        return ""
    caption = figure.find("figcaption")
    if not isinstance(caption, Tag):
        return ""
    text = caption.get_text(" ", strip=True)
    text = re.sub(r"\s+([\.,;!?])", r"\1", text)
    return re.sub(r"^Figure\s+\d+\.\s*", "", text, flags=re.IGNORECASE).strip()


def _convert_to_webp(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(source) as image:
        has_alpha = image.mode in {"RGBA", "LA"} or (
            image.mode == "P" and "transparency" in image.info
        )
        converted = image.convert("RGBA" if has_alpha else "RGB")
        converted.save(
            destination,
            "WEBP",
            quality=90,
            method=6,
            lossless=source.suffix.lower() == ".png",
        )


def _rewrite_extracted_images(
    soup: BeautifulSoup,
    *,
    document_stem: str,
) -> list[Path]:
    """Convert Pandoc-extracted images and rewrite their browser URLs."""
    web_directory_name = _safe_directory_name(document_stem)
    output_directory = MARKDOWN_GENERATED_IMAGES_DIR / web_directory_name
    written: list[Path] = []

    for number, image_tag in enumerate(soup.find_all("img"), start=1):
        if not isinstance(image_tag, Tag):
            continue
        source_value = image_tag.get("src")
        if not isinstance(source_value, str):
            raise ValueError(f"Image {number} has no usable src attribute.")

        source = Path(source_value)
        if not source.exists():
            raise FileNotFoundError(f"Pandoc-extracted image not found: {source}")

        output = output_directory / f"image-{number:02d}.webp"
        _convert_to_webp(source, output)
        written.append(output)
        image_tag["src"] = (
            f"/static/images/generated/{web_directory_name}/{output.name}"
        )

    if written:
        expected = {path.resolve() for path in written}
        for existing in output_directory.glob("*"):
            if existing.is_file() and existing.resolve() not in expected:
                existing.unlink()

    return written


def _add_image_accessibility_text(soup: BeautifulSoup) -> None:
    for number, image_tag in enumerate(soup.find_all("img"), start=1):
        if not isinstance(image_tag, Tag):
            continue
        caption = _caption_for_image(image_tag)
        alt_text = caption or f"Document image {number}"
        if not str(image_tag.get("alt", "")).strip():
            image_tag["alt"] = alt_text
        if not str(image_tag.get("title", "")).strip():
            image_tag["title"] = alt_text


def _document_specs(selected_sources: Iterable[str]) -> list[DocumentSpec]:
    """Build conversion specs from explicit names or every DOCX in the folder.

    Application configuration is consulted only for intentional output-name
    overrides. A Word document does not need a ``markdown_config.py`` entry to
    be converted.
    """
    configured = {
        spec["source"]: spec
        for spec in iter_document_specs()
    }
    selected = {Path(value).name for value in selected_sources}

    if selected:
        missing = sorted(
            name for name in selected
            if not (MARKDOWN_DOCX_DIR / name).is_file()
        )
        if missing:
            raise ValueError(
                "Word document(s) not found in "
                f"{MARKDOWN_DOCX_DIR}: {', '.join(missing)}"
            )
        source_names = sorted(selected)
    else:
        source_names = sorted(
            path.name
            for path in MARKDOWN_DOCX_DIR.glob("*.docx")
            if path.is_file() and not path.name.startswith("~$")
        )

    return [
        configured.get(source_name, {"source": source_name})
        for source_name in source_names
    ]


def convert_document(spec: DocumentSpec) -> tuple[Path, list[Path]]:
    source = MARKDOWN_DOCX_DIR / spec["source"]
    if not source.exists():
        raise FileNotFoundError(f"Configured Word document not found: {source}")

    output = MARKDOWN_OUTPUTS_DIR / output_name_for_spec(spec)
    MARKDOWN_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f"{source.stem}-media-") as temp_dir:
        media_root = Path(temp_dir) / "media"
        result = subprocess.run(
            [
                "pandoc",
                str(source),
                "--from=docx",
                "--to=html",
                "--wrap=none",
                f"--extract-media={media_root}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )

        soup = BeautifulSoup(result.stdout, "html.parser")
        _inject_css(soup)
        _promote_table_cell_figures(soup)
        generated_images = _rewrite_extracted_images(
            soup,
            document_stem=source.stem,
        )
        _normalize_captions(soup)
        _add_image_accessibility_text(soup)
        inject_tab_links(soup)
        output.write_text(str(soup), encoding="utf-8")

    return output, generated_images


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Word documents into website HTML."
    )
    parser.add_argument(
        "documents",
        nargs="*",
        help=(
            "Optional DOCX filenames from biochar_app/markdown/docx; "
            "omit to convert every DOCX in that directory."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        specs = _document_specs(args.documents)
    except ValueError as error:
        print(f"ERROR: {error}")
        return 2

    failed = False
    for spec in specs:
        source_name = spec["source"]
        print(f"\nConverting {source_name} -> {output_name_for_spec(spec)}")
        try:
            output, images = convert_document(spec)
            print(f"Saved HTML content: {output}")
            print(f"Generated WebP images: {len(images)}")
        except subprocess.CalledProcessError as error:
            failed = True
            detail = (error.stderr or "").strip() or str(error)
            print(f"ERROR: Pandoc failed for {source_name}: {detail}")
        except Exception as error:
            failed = True
            print(f"ERROR: Failed to convert {source_name}: {error}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
