#!/usr/bin/env python3
"""
biochar_app/scripts/convert_word_to_html.py

Convert Word (.docx) files in biochar_app/markdown/docx/ to HTML (via pandoc),
then post-process:
- inject CSS
- rewrite image src paths based on markdown_config.docx_markdown_config
- normalize figure/table captions with numbering
- write output HTML files into biochar_app/markdown/

Notes:
- This script writes HTML content (not Markdown) into .md files in some cases,
  matching your current workflow where serve_markdown() serves converted HTML.
- CRITICAL: Uses biochar_app.scripts.markdown_config as the single source of truth.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Optional, cast

from bs4 import BeautifulSoup
from bs4.element import Tag

from biochar_app.scripts.markdown_config import docx_markdown_config, modal_config, DocxConfig


# ---------------------------------------------------------------------
# Paths (match your repo layout)
# ---------------------------------------------------------------------

HERE = Path(__file__).resolve()
BIOCHAR_APP = HERE.parents[1]  # .../biochar_app
MARKDOWN_DIR = BIOCHAR_APP / "markdown"
DOCX_DIR = MARKDOWN_DIR / "docx"
CONVERTED_HTML_DIR = MARKDOWN_DIR / "converted_html"

DOCX_DIR.mkdir(parents=True, exist_ok=True)
CONVERTED_HTML_DIR.mkdir(parents=True, exist_ok=True)


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


def _normalize_figure_captions(soup: BeautifulSoup) -> None:
    figure_count = 1
    for fig in soup.find_all("figure"):
        if not isinstance(fig, Tag):
            continue
        cap = fig.find("figcaption")
        if not isinstance(cap, Tag):
            continue

        text = cap.get_text(strip=True)
        if not text:
            continue

        lower = text.lower()
        if not lower.startswith("figure"):
            cap.string = f"Figure {figure_count}. {text}"
        else:
            remainder = text.split(".", 1)[-1].strip() if "." in text else text
            if remainder.lower().startswith("figure"):
                remainder = remainder[len("figure"):].strip()
            remainder = remainder.lstrip("0123456789").lstrip().lstrip(".").strip()
            cap.string = f"Figure {figure_count}. {remainder}" if remainder else f"Figure {figure_count}."
        figure_count += 1


def _normalize_table_captions(soup: BeautifulSoup) -> None:
    table_count = 1
    for tbl in soup.find_all("table"):
        if not isinstance(tbl, Tag):
            continue
        cap = tbl.find("caption")
        if not isinstance(cap, Tag):
            continue

        text = cap.get_text(strip=True)
        if not text:
            continue

        lower = text.lower()
        if not lower.startswith("table"):
            cap.string = f"Table {table_count}. {text}"
        else:
            remainder = text.split(".", 1)[-1].strip() if "." in text else text
            if remainder.lower().startswith("table"):
                remainder = remainder[len("table"):].strip()
            remainder = remainder.lstrip("0123456789").lstrip().lstrip(".").strip()
            cap.string = f"Table {table_count}. {remainder}" if remainder else f"Table {table_count}."
        table_count += 1


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
        # spec: {"source": "...docx", "output": "...md"}
        if spec.get("source") == filename:
            return spec["output"]

    return Path(filename).with_suffix(".md").name


def _cfg_for_docx(filename: str) -> Optional[DocxConfig]:
    return docx_markdown_config.get(filename)


# ---------------------------------------------------------------------
# Main conversion loop
# ---------------------------------------------------------------------

def main() -> int:
    docx_files = sorted([p for p in DOCX_DIR.iterdir() if p.suffix.lower() == ".docx" and not p.name.startswith("~$")])

    if not docx_files:
        print(f"⚠️ No .docx files found in: {DOCX_DIR}")
        return 0

    for docx_path in docx_files:
        filename = docx_path.name
        output_name = _output_name_for_docx(filename)
        out_path = MARKDOWN_DIR / output_name

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

            _normalize_figure_captions(soup)
            _normalize_table_captions(soup)

            out_path.write_text(str(soup), encoding="utf-8")
            print(f"✅ Saved cleaned HTML to: {out_path}")

        except subprocess.CalledProcessError as e:
            err = (e.stderr or "").strip()
            print(f"❌ Pandoc failed for {filename}: {err or e}")
        except Exception as e:
            print(f"❌ Failed to convert {filename}: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())