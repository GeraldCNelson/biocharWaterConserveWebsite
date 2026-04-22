#!/usr/bin/env python3
"""
Find suspicious image references in converted Ward HTML and print nearby text context.

Example use:
    python find_image_context.py

Adjust HTML_PATH and SUSPICIOUS_IMAGES as needed.
"""

from __future__ import annotations

from pathlib import Path
from bs4 import BeautifulSoup, NavigableString, Tag

HERE = Path(__file__).resolve()
BIOCHAR_APP = HERE.parents[1]

HTML_PATH = BIOCHAR_APP / "data-processed" / "ward-html" / "ward_guide_20211118.html"
SUSPICIOUS_IMAGES = {
    "image52.png",
    "image54.png",
    "image55.png",
    "image180.png",
    "image330.png",
}

TEXT_WINDOW = 5
# number of text blocks before and after


def clean_text(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def collect_text_blocks(soup: BeautifulSoup) -> list[tuple[Tag, str]]:
    """
    Build a linear list of meaningful text-containing block tags in document order.
    """
    block_names = {"p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td", "th", "caption", "figcaption"}
    blocks: list[tuple[Tag, str]] = []

    for tag in soup.find_all(block_names):
        if not isinstance(tag, Tag):
            continue
        text = clean_text(tag.get_text(" ", strip=True))
        if text:
            blocks.append((tag, text))

    return blocks


def find_nearest_block_index(img_tag: Tag, blocks: list[tuple[Tag, str]]) -> int | None:
    """
    Find the first text block that is the image's parent, next sibling, or nearby ancestor context.
    """
    # Exact parent/sibling containment check first
    for i, (block, _) in enumerate(blocks):
        if block is img_tag:
            return i
        if block.find(lambda t: t is img_tag):
            return i

    # Walk upward from img to find a block ancestor
    current: Tag | None = img_tag
    while current is not None:
        for i, (block, _) in enumerate(blocks):
            if block is current:
                return i
        parent = current.parent
        current = parent if isinstance(parent, Tag) else None

    return None


def main() -> None:
    if not HTML_PATH.exists():
        raise FileNotFoundError(f"HTML file not found: {HTML_PATH}")

    soup = BeautifulSoup(HTML_PATH.read_text(encoding="utf-8"), "html.parser")
    blocks = collect_text_blocks(soup)

    found_any = False

    for img in soup.find_all("img"):
        if not isinstance(img, Tag):
            continue

        src = img.get("src", "")
        if not isinstance(src, str):
            continue

        filename = Path(src).name
        if filename not in SUSPICIOUS_IMAGES:
            continue

        found_any = True
        idx = find_nearest_block_index(img, blocks)

        print("=" * 100)
        print(f"IMAGE: {filename}")
        print(f"SRC:   {src}")

        if idx is None:
            print("Could not locate nearby text block.")
            continue

        start = max(0, idx - TEXT_WINDOW)
        end = min(len(blocks), idx + TEXT_WINDOW + 1)

        print(f"\nNearby text blocks ({start} to {end - 1}):\n")
        for j in range(start, end):
            marker = ">>" if j == idx else "  "
            print(f"{marker} [{j}] {blocks[j][1]}")

        print()

    if not found_any:
        print("No suspicious images found in the HTML.")


if __name__ == "__main__":
    main()