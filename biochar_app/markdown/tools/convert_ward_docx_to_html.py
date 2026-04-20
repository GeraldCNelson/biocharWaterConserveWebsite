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
from typing import Callable, Final, Iterable, Optional, cast

from bs4 import BeautifulSoup
from bs4.element import NavigableString, PageElement, Tag


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

HERE = Path(__file__).resolve()
BIOCHAR_APP = HERE.parents[1]

WARD_DOCX_DIR = BIOCHAR_APP / "data-processed" / "ward-docx"
WARD_HTML_DIR = BIOCHAR_APP / "data-processed" / "ward-html"
LAB_REFERENCE_MEDIA_DIR = BIOCHAR_APP / "static" / "lab_reference_media"

WARD_DOCX_DIR.mkdir(parents=True, exist_ok=True)
WARD_HTML_DIR.mkdir(parents=True, exist_ok=True)
LAB_REFERENCE_MEDIA_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

WARD_HTML_CSS: Final[str] = """
body {
  max-width: 1200px;
  margin: 0 auto;
  padding: 10px 20px;
  line-height: 1.22;
}

.lab-reference-content {
  font-size: 0.98rem;
  line-height: 1.22;
}

.lab-reference-content h1,
.lab-reference-content h2,
.lab-reference-content h3,
.lab-reference-content h4,
.lab-reference-content h5,
.lab-reference-content h6 {
  scroll-margin-top: 90px;
  line-height: 1.08;
}

.lab-reference-content h1 {
  margin: 0.55em 0 0.14em;
}

.lab-reference-content h2,
.lab-reference-content h3 {
  margin: 0.42em 0 0.12em;
}

.lab-reference-content h4,
.lab-reference-content h5,
.lab-reference-content h6 {
  margin: 0.34em 0 0.10em;
}

.lab-reference-content p,
.lab-reference-content ul,
.lab-reference-content ol {
  margin: 0.10em 0;
}

.lab-reference-content li {
  margin: 0.04em 0;
}

.lab-reference-content ul,
.lab-reference-content ol {
  padding-left: 1.2em;
}

.lab-reference-content hr {
  margin: 0.45em 0;
}

blockquote {
  margin: 0.16em 0 0.28em 0.85rem;
  padding-left: 0.55rem;
  border-left: 3px solid #d9d9d9;
}

img {
  max-width: 100%;
  height: auto;
}

.lab-reference-content img.lab-ref-image-inline {
  display: inline-block;
  max-width: 100%;
  height: auto;
}

.lab-reference-content img.lab-ref-image-figure {
  display: block;
  max-width: 100%;
  height: auto;
  margin: 0.22em auto;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin: 0.25em 0 0.45em;
  font-size: 0.95rem;
  line-height: 1.16;
}

th,
td {
  border: 1px solid #d0d0d0;
  padding: 0.18em 0.32em;
  vertical-align: top;
}

th {
  font-weight: 600;
  background-color: #f7f7f7;
}

td p,
th p,
td ul,
th ul,
td ol,
th ol {
  margin: 0;
}

td li,
th li {
  margin: 0;
}

caption,
figcaption {
  font-style: italic;
  margin-top: 0.14em;
}

.lab-ref-anchor {
  display: block;
  position: relative;
  top: -90px;
  visibility: hidden;
  height: 0;
  margin: 0;
  padding: 0;
}
""".strip()

OUTPUT_NAME_MAP: Final[dict[str, str]] = {
    "WardGuide-Master-20211118.docx": "ward_guide_20211118.html",
    "Ward-SHA-Guide-FINAL-May.docx": "ward_soil_health_guide_final_may.html",
    "Biological 2025-11-05.docx": "ward_biological_report_20251105.html",
    "Biological 2024-11-05.docx": "ward_biological_report_20241105.html",
    "Hay_NIRS_2025-11-03.docx": "ward_nirs_report_20251103.html",
    "Soil_SHA_2025-11-03.docx": "ward_soil_sha_report_20251103.html",
}

EXPLICIT_ANCHOR_MAP: Final[dict[str, dict[str, str]]] = {
    "ward_guide_20211118.html": {
        "PLFA": "plfa",
        "Soil Microorganisms": "soil-microorganisms",
    },
    "ward_soil_health_guide_final_may.html": {
        "Microbially Active Carbon (%MAC)": "microbially-active-carbon-mac",
        "Water Extractable Organic Carbon": "water-extractable-organic-carbon",
        "Water Extractable Organic Nitrogen": "water-extractable-organic-nitrogen",
        "Organic C to Organic N Ratio": "organic-c-to-organic-n-ratio",
        "Soil Health Score": "soil-health-score",
    },
}

IMAGE_REPLACEMENTS: Final[dict[str, list[dict[str, object]]]] = {
    "ward_guide_20211118.html": [
        {
            "mode": "replace_one",
            "target_src": "image180.png",
            "replacement_src": (
                "/static/lab_reference_media/"
                "ward_guide_20211118/replacements/"
                "fractions_of_feed.png"
            ),
            "note": (
                "Replace broken SmartArt-style extraction for Figure 2: "
                "Fractions of Feed Used in Analysis."
            ),
        },
        {
            "mode": "replace_one",
            "target_src": "image330.png",
            "replacement_src": (
                "/static/lab_reference_media/"
                "ward_guide_20211118/replacements/"
                "poly_p_sequestering_ring_compound.png"
            ),
            "note": (
                "Replace partial extraction near the poly-P micronutrient "
                "sequestering discussion."
            ),
        },
        {
            "mode": "replace_group",
            "target_src": "image52.jpeg",
            "delete_src": [
                "image55.png",
                "image58.png",
            ],
            "replacement_src": (
                "/static/lab_reference_media/"
                "ward_guide_20211118/replacements/"
                "potassium_availability_equilibrium_figure.png"
            ),
            "note": (
                "Replace multi-part extracted fragments with one composite "
                "potassium availability/equilibrium figure."
            ),
        },
    ],
}

# Decorative / section-divider images that should not dominate the rendered page.
DECORATIVE_IMAGE_BASENAMES: Final[dict[str, set[str]]] = {
    "ward_guide_20211118.html": {
        "image3.png",     # Ward Lab header
        "image10.jpeg",
        "image90.jpeg",
        "image29.jpeg",   # NIRS pipette image in heading area
        "image120.jpeg",  # FEED TESTING header graphic
        "image300.jpeg",  # transition / decorative full-page pipette image
        "image440.jpeg",  # SOIL TESTING header graphic
        "image610.jpeg",  # Mountain and field scenery
        "image640.jpeg",  # PLANT TESTING header graphic
        "image690.jpeg",  # Holding soil
        "image720.jpeg",  # WATER header graphic
        "image740.jpeg",  # End graphic
    },
}

BLOCK_TAGS: Final[tuple[str, ...]] = ("h1", "h2", "h3", "h4", "h5", "h6", "p", "div", "strong")

BIOLOGICAL_REPORT_OUTPUTS: Final[set[str]] = {
    "ward_biological_report_20251105.html",
    "ward_biological_report_20241105.html",
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def slugify(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s:()\-]", "", text)
    text = text.replace(":", "")
    text = text.replace("(", "").replace(")", "")
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")


def to_snake_case_filename(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


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


def normalize_text(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("\xa0", " ")
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", " ", text)
    return text


def ensure_head(soup: BeautifulSoup) -> Tag:
    head = soup.head
    if head is None:
        new_head = soup.new_tag("head")
        html_tag = soup.html
        if isinstance(html_tag, Tag):
            html_tag.insert(0, new_head)
        else:
            soup.insert(0, new_head)
        head = soup.head
    return cast(Tag, head)


def ensure_body(soup: BeautifulSoup) -> Tag:
    body = soup.body
    if body is None:
        new_body = soup.new_tag("body")
        html_tag = soup.html
        if isinstance(html_tag, Tag):
            html_tag.append(new_body)
        else:
            soup.append(new_body)
        body = soup.body
    return cast(Tag, body)


def inject_css(soup: BeautifulSoup) -> None:
    head = ensure_head(soup)
    style_tag = soup.new_tag("style")
    style_tag.string = WARD_HTML_CSS
    head.append(style_tag)


def wrap_body_content(soup: BeautifulSoup) -> None:
    body = ensure_body(soup)
    wrapper = soup.new_tag("div", attrs={"class": "lab-reference-content"})

    children: list[PageElement] = list(body.contents)
    for child in children:
        wrapper.append(child.extract())

    body.append(wrapper)


def collect_existing_ids(soup: BeautifulSoup) -> set[str]:
    seen: set[str] = set()
    for tag in soup.find_all(True):
        if not isinstance(tag, Tag):
            continue
        tag_id = tag.get("id")
        if isinstance(tag_id, str) and tag_id.strip():
            seen.add(tag_id.strip())
    return seen


def add_heading_ids(soup: BeautifulSoup) -> None:
    seen = collect_existing_ids(soup)

    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        if not isinstance(tag, Tag):
            continue

        text = tag.get_text(strip=True)
        if not text:
            continue

        tag_id = tag.get("id")
        if isinstance(tag_id, str) and tag_id.strip():
            continue

        slug = slugify(text)
        tag["id"] = unique_slug(slug, seen)


def add_explicit_named_anchors(soup: BeautifulSoup, output_name: str) -> None:
    explicit_map = EXPLICIT_ANCHOR_MAP.get(output_name, {})
    if not explicit_map:
        return

    seen = collect_existing_ids(soup)

    for tag in soup.find_all(list(BLOCK_TAGS)):
        if not isinstance(tag, Tag):
            continue

        text = tag.get_text(strip=True)
        if not text:
            continue

        wanted_id = explicit_map.get(text)
        if not wanted_id or wanted_id in seen:
            continue

        existing_id = tag.get("id")
        if isinstance(existing_id, str) and existing_id.strip():
            if existing_id != wanted_id:
                tag["id"] = wanted_id
            seen.add(wanted_id)
            continue

        tag["id"] = wanted_id
        seen.add(wanted_id)


def _get_block_tags_in_order(soup: BeautifulSoup) -> list[Tag]:
    out: list[Tag] = []
    for tag in soup.find_all(list(BLOCK_TAGS)):
        if isinstance(tag, Tag):
            out.append(tag)
    return out


def _find_first_matching_block(
    blocks: list[Tag],
    predicate: Callable[[str], bool],
) -> tuple[Optional[int], Optional[Tag]]:
    for idx, tag in enumerate(blocks):
        text = normalize_text(tag.get_text(" ", True))
        if text and predicate(text):
            return idx, tag
    return None, None


def _find_next_matching_block(
    blocks: list[Tag],
    start_idx: int,
    predicate: Callable[[str], bool],
    max_scan: int = 80,
    used_indexes: Optional[set[int]] = None,
) -> tuple[Optional[int], Optional[Tag]]:
    end_idx = min(len(blocks), start_idx + max_scan + 1)
    for idx in range(start_idx + 1, end_idx):
        if used_indexes is not None and idx in used_indexes:
            continue
        tag = blocks[idx]
        text = normalize_text(tag.get_text(" ", True))
        if text and predicate(text):
            return idx, tag
    return None, None


def _insert_anchor_before(tag: Tag, wanted_id: str, seen_ids: set[str]) -> bool:
    if wanted_id in seen_ids or tag.parent is None:
        return False

    prev = tag.previous_sibling
    while prev is not None and not isinstance(prev, Tag):
        prev = prev.previous_sibling

    if isinstance(prev, Tag):
        prev_id = prev.get("id")
        if isinstance(prev_id, str) and prev_id.strip() == wanted_id:
            seen_ids.add(wanted_id)
            return True

    soup = tag if isinstance(tag, BeautifulSoup) else tag.find_parent()
    root: Optional[BeautifulSoup] = None
    current: Optional[PageElement] = tag
    while current is not None:
        if isinstance(current, BeautifulSoup):
            root = current
            break
        current = current.parent

    if root is None:
        return False

    anchor = root.new_tag("span")
    anchor["id"] = wanted_id
    anchor["class"] = "lab-ref-anchor"
    tag.insert_before(anchor)
    seen_ids.add(wanted_id)
    return True


def add_biological_report_anchors(soup: BeautifulSoup, output_name: str) -> None:
    if output_name not in BIOLOGICAL_REPORT_OUTPUTS:
        return

    seen_ids = collect_existing_ids(soup)
    blocks = _get_block_tags_in_order(soup)
    used_block_indexes: set[int] = set()

    targets = [
        {
            "anchor_id": "diversity-index-ratings",
            "section_match": lambda t: "functional group diversity index" in t,
        },
        {
            "anchor_id": "fungi-bacteria-ratings",
            "section_match": lambda t: (
                "fungi:bacteria" in t
                or "fungi : bacteria" in t
                or ("community composition ratios" in t and "fungi" in t and "bacteria" in t)
            ),
        },
        {
            "anchor_id": "predator-prey-ratings",
            "section_match": lambda t: (
                "predator : prey" in t
                or "predator:prey" in t
                or ("predator" in t and "prey" in t and "your results" not in t)
            ),
        },
        {
            "anchor_id": "gram-pos-gram-neg-ratings",
            "section_match": lambda t: (
                "gram (+) : gram (-)" in t
                or "gram(+):gram(-)" in t
                or ("gram (+)" in t and "gram (-)" in t and "rating" not in t and "your results" not in t)
            ),
        },
    ]

    def is_scale_rating(text: str) -> bool:
        return text == "scale rating" or text.startswith("scale rating ")

    def is_diversity_scale_text(text: str) -> bool:
        return (
            "very poor" in text
            or "slightly below average" in text
            or "slightly above average" in text
            or "excellent" in text
        )

    for target in targets:
        wanted_id = target["anchor_id"]
        if wanted_id in seen_ids:
            continue

        matcher = cast(Callable[[str], bool], target["section_match"])
        start_idx, start_tag = _find_first_matching_block(blocks, matcher)
        if start_idx is None or start_tag is None:
            continue

        anchor_idx, anchor_tag = _find_next_matching_block(
            blocks,
            start_idx,
            is_scale_rating,
            max_scan=120,
            used_indexes=used_block_indexes,
        )

        if anchor_tag is None and wanted_id == "diversity-index-ratings":
            anchor_idx, anchor_tag = _find_next_matching_block(
                blocks,
                start_idx,
                is_diversity_scale_text,
                max_scan=120,
                used_indexes=used_block_indexes,
            )

        if anchor_tag is None:
            anchor_tag = start_tag
            anchor_idx = start_idx

        if anchor_idx is not None and _insert_anchor_before(anchor_tag, wanted_id, seen_ids):
            used_block_indexes.add(anchor_idx)


def add_table_ids_from_captions(soup: BeautifulSoup) -> None:
    seen = collect_existing_ids(soup)

    for table in soup.find_all("table"):
        if not isinstance(table, Tag):
            continue

        caption = table.find("caption")
        if isinstance(caption, Tag):
            text = caption.get_text(strip=True)
        else:
            prev = table.find_previous("p")
            text = prev.get_text(strip=True) if isinstance(prev, Tag) else ""

        match = re.search(r"\btable\s+(\d+)\b", text, re.IGNORECASE)
        if not match:
            continue

        table_num = match.group(1)
        title_slug = slugify(text)
        long_id = title_slug if title_slug else f"table-{table_num}"
        short_id = f"table-{table_num}"

        table_id = table.get("id")
        if isinstance(table_id, str) and table_id.strip():
            continue

        chosen_id = long_id if long_id not in seen else short_id
        chosen_id = unique_slug(chosen_id, seen)
        table["id"] = chosen_id


def replace_word_bookmark_ids_with_table_ids(soup: BeautifulSoup) -> None:
    mapping: dict[str, str] = {}

    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue

        href = str(anchor.get("href", "")).strip()
        text = anchor.get_text(strip=True)

        if not href.startswith("#_bookmark"):
            continue

        match = re.match(r"^\s*Table\s+(\d+)\b", text, flags=re.IGNORECASE)
        if not match:
            continue

        old_id = href[1:]
        new_id = f"table-{match.group(1)}"
        mapping[old_id] = new_id

    if not mapping:
        return

    for tag in soup.find_all(True):
        if not isinstance(tag, Tag):
            continue

        old_id = tag.get("id")
        old_name = tag.get("name")

        if isinstance(old_id, str) and old_id in mapping:
            new_id = mapping[old_id]
            tag["id"] = new_id

            parent_table = tag.find_parent("table")
            if isinstance(parent_table, Tag):
                parent_id = parent_table.get("id")
                if not (isinstance(parent_id, str) and parent_id.strip()):
                    parent_table["id"] = new_id

        if isinstance(old_name, str) and old_name in mapping:
            new_name = mapping[old_name]
            tag["name"] = new_name

            parent_table = tag.find_parent("table")
            if isinstance(parent_table, Tag):
                parent_id = parent_table.get("id")
                if not (isinstance(parent_id, str) and parent_id.strip()):
                    parent_table["id"] = new_name

    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue

        href = str(anchor.get("href", "")).strip()
        if not href.startswith("#"):
            continue

        old_id = href[1:]
        if old_id in mapping:
            anchor["href"] = f"#{mapping[old_id]}"


def remove_empty_paragraphs(soup: BeautifulSoup) -> None:
    for p in soup.find_all("p"):
        if not isinstance(p, Tag):
            continue
        if p.get_text(strip=True):
            continue
        if p.find(["img", "svg", "table"]):
            continue
        p.decompose()


def _basename_from_src(src: str) -> str:
    return Path(src.replace("\\", "/")).name


def rewrite_image_paths(soup: BeautifulSoup, media_url_prefix: str) -> None:
    media_url_prefix = media_url_prefix.rstrip("/")

    for img in soup.find_all("img"):
        if not isinstance(img, Tag):
            continue

        src_attr = img.get("src")
        if not isinstance(src_attr, str):
            continue

        src = src_attr.strip()

        if src.startswith("/static/") or src.startswith(("http://", "https://")):
            img["style"] = "max-width: 100%; height: auto;"
            continue

        if src.startswith("media/"):
            img["src"] = f"{media_url_prefix}/{src}"
            img["style"] = "max-width: 100%; height: auto;"
            continue

        if src.startswith("./media/"):
            img["src"] = f"{media_url_prefix}/{src[2:]}"
            img["style"] = "max-width: 100%; height: auto;"
            continue

        normalized_src = src.replace("\\", "/")
        marker_index = normalized_src.rfind("/media/")
        if marker_index != -1:
            relative_media_path = normalized_src[marker_index + 1:]  # keep "media/..."
            img["src"] = f"{media_url_prefix}/{relative_media_path}"

        img["style"] = "max-width: 100%; height: auto;"


def _remove_empty_ancestors(start_tag: Optional[Tag]) -> None:
    removable_names = {"p", "div", "blockquote", "span"}
    current = start_tag

    while isinstance(current, Tag):
        parent = current.parent if isinstance(current.parent, Tag) else None

        if current.name not in removable_names:
            break

        has_meaningful_text = any(
            isinstance(node, NavigableString) and str(node).strip()
            for node in current.contents
        )
        has_element_children = any(isinstance(node, Tag) for node in current.contents)

        if has_meaningful_text or has_element_children:
            break

        current.decompose()
        current = parent


def _paragraph_text_without_images(tag: Tag) -> str:
    text_parts: list[str] = []
    for child in tag.children:
        if isinstance(child, NavigableString):
            text_parts.append(str(child))
        elif isinstance(child, Tag) and child.name != "img":
            text_parts.append(child.get_text(" ", True))
    return normalize_text(" ".join(text_parts))


def _is_probable_page_footer_or_header(text: str) -> bool:
    if not text:
        return False

    patterns = [
        r"[›»]\s*\d+\s*$",                     # "Fertilizer Recommendations › 35"
        r"^\d+\s*[‹›»]?\s*$",                  # bare page number
        r"^[a-z0-9 ,&()\/\-\']+\s+[›»]\s*\d+$",
        r"^[a-z][a-z0-9 ,&()\/\-\']{3,}\s+\d+$",
    ]
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def remove_images_inside_headings(soup: BeautifulSoup) -> None:
    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        if not isinstance(heading, Tag):
            continue

        for img in heading.find_all("img"):
            if isinstance(img, Tag):
                img.decompose()


def remove_page_header_footer_paragraphs(soup: BeautifulSoup) -> None:
    for p in soup.find_all("p"):
        if not isinstance(p, Tag):
            continue

        text = normalize_text(p.get_text(" ", True))
        if text and _is_probable_page_footer_or_header(text):
            p.decompose()


def remove_decorative_transition_images(soup: BeautifulSoup, output_name: str) -> None:
    decorative = DECORATIVE_IMAGE_BASENAMES.get(output_name, set())
    if not decorative:
        return

    for img in list(soup.find_all("img")):
        if not isinstance(img, Tag):
            continue

        src_attr = img.get("src")
        if not isinstance(src_attr, str):
            continue

        basename = _basename_from_src(src_attr)
        if basename not in decorative:
            continue

        print(f"🗑 Removing decorative image: {basename} ({src_attr})")
        parent = img.parent if isinstance(img.parent, Tag) else None

        if isinstance(parent, Tag) and parent.name == "p":
            extra_text = _paragraph_text_without_images(parent)
            if not extra_text:
                parent.decompose()
                continue

        img.decompose()
        _remove_empty_ancestors(parent)


def normalize_image_classes(soup: BeautifulSoup) -> None:
    for img in soup.find_all("img"):
        if not isinstance(img, Tag):
            continue

        parent = img.parent if isinstance(img.parent, Tag) else None
        parent_name = parent.name if isinstance(parent, Tag) else ""

        existing_classes = img.get("class")
        classes: list[str]
        if isinstance(existing_classes, str):
            classes = [existing_classes]
        elif isinstance(existing_classes, list):
            classes = [str(c) for c in existing_classes]
        else:
            classes = []

        if parent_name in {"th", "td", "span", "a"}:
            if "lab-ref-image-inline" not in classes:
                classes.append("lab-ref-image-inline")
        else:
            if "lab-ref-image-figure" not in classes:
                classes.append("lab-ref-image-figure")

        img["class"] = classes
        img["style"] = "max-width: 100%; height: auto;"


def apply_image_replacements(soup: BeautifulSoup, output_name: str) -> None:
    print(f"🖼 apply_image_replacements called for: {output_name}")
    print(f"🖼 available replacement keys: {list(IMAGE_REPLACEMENTS.keys())}")

    rules = IMAGE_REPLACEMENTS.get(output_name, [])
    print(f"🖼 rule count for {output_name}: {len(rules)}")

    if not rules:
        print(f"📝 No image replacement rules for: {output_name}")
        return

    print(f"🛠 Applying image replacement rules for: {output_name}")

    for rule in rules:
        mode_obj = rule.get("mode")
        target_src_obj = rule.get("target_src")
        replacement_src_obj = rule.get("replacement_src")
        note_obj = rule.get("note", "")

        if not isinstance(mode_obj, str):
            print("  ⚠️ Skipping rule with invalid mode")
            continue
        if not isinstance(target_src_obj, str):
            print("  ⚠️ Skipping rule with invalid target_src")
            continue
        if not isinstance(replacement_src_obj, str):
            print("  ⚠️ Skipping rule with invalid replacement_src")
            continue

        mode = mode_obj
        target_src = target_src_obj
        replacement_src = replacement_src_obj
        note = note_obj if isinstance(note_obj, str) else ""

        target_basename = Path(target_src).name
        debug_note = f" ({note})" if note.strip() else ""

        print(f"  🔎 Rule: {mode} | target={target_basename}{debug_note}")

        if mode == "replace_one":
            replaced = False

            for img in soup.find_all("img"):
                if not isinstance(img, Tag):
                    continue

                src_attr = img.get("src")
                if not isinstance(src_attr, str):
                    continue

                current_basename = _basename_from_src(src_attr)
                if current_basename == target_basename:
                    print(f"    ↪ Found image: {src_attr}")
                    img["src"] = replacement_src
                    img["style"] = "max-width: 100%; height: auto;"
                    replaced = True
                    print(f"    ✅ Replaced with: {replacement_src}")
                    break

            if not replaced:
                print(f"    ⚠️ Target not found: {target_basename}")

        elif mode == "replace_group":
            delete_src_obj = rule.get("delete_src", [])
            delete_basenames = {
                Path(name).name
                for name in delete_src_obj
                if isinstance(name, str)
            }

            target_img: Optional[Tag] = None
            deleted_any = False

            for img in soup.find_all("img"):
                if not isinstance(img, Tag):
                    continue

                src_attr = img.get("src")
                if not isinstance(src_attr, str):
                    continue

                if _basename_from_src(src_attr) == target_basename:
                    target_img = img
                    break

            if target_img is not None:
                old_src = target_img.get("src", "")
                print(f"    ↪ Found group anchor image: {old_src}")
                target_img["src"] = replacement_src
                target_img["style"] = "max-width: 100%; height: auto;"
                print(f"    ✅ Replaced anchor with: {replacement_src}")
            else:
                print(f"    ⚠️ Group anchor not found: {target_basename}")

            for img in soup.find_all("img"):
                if not isinstance(img, Tag):
                    continue

                src_attr = img.get("src")
                if not isinstance(src_attr, str):
                    continue

                current_basename = _basename_from_src(src_attr)
                if current_basename in delete_basenames:
                    print(f"    🗑 Removing grouped fragment: {src_attr}")
                    parent = img.parent if isinstance(img.parent, Tag) else None
                    img.decompose()
                    _remove_empty_ancestors(parent)
                    deleted_any = True

            if not delete_basenames:
                print("    ℹ️ No delete_src entries for this group rule")
            elif not deleted_any:
                print(f"    ⚠️ No grouped fragments removed for: {sorted(delete_basenames)}")

        else:
            print(f"  ⚠️ Unknown replacement mode: {mode}")


def cleanup_html(soup: BeautifulSoup, output_name: str) -> None:
    inject_css(soup)
    wrap_body_content(soup)
    rewrite_image_paths(
        soup,
        f"/static/lab_reference_media/{output_name.replace('.html', '')}"
    )

    # HTML cleanup / presentation cleanup
    remove_images_inside_headings(soup)
    remove_page_header_footer_paragraphs(soup)
    remove_decorative_transition_images(soup, output_name)
    normalize_image_classes(soup)

    # Anchors / structure
    add_heading_ids(soup)
    add_explicit_named_anchors(soup, output_name)
    add_biological_report_anchors(soup, output_name)
    replace_word_bookmark_ids_with_table_ids(soup)
    add_table_ids_from_captions(soup)

    # Manual image fixes
    apply_image_replacements(soup, output_name)

    # Final cleanup
    remove_empty_paragraphs(soup)


def output_name_for_docx(docx_name: str) -> str:
    if docx_name in OUTPUT_NAME_MAP:
        mapped = OUTPUT_NAME_MAP[docx_name]
        stem = Path(mapped).stem
        return f"{to_snake_case_filename(stem)}.html"

    stem = Path(docx_name).stem
    return f"{to_snake_case_filename(stem)}.html"


def convert_docx_to_html(docx_path: Path, media_dir: Path) -> str:
    media_dir.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [
            "pandoc",
            str(docx_path),
            "-f", "docx",
            "-t", "html",
            "--standalone",
            "--wrap=none",
            "--extract-media", str(media_dir),
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
        media_dir = LAB_REFERENCE_MEDIA_DIR / out_name.replace(".html", "")

        print(f"📄 Processing DOCX: {docx_path.name}")
        print(f"📄 Output name resolved to: {out_name}")
        print(f"\n📄 Converting {docx_path.name} -> {out_path.name}")

        try:
            raw_html = convert_docx_to_html(docx_path, media_dir)
            soup = BeautifulSoup(raw_html, "html.parser")
            cleanup_html(soup, out_name)
            out_path.write_text(str(soup), encoding="utf-8")
            print(f"✅ Saved HTML to: {out_path}")
            print(f"🖼️ Extracted media to: {media_dir}")
        except subprocess.CalledProcessError as exc:
            err = (exc.stderr or "").strip()
            print(f"❌ Pandoc failed for {docx_path.name}: {err or exc}")
        except Exception as exc:
            print(f"❌ Failed to convert {docx_path.name}: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())