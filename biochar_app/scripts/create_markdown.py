"""
create_markdown.py  —  Convert Word (.docx) files into cleaned Markdown (.md)

Reads configuration exclusively from markdown_config.py:

  • docx_markdown_config: main page documents (intro, experiment, tech details)
  • modal_config:         modal “Directions” docs (Main, Summary)

For each configured .docx:

  1. Runs pandoc to convert .docx → GitHub-flavored Markdown (gfm).
  2. Replaces Pandoc's image/table placeholders with custom HTML using
     the metadata in markdown_config.py (figures + side-by-side pairs).
  3. Cleans up any leftover imageN.* references so we don't get 404s.
  4. Normalizes Pandoc's inline-math pattern $`...`$ → $...$ so that
     markdown-it + MathJax can render LaTeX properly.

Run manually whenever you update the source Word documents:

    (.venv) $ python biochar_app/scripts/create_markdown.py
"""

import re
import subprocess
from pathlib import Path

from markdown_config import docx_markdown_config, modal_config

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

# This file should live at: biochar_app/scripts/create_markdown.py
# So parent.parent should be: biochar_app/
BASE_DIR = Path(__file__).resolve().parent.parent  # -> biochar_app/

DOCX_DIR = BASE_DIR / "markdown" / "docx"   # source .docx files
MARKDOWN_DIR = BASE_DIR / "markdown"        # output .md files
IMAGES_DIR = BASE_DIR / "static" / "images"

MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------------
# Pandoc runner
# -------------------------------------------------------------------

def run_pandoc(input_path: Path, output_path: Path) -> None:
    """
    Run pandoc to convert docx → GitHub-flavored markdown (gfm).

    If the input file does not exist, log a warning and skip.
    """
    if not input_path.exists():
        print(f"⚠️ Skipping pandoc: source file not found: {input_path}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "pandoc",
        str(input_path),
        "-f", "docx",
        "-t", "gfm",
        "--wrap=none",
        "-o", str(output_path),
    ]
    print(f"📄 pandoc: {input_path.name} → {output_path.name}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Pandoc failed for {input_path}: {e}")


# -------------------------------------------------------------------
# Cleanup helpers
# -------------------------------------------------------------------

def _cleanup_loose_pandoc_images(md_text: str) -> str:
    """
    Remove loose Pandoc image references that we did not explicitly replace.

    We leave <figure> and <table> alone so they can be used as placeholders
    for our custom blocks. This only strips:
      - Markdown image syntax pointing to imageN.*
      - Raw <img src="imageN.*"> or <img src="media/imageN.*">
    """

    # 1) Markdown image syntax: ![alt](image2.jpg) or ![alt](media/image2.jpg)
    md_image_pattern = re.compile(
        r"!\[[^\]]*]\((?:media/)?image\d+\.(?:png|jpe?g|gif)\)",
        re.IGNORECASE,
    )
    md_text = md_image_pattern.sub("", md_text)

    # 2) HTML <img> tags that reference imageN.* or media/imageN.*
    html_img_pattern = re.compile(
        r"<img[^>]+src=\"(?:media/)?image\d+\.[^\"]*\"[^>]*>",
        re.IGNORECASE,
    )
    md_text = html_img_pattern.sub("", md_text)

    return md_text


# -------------------------------------------------------------------
# Main docs (intro, experiment, tech details)
# -------------------------------------------------------------------

def process_main_docs() -> None:
    """
    Convert the main Word docs to markdown and post-process images / cleanup
    according to docx_markdown_config.
    """
    for docx_name, cfg in docx_markdown_config.items():
        src_docx = DOCX_DIR / docx_name
        out_md = MARKDOWN_DIR / cfg["output_md"]

        print(f"\n📄 Processing main doc: {docx_name} → {out_md.name}")
        run_pandoc(src_docx, out_md)

        if not out_md.exists():
            # pandoc failed or source missing
            continue

        md_text = out_md.read_text(encoding="utf-8")

        images = cfg.get("images", []) or []
        side_pairs = cfg.get("side_by_side") or []

        # Filenames that participate in a side-by-side pair.
        # We do NOT give these their own single <figure> blocks.
        side_image_names = {f for pair in side_pairs for f in pair}

        # ------------------------------------------------------------------
        # 1. Replace the side-by-side table placeholder (if any)
        # ------------------------------------------------------------------
        if side_pairs:
            # For now we assume exactly one side-by-side pair list (intro: Figures 2 & 3).
            pair = side_pairs[0]
            img1 = next((img for img in images if img["file"] == pair[0]), None)
            img2 = next((img for img in images if img["file"] == pair[1]), None)

            if img1 and img2:
                # Use title if provided, otherwise fall back to alt.
                title1 = img1.get("title") or img1.get("alt", "")
                title2 = img2.get("title") or img2.get("alt", "")

                side_table_block = f"""
<table>
  <colgroup>
    <col style="width: 50%" />
    <col style="width: 50%" />
  </colgroup>
  <thead>
    <tr>
      <th>
        <p>
          <img src="/static/images/{img1['file']}"
               alt="{img1['alt']}"
               title="{title1}"
               style="max-width: 70%; height: auto; display: block; margin: 0 auto;" />
        </p>
        <p><em>{img1['caption']}</em></p>
      </th>
      <th>
        <p>
          <img src="/static/images/{img2['file']}"
               alt="{img2['alt']}"
               title="{title2}"
               style="max-width: 70%; height: auto; display: block; margin: 0 auto;" />
        </p>
        <p><em>{img2['caption']}</em></p>
      </th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
"""

                # Find the FIRST table that appears to be an image table
                # (contains "image" or "media/") and replace it in-place.
                table_pattern = re.compile(r"<table>.*?</table>", re.DOTALL | re.IGNORECASE)

                def _replace_first_image_table(match_iter, text, replacement):
                    for m in match_iter:
                        if "image" in m.group(0) or "media/" in m.group(0):
                            return text.replace(m.group(0), replacement, 1)
                    return text  # no match; caller can decide to append

                md_text_before = md_text
                md_text = _replace_first_image_table(
                    table_pattern.finditer(md_text),
                    md_text,
                    side_table_block,
                )

                # Fallback: if nothing changed, append at the end.
                if md_text == md_text_before:
                    print("ℹ️ No Pandoc image table found; appending side-by-side block at end.")
                    md_text += f"\n{side_table_block}\n"

        # ------------------------------------------------------------------
        # 2. Replace single-image <figure> placeholders (e.g., Figure 1)
        # ------------------------------------------------------------------
        # Grab all <figure> blocks as potential placeholders.
        figure_pattern = re.compile(r"<figure>.*?</figure>", re.DOTALL | re.IGNORECASE)
        figure_matches = figure_pattern.findall(md_text)

        # For intro.docx this will pick up the original location of Figure 1.
        for img_info in images:
            filename = img_info["file"]

            # Skip images that belong to side-by-side pairs; they’re already handled.
            if filename in side_image_names:
                continue

            alt = img_info.get("alt", "")
            caption = img_info.get("caption", "")
            title = img_info.get("title") or alt
            img_path = f"/static/images/{filename}"

            new_block = (
                "<figure>\n"
                f'  <img src="{img_path}" alt="{alt}" title="{title}" '
                'style="max-width: 70%; height: auto; display: block; margin: 0 auto;" />\n'
                '  <figcaption style="text-align: center;">'
                f"<p><em>{caption}</em></p></figcaption>\n"
                "</figure>\n"
            )

            if figure_matches:
                placeholder = figure_matches.pop(0)
                md_text = md_text.replace(placeholder, new_block, 1)
            else:
                # No placeholder left; append at the end as a last resort.
                md_text += f"\n{new_block}\n"

        # ------------------------------------------------------------------
        # 3. Clean up any leftover imageN.* references that we didn't touch
        # ------------------------------------------------------------------
        md_text = _cleanup_loose_pandoc_images(md_text)

        # ------------------------------------------------------------------
        # 4. Fix Pandoc's inline-math pattern $`...`$ so markdown-it + MathJax
        # see a normal $...$ TeX string instead of a <code>...</code> block.
        #
        # Example Pandoc output:
        #   $`\frac{T_{experiment}}{T_{reference}}`$
        # becomes:
        #   $\frac{T_{experiment}}{T_{reference}}$
        # ------------------------------------------------------------------
        inline_math_pattern = re.compile(r"\$`([^`]+)`\$")
        md_text = inline_math_pattern.sub(r"$\1$", md_text)

        out_md.write_text(md_text, encoding="utf-8")
        print(f"✅ Finished: {out_md.name}")


# -------------------------------------------------------------------
# Modal docs (help_main, help_summary)
# -------------------------------------------------------------------

def process_modals() -> None:
    """
    Convert modal Word docs to markdown based on modal_config.

    No special image handling here—just straight pandoc conversion.
    """
    for modal_id, modal in modal_config.items():
        src = DOCX_DIR / modal["source"]
        out = MARKDOWN_DIR / modal["output"]

        print(f"\n📄 Processing modal '{modal_id}': {src.name} → {out.name}")
        if not src.exists():
            print(f"⚠️ Modal source not found for '{modal_id}': {src}")
            continue

        run_pandoc(src, out)
        if out.exists():
            print(f"✅ Modal converted: {out.name}")


# -------------------------------------------------------------------
# Script entrypoint
# -------------------------------------------------------------------

if __name__ == "__main__":
    print(f"📂 DOCX_DIR     = {DOCX_DIR}")
    print(f"📂 MARKDOWN_DIR = {MARKDOWN_DIR}")
    print(f"📂 IMAGES_DIR   = {IMAGES_DIR}")

    process_main_docs()
    process_modals()

    print("\n🎉 Markdown generation complete.")