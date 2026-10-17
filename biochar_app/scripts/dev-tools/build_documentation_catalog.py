#!/usr/bin/env python3
"""Generate a searchable catalog of Markdown documentation in biochar_app.

Run from the repository root::

    python biochar_app/scripts/dev-tools/build_documentation_catalog.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from biochar_app.config.paths import BASE_DIR  # noqa: E402


OUTPUT_PATH = BASE_DIR / "docs" / "documentation_catalog.md"
SKIP_PARTS = {".biochar_py313", "node_modules", "__pycache__"}


@dataclass(frozen=True)
class DocumentEntry:
    title: str
    path: str
    category: str
    status: str
    description: str


def clean_markdown(text: str) -> str:
    text = re.sub(r"!\[[^]]*]\([^)]*\)", "", text)
    text = re.sub(r"\[([^]]+)]\([^)]*\)", r"\1", text)
    text = re.sub(r"[`*_>#]", "", text)
    return " ".join(text.split()).strip()


def document_title(path: Path, text: str) -> str:
    for line in text.splitlines():
        match = re.match(r"^#\s+(.+?)\s*$", line)
        if match:
            return clean_markdown(match.group(1))
    return path.stem.replace("_", " ").replace("-", " ").title()


def document_description(text: str) -> str:
    paragraph: list[str] = []
    in_code = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("```"):
            in_code = not in_code
            continue
        if in_code or not line:
            if paragraph:
                break
            continue
        if line.startswith(("#", "---", "|", "- [", "* [")):
            continue
        metadata_line = clean_markdown(line)
        if re.match(
            r"^(Document Status|Last Updated|Status|Version|Project|"
            r"GitHub repository|Project website|Repository location|"
            r"Reviewed branch|Review date|Scope):",
            metadata_line,
            re.IGNORECASE,
        ):
            continue
        paragraph.append(line)
        if len(" ".join(paragraph)) >= 180:
            break
    description = clean_markdown(" ".join(paragraph))
    if len(description) > 220:
        description = description[:217].rstrip() + "..."
    return description or "No summary found."


def classify(path: Path) -> tuple[str, str]:
    relative = path.relative_to(BASE_DIR)
    parts = relative.parts
    path_text = relative.as_posix().lower()

    if "archive" in path_text:
        return "Archive and historical notes", "historical"
    if path == OUTPUT_PATH or "outputs_md" in parts:
        return "Generated website content", "generated"
    if "data-processed" in parts or "diagnostics/reports" in path_text:
        return "Generated analyses and reports", "generated"
    if relative == Path("docs/application_architecture_status.md"):
        return "Project development", "current"
    if parts[:2] == ("docs", "operations"):
        return "Operations and workflows", "current"
    if parts[:2] == ("docs", "project") or path == OUTPUT_PATH:
        return "Project development", "current"
    if parts[:2] == ("docs", "research"):
        return "Research and scientific methods", "current"
    if parts[:2] in {
        ("docs", "reference_manuals"),
        ("docs", "reference_notes"),
    }:
        return "Technical reference", "reference"
    if "geospatial" in parts:
        return "Geospatial workflows", "current"
    if "readmes" in parts or path.name.lower() == "readme.md":
        return "Component guides and README files", "current"
    if "chat_summaries" in parts:
        return "Research and working notes", "reference"
    if parts and parts[0] == "markdown":
        return "Website and technical reference", "reference"
    return "Other project documentation", "reference"


def tracked_markdown_paths() -> list[Path]:
    """Return Markdown files represented in the repository index.

    The catalog describes repository documentation, not local drafts or
    generated reports that have not been selected for version control. New
    documents appear as soon as they are staged, so the catalog can be rebuilt
    in the same commit that introduces them.
    """
    result = subprocess.run(
        [
            "git",
            "ls-files",
            "--cached",
            "-z",
            "--",
            str(BASE_DIR.relative_to(REPOSITORY_ROOT)),
        ],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        check=True,
    )
    paths = []
    for raw_path in result.stdout.split(b"\0"):
        if not raw_path:
            continue
        path = REPOSITORY_ROOT / raw_path.decode("utf-8", errors="surrogateescape")
        if path.suffix.lower() == ".md" and path.is_file():
            paths.append(path)
    return sorted(paths)


def collect_documents() -> list[DocumentEntry]:
    entries: list[DocumentEntry] = []
    for path in tracked_markdown_paths():
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        category, status = classify(path)
        entries.append(
            DocumentEntry(
                title=document_title(path, text),
                path=path.relative_to(BASE_DIR.parent).as_posix(),
                category=category,
                status=status,
                description=document_description(text),
            )
        )
    return entries


def render_catalog(entries: list[DocumentEntry]) -> str:
    categories = sorted({entry.category for entry in entries})
    lines = [
        "# Documentation Catalog",
        "",
        "This generated index lists Markdown documentation throughout `biochar_app`.",
        "Paths are relative to the repository root. Regenerate it with:",
        "",
        "```bash",
        "python biochar_app/scripts/dev-tools/build_documentation_catalog.py",
        "```",
        "",
        f"Documents indexed: **{len(entries)}**",
        "",
        "Status meanings: **current** is maintained guidance; **reference** is useful",
        "background; **generated** is produced by code; **historical** is archived context.",
        "",
    ]

    for category in categories:
        lines.extend([f"## {category}", ""])
        for entry in sorted(
            (item for item in entries if item.category == category),
            key=lambda item: (item.title.casefold(), item.path),
        ):
            lines.append(f"### {entry.title}")
            lines.append("")
            lines.append(f"- **Path:** `{entry.path}`")
            lines.append(f"- **Status:** {entry.status}")
            lines.append(f"- **Summary:** {entry.description}")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    entries = collect_documents()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(render_catalog(entries), encoding="utf-8")
    print(f"Wrote {len(entries)} entries to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
