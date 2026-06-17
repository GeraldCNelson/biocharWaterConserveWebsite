#!/usr/bin/env python3
"""
summarize_diagnostics_metadata.py

Scan Python files in biochar_app/diagnostics, extract module metadata, and
write a compact text inventory.

Run from repo root:
    python biochar_app/scripts/dev-tools/summarize_diagnostics_metadata.py
"""

from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DIAGNOSTICS_DIR = PROJECT_ROOT / "biochar_app" / "diagnostics"
OUTPUT_PATH = DIAGNOSTICS_DIR / "diagnostics_python_metadata_summary.txt"


def get_module_docstring(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8")
        module = ast.parse(text)
        return ast.get_docstring(module) or ""
    except Exception as exc:
        return f"[Could not parse docstring: {exc}]"


def get_imports(path: Path) -> list[str]:
    imports: list[str] = []

    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return imports

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imports.append(module)

    return sorted(set(imports))


def get_top_level_functions(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    return [
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    ]


def get_constants(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    constants: list[str] = []

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    constants.append(target.id)
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Name) and target.id.isupper():
                constants.append(target.id)

    return constants


def find_keywords(text: str) -> list[str]:
    keywords = [
        "duplicate",
        "duplicates",
        "gap",
        "gaps",
        "clock",
        "timestamp",
        "timezone",
        "time zone",
        "setclock",
        "set clock",
        "jump",
        "jumps",
        "logger",
        "irrigation",
        "S3M",
        "PB9",
    ]

    lower = text.lower()
    found = []

    for keyword in keywords:
        if keyword.lower() in lower:
            found.append(keyword)

    return found


def summarize_file(path: Path) -> str:
    rel = path.relative_to(PROJECT_ROOT)
    text = path.read_text(encoding="utf-8", errors="replace")
    docstring = get_module_docstring(path)
    imports = get_imports(path)
    functions = get_top_level_functions(path)
    constants = get_constants(path)
    keywords = find_keywords(text)

    lines: list[str] = []
    lines.append("=" * 88)
    lines.append(str(rel))
    lines.append("-" * 88)

    lines.append("Keywords found:")
    lines.append(", ".join(keywords) if keywords else "None")

    lines.append("")
    lines.append("Top-level constants:")
    lines.append(", ".join(constants) if constants else "None")

    lines.append("")
    lines.append("Top-level functions:")
    lines.append(", ".join(functions) if functions else "None")

    lines.append("")
    lines.append("Imports:")
    lines.append(", ".join(imports) if imports else "None")

    lines.append("")
    lines.append("Module docstring:")
    lines.append(docstring.strip() if docstring.strip() else "None")

    lines.append("")

    return "\n".join(lines)


def main() -> None:
    if not DIAGNOSTICS_DIR.exists():
        raise FileNotFoundError(f"Diagnostics directory not found: {DIAGNOSTICS_DIR}")

    py_files = sorted(DIAGNOSTICS_DIR.rglob("*.py"))

    output_lines: list[str] = []
    output_lines.append("Diagnostics Python Metadata Summary")
    output_lines.append("=" * 88)
    output_lines.append(f"Diagnostics directory: {DIAGNOSTICS_DIR}")
    output_lines.append(f"Python files found: {len(py_files)}")
    output_lines.append("")

    for path in py_files:
        output_lines.append(summarize_file(path))

    OUTPUT_PATH.write_text("\n".join(output_lines), encoding="utf-8")

    print(f"Wrote diagnostics metadata summary:")
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()