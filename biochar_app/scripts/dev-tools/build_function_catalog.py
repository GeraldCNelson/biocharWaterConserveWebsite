#!/usr/bin/env python3
"""Generate searchable Python/JavaScript function catalogs.

Outputs
-------
- ``biochar_app/docs/project/function_catalog.md`` for human searching.
- ``biochar_app/docs/project/function_catalog.json`` for tooling.

Run from the repository root::

    python biochar_app/scripts/dev-tools/build_function_catalog.py
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from biochar_app.config.paths import BASE_DIR


MARKDOWN_OUTPUT = BASE_DIR / "docs" / "project" / "function_catalog.md"
JSON_OUTPUT = BASE_DIR / "docs" / "project" / "function_catalog.json"
SKIP_PARTS = {
    ".biochar_py313",
    "node_modules",
    "__pycache__",
    "vendor",
    "archive",
    "data-processed",
    "data-raw",
}


@dataclass(frozen=True)
class FunctionEntry:
    language: str
    path: str
    line: int
    qualified_name: str
    signature: str
    kind: str
    summary: str
    docstring: str
    decorators: tuple[str, ...]
    exported: bool


def summary_line(text: str) -> str:
    for paragraph in text.strip().split("\n\n"):
        cleaned = " ".join(line.strip() for line in paragraph.splitlines()).strip()
        if cleaned:
            return cleaned
    return ""


def safe_unparse(node: ast.AST | None) -> str:
    if node is None:
        return ""
    try:
        return ast.unparse(node)
    except Exception:
        return ""


class PythonFunctionVisitor(ast.NodeVisitor):
    def __init__(self, relative_path: str) -> None:
        self.relative_path = relative_path
        self.scope: list[tuple[str, str]] = []
        self.entries: list[FunctionEntry] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append((node.name, "class"))
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._record_function(node, is_async=False)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._record_function(node, is_async=True)

    def _record_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        is_async: bool,
    ) -> None:
        parent_is_class = bool(self.scope and self.scope[-1][1] == "class")
        parent_is_function = bool(self.scope and self.scope[-1][1] == "function")
        if parent_is_class:
            kind = "async method" if is_async else "method"
        elif parent_is_function:
            kind = "nested async function" if is_async else "nested function"
        else:
            kind = "async function" if is_async else "function"

        qualified = ".".join([*(name for name, _ in self.scope), node.name])
        arguments = safe_unparse(node.args)
        returns = safe_unparse(node.returns)
        signature = f"({arguments})" + (f" -> {returns}" if returns else "")
        docstring = ast.get_docstring(node, clean=True) or ""
        decorators = tuple(safe_unparse(item) for item in node.decorator_list)
        self.entries.append(
            FunctionEntry(
                language="Python",
                path=self.relative_path,
                line=node.lineno,
                qualified_name=qualified,
                signature=signature,
                kind=kind,
                summary=summary_line(docstring) or "No docstring.",
                docstring=docstring,
                decorators=decorators,
                exported=not node.name.startswith("_"),
            )
        )

        self.scope.append((node.name, "function"))
        self.generic_visit(node)
        self.scope.pop()


def iter_source_files(pattern: str) -> Iterable[Path]:
    for path in sorted(BASE_DIR.rglob(pattern)):
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        yield path


def collect_python_functions() -> tuple[list[FunctionEntry], list[str]]:
    entries: list[FunctionEntry] = []
    errors: list[str] = []
    for path in iter_source_files("*.py"):
        relative = path.relative_to(BASE_DIR.parent).as_posix()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative)
        except (OSError, SyntaxError, UnicodeDecodeError) as error:
            errors.append(f"{relative}: {error}")
            continue
        visitor = PythonFunctionVisitor(relative)
        visitor.visit(tree)
        entries.extend(visitor.entries)
    return entries, errors


JS_PATTERNS = (
    re.compile(
        r"(?P<export>\bexport\s+)?(?P<async>\basync\s+)?function\s+"
        r"(?P<name>[A-Za-z_$][\w$]*)\s*\((?P<args>[^)]*)\)"
    ),
    re.compile(
        r"(?P<export>\bexport\s+)?(?:const|let|var)\s+"
        r"(?P<name>[A-Za-z_$][\w$]*)\s*=\s*(?P<async>async\s+)?"
        r"(?:\((?P<args>[^)]*)\)|(?P<single>[A-Za-z_$][\w$]*))\s*=>"
    ),
)
JS_METHOD_PATTERN = re.compile(
    r"(?m)^[ \t]*(?P<async>async\s+)?(?P<name>[A-Za-z_$][\w$]*)\s*"
    r"\((?P<args>[^)]*)\)\s*\{"
)
JS_RESERVED_WORDS = {"catch", "for", "if", "switch", "while", "with"}


def nearest_jsdoc(lines: list[str], line_index: int) -> str:
    index = line_index - 1
    while index >= 0 and not lines[index].strip():
        index -= 1
    if index < 0 or "*/" not in lines[index]:
        return ""

    block: list[str] = []
    while index >= 0:
        block.append(lines[index])
        if "/**" in lines[index]:
            break
        index -= 1
    if not block or "/**" not in block[-1]:
        return ""

    text = "\n".join(reversed(block))
    text = re.sub(r"^\s*/\*\*|\*/\s*$", "", text, flags=re.DOTALL)
    cleaned_lines = [re.sub(r"^\s*\*\s?", "", line) for line in text.splitlines()]
    description = []
    for line in cleaned_lines:
        if line.strip().startswith("@"):  # stop before JSDoc tags
            break
        description.append(line)
    return " ".join(" ".join(description).split())


def collect_javascript_from_text(
    text: str,
    relative_path: str,
    *,
    line_offset: int = 0,
) -> list[FunctionEntry]:
    entries: list[FunctionEntry] = []
    lines = text.splitlines()
    occupied: set[tuple[int, str]] = set()
    for pattern in JS_PATTERNS:
        for match in pattern.finditer(text):
            name = match.group("name")
            line_index = text.count("\n", 0, match.start())
            identity = (line_index, name)
            if identity in occupied:
                continue
            occupied.add(identity)
            args = match.groupdict().get("args") or match.groupdict().get("single") or ""
            is_async = bool(match.groupdict().get("async"))
            exported = bool(match.groupdict().get("export"))
            docstring = nearest_jsdoc(lines, line_index)
            entries.append(
                FunctionEntry(
                    language="JavaScript",
                    path=relative_path,
                    line=line_offset + line_index + 1,
                    qualified_name=name,
                    signature=f"({args.strip()})",
                    kind="async function" if is_async else "function",
                    summary=docstring or "No JSDoc summary.",
                    docstring=docstring,
                    decorators=(),
                    exported=exported,
                )
            )

    # Class and object-literal methods use ``name(args) {`` rather than the
    # function keyword or an arrow. Reserved control-flow forms are excluded.
    for match in JS_METHOD_PATTERN.finditer(text):
        name = match.group("name")
        if name in JS_RESERVED_WORDS:
            continue
        line_index = text.count("\n", 0, match.start())
        identity = (line_index, name)
        if identity in occupied:
            continue
        occupied.add(identity)
        is_async = bool(match.groupdict().get("async"))
        docstring = nearest_jsdoc(lines, line_index)
        entries.append(
            FunctionEntry(
                language="JavaScript",
                path=relative_path,
                line=line_offset + line_index + 1,
                qualified_name=name,
                signature=f"({match.group('args').strip()})",
                kind="async method" if is_async else "method",
                summary=docstring or "No JSDoc summary.",
                docstring=docstring,
                decorators=(),
                exported=False,
            )
        )
    return entries


def collect_javascript_functions() -> list[FunctionEntry]:
    entries: list[FunctionEntry] = []
    for path in iter_source_files("*.js"):
        relative = path.relative_to(BASE_DIR.parent).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        entries.extend(collect_javascript_from_text(text, relative))

    script_re = re.compile(r"<script\b[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
    for path in iter_source_files("*.html"):
        relative = path.relative_to(BASE_DIR.parent).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in script_re.finditer(text):
            line_offset = text.count("\n", 0, match.start(1))
            entries.extend(
                collect_javascript_from_text(
                    match.group(1), relative, line_offset=line_offset
                )
            )
    return entries


def markdown_safe(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render_markdown(entries: list[FunctionEntry], errors: list[str]) -> str:
    python_count = sum(entry.language == "Python" for entry in entries)
    javascript_count = sum(entry.language == "JavaScript" for entry in entries)
    lines = [
        "# Function Catalog",
        "",
        "This file is generated from Python AST metadata and JavaScript source patterns.",
        "Regenerate both the Markdown and JSON catalogs with:",
        "",
        "```bash",
        "python biochar_app/scripts/dev-tools/build_function_catalog.py",
        "```",
        "",
        f"- Python functions and methods: **{python_count}**",
        f"- JavaScript functions: **{javascript_count}**",
        f"- Files with Python parse errors: **{len(errors)}**",
        "",
        "The JavaScript inventory is intentionally heuristic; dynamically created functions",
        "and some class/object method syntaxes may require manual review.",
        "",
    ]

    for language in ("Python", "JavaScript"):
        lines.extend([f"## {language}", ""])
        language_entries = [entry for entry in entries if entry.language == language]
        paths = sorted({entry.path for entry in language_entries})
        for path in paths:
            lines.extend([f"### `{path}`", ""])
            for entry in sorted(
                (item for item in language_entries if item.path == path),
                key=lambda item: (item.line, item.qualified_name),
            ):
                exported = "public/exported" if entry.exported else "internal"
                lines.append(
                    f"- **`{entry.qualified_name}{markdown_safe(entry.signature)}`** "
                    f"— line {entry.line}; {entry.kind}; {exported}. "
                    f"{markdown_safe(entry.summary)}"
                )
            lines.append("")

    if errors:
        lines.extend(["## Python parse errors", ""])
        lines.extend(f"- `{markdown_safe(error)}`" for error in errors)
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    python_entries, errors = collect_python_functions()
    javascript_entries = collect_javascript_functions()
    entries = sorted(
        [*python_entries, *javascript_entries],
        key=lambda item: (item.language, item.path, item.line, item.qualified_name),
    )

    MARKDOWN_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    MARKDOWN_OUTPUT.write_text(render_markdown(entries, errors), encoding="utf-8")
    JSON_OUTPUT.write_text(
        json.dumps(
            {
                "counts": {
                    "total": len(entries),
                    "python": len(python_entries),
                    "javascript": len(javascript_entries),
                    "python_parse_errors": len(errors),
                },
                "parse_errors": errors,
                "functions": [asdict(entry) for entry in entries],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        f"Wrote {len(entries)} function entries to {MARKDOWN_OUTPUT} and {JSON_OUTPUT}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
