#!/usr/bin/env python3
"""
inspect_config_exports.py

Developer diagnostic tool for inventorying public symbols in biochar_app/config.

Features:
- Groups outputs by module
- Suggests curated __init__.py import blocks automatically
- Searches the codebase for import usage of discovered symbols
- Searches the codebase for broader text usage of discovered symbols

Suggestion policy (Option C):
- include symbols with at least one import hit
- plus symbols in MANUAL_EXPORT_ALLOWLIST
- still respect include rules for constants/classes/functions
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[1] if HERE.parent.name == "diagnostics" else Path.cwd()
CONFIG_DIR = PROJECT_ROOT / "config"

SKIP_FILES = {"__init__.py",
              "scripts/config.py",
              }

SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".idea",
    ".vscode",
    ".biochar_py313",
    "venv",
    ".venv",
    "node_modules",
    "archive",
    "diagnostics,"

}

SEARCH_SUFFIXES = {".py"}

INCLUDE_CONSTANTS_IN_SUGGESTIONS = True
INCLUDE_CLASSES_IN_SUGGESTIONS = True
INCLUDE_FUNCTIONS_IN_SUGGESTIONS = False

EXCLUDE_NAMES: set[str] = set()

# Option C: always expose these even if current import hits are zero
MANUAL_EXPORT_ALLOWLIST: set[str] = {
    "SENSOR_DEPTH_CODES",
    "SENSOR_DEPTH_LABELS",
    "SENSOR_DEPTH_VALUES",
    "DEFAULT_SENSOR_DEPTH_CODE",
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ModuleSymbols:
    module_name: str
    file_path: Path
    constants: list[str]
    functions: list[str]
    classes: list[str]

    @property
    def all_symbols(self) -> list[str]:
        return self.constants + self.functions + self.classes

    def candidate_symbols(self) -> list[str]:
        names: list[str] = []

        if INCLUDE_CONSTANTS_IN_SUGGESTIONS:
            names.extend(self.constants)
        if INCLUDE_CLASSES_IN_SUGGESTIONS:
            names.extend(self.classes)
        if INCLUDE_FUNCTIONS_IN_SUGGESTIONS:
            names.extend(self.functions)

        return [name for name in names if name not in EXCLUDE_NAMES]


@dataclass(frozen=True)
class UsageHit:
    path: Path
    line_no: int
    line_text: str


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def is_public_constant_name(name: str) -> bool:
    return name.isupper() and not name.startswith("_")


def extract_module_symbols(py_file: Path) -> ModuleSymbols:
    with py_file.open("r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=str(py_file))

    constants: list[str] = []
    functions: list[str] = []
    classes: list[str] = []

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.name.startswith("_"):
                functions.append(node.name)

        elif isinstance(node, ast.ClassDef):
            if not node.name.startswith("_"):
                classes.append(node.name)

        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and is_public_constant_name(target.id):
                    constants.append(target.id)

        elif isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Name) and is_public_constant_name(target.id):
                constants.append(target.id)

    return ModuleSymbols(
        module_name=py_file.stem,
        file_path=py_file,
        constants=sorted(set(constants)),
        functions=sorted(set(functions)),
        classes=sorted(set(classes)),
    )


def iter_config_modules(config_dir: Path) -> Iterable[Path]:
    for py_file in sorted(config_dir.glob("*.py")):
        if py_file.name in SKIP_FILES:
            continue
        yield py_file


# ---------------------------------------------------------------------------
# Search helpers
# ---------------------------------------------------------------------------

def iter_project_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        rel_parts = set(path.relative_to(root).parts)
        if rel_parts & SKIP_DIRS:
            continue
        if path.suffix not in SEARCH_SUFFIXES:
            continue
        yield path


def is_config_file(path: Path) -> bool:
    try:
        rel = path.relative_to(PROJECT_ROOT)
    except ValueError:
        return False
    return len(rel.parts) >= 2 and rel.parts[0] == "config"


def import_hit_matches_symbol(line: str, symbol: str) -> bool:
    line_s = line.strip()
    if "import" not in line_s:
        return False
    if symbol not in line_s:
        return False

    patterns = (
        "from biochar_app.config import",
        "from biochar_app.config.",
        "from biochar_app.scripts.config import",
        "import biochar_app.config",
        "import biochar_app.scripts.config",
    )
    return any(p in line_s for p in patterns)


def collect_import_hits_for_symbol(symbol: str) -> list[UsageHit]:
    hits: list[UsageHit] = []

    for py_file in iter_project_python_files(PROJECT_ROOT):
        if is_config_file(py_file):
            continue

        try:
            text = py_file.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        for i, line in enumerate(text.splitlines(), start=1):
            if import_hit_matches_symbol(line, symbol):
                hits.append(UsageHit(py_file, i, line.strip()))

    return hits


def collect_text_hits_for_symbol(symbol: str) -> list[UsageHit]:
    hits: list[UsageHit] = []

    for py_file in iter_project_python_files(PROJECT_ROOT):
        if is_config_file(py_file):
            continue

        try:
            text = py_file.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        for i, line in enumerate(text.splitlines(), start=1):
            if symbol in line:
                hits.append(UsageHit(py_file, i, line.strip()))

    return hits


def build_import_hit_index(modules: list[ModuleSymbols]) -> dict[str, list[UsageHit]]:
    hit_index: dict[str, list[UsageHit]] = {}
    for mod in modules:
        for symbol in mod.all_symbols:
            hit_index[symbol] = collect_import_hits_for_symbol(symbol)
    return hit_index


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def print_grouped_inventory(modules: list[ModuleSymbols]) -> None:
    print("\n=== CONFIG SYMBOL INVENTORY (grouped by module) ===")

    for mod in modules:
        print(f"\n[{mod.module_name}]  ({mod.file_path.name})")

        if mod.constants:
            print("  constants:")
            for name in mod.constants:
                print(f"    - {name}")
        else:
            print("  constants: none")

        if mod.functions:
            print("  functions:")
            for name in mod.functions:
                print(f"    - {name}")
        else:
            print("  functions: none")

        if mod.classes:
            print("  classes:")
            for name in mod.classes:
                print(f"    - {name}")
        else:
            print("  classes: none")


def build_init_block(
    modules: list[ModuleSymbols],
    import_hit_index: dict[str, list[UsageHit]],
) -> str:
    lines: list[str] = []
    lines.append('"""')
    lines.append("biochar_app.config")
    lines.append("")
    lines.append("Curated public config exports.")
    lines.append('"""')
    lines.append("")

    exported_names: list[str] = []

    for mod in modules:
        candidate_names = mod.candidate_symbols()
        selected_names = [
            name
            for name in candidate_names
            if name in MANUAL_EXPORT_ALLOWLIST or len(import_hit_index.get(name, [])) > 0
        ]

        if not selected_names:
            continue

        lines.append(f"from .{mod.module_name} import (")
        for name in selected_names:
            lines.append(f"    {name},")
            exported_names.append(name)
        lines.append(")")
        lines.append("")

    if exported_names:
        lines.append("__all__ = [")
        for name in exported_names:
            lines.append(f'    "{name}",')
        lines.append("]")
    else:
        lines.append("__all__: list[str] = []")

    return "\n".join(lines)


def print_suggested_init(
    modules: list[ModuleSymbols],
    import_hit_index: dict[str, list[UsageHit]],
) -> None:
    print("\n=== SUGGESTED config/__init__.py BLOCK (curated, Option C) ===\n")
    print(build_init_block(modules, import_hit_index))


def print_usage_report(modules: list[ModuleSymbols]) -> None:
    print("\n=== IMPORT / USAGE SEARCH ===")

    for mod in modules:
        print(f"\n[{mod.module_name}]")

        for symbol in mod.all_symbols:
            import_hits = collect_import_hits_for_symbol(symbol)
            text_hits = collect_text_hits_for_symbol(symbol)

            import_keys = {(h.path, h.line_no, h.line_text) for h in import_hits}
            text_only_hits = [
                h for h in text_hits
                if (h.path, h.line_no, h.line_text) not in import_keys
            ]

            print(f"  {symbol}")
            print(f"    import hits: {len(import_hits)}")
            for hit in import_hits[:8]:
                rel = hit.path.relative_to(PROJECT_ROOT)
                print(f"      - {rel}:{hit.line_no}: {hit.line_text}")
            if len(import_hits) > 8:
                print(f"      ... {len(import_hits) - 8} more import hits")

            print(f"    text hits:   {len(text_only_hits)}")
            for hit in text_only_hits[:8]:
                rel = hit.path.relative_to(PROJECT_ROOT)
                print(f"      - {rel}:{hit.line_no}: {hit.line_text}")
            if len(text_only_hits) > 8:
                print(f"      ... {len(text_only_hits) - 8} more text hits")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not CONFIG_DIR.exists():
        raise FileNotFoundError(f"Config directory not found: {CONFIG_DIR}")

    modules = [extract_module_symbols(py_file) for py_file in iter_config_modules(CONFIG_DIR)]
    import_hit_index = build_import_hit_index(modules)

    print(f"Project root : {PROJECT_ROOT}")
    print(f"Config dir   : {CONFIG_DIR}")

    print_grouped_inventory(modules)
    print_suggested_init(modules, import_hit_index)
    print_usage_report(modules)


if __name__ == "__main__":
    main()