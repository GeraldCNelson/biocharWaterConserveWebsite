#!/usr/bin/env python3
"""
find_unused_scripts.py

Heuristic tool to identify Python scripts/modules that are not imported by a target
codebase. This flags "not imported" files, which often correspond to one-off scripts,
old versions, or utilities run manually.

Usage (examples):
  python find_unused_scripts.py --project-root . --package-dir biochar_app --scripts-dir biochar/scripts
  python find_unused_scripts.py --project-root . --package-dir biochar_app --scripts-dir biochar/scripts --json out.json
  python find_unused_scripts.py --project-root . --package-dir biochar_app --scripts-dir biochar/scripts --include-subpackages
"""

from __future__ import annotations

import argparse
import ast
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional, Set, Tuple


@dataclass(frozen=True)
class ImportHit:
    importer_file: str
    imported_name: str  # as written (module or from-module)


def iter_py_files(root: Path) -> Iterator[Path]:
    for p in root.rglob("*.py"):
        # skip __pycache__ and hidden dirs
        parts = {part for part in p.parts}
        if "__pycache__" in parts:
            continue
        if any(part.startswith(".") for part in p.parts):
            continue
        yield p


def safe_read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            return path.read_text(encoding="latin-1")
        except Exception:
            return None
    except Exception:
        return None


def parse_imports(py_file: Path) -> Tuple[Set[str], Set[str]]:
    """
    Returns:
      (import_modules, from_modules)

    import x.y as z -> "x.y"
    from x.y import z -> "x.y"
    """
    text = safe_read_text(py_file)
    if text is None:
        return set(), set()

    try:
        tree = ast.parse(text, filename=str(py_file))
    except SyntaxError:
        # Skip files that aren't parseable for any reason
        return set(), set()

    import_mods: Set[str] = set()
    from_mods: Set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name:
                    import_mods.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            # node.module can be None for "from . import x"
            if node.module:
                from_mods.add(node.module)
            # We ignore relative level resolution; this is heuristic.

    return import_mods, from_mods


def file_to_module(project_root: Path, file_path: Path) -> str:
    """
    Convert a file path like:
      <root>/biochar/scripts/convert_word_to_html.py
    into a module-ish name:
      biochar.scripts.convert_word_to_html
    """
    rel = file_path.relative_to(project_root)
    rel_no_suffix = rel.with_suffix("")
    return ".".join(rel_no_suffix.parts)


def module_prefixes(mod: str) -> Set[str]:
    """
    "a.b.c" -> {"a", "a.b", "a.b.c"}
    Useful because code might import only "a.b" while the file is "a.b.c".
    """
    parts = mod.split(".")
    out = set()
    for i in range(1, len(parts) + 1):
        out.add(".".join(parts[:i]))
    return out


def is_likely_manual_script(path: Path) -> bool:
    """
    Heuristic: if the script looks like a CLI entrypoint, it may not be imported.
    We'll still report it, but you can optionally filter these out in output.
    """
    text = safe_read_text(path)
    if not text:
        return False
    # crude signals
    if "if __name__ == \"__main__\"" in text:
        return True
    if "argparse" in text:
        return True
    return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=".", help="Repo root (default: .)")
    ap.add_argument("--package-dir", required=True, help="Core package directory to scan for imports (e.g. biochar_app)")
    ap.add_argument("--scripts-dir", required=True, help="Directory containing scripts/modules to check (e.g. biochar/scripts)")
    ap.add_argument("--json", default=None, help="Optional JSON output path")
    ap.add_argument(
        "--exclude-manual",
        action="store_true",
        help="If set, exclude scripts that look like manual CLIs (argparse/__main__).",
    )
    args = ap.parse_args()

    project_root = Path(args.project_root).resolve()
    package_dir = (project_root / args.package_dir).resolve()
    scripts_dir = (project_root / args.scripts_dir).resolve()

    if not package_dir.exists():
        raise SystemExit(f"package-dir not found: {package_dir}")
    if not scripts_dir.exists():
        raise SystemExit(f"scripts-dir not found: {scripts_dir}")

    imported_modules: Set[str] = set()

    # Scan package for imports
    for py in iter_py_files(package_dir):
        import_mods, from_mods = parse_imports(py)
        imported_modules.update(import_mods)
        imported_modules.update(from_mods)

    # Expand prefixes so importing "biochar.scripts" counts as using submodules
    imported_with_prefixes: Set[str] = set()
    for mod in imported_modules:
        imported_with_prefixes.update(module_prefixes(mod))

    # Gather candidate scripts
    candidates = []
    for py in iter_py_files(scripts_dir):
        modname = file_to_module(project_root, py)
        candidates.append((py, modname))

    # Determine "not imported"
    unused = []
    for py, modname in sorted(candidates, key=lambda x: x[1]):
        if args.exclude_manual and is_likely_manual_script(py):
            continue

        # Consider used if any prefix of the file module is imported
        # or if the module itself/prefix appears in imports.
        prefixes = module_prefixes(modname)
        used = any(p in imported_with_prefixes for p in prefixes)

        if not used:
            unused.append(
                {
                    "file": str(py.relative_to(project_root)),
                    "module_guess": modname,
                    "looks_like_manual_cli": is_likely_manual_script(py),
                }
            )

    # Print report
    print(f"Scanned package imports in: {package_dir}")
    print(f"Checked scripts in:       {scripts_dir}")
    print()
    print(f"Imported module names (unique): {len(imported_with_prefixes)}")
    print(f"Scripts checked:                {len(candidates)}")
    print(f"Likely NOT imported:            {len(unused)}")
    print()

    for item in unused:
        flag = " (manual?)" if item["looks_like_manual_cli"] else ""
        print(f"- {item['file']}{flag}")

    if args.json:
        out_path = (project_root / args.json).resolve()
        out_path.write_text(json.dumps(unused, indent=2), encoding="utf-8")
        print()
        print(f"Wrote JSON: {out_path}")


if __name__ == "__main__":
    main()