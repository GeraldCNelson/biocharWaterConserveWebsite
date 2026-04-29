#!/usr/bin/env python3
"""
Find functions defined in the package that are never referenced anywhere.

Heuristic:
- Collect all top-level function definitions.
- Collect all call names + attribute call names.
- Report functions never referenced.

Does NOT detect dynamic calls, reflection, decorators, etc.
"""

from __future__ import annotations

import ast
from pathlib import Path
from collections import defaultdict

PACKAGE_ROOT = Path("biochar_app")


def get_py_files(root: Path):
    return list(root.rglob("*.py"))


def collect_definitions(py_files):
    defs = defaultdict(list)

    for path in py_files:
        try:
            tree = ast.parse(path.read_text())
        except Exception:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                if not node.name.startswith("_"):
                    defs[node.name].append(path)

    return defs


def collect_references(py_files):
    refs = set()

    for path in py_files:
        try:
            tree = ast.parse(path.read_text())
        except Exception:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    refs.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    refs.add(node.func.attr)

    return refs


def main():
    py_files = get_py_files(PACKAGE_ROOT)
    defs = collect_definitions(py_files)
    refs = collect_references(py_files)

    unused = []

    for func_name, paths in defs.items():
        if func_name not in refs:
            unused.append((func_name, paths))

    print(f"\nTotal functions defined: {len(defs)}")
    print(f"Functions referenced:    {len(refs)}")
    print(f"Likely UNUSED:           {len(unused)}\n")

    for func_name, paths in sorted(unused):
        for p in paths:
            print(f"{func_name}  ->  {p}")


if __name__ == "__main__":
    main()