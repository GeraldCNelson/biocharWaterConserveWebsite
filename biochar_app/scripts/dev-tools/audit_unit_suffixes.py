#!/usr/bin/env python3
"""
Audit variable names containing _gal or _L.

Example:
    python biochar_app/scripts/development/audit_unit_suffixes.py
"""

from collections import Counter
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]

SEARCH_EXTENSIONS = {".py"}

SKIP_DIRS = {
    ".git",
    ".idea",
    ".venv",
    ".biochar_py313",
    "__pycache__",
    "node_modules",
}

TOKEN_RE = re.compile(
    r"\b(?P<token>[A-Za-z_][A-Za-z0-9_]*_"
    r"(?P<suffix>gal|L|pct|degF|degC|in|ft|mm|cm|m|gpm|gph|psi|kPa|"
    r"dS_per_m|ng_per_g|ug_per_g|mg_per_kg|ppm|ppb))\b"
)

def iter_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file():
            continue

        if path.suffix not in SEARCH_EXTENSIONS:
            continue

        if any(part in SKIP_DIRS for part in path.parts):
            continue

        yield path

def main():
    counts = Counter()
    suffix_counts = Counter()
    total_hits = 0

    print("\n========== UNIT SUFFIX AUDIT ==========\n")

    for path in sorted(iter_files(ROOT)):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue

        file_has_hits = False

        for lineno, line in enumerate(lines, start=1):
            matches = list(TOKEN_RE.finditer(line))

            if not matches:
                continue

            if not file_has_hits:
                print(path.relative_to(ROOT))
                file_has_hits = True

            for match in matches:
                token = match.group("token")
                suffix = match.group("suffix")

                counts[token] += 1
                suffix_counts[suffix] += 1
                total_hits += 1

            print(f"  {lineno:5d}: {line.rstrip()}")

        if file_has_hits:
            print()

    print("\n========== SUMMARY BY VARIABLE ==========\n")

    for token, n in sorted(counts.items()):
        print(f"{token:<40} {n:5d}")

    print("\n========== SUMMARY BY SUFFIX ==========\n")

    for suffix, n in sorted(suffix_counts.items()):
        print(f"_{suffix:<15} {n:5d}")

    print("\n--------------------------------")
    print(f"Unique variables : {len(counts)}")
    print(f"Total occurrences: {total_hits}")

if __name__ == "__main__":
    main()