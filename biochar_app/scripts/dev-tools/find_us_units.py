#!/usr/bin/env python3
import re
from pathlib import Path

# reuse exactly the same suffix‐patterns you’ll convert later
_SUFFIX_TO_VAR = {
    r"_degF$"    : "temp",
    r"_in$"      : "precip",
    r"_gal$"     : "irrigation",
    r"_swc_in$"  : "swc",
}

# build one big regex, but drop the $ since we want to find anywhere in the line
pattern = re.compile(
    r"(" + "|".join(pat.rstrip(r"\$") for pat in _SUFFIX_TO_VAR.keys()) + r")"
)

for py_file in Path("biochar_app").rglob("*.py"):
    for i, line in enumerate(py_file.open("r", encoding="utf-8"), start=1):
        if pattern.search(line):
            print(f"{py_file}:{i}: {line.rstrip()}")