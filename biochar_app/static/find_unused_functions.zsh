#!/usr/bin/env zsh
#
# find_unused_functions.zsh
#
# Scan the project for Python function definitions and count how often
# each function name is called.
#
# Rules (per Jerry):
#   - Start at the project root containing `biochar_app`
#   - Include:
#       * biochar_app/ (core app)
#       * biochar_app/pakbus/core
#       * biochar_app/pakbus/scripts
#   - Exclude:
#       * any `_archive` directory
#       * any `docs` directory
#       * all other pakbus subdirectories
#
# Outputs (in project root):
#   - all_defs_with_paths.txt        (name, file, lineno)
#   - function_usage_report.txt      (name, file, lineno, usage_count)
#

set -euo pipefail

echo "📌 Script directory:"
SCRIPT_DIR=${0:a:h}
echo "    $SCRIPT_DIR"
echo

# -------------------------------------------------------------------
# 1) Locate project root (directory containing `biochar_app`)
# -------------------------------------------------------------------
echo "🔎 Looking for project root (directory containing biochar_app)..."

PROJECT_ROOT=""
CUR="$SCRIPT_DIR"
while [[ -n "$CUR" && "$CUR" != "/" ]]; do
  if [[ -d "$CUR/biochar_app" ]]; then
    PROJECT_ROOT="$CUR"
    break
  fi
  echo "   → checking: $CUR"
  CUR=${CUR:h}
done

if [[ -z "$PROJECT_ROOT" ]]; then
  echo "❌ Could not find project root (no biochar_app directory found)."
  exit 1
fi

echo "✅ Project root found:"
echo "    $PROJECT_ROOT"
echo

APP_ROOT="$PROJECT_ROOT/biochar_app"

# -------------------------------------------------------------------
# 2) Run Python 3 to:
#    - collect function defs (name, file, lineno)
#    - count usage via AST Call nodes
#    - write the two report files
# -------------------------------------------------------------------
python3 << 'PY'
import ast
import sys
from pathlib import Path

# Base paths (derived from where this script lives)
script_dir = Path(__file__).resolve().parent
project_root = script_dir
# Walk up until we find biochar_app (mirror the zsh logic, but safer here)
while project_root != project_root.parent:
    if (project_root / "biochar_app").is_dir():
        break
    project_root = project_root.parent

app_root = project_root / "biochar_app"

print(f"📂 Python AST scan under:\n    {app_root}")

# Build list of Python files with filters:
#   - exclude any path containing "_archive" or "docs"
#   - if "pakbus" is in the path, only keep if next part is "core" or "scripts"
python_files = []

for path in sorted(app_root.rglob("*.py")):
    parts = path.parts

    # skip _archive and docs anywhere in the path
    if "_archive" in parts or "docs" in parts:
        continue

    if "pakbus" in parts:
        try:
            idx = parts.index("pakbus")
        except ValueError:
            continue
        subdir = parts[idx + 1] if idx + 1 < len(parts) else ""
        if subdir not in {"core", "scripts"}:
            # skip other pakbus subdirectories
            continue

    python_files.append(path)

print(f"   → Using {len(python_files)} Python files after filters.\n")

# -------------------------------------------------------------------
# 3) Collect function definitions & usage counts
# -------------------------------------------------------------------
from collections import Counter

defs = []            # (name, relpath, lineno)
usage = Counter()    # name -> number of Call occurrences

for path in python_files:
    try:
        src = path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"⚠️  Failed to read {path}: {e}", file=sys.stderr)
        continue

    try:
        tree = ast.parse(src, filename=str(path))
    except SyntaxError as e:
        print(f"⚠️  SyntaxError in {path}: {e}", file=sys.stderr)
        continue

    rel = path.relative_to(project_root).as_posix()

    # collect defs and calls in a single walk
    for node in ast.walk(tree):
        # function & async function defs
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defs.append((node.name, rel, node.lineno))

        # calls: foo(...) or module.foo(...)
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                usage[func.id] += 1
            elif isinstance(func, ast.Attribute):
                usage[func.attr] += 1

print(f"📄 Found {len(defs)} function definitions.")
print(f"   Unique names: {len({d[0] for d in defs})}")

# -------------------------------------------------------------------
# 4) Write reports
# -------------------------------------------------------------------
out_defs   = project_root / "all_defs_with_paths.txt"
out_usage  = project_root / "function_usage_report.txt"

# a) all_defs_with_paths.txt
with out_defs.open("w", encoding="utf-8") as f:
    for name, rel, lineno in sorted(defs, key=lambda t: (t[1], t[2], t[0])):
        f.write(f"{name}\t{rel}\t{lineno}\n")

# b) function_usage_report.txt
with out_usage.open("w", encoding="utf-8") as f:
    f.write("# function_name\tfile\tlineno\tusage_count\n")
    # Sort by function name, then file, then line
    for name, rel, lineno in sorted(defs, key=lambda t: (t[0], t[1], t[2])):
        count = usage.get(name, 0)
        f.write(f"{name}\t{rel}\t{lineno}\t{count}\n")

print("\n🎉 Done!")
print(f"➡ {out_defs}")
print(f"➡ {out_usage}")
PY