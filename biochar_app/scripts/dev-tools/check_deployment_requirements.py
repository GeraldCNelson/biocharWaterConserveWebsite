#!/usr/bin/env python3
"""Validate files and generated data required by a deployed website.

Run from the repository root before committing and again on each server after
``git pull`` and data transfer::

    python biochar_app/scripts/dev-tools/check_deployment_requirements.py

Use ``--git-only`` before committing when externally transferred generated
data are not available locally. Use ``--json`` for machine-readable output.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

# Direct execution sets ``sys.path[0]`` to ``scripts/dev-tools`` rather than the
# repository root. Add the root before importing the application package so the
# documented command works without an external PYTHONPATH setting.
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from biochar_app.config.deployment_manifest import DEPLOYMENT_REQUIREMENTS
from biochar_app.config.paths import BASE_DIR


PROJECT_ROOT = BASE_DIR.parent


@dataclass(frozen=True)
class CheckResult:
    key: str
    path: str
    delivery: str
    ok: bool
    detail: str


def is_git_tracked(path: Path) -> bool:
    """Return whether Git tracks ``path`` relative to the repository root."""
    try:
        relative = path.resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError:
        return False

    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", str(relative)],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def directory_has_files(path: Path) -> bool:
    return any(candidate.is_file() for candidate in path.rglob("*"))


def looks_like_readable_pdf(path: Path) -> bool:
    if path.suffix.lower() != ".pdf":
        return True
    try:
        return path.read_bytes()[:5] == b"%PDF-"
    except OSError:
        return False


def check_requirement(requirement: dict[str, str]) -> CheckResult:
    path = Path(requirement["path"])
    kind = requirement["kind"]
    delivery = requirement["delivery"]

    if kind == "file":
        if not path.is_file():
            return CheckResult(
                requirement["key"], str(path), delivery, False, "missing file"
            )
        if path.stat().st_size == 0:
            return CheckResult(
                requirement["key"], str(path), delivery, False, "empty file"
            )
        if not looks_like_readable_pdf(path):
            return CheckResult(
                requirement["key"], str(path), delivery, False, "invalid PDF header"
            )
    elif kind == "directory":
        if not path.is_dir():
            return CheckResult(
                requirement["key"], str(path), delivery, False, "missing directory"
            )
        if not directory_has_files(path):
            return CheckResult(
                requirement["key"], str(path), delivery, False, "directory is empty"
            )
    else:
        return CheckResult(
            requirement["key"], str(path), delivery, False, f"unknown kind: {kind}"
        )

    if delivery == "git" and not is_git_tracked(path):
        return CheckResult(
            requirement["key"], str(path), delivery, False, "exists but is not tracked by Git"
        )

    return CheckResult(requirement["key"], str(path), delivery, True, "ok")


def run_checks(*, git_only: bool = False) -> list[CheckResult]:
    requirements = [
        requirement
        for requirement in DEPLOYMENT_REQUIREMENTS
        if not git_only or requirement["delivery"] == "git"
    ]
    return [check_requirement(requirement) for requirement in requirements]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--git-only",
        action="store_true",
        help="Check only assets delivered through Git.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Write results as JSON instead of a human-readable report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results = run_checks(git_only=args.git_only)

    if args.json:
        print(json.dumps([asdict(result) for result in results], indent=2))
    else:
        for result in results:
            marker = "PASS" if result.ok else "FAIL"
            print(f"[{marker}] {result.key}: {result.detail}")
            if not result.ok:
                print(f"       {result.path}")
        passed = sum(result.ok for result in results)
        print(f"\nDeployment requirements: {passed}/{len(results)} passed")

    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
