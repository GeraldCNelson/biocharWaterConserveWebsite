"""Focused checks for deployment requirements and generated project indexes."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from biochar_app.config.deployment_manifest import DEPLOYMENT_REQUIREMENTS
from biochar_app.config.paths import BASE_DIR


PROJECT_ROOT = BASE_DIR.parent
DEV_TOOLS = BASE_DIR / "scripts" / "dev-tools"
DOC_CATALOG = BASE_DIR / "docs" / "documentation_catalog.md"
FUNCTION_CATALOG_JSON = BASE_DIR / "docs" / "project" / "function_catalog.json"


def test_deployment_requirement_keys_are_unique() -> None:
    keys = [requirement["key"] for requirement in DEPLOYMENT_REQUIREMENTS]
    assert len(keys) == len(set(keys))
    assert any(key.startswith("biochar-report:") for key in keys)


def test_git_deployment_requirements_pass_preflight() -> None:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        [
            sys.executable,
            str(DEV_TOOLS / "check_deployment_requirements.py"),
            "--git-only",
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_documentation_catalog_contains_operational_guides() -> None:
    text = DOC_CATALOG.read_text(encoding="utf-8")
    assert "# Documentation Catalog" in text
    assert "biochar_app/docs/operations/deploy_to_main.md" in text
    assert "build_documentation_catalog.py" in text


def test_function_catalog_is_searchable_and_parseable() -> None:
    payload = json.loads(FUNCTION_CATALOG_JSON.read_text(encoding="utf-8"))
    assert payload["counts"]["python"] > 1000
    assert payload["counts"]["javascript"] > 20
    assert payload["counts"]["python_parse_errors"] == len(payload["parse_errors"])
    assert all(
        error.startswith("biochar_app/pakbus/")
        for error in payload["parse_errors"]
    ), payload["parse_errors"]

    names = {entry["qualified_name"] for entry in payload["functions"]}
    assert "biochar_lab_report" in names
    assert "build_deployment_requirements" in names
