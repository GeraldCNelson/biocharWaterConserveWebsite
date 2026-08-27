"""Declarative requirements checked before test or production deployment.

Delivery modes
--------------
``git``
    The path must exist, be nonempty, and be tracked by Git.
``external``
    The path is intentionally transferred separately (for example by rsync)
    and must exist and be nonempty on the target system.

Whenever possible, requirements are derived from their authoritative feature
configuration instead of repeating filenames here.
"""

from __future__ import annotations

from typing import Literal, TypedDict

from biochar_app.config.biochar_lab_reports import (
    BIOCHAR_LAB_REPORTS,
    biochar_lab_report_path,
)
from biochar_app.config.paths import (
    DOWNLOADS_BASE_DIR,
    MARKDOWN_OUTPUTS_DIR,
    PARQUET_SUMMARY_DIR,
)
from biochar_app.markdown.tools.markdown_config import (
    iter_document_specs,
    output_name_for_spec,
)


class DeploymentRequirement(TypedDict):
    key: str
    path: str
    delivery: Literal["git", "external"]
    kind: Literal["file", "directory"]
    description: str


def build_deployment_requirements() -> tuple[DeploymentRequirement, ...]:
    """Return deployment requirements assembled from feature configuration."""
    requirements: list[DeploymentRequirement] = []

    for report_key in sorted(BIOCHAR_LAB_REPORTS):
        requirements.append(
            {
                "key": f"biochar-report:{report_key}",
                "path": str(biochar_lab_report_path(report_key)),
                "delivery": "git",
                "kind": "file",
                "description": "Biochar laboratory source report",
            }
        )

    for spec in iter_document_specs():
        output_name = output_name_for_spec(spec)
        requirements.append(
            {
                "key": f"website-document:{output_name}",
                "path": str(MARKDOWN_OUTPUTS_DIR / output_name),
                "delivery": "git",
                "kind": "file",
                "description": "Generated Word-authored website content",
            }
        )

    requirements.extend(
        [
            {
                "key": "generated-data:parquet-summary",
                "path": str(PARQUET_SUMMARY_DIR),
                "delivery": "external",
                "kind": "directory",
                "description": "Generated parquet summaries transferred by rsync",
            },
            {
                "key": "generated-data:downloads",
                "path": str(DOWNLOADS_BASE_DIR),
                "delivery": "external",
                "kind": "directory",
                "description": "Generated bulk-download products transferred by rsync",
            },
        ]
    )

    return tuple(requirements)


DEPLOYMENT_REQUIREMENTS = build_deployment_requirements()
