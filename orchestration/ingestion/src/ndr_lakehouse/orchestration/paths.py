"""Canonical filesystem locations for the Dagster/dbt package."""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[5]

DEFAULT_USE_CASE_ROOT = REPO_ROOT / "use_case"
