"""Canonical filesystem locations for the Dagster/dbt package."""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[5]

ASSET_PROPERTY_DIR = PACKAGE_ROOT / "asset_property"
DBT_PROJECT_DIR = REPO_ROOT / "dbt_lakehouse"
DBT_SOURCES_PATH = DBT_PROJECT_DIR / "models" / "staging" / "_sources.yml"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"
