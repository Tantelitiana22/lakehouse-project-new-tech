"""Dagster Definitions assembly for the Open Data Lakehouse."""

from __future__ import annotations

import logging

from dagster import Definitions

from ndr_lakehouse.orchestration.assets import build_ingestion_assets_from_yaml
from ndr_lakehouse.orchestration.dbt_assets import build_dbt_assets
from ndr_lakehouse.orchestration.dbt_source_sync import sync_dbt_sources
from ndr_lakehouse.orchestration.paths import (
    ASSET_PROPERTY_DIR,
    DBT_MANIFEST_PATH,
    DBT_PROJECT_DIR,
    DBT_SOURCES_PATH,
)

logger = logging.getLogger(__name__)


def build_definitions() -> Definitions:
    """Assemble ingestion, dbt and semantic assets into one lineage graph."""

    if ASSET_PROPERTY_DIR.exists():
        try:
            sync_dbt_sources(
                asset_dir=ASSET_PROPERTY_DIR,
                output_path=DBT_SOURCES_PATH,
                source_label="orchestration/ingestion/src/ndr_lakehouse/asset_property/*.yaml",
            )
        except OSError as exc:
            logger.warning("Unable to synchronise dbt sources: %s", exc)

    ingestion_assets = build_ingestion_assets_from_yaml(asset_dir=ASSET_PROPERTY_DIR)
    dbt_assets = build_dbt_assets(
        manifest_path=DBT_MANIFEST_PATH,
        project_dir=DBT_PROJECT_DIR,
    )
    assets = [*ingestion_assets, *dbt_assets]

    # TODO(Task 8): pass resources=build_resource_bundle(use_case, ...) once
    # build_definitions takes a use case.
    return Definitions(assets=assets)
