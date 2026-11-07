"""Dagster Definitions assembly for one use case."""

from __future__ import annotations

import logging
from pathlib import Path

from dagster import Definitions

from ndr_lakehouse.asset_property.use_case import resolve_use_case
from ndr_lakehouse.orchestration.assets import build_ingestion_assets_from_yaml
from ndr_lakehouse.orchestration.dbt_assets import (
    build_dbt_assets,
    build_metricflow_asset,
)
from ndr_lakehouse.orchestration.dbt_source_sync import sync_dbt_sources
from ndr_lakehouse.orchestration.paths import DEFAULT_USE_CASE_ROOT
from ndr_lakehouse.orchestration.resources import build_resource_bundle

logger = logging.getLogger(__name__)


def build_definitions(
    use_case: str | Path | None = None,
    *,
    register_dbt: bool = True,
    sync_sources: bool = True,
    root: Path | None = None,
) -> Definitions:
    """Assemble one use case's ingestion, dbt and semantic assets.

    The use case is resolved from ``root`` (default: the repository's
    ``use_case/`` directory) by name, or by the single manifest marked
    ``default: true``. There is no implicit asset directory: everything the
    graph needs comes from the manifest.

    ``sync_sources`` only has an effect when ``register_dbt`` is true: the
    generated ``_sources.yml`` is consumed by dbt, so a dbt-free graph does not
    rewrite the dbt project on disk.
    """

    config = resolve_use_case(root or DEFAULT_USE_CASE_ROOT, use_case)

    if sync_sources and register_dbt:
        try:
            asset_label = config.assets_path.relative_to(DEFAULT_USE_CASE_ROOT)
        except ValueError:
            asset_label = config.assets_path
        try:
            sync_dbt_sources(
                asset_dir=config.assets_path,
                output_path=config.dbt_sources_file,
                source_label=f"{asset_label}/*.yaml",
            )
        except OSError as exc:
            logger.warning("Unable to synchronise dbt sources: %s", exc)

    assets = build_ingestion_assets_from_yaml(asset_dir=config.assets_path, recursive=False)
    resources = build_resource_bundle(config, register_dbt=register_dbt)

    # The resource bundle is the single source of truth for "dbt is wired": it
    # already required the project file, the compiled manifest and a usable
    # ``DbtCliResource``. Deriving the assets from it keeps the bundle and the
    # graph in agreement, so a half-configured dbt never yields assets whose
    # upstream resources are missing (or a semantic asset with phantom deps).
    if "dbt" in resources:
        dbt_assets = build_dbt_assets(
            manifest_path=config.dbt_manifest_path,
            project_dir=config.dbt_project_path,
        )
        if dbt_assets:
            assets = [
                *assets,
                *dbt_assets,
                build_metricflow_asset(
                    gold_models=config.gold_models,
                    dbt_project_dir=config.dbt_project_path,
                    metric=config.semantic_metric,
                ),
            ]

    return Definitions(assets=assets, resources=resources)
