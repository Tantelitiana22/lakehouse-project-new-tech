"""Dagster assets for dbt models and the Semantic Layer validation.

dbt v2 exposes no Python API, so the asset graph is derived from the compiled
``target/manifest.json`` (written by ``dbt parse``) and executed through the CLI
resource. Every dbt model becomes a Dagster asset whose key comes from the
model's ``meta.dagster.asset_key``, so a source's Bronze table and the model
reading it are wired with real lineage instead of a shared tag.

The Semantic Layer check maps onto ``dbt parse``. ``dbt sl validate`` would be
the precise equivalent of v1's ``mf validate-configs``, but every ``dbt sl``
subcommand requires a ``dbt_cloud.yml`` project and therefore cannot run in this
self-hosted lakehouse. ``dbt parse`` still compiles the semantic YAML, so it
rejects an unknown ``agg`` variant, an invalid entity type or a ``granularity``
that is not a valid time grain; it simply does not resolve cross-metric
references (a dangling ``numerator``), which is a Cloud-only check.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from dagster import (
    AssetKey,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ndr_lakehouse.domain.errors import DbtCommandError
from ndr_lakehouse.orchestration.dbt_runner import DbtCliResource

logger = logging.getLogger(__name__)

DEFAULT_DBT_RESOURCE = "dbt"

_ManifestNode = dict[str, Any]


def _dagster_asset_key(node: _ManifestNode) -> AssetKey | None:
    """Read ``meta.dagster.asset_key`` off a manifest node, if present.

    dbt v2 rejects a bare ``meta:`` block on sources and stores it under
    ``config.meta`` instead, so both locations are read.
    """

    meta = node.get("meta") or {}
    configured = (node.get("config") or {}).get("meta") or {}
    for candidate in (meta, configured):
        dagster = candidate.get("dagster") or {}
        key = dagster.get("asset_key")
        if key:
            return AssetKey(list(key))
    return None


def _index_manifest(
    manifest: dict[str, Any],
    *,
    catalog_name: str | None = None,
) -> dict[str, AssetKey]:
    """Map every source/model unique_id to the Dagster key it represents."""

    index: dict[str, AssetKey] = {}

    for unique_id, node in (manifest.get("sources") or {}).items():
        # Fall back to the ``(source_name, table_name)`` pair that the ingestion
        # factory already registers when no explicit Dagster key was declared.
        index[unique_id] = _dagster_asset_key(node) or AssetKey([node["source_name"], node["name"]])

    for unique_id, node in (manifest.get("nodes") or {}).items():
        if node.get("resource_type") == "model":
            explicit = _dagster_asset_key(node)
            if explicit is not None:
                index[unique_id] = explicit
            elif catalog_name:
                schema_name = str(node.get("schema") or "gold")
                index[unique_id] = AssetKey([catalog_name, schema_name, node["name"]])
            else:
                index[unique_id] = AssetKey([node["name"]])

    return index


def _iter_models(
    manifest: dict[str, Any],
    model_names: Iterable[str] | None,
) -> Iterable[_ManifestNode]:
    """Yield manifest model nodes, optionally restricted to ``model_names``."""

    wanted = set(model_names) if model_names is not None else None
    for node in (manifest.get("nodes") or {}).values():
        if node.get("resource_type") != "model":
            continue
        if wanted is not None and node.get("name") not in wanted:
            continue
        yield node


def _build_model_asset(
    node: _ManifestNode,
    *,
    index: dict[str, AssetKey],
    project_dir: Path,
    resource_key: str,
    catalog_name: str | None = None,
    on_model_materialized: Callable[[str, str], Any] | None = None,
) -> AssetsDefinition:
    """Build one Dagster asset that materialises a single dbt model."""

    model_name: str = node["name"]
    schema_name: str = str(node.get("schema") or "gold")
    asset_key = index[node["unique_id"]]
    key_prefix = list(asset_key.path[:-1]) or None
    upstream = [index[dep] for dep in (node.get("depends_on") or {}).get("nodes", ()) if dep in index]
    group_name = schema_name if catalog_name else "dbt"
    tags = (
        {
            "catalog": catalog_name,
            "zone": schema_name,
            "layer": schema_name,
            "dbt_model": model_name,
        }
        if catalog_name
        else {"layer": "dbt", "dbt_model": model_name}
    )

    @asset(
        name=asset_key.path[-1],
        key_prefix=key_prefix,
        deps=upstream or None,
        group_name=group_name,
        compute_kind="dbt",
        description=node.get("description") or f"dbt model {model_name}",
        tags=tags,
        required_resource_keys={resource_key},
    )
    # `context` is deliberately left unannotated, like every other asset in this
    # package: with `from __future__ import annotations` the annotation reaches
    # Dagster as the string "AssetExecutionContext" and its definition check
    # rejects it.
    def _dbt_model_asset(context) -> MaterializeResult:
        """Run ``dbt run --select <model>`` and surface its output."""

        dbt: DbtCliResource = getattr(context.resources, resource_key)
        result = dbt.invoke(["run", "--select", model_name])
        context.add_output_metadata(
            {
                **dbt.metadata(result),
                "project_dir": MetadataValue.path(str(project_dir)),
            }
        )
        if result.returncode != 0:
            raise DbtCommandError(f"dbt run --select {model_name} failed ({result.returncode})")
        if on_model_materialized is not None:
            on_model_materialized(schema_name, model_name)
        return MaterializeResult()

    return _dbt_model_asset


def build_dbt_assets(
    manifest_path: str | Path | None = None,
    project_dir: str | Path | None = None,
    *,
    model_names: Sequence[str] | None = None,
    resource_key: str = DEFAULT_DBT_RESOURCE,
    catalog_name: str | None = None,
    on_model_materialized: Callable[[str, str], Any] | None = None,
) -> list[AssetsDefinition]:
    """Build one Dagster asset per dbt model, from the compiled manifest.

    Returns an empty list when the manifest has not been compiled yet, so a use
    case without ``dbt parse`` still yields a valid graph.
    """

    if manifest_path is None or project_dir is None:
        logger.info("dbt assets need both a manifest path and a project dir; skipping")
        return []

    resolved_manifest = Path(manifest_path)
    resolved_project = Path(project_dir)
    if not resolved_manifest.exists():
        logger.warning(
            "dbt manifest not found at %s; dbt assets are not registered yet. Run `dbt parse` to compile it.",
            resolved_manifest,
        )
        return []

    manifest = json.loads(resolved_manifest.read_text(encoding="utf-8"))
    index = _index_manifest(manifest, catalog_name=catalog_name)

    assets = [
        _build_model_asset(
            node,
            index=index,
            project_dir=resolved_project,
            resource_key=resource_key,
            catalog_name=catalog_name,
            on_model_materialized=on_model_materialized,
        )
        for node in _iter_models(manifest, model_names)
    ]
    logger.info("Registered %d dbt assets from %s", len(assets), resolved_manifest)
    return assets


def build_metricflow_asset(
    *,
    gold_models: Sequence[str],
    dbt_project_dir: Path,
    metric: str,
    resource_key: str = DEFAULT_DBT_RESOURCE,
    catalog_name: str | None = None,
) -> AssetsDefinition:
    """Build the Semantic Layer validation asset for one use case's Gold models.

    Runs ``dbt parse`` instead of the retired ``mf validate-configs`` CLI, and
    declares the Gold models as dependencies so the check never runs against an
    unbuilt graph. ``dbt sl validate`` is deliberately avoided: it needs a
    ``dbt_cloud.yml`` project, so it fails on a self-hosted lakehouse.
    """

    project_dir = Path(dbt_project_dir)
    deps = [AssetKey([catalog_name, "gold", name]) if catalog_name else AssetKey(name) for name in gold_models]
    tags: dict[str, str] = {"layer": "semantic", "owner": "data-platform"}
    if catalog_name:
        tags["catalog"] = catalog_name
        tags["zone"] = "semantic"

    @asset(
        name="metricflow_semantic_validation",
        deps=deps,
        group_name="semantic",
        compute_kind="dbt",
        description=(
            "Compile the Semantic Layer semantic graph (entities, dimensions, simple metrics "
            f"and the {metric} ratio metric) after Gold models are materialised."
        ),
        tags=tags,
        required_resource_keys={resource_key},
    )
    def metricflow_semantic_validation(context) -> dict[str, Any]:
        """Compile the semantic YAML with ``dbt parse`` and fail on any violation."""

        dbt: DbtCliResource = getattr(context.resources, resource_key)
        result = dbt.invoke(["parse"])
        context.add_output_metadata(
            {
                **dbt.metadata(result),
                "project_dir": MetadataValue.path(str(project_dir)),
                "metric": MetadataValue.text(metric),
            }
        )
        if result.returncode != 0:
            raise DbtCommandError(f"Semantic Layer validation failed ({result.returncode}): {result.stderr.strip()}")
        return {
            "return_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    return metricflow_semantic_validation
