"""Tests for the dbt/MetricFlow asset integration without a dbt runtime."""

from __future__ import annotations

import json
from pathlib import Path

from dagster import AssetKey, materialize

import ndr_lakehouse.orchestration.dbt_assets as dbt_assets_module
from ndr_lakehouse.orchestration.dbt_assets import build_dbt_assets, build_metricflow_asset
from ndr_lakehouse.orchestration.dbt_runner import DbtCliResource, DbtCommandResult


def test_dbt_assets_are_skipped_without_manifest(tmp_path):
    # Given
    missing_manifest = tmp_path / "missing.json"

    # When
    assets = build_dbt_assets(manifest_path=missing_manifest)

    # Then
    assert assets == []


def test_metricflow_asset_depends_on_configured_gold_models():
    # Given
    models = ["dim_a", "fct_b"]

    # When
    asset = build_metricflow_asset(
        gold_models=models,
        dbt_project_dir=Path("/tmp/dbt"),
        metric="prix",
    )
    key = next(iter(asset.keys))

    # Then
    assert set(asset.asset_deps[key]) == {AssetKey("dim_a"), AssetKey("fct_b")}


def test_generic_module_has_no_use_case_constants():
    # Given / # When
    exports = dir(dbt_assets_module)

    # Then
    assert "GOLD_MODEL_NAMES" not in exports
    assert "GOLD_ASSET_KEYS" not in exports
    assert "metricflow_semantic_validation" not in exports


def test_build_dbt_assets_scopes_keys_and_invokes_materialized_hook(tmp_path, mocker):
    # Given
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "sources": {
                    "source.proj.dedup.src_a": {
                        "source_name": "dedup",
                        "name": "src_a",
                        "config": {"meta": {"dagster": {"asset_key": ["demo_cat", "deduplication_zone", "src_a"]}}},
                    }
                },
                "nodes": {
                    "model.proj.dim_a": {
                        "unique_id": "model.proj.dim_a",
                        "resource_type": "model",
                        "name": "dim_a",
                        "schema": "gold",
                        "description": "Dim A",
                        "depends_on": {"nodes": ["source.proj.dedup.src_a"]},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    recorded: list[tuple[str, str]] = []
    dbt = DbtCliResource(project_dir=str(tmp_path))
    mocker.patch.object(DbtCliResource, "invoke", return_value=DbtCommandResult(("dbt",), 0, "ok", ""))

    # When
    assets = build_dbt_assets(
        manifest_path=manifest,
        project_dir=tmp_path,
        model_names=["dim_a"],
        catalog_name="demo_cat",
        on_model_materialized=lambda schema, model: recorded.append((schema, model)),
    )
    metricflow = build_metricflow_asset(
        gold_models=["dim_a"],
        dbt_project_dir=tmp_path,
        metric="prix",
        catalog_name="demo_cat",
    )
    result = materialize(assets, resources={"dbt": dbt})

    # Then
    assert len(assets) == 1
    assert assets[0].key == AssetKey(["demo_cat", "gold", "dim_a"])
    assert set(assets[0].asset_deps[assets[0].key]) == {AssetKey(["demo_cat", "deduplication_zone", "src_a"])}
    assert set(metricflow.asset_deps[metricflow.key]) == {AssetKey(["demo_cat", "gold", "dim_a"])}
    assert result.success
    assert recorded == [("gold", "dim_a")]
