"""Tests for the dbt/MetricFlow asset integration without a dbt runtime."""

from __future__ import annotations

from dagster import AssetKey
from ndr_lakehouse.orchestration.dbt_assets import (
    GOLD_ASSET_KEYS,
    build_dbt_assets,
    metricflow_semantic_validation,
)


def test_gold_asset_keys_are_the_three_gold_models():
    assert GOLD_ASSET_KEYS == [
        AssetKey("dim_communes"),
        AssetKey("fct_mutations"),
        AssetKey("fct_lots_batis"),
    ]


def test_dbt_assets_are_skipped_without_manifest(tmp_path):
    assert build_dbt_assets(manifest_path=tmp_path / "missing.json") == []


def test_metricflow_validation_asset_key_and_dependencies():
    assert metricflow_semantic_validation.keys == {AssetKey("metricflow_semantic_validation")}
    assert set(
        metricflow_semantic_validation.asset_deps[AssetKey("metricflow_semantic_validation")]
    ) == set(GOLD_ASSET_KEYS)
