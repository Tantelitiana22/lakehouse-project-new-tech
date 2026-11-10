"""Tests for the dbt/MetricFlow asset integration without a dbt runtime."""

from __future__ import annotations

from pathlib import Path

from dagster import AssetKey

from ndr_lakehouse.orchestration.dbt_assets import build_dbt_assets


def test_dbt_assets_are_skipped_without_manifest(tmp_path):
    assert build_dbt_assets(manifest_path=tmp_path / "missing.json") == []


def test_metricflow_asset_depends_on_configured_gold_models():
    from ndr_lakehouse.orchestration.dbt_assets import build_metricflow_asset

    asset = build_metricflow_asset(
        gold_models=["dim_a", "fct_b"],
        dbt_project_dir=Path("/tmp/dbt"),
        metric="prix",
    )
    key = next(iter(asset.keys))
    assert set(asset.asset_deps[key]) == {AssetKey("dim_a"), AssetKey("fct_b")}


def test_generic_module_has_no_use_case_constants():
    import ndr_lakehouse.orchestration.dbt_assets as mod

    assert not hasattr(mod, "GOLD_MODEL_NAMES")
    assert not hasattr(mod, "GOLD_ASSET_KEYS")
    assert not hasattr(mod, "metricflow_semantic_validation")
