"""The repo-local use_case package is the Dagster entrypoint."""

from __future__ import annotations

from pathlib import Path

from dagster import AssetKey


def test_packaged_use_case_builds_the_full_graph():
    import use_case

    keys = use_case.defs.resolve_all_asset_keys()
    assert AssetKey(["landing", "communes_geo_77"]) in keys
    assert AssetKey(["lakehouse", "bronze", "dvf_mutations_77"]) in keys
    assert AssetKey(["lakehouse", "bronze", "dvf_lots_batis_77"]) in keys

    # Pin the whole graph, not a sample: this environment has no dbt extra and no
    # compiled manifest, so exactly the six ingestion keys are expected and the
    # semantic asset must be absent rather than dangling on phantom deps.
    assert len(keys) == 6
    assert AssetKey("metricflow_semantic_validation") not in keys
    assert AssetKey("lakehouse_dbt_assets") not in keys
    assert sorted(use_case.defs.resources) == [
        "native_ingestor",
        "spark_iceberg",
        "uc_provisioner",
    ]


def test_no_asset_yaml_remains_in_the_library():
    # Anchor on this file, not on an imported package: a stale PYTHONPATH would
    # otherwise make this assert about a different checkout.
    library = Path(__file__).parents[2] / "src" / "ndr_lakehouse" / "asset_property"
    assert library.is_dir()
    assert list(library.glob("*.yaml")) == []


def test_use_case_package_is_not_inside_the_library():
    import use_case

    assert "ndr_lakehouse" not in Path(use_case.__file__).parts
