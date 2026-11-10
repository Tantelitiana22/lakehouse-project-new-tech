"""The repo-local use_case package is the Dagster entrypoint."""

from __future__ import annotations

from pathlib import Path

from dagster import AssetKey


def test_packaged_use_case_builds_the_full_graph():
    import use_case

    keys = set(use_case.defs.resolve_all_asset_keys())
    assert AssetKey(["landing", "communes_geo_77"]) in keys
    assert AssetKey(["lakehouse", "bronze", "dvf_mutations_77"]) in keys
    assert AssetKey(["lakehouse", "bronze", "dvf_lots_batis_77"]) in keys

    # Pin the ingestion half of the graph, not a sample: these six keys are what
    # the asset YAML produce and they must always be present.
    ingestion_keys = {key for key in keys if key.path[0] in {"landing", "lakehouse"}}
    assert len(ingestion_keys) == 6

    # The dbt half is environment-dependent on purpose: it only appears when the
    # ``dbt`` extra is installed *and* `dbt parse` has written a manifest (both
    # are gitignored, so a clean checkout has neither). Assert the coupling
    # instead of the count, which is the property that can actually break: dbt
    # models are never registered without the resource that runs them.
    dbt_model_keys = keys - ingestion_keys - {AssetKey("metricflow_semantic_validation")}
    if dbt_model_keys:
        assert "dbt" in use_case.defs.resources
    else:
        assert "dbt" not in use_case.defs.resources

    # Whatever path was taken, the generic library must not hardcode use-case
    # asset names, and the retired dagster-dbt asset must never come back.
    assert AssetKey("lakehouse_dbt_assets") not in keys
    assert {"native_ingestor", "spark_iceberg", "uc_provisioner"} <= use_case.defs.resources.keys()


def test_no_asset_yaml_remains_in_the_library():
    # Anchor on this file, not on an imported package: a stale PYTHONPATH would
    # otherwise make this assert about a different checkout.
    library = Path(__file__).parents[2] / "src" / "ndr_lakehouse" / "asset_property"
    assert library.is_dir()
    assert list(library.glob("*.yaml")) == []


def test_use_case_package_is_not_inside_the_library():
    import use_case

    assert "ndr_lakehouse" not in Path(use_case.__file__).parts
