"""The repo-local use_case package is the Dagster entrypoint."""

from __future__ import annotations

from pathlib import Path

from dagster import AssetKey


def test_packaged_use_case_builds_the_full_graph():
    # Given
    import use_case

    expected_ingestion_keys = {
        AssetKey(["real_estate_77", "landing_zone", "communes_geo_77"]),
        AssetKey(["real_estate_77", "deduplication_zone", "communes_geo_77"]),
        AssetKey(["real_estate_77", "landing_zone", "dvf_mutations_77"]),
        AssetKey(["real_estate_77", "deduplication_zone", "dvf_mutations_77"]),
        AssetKey(["real_estate_77", "landing_zone", "dvf_lots_batis_77"]),
        AssetKey(["real_estate_77", "deduplication_zone", "dvf_lots_batis_77"]),
    }

    # When
    keys = set(use_case.defs.resolve_all_asset_keys())
    dbt_keys = (
        keys
        - expected_ingestion_keys
        - {
            AssetKey("metricflow_semantic_validation"),
            AssetKey(["real_estate_77", "gold", "metricflow_semantic_validation"]),
        }
    )

    # Then
    assert expected_ingestion_keys <= keys
    assert ("dbt" in use_case.defs.resources) is bool(dbt_keys)
    # Whatever path was taken, the generic library must not hardcode use-case
    # asset names, and the retired dagster-dbt asset must never come back.
    assert AssetKey("lakehouse_dbt_assets") not in keys
    assert {"native_ingestor", "spark_iceberg", "uc_provisioner"} <= use_case.defs.resources.keys()


def test_no_asset_yaml_remains_in_the_library():
    # Given
    # Anchor on this file, not on an imported package: a stale PYTHONPATH would
    # otherwise make this assert about a different checkout.
    library = Path(__file__).parents[2] / "src" / "ndr_lakehouse" / "asset_property"

    # When
    yaml_files = list(library.glob("*.yaml"))

    # Then
    assert library.is_dir()
    assert not yaml_files


def test_use_case_package_is_not_inside_the_library():
    # Given
    import use_case

    # When
    parts = Path(use_case.__file__).parts

    # Then
    assert "ndr_lakehouse" not in parts
