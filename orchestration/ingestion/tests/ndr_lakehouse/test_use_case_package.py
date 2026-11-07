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


def test_no_asset_yaml_remains_in_the_library():
    import ndr_lakehouse

    asset_property = Path(ndr_lakehouse.__file__).parent / "asset_property"
    assert list(asset_property.glob("*.yaml")) == []


def test_use_case_package_is_not_inside_the_library():
    import use_case

    assert "ndr_lakehouse" not in Path(use_case.__file__).parts
