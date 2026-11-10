"""Tests for the strict Pydantic asset_property contracts."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from ndr_lakehouse.asset_property.schema_validator import (
    discover_asset_configs,
    load_asset_config_file,
    parse_asset_config,
    parse_platform_config,
)


@pytest.mark.parametrize(
    ("pagination", "expected"),
    [
        ({"strategy": "none"}, "none"),
        (
            {
                "strategy": "offset",
                "offset_param": "offset",
                "limit_param": "limit",
                "limit_value": 100,
                "records_json_path": "data.items",
            },
            "offset",
        ),
        (
            {
                "strategy": "cursor",
                "cursor_param": "cursor",
                "next_cursor_json_path": "meta.next_cursor",
            },
            "cursor",
        ),
        (
            {
                "strategy": "link_header",
                "records_json_path": "records",
            },
            "link_header",
        ),
        (
            {
                "strategy": "page_number",
                "page_param": "page",
                "limit_param": "page_size",
                "limit_value": 50,
            },
            "page_number",
        ),
        (
            {
                "strategy": "next_url",
                "next_url_json_path": "links.next",
            },
            "next_url",
        ),
    ],
)
def test_pagination_strategies_parse(asset_payload_factory, pagination, expected):
    payload = asset_payload_factory(pagination=pagination)
    config = parse_asset_config(payload)[0]
    assert config.source_properties.pagination.strategy == expected


def test_records_json_path_falls_back_to_pagination(asset_payload_factory):
    payload = asset_payload_factory(
        pagination={
            "strategy": "offset",
            "offset_param": "offset",
            "limit_param": "limit",
            "limit_value": 10,
            "records_json_path": "payload.records",
        }
    )
    source = parse_asset_config(payload)[0].source_properties
    assert source.records_json_path == "payload.records"


def test_invalid_pagination_contract_is_rejected(asset_payload_factory):
    payload = asset_payload_factory(pagination={"strategy": "offset"})
    with pytest.raises(ValidationError, match="requires"):
        parse_asset_config(payload)


def test_merge_requires_primary_keys(asset_payload_factory):
    payload = asset_payload_factory(catalog_overrides={"write_mode": "merge", "primary_keys": []})
    with pytest.raises(ValidationError, match="write_mode='merge'"):
        parse_asset_config(payload)


def test_merge_accepts_composite_primary_keys(asset_payload_factory):
    payload = asset_payload_factory(
        catalog_overrides={
            "write_mode": "merge",
            "primary_keys": ["id_mutation", "id_parcelle"],
        }
    )
    catalog = parse_asset_config(payload)[0].catalog_properties
    assert catalog.primary_keys == ["id_mutation", "id_parcelle"]
    assert catalog.full_table_name == "lakehouse.bronze.demo_table"


def test_default_iceberg_tblproperties_are_injected(asset_payload_factory):
    payload = asset_payload_factory(
        catalog_overrides={"iceberg_tblproperties": {}},
    )
    properties = parse_asset_config(payload)[0].catalog_properties.iceberg_tblproperties
    assert properties["format-version"] == "2"
    assert properties["write.parquet.compression-codec"] == "zstd"
    assert properties["write.merge.mode"] == "merge-on-read"


def test_unknown_keys_are_forbidden(asset_payload_factory):
    payload = asset_payload_factory(source_overrides={"unexpected": "value"})
    with pytest.raises(ValidationError):
        parse_asset_config(payload)


def test_duplicate_asset_names_are_rejected_in_discovery(tmp_path: Path, asset_payload):
    (tmp_path / "platform.yaml").write_text("platform: {}\n", encoding="utf-8")
    (tmp_path / "a.yaml").write_text(__import__("yaml").safe_dump(asset_payload), encoding="utf-8")
    (tmp_path / "b.yaml").write_text(__import__("yaml").safe_dump(asset_payload), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate asset name"):
        discover_asset_configs(tmp_path)


def test_duplicate_asset_name_error_is_a_value_error():
    from ndr_lakehouse.domain.errors import DuplicateAssetNameError, LakehouseError

    assert issubclass(DuplicateAssetNameError, ValueError)
    assert issubclass(DuplicateAssetNameError, LakehouseError)


def test_recursive_discovery_finds_nested_assets(tmp_path: Path, asset_payload):
    import yaml

    nested = tmp_path / "sub"
    nested.mkdir()
    (nested / "a.yaml").write_text(yaml.safe_dump(asset_payload), encoding="utf-8")
    assert [c.name for c in discover_asset_configs(tmp_path, recursive=True)] == ["demo_source"]
    assert discover_asset_configs(tmp_path, recursive=False) == []


def test_load_asset_file_roundtrip(tmp_path: Path, asset_payload):
    import yaml

    path = tmp_path / "source.yaml"
    path.write_text(yaml.safe_dump(asset_payload), encoding="utf-8")
    configs = load_asset_config_file(path)
    assert [config.name for config in configs] == ["demo_source"]


def test_platform_config_parses_and_builds_iceberg_uri():
    platform = parse_platform_config(
        {
            "platform": {
                "storage": {
                    "endpoint_url": "http://rustfs:9000",
                    "landing_bucket": "landing-zone",
                    "lakehouse_bucket": "lakehouse",
                },
                "unity_catalog": {
                    "base_url": "http://unity-catalog:8080/",
                    "catalog_name": "lakehouse",
                    "schema_name": "bronze",
                },
                "spark": {"warehouse": "s3://lakehouse/warehouse"},
            }
        }
    )
    assert platform.unity_catalog.base_url == "http://unity-catalog:8080"
    assert platform.unity_catalog.iceberg_rest_uri == "http://unity-catalog:8080/api/2.1/unity-catalog/iceberg"
