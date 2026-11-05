"""Tests for the dynamic Dagster factory, lineage and dbt source sync."""

from __future__ import annotations

from pathlib import Path

import yaml
from dagster import AssetKey, materialize
from ndr_lakehouse.iceberg.resource import SparkIcebergResource
from ndr_lakehouse.landing.resource import NativeIngestorResource
from ndr_lakehouse.orchestration.assets import (
    build_ingestion_assets_from_yaml,
)
from ndr_lakehouse.orchestration.dbt_source_sync import (
    build_sources_document,
    generate_sources_yaml,
)
from ndr_lakehouse.provisioning.resource import (
    UnityCatalogProvisionerResource,
)


def test_factory_creates_landing_and_iceberg_pairs(sample_asset_config):
    assets = build_ingestion_assets_from_yaml(configs=[sample_asset_config])

    keys = set()
    for asset in assets:
        keys.update(asset.keys)

    assert len(assets) == 2
    assert AssetKey(["landing", "demo_source"]) in keys
    assert AssetKey(["lakehouse", "bronze", "demo_table"]) in keys


def test_dbt_sources_document_injects_dagster_asset_key(sample_asset_config):
    document = build_sources_document([sample_asset_config])

    assert document["version"] == 2
    source = document["sources"][0]
    assert source["name"] == "bronze"
    assert "database" not in source
    assert source["schema"] == "bronze"
    table = source["tables"][0]
    assert table["name"] == "demo_table"
    assert table["meta"]["dagster"]["asset_key"] == [
        "lakehouse",
        "bronze",
        "demo_table",
    ]


def test_generate_sources_yaml_writes_valid_file(tmp_path: Path, sample_asset_config):
    output = generate_sources_yaml([sample_asset_config], tmp_path / "_sources.yml")

    payload = yaml.safe_load(output.read_text(encoding="utf-8"))
    assert payload["sources"][0]["tables"][0]["name"] == "demo_table"


def test_materialize_landing_and_iceberg_assets_in_memory(mocker, sample_asset_config):
    provisioner = UnityCatalogProvisionerResource()
    ingestor = NativeIngestorResource()
    spark = SparkIcebergResource()

    mocker.patch.object(UnityCatalogProvisionerResource, "ensure_buckets", return_value={})
    mocker.patch.object(
        UnityCatalogProvisionerResource,
        "ensure_catalog_and_schema",
        return_value={},
    )
    mocker.patch.object(
        NativeIngestorResource,
        "ingest",
        return_value="s3a://landing-zone/raw/demo/ingest_date=2026-01-01",
    )
    mocker.patch.object(
        SparkIcebergResource,
        "load",
        return_value={
            "table": "lakehouse.bronze.demo_table",
            "row_count": 42,
            "write_mode": "overwrite",
            "table_existed": False,
            "table_properties": {"format-version": "2"},
            "snapshot": {"snapshot_id": "123"},
            "source_name": "demo_source",
            "landing_uri": "s3a://landing-zone/raw/demo",
        },
    )

    assets = build_ingestion_assets_from_yaml(configs=[sample_asset_config])
    result = materialize(
        assets,
        resources={
            "uc_provisioner": provisioner,
            "native_ingestor": ingestor,
            "spark_iceberg": spark,
        },
    )

    assert result.success
    NativeIngestorResource.ingest.assert_called_once_with(sample_asset_config)
    SparkIcebergResource.load.assert_called_once_with(
        sample_asset_config,
        "s3a://landing-zone/raw/demo/ingest_date=2026-01-01",
    )
    materializations = result.asset_materializations_for_node("lakehouse__bronze__demo_table")
    assert len(materializations) == 1
    assert "row_count" in materializations[0].metadata
    assert "snapshot" in materializations[0].metadata
