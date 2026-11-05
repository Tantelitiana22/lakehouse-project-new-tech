"""Shared fixtures: declarative payloads, mocked S3 and mocked Spark APIs."""

from __future__ import annotations

import copy
from datetime import date
from typing import Any

import pytest
from ndr_lakehouse.asset_property.schema_validator import (
    AssetConfig,
    parse_asset_config,
)


@pytest.fixture
def asset_payload() -> dict[str, Any]:
    """A complete, valid asset_property document for a generic source."""

    return {
        "assets": [
            {
                "name": "demo_source",
                "description": "Generic demo source used by the unit suite",
                "group_name": "bronze",
                "owner": "data-platform",
                "tags": {"domain": "test", "layer": "bronze"},
                "source_properties": {
                    "type": "http",
                    "url": "https://api.example.test/records",
                    "method": "GET",
                    "params": {"limit": "2"},
                    "headers": {"Accept": "application/json"},
                    "native_format": "json",
                    "landing_bucket": "landing-zone",
                    "s3_prefix": "raw/demo",
                    "pagination": {
                        "strategy": "none",
                    },
                },
                "catalog_properties": {
                    "catalog_name": "lakehouse",
                    "schema_name": "bronze",
                    "table_name": "demo_table",
                    "write_mode": "overwrite",
                    "primary_keys": ["id"],
                    "dedup_order_by": "_ingested_at",
                    "auto_evolve_schema": True,
                    "partition_by": ["code_commune"],
                    "read_options": {"multiLine": "true"},
                    "iceberg_tblproperties": {
                        "format-version": "2",
                        "write.parquet.compression-codec": "zstd",
                        "write.merge.mode": "merge-on-read",
                    },
                },
            }
        ]
    }


@pytest.fixture
def asset_payload_factory(asset_payload):
    """Return a factory that deep-copies the base payload before overriding."""

    def _factory(
        *,
        source_overrides: dict[str, Any] | None = None,
        catalog_overrides: dict[str, Any] | None = None,
        pagination: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = copy.deepcopy(asset_payload)
        asset = payload["assets"][0]
        if source_overrides:
            asset["source_properties"].update(source_overrides)
        if catalog_overrides:
            asset["catalog_properties"].update(catalog_overrides)
        if pagination is not None:
            asset["source_properties"]["pagination"] = pagination
        return payload

    return _factory


@pytest.fixture
def sample_asset_config(asset_payload) -> AssetConfig:
    return parse_asset_config(asset_payload)[0]


@pytest.fixture
def ingest_date() -> date:
    return date(2026, 1, 1)


@pytest.fixture
def mock_s3(mocker):
    """Patch boto3.client globally and return the mock S3 client."""

    client = mocker.MagicMock(name="s3-client")
    mocker.patch("boto3.client", return_value=client)
    return client


@pytest.fixture
def mock_spark_session(mocker):
    """A MagicMock SparkSession with catalog/table/read/write surfaces."""

    spark = mocker.MagicMock(name="spark-session")
    spark.table.return_value.columns = []
    spark.catalog.tableExists.return_value = False
    return spark
