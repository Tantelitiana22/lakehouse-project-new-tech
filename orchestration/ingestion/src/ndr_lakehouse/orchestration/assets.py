"""Dynamic Dagster assets built from asset_property YAML.

Each source produces exactly two linked assets:

* ``AssetKey(["landing", <name>])``             -> native RustFS landing zone
* ``AssetKey([catalog, schema, table])``        -> Unity Catalog Iceberg table
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from dagster import AssetIn, AssetKey, AssetsDefinition, MetadataValue, asset

from ndr_lakehouse.asset_property.schema_validator import (
    AssetConfig,
    discover_asset_configs,
)
from ndr_lakehouse.iceberg.resource import SparkIcebergResource
from ndr_lakehouse.landing.resource import NativeIngestorResource
from ndr_lakehouse.orchestration.paths import ASSET_PROPERTY_DIR
from ndr_lakehouse.provisioning.resource import UnityCatalogProvisionerResource

logger = logging.getLogger(__name__)

DEFAULT_ASSET_PROPERTY_DIR = ASSET_PROPERTY_DIR


def discover_ingestion_configs(
    asset_dir: str | Path | None = None,
) -> list[AssetConfig]:
    """Discover and validate every non-platform YAML source definition."""

    return discover_asset_configs(asset_dir or DEFAULT_ASSET_PROPERTY_DIR)


def _landing_asset(config: AssetConfig) -> AssetsDefinition:
    landing_key = AssetKey(config.landing_asset_key)
    catalog = config.catalog_properties
    source = config.source_properties

    @asset(
        name=config.name,
        key_prefix=["landing"],
        group_name="landing",
        compute_kind="python",
        description=config.description or f"Native landing asset for {config.name}",
        tags={**config.tags, "owner": config.owner, "layer": "landing"},
    )
    def _landing(
        context,
        uc_provisioner: UnityCatalogProvisionerResource,
        native_ingestor: NativeIngestorResource,
    ) -> str:
        uc_provisioner.ensure_buckets()
        uc_provisioner.ensure_catalog_and_schema(catalog.catalog_name, catalog.schema_name)
        s3a_uri = native_ingestor.ingest(config)
        context.add_output_metadata(
            {
                "s3a_uri": MetadataValue.text(s3a_uri),
                "asset_name": MetadataValue.text(config.name),
                "source_url": MetadataValue.text(source.url),
                "source_type": MetadataValue.text(source.type),
                "native_format": MetadataValue.text(source.native_format),
                "pagination_strategy": MetadataValue.text(source.pagination.strategy),
                "landing_asset_key": MetadataValue.text("/".join(landing_key.path)),
            }
        )
        return s3a_uri

    return _landing


def _iceberg_asset(config: AssetConfig, landing_key: AssetKey) -> AssetsDefinition:
    catalog = config.catalog_properties

    @asset(
        name=catalog.table_name,
        key_prefix=[catalog.catalog_name, catalog.schema_name],
        ins={"landing_uri": AssetIn(key=landing_key)},
        group_name=config.group_name,
        compute_kind="spark",
        description=config.description or f"Iceberg Bronze table {catalog.full_table_name}",
        tags={
            **config.tags,
            "owner": config.owner,
            "layer": config.group_name,
            "write_mode": catalog.write_mode,
        },
    )
    def _iceberg(
        context,
        landing_uri: str,
        spark_iceberg: SparkIcebergResource,
    ) -> dict[str, Any]:
        result = spark_iceberg.load(config, landing_uri)
        context.add_output_metadata(
            {
                "table": MetadataValue.text(str(result.get("table"))),
                "row_count": MetadataValue.int(int(result.get("row_count", 0))),
                "write_mode": MetadataValue.text(str(result.get("write_mode"))),
                "table_existed": MetadataValue.bool(bool(result.get("table_existed", False))),
                "table_properties": MetadataValue.json(result.get("table_properties", {})),
                "snapshot": MetadataValue.json(result.get("snapshot", {})),
                "landing_uri": MetadataValue.text(landing_uri),
            }
        )
        return result

    return _iceberg


def build_ingestion_assets_from_yaml(
    asset_dir: str | Path | None = None,
    configs: Iterable[AssetConfig] | None = None,
) -> list[AssetsDefinition]:
    """Build one landing + one Iceberg asset per configured source."""

    resolved = list(configs) if configs is not None else discover_ingestion_configs(asset_dir)

    assets: list[AssetsDefinition] = []
    for config in resolved:
        landing_key = AssetKey(config.landing_asset_key)
        iceberg_key = AssetKey(config.iceberg_asset_key)
        if landing_key == iceberg_key:
            raise ValueError(f"asset key collision for {config.name!r}")
        logger.debug("Building ingestion assets %s -> %s", landing_key, iceberg_key)
        assets.append(_landing_asset(config))
        assets.append(_iceberg_asset(config, landing_key))
    return assets
