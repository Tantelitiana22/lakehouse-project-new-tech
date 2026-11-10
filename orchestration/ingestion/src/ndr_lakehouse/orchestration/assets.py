"""Dynamic Dagster assets built from asset_property YAML.

Each source produces exactly two linked assets:

* ``AssetKey(["landing", <name>])``             -> native RustFS landing zone
* ``AssetKey([catalog, schema, table])``        -> Unity Catalog Iceberg table
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

from dagster import AssetIn, AssetKey, AssetsDefinition, MetadataValue, asset

from ndr_lakehouse.asset_property.schema_validator import (
    AssetConfig,
    discover_asset_configs,
)
from ndr_lakehouse.domain.ports import HandlerContext, UseCaseHandler
from ndr_lakehouse.iceberg.resource import SparkIcebergResource
from ndr_lakehouse.landing.resource import NativeIngestorResource
from ndr_lakehouse.orchestration.handlers import NullHandler, resolve_handler
from ndr_lakehouse.provisioning.resource import UnityCatalogProvisionerResource

logger = logging.getLogger(__name__)


class _LandingAdapter:
    """Expose a Dagster resource as the ``LandingPort`` the handler expects."""

    def __init__(self, resource: NativeIngestorResource) -> None:
        self._resource = resource

    def ingest(self, config: AssetConfig) -> str:
        """Hide ``NativeIngestorResource.ingest``'s extra optional argument."""

        return self._resource.ingest(config)


class _IcebergAdapter:
    """Expose a Dagster resource as the ``IcebergPort`` a handler expects."""

    def __init__(self, resource: SparkIcebergResource) -> None:
        self._resource = resource

    def load(self, config: AssetConfig, landing_uri: str) -> dict[str, Any]:
        return self._resource.load(config, landing_uri)


class _UnboundPort:
    """Placeholder for a port the platform path never calls.

    An asset without a ``handler:`` keeps exactly the Dagster resource
    requirements it had before handlers existed, so the resource backing a
    port it cannot use is not bound. Reaching that port is a wiring bug, not a
    supported path, hence the hard failure.
    """

    def __init__(self, name: str) -> None:
        self._name = name

    def _fail(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(
            f"the {self._name} port is unbound on an asset without a handler; " "set 'handler:' if the asset needs it"
        )

    ingest = _fail
    load = _fail


def _resolve_asset_handler(config: AssetConfig) -> UseCaseHandler:
    """Resolve the configured handler once, at asset-build time."""

    if not config.handler:
        return NullHandler()
    return resolve_handler(config.handler, asset_name=config.name)


def _handler_settings(config: AssetConfig) -> Mapping[str, str]:
    """Read-only platform settings a handler may need; nothing else is exposed."""

    return MappingProxyType(
        {
            "full_table_name": config.catalog_properties.full_table_name,
            "asset_name": config.name,
        }
    )


def discover_ingestion_configs(
    asset_dir: str | Path,
    *,
    recursive: bool = False,
) -> list[AssetConfig]:
    """Discover and validate every non-platform YAML source definition."""

    return discover_asset_configs(asset_dir, recursive=recursive)


def _land(
    context: Any,
    *,
    config: AssetConfig,
    landing_key: AssetKey,
    handler: UseCaseHandler,
    settings: Mapping[str, str],
    uc_provisioner: UnityCatalogProvisionerResource,
    native_ingestor: NativeIngestorResource,
    iceberg_port: Any,
) -> str:
    """Run one landing materialisation for any handler variant."""

    catalog = config.catalog_properties
    source = config.source_properties

    # Provisioning is platform infrastructure: it always runs, before the
    # handler or the generic path, and a handler can never override it.
    uc_provisioner.ensure_buckets()
    uc_provisioner.ensure_catalog_and_schema(catalog.catalog_name, catalog.schema_name)

    ctx = HandlerContext(
        landing=_LandingAdapter(native_ingestor),
        iceberg=iceberg_port,
        settings=settings,
    )
    result = handler.land(ctx, config)
    s3a_uri = result if result is not None else native_ingestor.ingest(config)
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


def _landing_asset(config: AssetConfig) -> AssetsDefinition:
    landing_key = AssetKey(config.landing_asset_key)
    handler = _resolve_asset_handler(config)
    settings = _handler_settings(config)

    common: dict[str, Any] = {
        "name": config.name,
        "key_prefix": ["landing"],
        "group_name": "landing",
        "compute_kind": "python",
        "description": config.description or f"Native landing asset for {config.name}",
        "tags": {**config.tags, "owner": config.owner, "layer": "landing"},
    }

    # Same predicate as `_resolve_asset_handler`: an empty or whitespace-only
    # `handler:` is no handler, so it must take the platform-only path.
    if not config.handler:

        @asset(**common)
        def _landing_default(
            context,
            uc_provisioner: UnityCatalogProvisionerResource,
            native_ingestor: NativeIngestorResource,
        ) -> str:
            return _land(
                context,
                config=config,
                landing_key=landing_key,
                handler=handler,
                settings=settings,
                uc_provisioner=uc_provisioner,
                native_ingestor=native_ingestor,
                iceberg_port=_UnboundPort("iceberg"),
            )

        return _landing_default

    @asset(**common)
    def _landing(
        context,
        uc_provisioner: UnityCatalogProvisionerResource,
        native_ingestor: NativeIngestorResource,
        spark_iceberg: SparkIcebergResource,
    ) -> str:
        return _land(
            context,
            config=config,
            landing_key=landing_key,
            handler=handler,
            settings=settings,
            uc_provisioner=uc_provisioner,
            native_ingestor=native_ingestor,
            iceberg_port=_IcebergAdapter(spark_iceberg),
        )

    return _landing


def _load(
    context: Any,
    *,
    config: AssetConfig,
    landing_uri: str,
    handler: UseCaseHandler,
    settings: Mapping[str, str],
    native_ingestor: NativeIngestorResource | None,
    spark_iceberg: SparkIcebergResource,
) -> dict[str, Any]:
    """Run one Iceberg materialisation for any handler variant."""

    ctx = HandlerContext(
        landing=(_LandingAdapter(native_ingestor) if native_ingestor is not None else _UnboundPort("landing")),
        iceberg=_IcebergAdapter(spark_iceberg),
        settings=settings,
    )
    override = handler.load(ctx, config, landing_uri)
    result = spark_iceberg.load(config, landing_uri)
    if override:
        result = {**result, **override}
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


def _iceberg_asset(config: AssetConfig, landing_key: AssetKey) -> AssetsDefinition:
    catalog = config.catalog_properties
    handler = _resolve_asset_handler(config)
    settings = _handler_settings(config)

    common: dict[str, Any] = {
        "name": catalog.table_name,
        "key_prefix": [catalog.catalog_name, catalog.schema_name],
        "ins": {"landing_uri": AssetIn(key=landing_key)},
        "group_name": config.group_name,
        "compute_kind": "spark",
        "description": config.description or f"Iceberg Bronze table {catalog.full_table_name}",
        "tags": {
            **config.tags,
            "owner": config.owner,
            "layer": config.group_name,
            "write_mode": catalog.write_mode,
        },
    }

    # Same predicate as `_resolve_asset_handler`; see `_landing_asset`.
    if not config.handler:

        @asset(**common)
        def _iceberg_default(
            context,
            landing_uri: str,
            spark_iceberg: SparkIcebergResource,
        ) -> dict[str, Any]:
            return _load(
                context,
                config=config,
                landing_uri=landing_uri,
                handler=handler,
                settings=settings,
                native_ingestor=None,
                spark_iceberg=spark_iceberg,
            )

        return _iceberg_default

    @asset(**common)
    def _iceberg(
        context,
        landing_uri: str,
        native_ingestor: NativeIngestorResource,
        spark_iceberg: SparkIcebergResource,
    ) -> dict[str, Any]:
        return _load(
            context,
            config=config,
            landing_uri=landing_uri,
            handler=handler,
            settings=settings,
            native_ingestor=native_ingestor,
            spark_iceberg=spark_iceberg,
        )

    return _iceberg


def build_ingestion_assets_from_yaml(
    asset_dir: str | Path | None = None,
    configs: Iterable[AssetConfig] | None = None,
    *,
    recursive: bool = False,
) -> list[AssetsDefinition]:
    """Build one landing + one Iceberg asset per configured source."""

    if configs is not None:
        resolved = list(configs)
    elif asset_dir is not None:
        resolved = discover_ingestion_configs(asset_dir, recursive=recursive)
    else:
        raise ValueError("build_ingestion_assets_from_yaml needs asset_dir or configs")

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
