"""Declarative asset property contracts and YAML discovery."""

from ndr_lakehouse.asset_property.schema_validator import (
    AssetConfig,
    CatalogProperties,
    PaginationConfig,
    PlatformConfig,
    SourceProperties,
    discover_asset_configs,
    load_asset_config_file,
    load_asset_configs,
    load_platform_config,
    parse_asset_config,
    parse_platform_config,
)

__all__ = [
    "AssetConfig",
    "CatalogProperties",
    "PaginationConfig",
    "PlatformConfig",
    "SourceProperties",
    "discover_asset_configs",
    "load_asset_config_file",
    "load_asset_configs",
    "load_platform_config",
    "parse_asset_config",
    "parse_platform_config",
]
