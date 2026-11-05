"""Strict Pydantic v2 contracts for the declarative ``asset_property`` layer.

Every ingestion behaviour in the platform is expressed by these models. No
business, source, table or storage detail is hard-coded in Python; operators
add a YAML file and the Dagster factory, Spark loader and dbt source sync pick
it up automatically.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ndr_lakehouse.domain.errors import DuplicateAssetNameError

Identifier = str

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_S3_PREFIX_RE = re.compile(r"^[A-Za-z0-9._\-/]+$")
_PLATFORM_FILENAME = "platform.yaml"

PaginationStrategy = Literal[
    "none",
    "page_number",
    "offset",
    "cursor",
    "next_url",
    "link_header",
]
WriteMode = Literal["overwrite", "append", "merge"]
NativeFormat = Literal["json", "csv", "csv.gz", "parquet"]
SourceType = Literal["http", "s3"]


class _StrictModel(BaseModel):
    """Base model that rejects unknown keys, catching typo-driven misconfig."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class StorageConfig(_StrictModel):
    """RustFS / S3-compatible object storage connection settings."""

    endpoint_url: str = "http://rustfs:9000"
    access_key_id: str = "rustfsadmin"
    secret_access_key: str = "rustfssecret"
    region: str = "us-east-1"
    landing_bucket: str = "landing-zone"
    lakehouse_bucket: str = "lakehouse"
    use_path_style: bool = True

    @field_validator("endpoint_url")
    @classmethod
    def _validate_endpoint(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("endpoint_url must start with http:// or https://")
        return value.rstrip("/")

    @field_validator("landing_bucket", "lakehouse_bucket")
    @classmethod
    def _validate_bucket(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", value):
            raise ValueError(f"invalid S3 bucket name: {value!r}")
        return value


class UnityCatalogConfig(_StrictModel):
    """Unity Catalog OSS REST connection settings."""

    base_url: str = "http://unity-catalog:8080"
    api_version: str = "2.1"
    catalog_name: str = "lakehouse"
    schema_name: str = "bronze"
    storage_root: str = "s3://lakehouse/warehouse"

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return value.rstrip("/")

    @field_validator("storage_root")
    @classmethod
    def _validate_storage_root(cls, value: str) -> str:
        if not value.startswith("s3://"):
            raise ValueError("storage_root must be an s3:// URI")
        return value

    @property
    def iceberg_rest_uri(self) -> str:
        return f"{self.base_url}/api/{self.api_version}/unity-catalog/iceberg"


class SparkConfig(_StrictModel):
    """Spark runtime settings used by the generic Iceberg loader."""

    master: str = "local[*]"
    app_name: str = "lakehouse-ingestion"
    warehouse: str = "s3://lakehouse/warehouse"


class PlatformConfig(_StrictModel):
    """Global platform endpoints, credentials and runtime defaults."""

    storage: StorageConfig = Field(default_factory=StorageConfig)
    unity_catalog: UnityCatalogConfig = Field(default_factory=UnityCatalogConfig)
    spark: SparkConfig = Field(default_factory=SparkConfig)

    def with_env_overrides(self) -> PlatformConfig:
        """Return a copy where documented environment variables win over YAML.

        This keeps credentials out of version control while leaving every
        non-secret endpoint in the declarative config.
        """

        storage = self.storage.model_copy(
            update={
                key: os.environ[env]
                for key, env in (
                    ("endpoint_url", "S3_ENDPOINT_URL"),
                    ("access_key_id", "S3_ACCESS_KEY_ID"),
                    ("secret_access_key", "S3_SECRET_ACCESS_KEY"),
                    ("region", "S3_REGION"),
                    ("landing_bucket", "LANDING_BUCKET"),
                    ("lakehouse_bucket", "LAKEHOUSE_BUCKET"),
                )
                if env in os.environ
            }
        )
        unity = self.unity_catalog.model_copy(
            update={
                key: os.environ[env]
                for key, env in (
                    ("base_url", "UC_BASE_URL"),
                    ("catalog_name", "UC_CATALOG"),
                    ("schema_name", "UC_SCHEMA"),
                    ("storage_root", "UC_STORAGE_ROOT"),
                )
                if env in os.environ
            }
        )
        spark = self.spark.model_copy(
            update={
                key: os.environ[env]
                for key, env in (
                    ("master", "SPARK_MASTER"),
                    ("warehouse", "SPARK_WAREHOUSE"),
                    ("app_name", "SPARK_APP_NAME"),
                )
                if env in os.environ
            }
        )
        return PlatformConfig(storage=storage, unity_catalog=unity, spark=spark)


class PaginationConfig(_StrictModel):
    """Strategy-specific pagination contract.

    Validation is intentionally strict: an ``offset`` strategy without an
    offset parameter fails at load time rather than silently downloading only
    the first page in production.
    """

    strategy: PaginationStrategy = "none"
    limit_param: str | None = None
    limit_value: int | None = None
    page_param: str | None = None
    page_start: int = 1
    offset_param: str | None = None
    offset_start: int = 0
    cursor_param: str | None = None
    initial_cursor: str | None = None
    next_cursor_json_path: str | None = None
    next_url_json_path: str | None = None
    records_json_path: str | None = None
    max_pages: int = Field(default=100, ge=1, le=100_000)
    rate_limit_sleep_sec: float = Field(default=0.0, ge=0.0, le=3600.0)

    @field_validator("limit_value")
    @classmethod
    def _validate_limit(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("limit_value must be greater than zero")
        return value

    @model_validator(mode="after")
    def _validate_strategy_fields(self) -> PaginationConfig:
        strategy = self.strategy
        if strategy == "page_number":
            missing = [
                name
                for name, value in (
                    ("page_param", self.page_param),
                    ("limit_param", self.limit_param),
                    ("limit_value", self.limit_value),
                )
                if value is None
            ]
        elif strategy == "offset":
            missing = [
                name
                for name, value in (
                    ("offset_param", self.offset_param),
                    ("limit_param", self.limit_param),
                    ("limit_value", self.limit_value),
                )
                if value is None
            ]
        elif strategy == "cursor":
            missing = [
                name
                for name, value in (
                    ("cursor_param", self.cursor_param),
                    ("next_cursor_json_path", self.next_cursor_json_path),
                )
                if value is None
            ]
        elif strategy == "next_url":
            missing = ["next_url_json_path"] if self.next_url_json_path is None else []
        else:
            missing = []

        if missing:
            raise ValueError(f"pagination strategy {strategy!r} requires: {', '.join(missing)}")
        return self


class SourceProperties(_StrictModel):
    """Extraction contract for the native landing-zone ingest."""

    type: SourceType = "http"
    url: str = Field(min_length=1)
    method: Literal["GET", "POST"] = "GET"
    params: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    json_body: dict[str, Any] | None = None
    native_format: NativeFormat = "json"
    landing_bucket: str = "landing-zone"
    s3_prefix: str = Field(min_length=1)
    records_json_path: str | None = None
    pagination: PaginationConfig = Field(default_factory=PaginationConfig)

    @field_validator("s3_prefix")
    @classmethod
    def _validate_prefix(cls, value: str) -> str:
        cleaned = value.strip("/")
        if not cleaned or not _S3_PREFIX_RE.fullmatch(cleaned):
            raise ValueError("s3_prefix must contain only letters, digits, '.', '_', '-' and '/'")
        if ".." in cleaned.split("/"):
            raise ValueError("s3_prefix must not contain '..' segments")
        return cleaned

    @model_validator(mode="after")
    def _normalise_records_path(self) -> SourceProperties:
        if self.type == "s3" and not self.url.startswith("s3://"):
            raise ValueError("source type 's3' requires an s3:// url")
        if self.type == "http" and not self.url.startswith(("http://", "https://")):
            raise ValueError("source type 'http' requires an http(s):// url")
        if self.records_json_path is None:
            self.records_json_path = self.pagination.records_json_path
        if self.records_json_path is not None and self.pagination.records_json_path is None:
            self.pagination.records_json_path = self.records_json_path
        return self


class CatalogProperties(_StrictModel):
    """Iceberg + Unity Catalog integration contract."""

    catalog_name: Identifier = "lakehouse"
    schema_name: Identifier = "bronze"
    table_name: Identifier
    write_mode: WriteMode = "overwrite"
    primary_keys: list[str] = Field(default_factory=list)
    dedup_order_by: str = "_ingested_at"
    auto_evolve_schema: bool = True
    partition_by: list[str] = Field(default_factory=list)
    read_options: dict[str, str] = Field(default_factory=dict)
    iceberg_tblproperties: dict[str, str] = Field(default_factory=dict)
    merge_delete_condition: str | None = None
    merge_matched_update_condition: str | None = None
    merge_not_matched_insert_condition: str | None = None
    merge_not_matched_by_source_delete_condition: str | None = None

    @field_validator("catalog_name", "schema_name", "table_name")
    @classmethod
    def _validate_identifier(cls, value: str) -> str:
        if not _IDENTIFIER_RE.fullmatch(value):
            raise ValueError(
                f"invalid Unity Catalog identifier {value!r}; expected [A-Za-z_][A-Za-z0-9_]*"
            )
        return value

    @field_validator("primary_keys", "partition_by")
    @classmethod
    def _validate_unique_columns(cls, value: list[str]) -> list[str]:
        cleaned = [column.strip() for column in value if column.strip()]
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("column lists must not contain duplicates")
        return cleaned

    @model_validator(mode="after")
    def _validate_write_contract(self) -> CatalogProperties:
        if self.write_mode == "merge" and not self.primary_keys:
            raise ValueError("write_mode='merge' requires a non-empty primary_keys list")
        if not self.dedup_order_by:
            raise ValueError("dedup_order_by must not be empty")
        if self.write_mode != "merge" and (
            self.merge_delete_condition
            or self.merge_matched_update_condition
            or self.merge_not_matched_insert_condition
            or self.merge_not_matched_by_source_delete_condition
        ):
            raise ValueError("merge conditions require write_mode='merge'")
        defaults = {
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
            "write.merge.mode": "merge-on-read",
        }
        for key, value in defaults.items():
            self.iceberg_tblproperties.setdefault(key, value)
        return self

    @property
    def full_table_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"


class AssetConfig(_StrictModel):
    """One declarative source -> landing -> Iceberg asset definition."""

    name: str
    description: str = ""
    group_name: str = "bronze"
    owner: str = "data-platform"
    tags: dict[str, str] = Field(default_factory=dict)
    source_properties: SourceProperties
    catalog_properties: CatalogProperties
    handler: str | None = None

    @field_validator("name", "group_name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        if not _IDENTIFIER_RE.fullmatch(value):
            raise ValueError(f"invalid asset name {value!r}; expected [A-Za-z_][A-Za-z0-9_]*")
        return value

    @property
    def landing_asset_key(self) -> list[str]:
        return ["landing", self.name]

    @property
    def iceberg_asset_key(self) -> list[str]:
        catalog = self.catalog_properties
        return [catalog.catalog_name, catalog.schema_name, catalog.table_name]


class AssetFile(_StrictModel):
    """Container for a YAML file declaring one or more assets."""

    assets: list[AssetConfig] = Field(default_factory=list)


def _read_yaml(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"asset_property file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if payload is None:
        raise ValueError(f"asset_property file is empty: {path}")
    return payload


def parse_asset_config(payload: dict[str, Any] | list[dict[str, Any]]) -> list[AssetConfig]:
    """Parse an in-memory YAML payload into validated asset configs."""

    if isinstance(payload, list):
        return [AssetConfig.model_validate(item) for item in payload]
    if not isinstance(payload, dict):
        raise ValueError("asset config payload must be a mapping or a list")

    if "assets" in payload:
        raw_assets = payload["assets"]
        if not isinstance(raw_assets, list) or not raw_assets:
            raise ValueError("'assets' must be a non-empty list")
        return [AssetConfig.model_validate(item) for item in raw_assets]
    if "asset" in payload:
        return [AssetConfig.model_validate(payload["asset"])]
    return [AssetConfig.model_validate(payload)]


def load_asset_config_file(path: str | Path) -> list[AssetConfig]:
    """Load and validate one YAML file (which may declare several assets)."""

    return parse_asset_config(_read_yaml(Path(path)))


def load_asset_configs(path: str | Path) -> list[AssetConfig]:
    """Alias kept for callers that treat a file as a config collection."""

    return load_asset_config_file(path)


def discover_asset_configs(asset_dir: str | Path, *, recursive: bool = False) -> list[AssetConfig]:
    """Scan a directory for asset declarations, excluding ``platform.yaml``.

    Only the top-level ``*.yaml`` files are read by default. Pass
    ``recursive=True`` to walk sub-directories with ``**/*.yaml``, which is how
    a use-case ``assets/`` package is discovered. Both modes return configs in
    sorted path order and skip ``platform.yaml`` at any depth.
    """

    directory = Path(asset_dir)
    if not directory.exists():
        raise FileNotFoundError(f"asset_property directory not found: {directory}")

    pattern = "**/*.yaml" if recursive else "*.yaml"
    configs: list[AssetConfig] = []
    seen: dict[str, Path] = {}
    for path in sorted(directory.glob(pattern)):
        if path.name == _PLATFORM_FILENAME:
            continue
        for config in load_asset_config_file(path):
            if config.name in seen:
                raise DuplicateAssetNameError(
                    f"duplicate asset name {config.name!r} in {seen[config.name]} and {path}"
                )
            seen[config.name] = path
            configs.append(config)
    return configs


def parse_platform_config(payload: dict[str, Any]) -> PlatformConfig:
    """Parse the platform block, accepting both nested and flat documents."""

    if not isinstance(payload, dict):
        raise ValueError("platform config payload must be a mapping")
    platform_payload = payload.get("platform", payload)
    return PlatformConfig.model_validate(platform_payload)


def load_platform_config(path: str | Path) -> PlatformConfig:
    """Load platform.yaml and apply environment-variable overrides."""

    return parse_platform_config(_read_yaml(Path(path))).with_env_overrides()
