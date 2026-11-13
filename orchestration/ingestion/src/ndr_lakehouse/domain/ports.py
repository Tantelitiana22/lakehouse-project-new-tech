"""Dependency-inversion ports shared by all layers.

High-level services depend on these protocols, never on boto3/httpx/Spark
directly. Concrete adapters live in ``ndr_lakehouse.infrastructure`` and
``ndr_lakehouse.iceberg``.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ndr_lakehouse.asset_property.schema_validator import AssetConfig


class ObjectStore(Protocol):
    """Port for S3-compatible object storage (RustFS)."""

    def ensure_bucket(self, bucket: str) -> bool: ...

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str,
    ) -> None: ...

    def copy_object(
        self,
        target_bucket: str,
        target_key: str,
        source_bucket: str,
        source_key: str,
    ) -> None: ...


class HttpResponse(Protocol):
    """Minimal HTTP response surface required by the landing layer."""

    status_code: int
    headers: Mapping[str, str]
    content: bytes

    def json(self) -> Any: ...

    def raise_for_status(self) -> None: ...

    def read(self) -> bytes: ...

    def iter_bytes(self, chunk_size: int = ...) -> Iterator[bytes]: ...


class HttpTransport(Protocol):
    """Port for an HTTP client with normal and streaming requests."""

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> HttpResponse: ...

    def stream(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> AbstractContextManager[HttpResponse]: ...

    def close(self) -> None: ...


class CatalogGateway(Protocol):
    """Idempotency-free Unity Catalog REST operations."""

    def catalog_exists(self, catalog_name: str) -> bool: ...

    def create_catalog(
        self,
        catalog_name: str,
        storage_root: str,
        comment: str,
    ) -> bool: ...

    def schema_exists(self, catalog_name: str, schema_name: str) -> bool: ...

    def create_schema(
        self,
        catalog_name: str,
        schema_name: str,
        comment: str,
    ) -> bool: ...

    def table_exists(self, catalog_name: str, schema_name: str, table_name: str) -> bool: ...

    def delete_table(self, catalog_name: str, schema_name: str, table_name: str) -> bool: ...

    def create_table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        storage_location: str,
        columns: Sequence[Mapping[str, Any]],
        comment: str = "",
        properties: Mapping[str, str] | None = None,
    ) -> bool: ...


class SparkSessionProvider(Protocol):
    """Port for creating a SparkSession configured for Unity Catalog."""

    def create(self, catalog_name: str | None = None) -> Any: ...


class LandingPort(Protocol):
    """Port for landing raw records and returning the landing URI."""

    def ingest(self, config: AssetConfig) -> str: ...


class IcebergPort(Protocol):
    """Port for loading a landed dataset into Iceberg and reporting a summary."""

    def load(self, config: AssetConfig, landing_uri: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class HandlerContext:
    """Collaborators and settings handed to a use-case handler."""

    landing: LandingPort
    iceberg: IcebergPort
    settings: Mapping[str, str]


class UseCaseHandler(Protocol):
    """Escape hatch letting a use case override landing and/or loading per asset."""

    def land(self, context: HandlerContext, config: AssetConfig) -> str | None: ...

    def load(self, context: HandlerContext, config: AssetConfig, landing_uri: str) -> dict[str, Any] | None: ...
