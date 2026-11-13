"""Idempotent Unity Catalog catalog/schema/table provisioning."""

from __future__ import annotations

import fcntl
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ndr_lakehouse.domain.ports import CatalogGateway

_DEFAULT_LOCK_PATH = Path("/tmp/ndr_lakehouse_uc.lock")


@contextmanager
def _provisioning_lock(lock_path: Path = _DEFAULT_LOCK_PATH) -> Iterator[None]:
    """Serialize UC catalog/schema/table registration across worker processes."""

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class NamespaceProvisioner:
    """Adds the check-before-create policy on top of the catalog gateway."""

    def __init__(self, gateway: CatalogGateway, storage_root: str) -> None:
        self.gateway = gateway
        self.storage_root = storage_root.rstrip("/")

    def ensure_catalog(self, catalog_name: str) -> bool:
        with _provisioning_lock():
            return self._ensure_catalog_unlocked(catalog_name)

    def _ensure_catalog_unlocked(self, catalog_name: str) -> bool:
        if self.gateway.catalog_exists(catalog_name):
            return False
        return self.gateway.create_catalog(
            catalog_name=catalog_name,
            storage_root=f"{self.storage_root}/{catalog_name}",
            comment=f"Managed by ndr_lakehouse: {catalog_name}",
        )

    def ensure_schema(self, catalog_name: str, schema_name: str) -> bool:
        with _provisioning_lock():
            return self._ensure_schema_unlocked(catalog_name, schema_name)

    def _ensure_schema_unlocked(self, catalog_name: str, schema_name: str) -> bool:
        if self.gateway.schema_exists(catalog_name, schema_name):
            return False
        return self.gateway.create_schema(
            catalog_name=catalog_name,
            schema_name=schema_name,
            comment=f"Managed by ndr_lakehouse: {catalog_name}.{schema_name}",
        )

    def ensure(self, catalog_name: str, schema_name: str) -> dict[str, bool]:
        with _provisioning_lock():
            return {
                "catalog_created": self._ensure_catalog_unlocked(catalog_name),
                "schema_created": self._ensure_schema_unlocked(catalog_name, schema_name),
            }

    def ensure_table(
        self,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        storage_location: str,
        columns: Sequence[Mapping[str, Any]],
        comment: str = "",
        properties: Mapping[str, str] | None = None,
    ) -> bool:
        """Ensure namespace exists and register/refresh the table metadata."""

        with _provisioning_lock():
            self._ensure_catalog_unlocked(catalog_name)
            self._ensure_schema_unlocked(catalog_name, schema_name)
            if self.gateway.table_exists(catalog_name, schema_name, table_name):
                self.gateway.delete_table(catalog_name, schema_name, table_name)
            return self.gateway.create_table(
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
                storage_location=storage_location,
                columns=columns,
                comment=comment,
                properties=properties,
            )
