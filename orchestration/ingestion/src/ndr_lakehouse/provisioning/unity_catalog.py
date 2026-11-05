"""Idempotent Unity Catalog catalog/schema provisioning."""

from __future__ import annotations

from ndr_lakehouse.domain.ports import CatalogGateway


class NamespaceProvisioner:
    """Adds the check-before-create policy on top of the catalog gateway."""

    def __init__(self, gateway: CatalogGateway, storage_root: str) -> None:
        self.gateway = gateway
        self.storage_root = storage_root.rstrip("/")

    def ensure_catalog(self, catalog_name: str) -> bool:
        if self.gateway.catalog_exists(catalog_name):
            return False
        return self.gateway.create_catalog(
            catalog_name=catalog_name,
            storage_root=f"{self.storage_root}/{catalog_name}",
            comment=f"Managed by ndr_lakehouse: {catalog_name}",
        )

    def ensure_schema(self, catalog_name: str, schema_name: str) -> bool:
        if self.gateway.schema_exists(catalog_name, schema_name):
            return False
        return self.gateway.create_schema(
            catalog_name=catalog_name,
            schema_name=schema_name,
            comment=f"Managed by ndr_lakehouse: {catalog_name}.{schema_name}",
        )

    def ensure(self, catalog_name: str, schema_name: str) -> dict[str, bool]:
        return {
            "catalog_created": self.ensure_catalog(catalog_name),
            "schema_created": self.ensure_schema(catalog_name, schema_name),
        }
