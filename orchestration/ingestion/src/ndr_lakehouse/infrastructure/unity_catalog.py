"""Unity Catalog OSS REST adapter for the catalog-gateway port."""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


class UnityCatalogRestClient:
    """Low-level Unity Catalog REST operations (no idempotency policy)."""

    def __init__(
        self,
        base_url: str,
        api_version: str = "2.1",
        timeout_seconds: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self._client = client or httpx.Client(
            base_url=self.base_url,
            timeout=timeout_seconds,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

    def _path(self, resource: str) -> str:
        return f"/api/{self.api_version}/unity-catalog/{resource}"

    def catalog_exists(self, catalog_name: str) -> bool:
        response = self._client.get(self._path(f"catalogs/{catalog_name}"))
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def create_catalog(self, catalog_name: str, storage_root: str, comment: str) -> bool:
        response = self._client.post(
            self._path("catalogs"),
            json={
                "name": catalog_name,
                "comment": comment,
                "storage_root": storage_root,
            },
        )
        if response.status_code == 409:
            return False
        response.raise_for_status()
        return True

    def schema_exists(self, catalog_name: str, schema_name: str) -> bool:
        response = self._client.get(self._path(f"schemas/{catalog_name}.{schema_name}"))
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def create_schema(
        self,
        catalog_name: str,
        schema_name: str,
        comment: str,
    ) -> bool:
        response = self._client.post(
            self._path("schemas"),
            json={
                "name": schema_name,
                "catalog_name": catalog_name,
                "comment": comment,
            },
        )
        if response.status_code == 409:
            return False
        response.raise_for_status()
        return True

    def close(self) -> None:
        self._client.close()
