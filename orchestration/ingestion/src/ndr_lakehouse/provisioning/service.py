"""Provisioning use case."""

from __future__ import annotations

from ndr_lakehouse.provisioning.buckets import BucketProvisioner
from ndr_lakehouse.provisioning.unity_catalog import NamespaceProvisioner


class ProvisioningService:
    """Create storage buckets and Unity Catalog namespaces idempotently."""

    def __init__(
        self,
        buckets: BucketProvisioner,
        namespaces: NamespaceProvisioner,
    ) -> None:
        self.buckets = buckets
        self.namespaces = namespaces

    def provision(self, catalog_name: str, schema_name: str) -> dict[str, object]:
        return {
            "buckets": self.buckets.ensure_all(),
            **self.namespaces.ensure(catalog_name, schema_name),
        }
