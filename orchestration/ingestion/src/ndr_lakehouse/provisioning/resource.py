"""Dagster adapter for the provisioning layer."""

from __future__ import annotations

from dagster import ConfigurableResource

from ndr_lakehouse.infrastructure.object_store import Boto3ObjectStore
from ndr_lakehouse.infrastructure.unity_catalog import UnityCatalogRestClient
from ndr_lakehouse.provisioning.buckets import BucketProvisioner
from ndr_lakehouse.provisioning.service import ProvisioningService
from ndr_lakehouse.provisioning.unity_catalog import NamespaceProvisioner


class UnityCatalogProvisionerResource(ConfigurableResource):
    """Dagster resource exposing the provisioning use case."""

    endpoint_url: str = "http://rustfs:9000"
    access_key_id: str = "rustfsadmin"
    secret_access_key: str = "rustfssecret"
    region: str = "us-east-1"
    landing_bucket: str = "landing-zone"
    lakehouse_bucket: str = "lakehouse"
    uc_base_url: str = "http://unity-catalog:8080"
    uc_api_version: str = "2.1"
    uc_storage_root: str = "s3://lakehouse/warehouse"
    timeout_seconds: float = 30.0
    use_path_style: bool = True

    def _object_store(self) -> Boto3ObjectStore:
        return Boto3ObjectStore(
            endpoint_url=self.endpoint_url,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            region=self.region,
            use_path_style=self.use_path_style,
        )

    def _bucket_provisioner(self) -> BucketProvisioner:
        return BucketProvisioner(
            object_store=self._object_store(),
            buckets=(self.landing_bucket, self.lakehouse_bucket),
        )

    def _catalog_client(self) -> UnityCatalogRestClient:
        return UnityCatalogRestClient(
            base_url=self.uc_base_url,
            api_version=self.uc_api_version,
            timeout_seconds=self.timeout_seconds,
        )

    def ensure_bucket(self, bucket: str) -> bool:
        return self._bucket_provisioner().ensure(bucket)

    def ensure_buckets(self) -> dict[str, bool]:
        return self._bucket_provisioner().ensure_all()

    def ensure_catalog_and_schema(
        self,
        catalog_name: str,
        schema_name: str,
    ) -> dict[str, bool]:
        client = self._catalog_client()
        try:
            namespaces = NamespaceProvisioner(
                gateway=client,
                storage_root=self.uc_storage_root,
            )
            return namespaces.ensure(catalog_name, schema_name)
        finally:
            client.close()

    def ensure_catalog(self, catalog_name: str) -> bool:
        client = self._catalog_client()
        try:
            return NamespaceProvisioner(
                gateway=client,
                storage_root=self.uc_storage_root,
            ).ensure_catalog(catalog_name)
        finally:
            client.close()

    def ensure_schema(self, catalog_name: str, schema_name: str) -> bool:
        client = self._catalog_client()
        try:
            return NamespaceProvisioner(
                gateway=client,
                storage_root=self.uc_storage_root,
            ).ensure_schema(catalog_name, schema_name)
        finally:
            client.close()

    def provision(self, catalog_name: str, schema_name: str) -> dict[str, object]:
        client = self._catalog_client()
        try:
            service = ProvisioningService(
                buckets=self._bucket_provisioner(),
                namespaces=NamespaceProvisioner(
                    gateway=client,
                    storage_root=self.uc_storage_root,
                ),
            )
            return service.provision(catalog_name, schema_name)
        finally:
            client.close()
