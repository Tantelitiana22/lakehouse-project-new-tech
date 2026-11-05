"""Dagster adapter for the Delta UniForm conversion layer."""

from __future__ import annotations

from typing import Any

from dagster import ConfigurableResource

from ndr_lakehouse.asset_property.schema_validator import AssetConfig
from ndr_lakehouse.iceberg.merge import DeltaMergeExecutor
from ndr_lakehouse.iceberg.properties import TablePropertiesSynchronizer
from ndr_lakehouse.iceberg.reader import NativeDatasetReader
from ndr_lakehouse.iceberg.schema import SchemaEvolutionService
from ndr_lakehouse.iceberg.service import IcebergLoadService, SnapshotInspector
from ndr_lakehouse.iceberg.session import SparkSessionFactory, SparkSettings
from ndr_lakehouse.iceberg.transformer import DataFrameTransformer
from ndr_lakehouse.iceberg.writers import WriteStrategyFactory


class SparkIcebergResource(ConfigurableResource):
    """Dagster resource that wires the Delta UniForm conversion use case."""

    endpoint_url: str = "http://rustfs:9000"
    access_key_id: str = "rustfsadmin"
    secret_access_key: str = "rustfssecret"
    region: str = "us-east-1"
    uc_iceberg_uri: str = "http://unity-catalog:8080/api/2.1/unity-catalog/iceberg"
    warehouse: str = "s3://lakehouse/warehouse"
    catalog_name: str = "lakehouse"
    master: str = "local[*]"
    app_name: str = "lakehouse-ingestion"
    use_path_style: bool = True
    spark_jars_dir: str = "/opt/spark-jars"

    def _settings(self) -> SparkSettings:
        return SparkSettings(
            endpoint_url=self.endpoint_url,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            region=self.region,
            uc_iceberg_uri=self.uc_iceberg_uri,
            warehouse=self.warehouse,
            master=self.master,
            app_name=self.app_name,
            use_path_style=self.use_path_style,
            spark_jars_dir=self.spark_jars_dir,
        )

    def _service(self) -> IcebergLoadService:
        settings = self._settings()
        return IcebergLoadService(
            session_provider=SparkSessionFactory(settings),
            reader=NativeDatasetReader(),
            transformer=DataFrameTransformer(),
            schema_evolution=SchemaEvolutionService(),
            writers=WriteStrategyFactory.default(DeltaMergeExecutor()),
            properties=TablePropertiesSynchronizer(),
            snapshots=SnapshotInspector(),
        )

    def get_spark_session(self, catalog_name: str | None = None) -> Any:
        return SparkSessionFactory(self._settings()).create(catalog_name or self.catalog_name)

    def load(self, config: AssetConfig, landing_uri: str) -> dict[str, Any]:
        return self._service().load(config, landing_uri)
