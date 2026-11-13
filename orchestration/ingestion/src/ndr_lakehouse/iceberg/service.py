"""Iceberg load use case."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from ndr_lakehouse.asset_property.schema_validator import AssetConfig, CatalogProperties
from ndr_lakehouse.domain.ports import SparkSessionProvider
from ndr_lakehouse.iceberg.delta_api import ensure_table_registered, table_location
from ndr_lakehouse.iceberg.properties import TablePropertiesSynchronizer
from ndr_lakehouse.iceberg.reader import NativeDatasetReader
from ndr_lakehouse.iceberg.schema import SchemaEvolutionService
from ndr_lakehouse.iceberg.transformer import DataFrameTransformer
from ndr_lakehouse.iceberg.writers import WriteStrategyFactory
from ndr_lakehouse.provisioning.uc_columns import columns_from_spark_schema

logger = logging.getLogger(__name__)


class SnapshotInspector:
    def latest(self, spark: Any, full_table_name: str) -> dict[str, Any]:
        try:
            rows = spark.sql(
                f"SELECT snapshot_id, committed_at FROM {full_table_name}.snapshots "
                "ORDER BY committed_at DESC LIMIT 1"
            ).collect()
        except Exception:
            return {}
        if not rows:
            return {}
        row = rows[0]
        data = row.asDict() if hasattr(row, "asDict") else {}
        return {key: str(value) for key, value in data.items()}


class IcebergLoadService:
    """Read native landing files, transform, deduplicate and write Iceberg."""

    def __init__(
        self,
        session_provider: SparkSessionProvider,
        reader: NativeDatasetReader,
        transformer: DataFrameTransformer,
        schema_evolution: SchemaEvolutionService,
        writers: WriteStrategyFactory,
        properties: TablePropertiesSynchronizer,
        snapshots: SnapshotInspector,
        table_registrar: Callable[[CatalogProperties, str, list[dict[str, Any]]], Any] | None = None,
    ) -> None:
        self.session_provider = session_provider
        self.reader = reader
        self.transformer = transformer
        self.schema_evolution = schema_evolution
        self.writers = writers
        self.properties = properties
        self.snapshots = snapshots
        self.table_registrar = table_registrar

    def load(self, config: AssetConfig, landing_uri: str) -> dict[str, Any]:
        source = config.source_properties
        catalog = config.catalog_properties
        full_table_name = catalog.full_table_name
        spark = self.session_provider.create(catalog.catalog_name)

        df = self.reader.read(
            spark,
            source,
            landing_uri,
            read_options=catalog.read_options,
        )
        df = self.transformer.transform(
            df,
            source,
            asset_name=config.name,
            landing_uri=landing_uri,
        )
        df = self.transformer.deduplicate(
            df,
            primary_keys=catalog.primary_keys,
            dedup_order_by=catalog.dedup_order_by,
        )

        table_existed = ensure_table_registered(spark, full_table_name)
        if not table_existed:
            logger.info("Creating Iceberg table %s", full_table_name)
            self.writers.create("overwrite").write(spark, df, catalog)
        else:
            if catalog.auto_evolve_schema:
                self.schema_evolution.evolve(spark, df, full_table_name)
            self.writers.create(catalog.write_mode).write(spark, df, catalog)
            self.properties.sync(
                spark,
                full_table_name,
                catalog.iceberg_tblproperties,
            )

        if self.table_registrar is not None:
            columns = columns_from_spark_schema(getattr(df, "schema", None))
            if not columns:
                try:
                    columns = columns_from_spark_schema(getattr(spark.table(full_table_name), "schema", None))
                except Exception:
                    columns = []
            self.table_registrar(catalog, table_location(spark, full_table_name), columns)

        return {
            "table": full_table_name,
            "row_count": int(spark.table(full_table_name).count()),
            "write_mode": catalog.write_mode,
            "table_existed": table_existed,
            "table_properties": catalog.iceberg_tblproperties,
            "snapshot": self.snapshots.latest(spark, full_table_name),
            "source_name": config.name,
            "landing_uri": landing_uri,
        }
