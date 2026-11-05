"""Conversion layer: Spark reading, sanitisation, dedup and Delta UniForm writes."""

from ndr_lakehouse.iceberg.merge import DeltaMergeExecutor
from ndr_lakehouse.iceberg.reader import NativeDatasetReader
from ndr_lakehouse.iceberg.resource import SparkIcebergResource
from ndr_lakehouse.iceberg.schema import SchemaEvolutionService
from ndr_lakehouse.iceberg.service import IcebergLoadService
from ndr_lakehouse.iceberg.session import SparkSessionFactory, SparkSettings
from ndr_lakehouse.iceberg.transformer import (
    DataFrameTransformer,
    sanitize_column_name,
    sanitize_columns,
)
from ndr_lakehouse.iceberg.writers import (
    AppendWriteStrategy,
    MergeWriteStrategy,
    OverwriteWriteStrategy,
    WriteStrategyFactory,
)

__all__ = [
    "AppendWriteStrategy",
    "DataFrameTransformer",
    "DeltaMergeExecutor",
    "IcebergLoadService",
    "MergeWriteStrategy",
    "NativeDatasetReader",
    "OverwriteWriteStrategy",
    "SchemaEvolutionService",
    "SparkIcebergResource",
    "SparkSessionFactory",
    "SparkSettings",
    "WriteStrategyFactory",
    "sanitize_column_name",
    "sanitize_columns",
]
