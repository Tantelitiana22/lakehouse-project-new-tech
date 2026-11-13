"""Delta UniForm write strategies (Strategy + Factory patterns).

Tables are Delta tables with UniForm enabled so Iceberg readers can consume
the same storage. Merge uses Delta's fluent ``DeltaTable.merge`` API rather
than a SQL string.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from ndr_lakehouse.asset_property.schema_validator import CatalogProperties
from ndr_lakehouse.domain.errors import UnsupportedWriteModeError
from ndr_lakehouse.iceberg.delta_api import delta_table_create_or_replace
from ndr_lakehouse.iceberg.merge import DeltaMergeExecutor
from ndr_lakehouse.iceberg.properties import with_uniform_iceberg_properties

TableCreator = Callable[[Any], Any]


class WriteStrategy(ABC):
    @abstractmethod
    def write(self, spark: Any, df: Any, catalog: CatalogProperties) -> None: ...


class OverwriteWriteStrategy(WriteStrategy):
    """Create/replace a Delta table with UniForm, then load the DataFrame."""

    def __init__(self, table_creator: TableCreator = delta_table_create_or_replace):
        self._table_creator = table_creator

    def write(self, spark: Any, df: Any, catalog: CatalogProperties) -> None:
        builder = self._table_creator(spark).tableName(catalog.full_table_name).addColumns(df.schema)
        if catalog.partition_by:
            builder = builder.partitionedBy(*catalog.partition_by)
        for key, value in with_uniform_iceberg_properties(catalog.iceberg_tblproperties).items():
            builder = builder.property(key, str(value))
        builder.execute()

        (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(catalog.full_table_name))


class AppendWriteStrategy(WriteStrategy):
    def write(self, spark: Any, df: Any, catalog: CatalogProperties) -> None:
        (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(catalog.full_table_name))


class MergeWriteStrategy(WriteStrategy):
    def __init__(self, executor: DeltaMergeExecutor | None = None) -> None:
        self.executor = executor or DeltaMergeExecutor()

    def write(self, spark: Any, df: Any, catalog: CatalogProperties) -> None:
        self.executor.execute(spark=spark, df=df, catalog=catalog)


class WriteStrategyFactory:
    """Resolve write modes without modifying the load service."""

    def __init__(self) -> None:
        self._builders: dict[str, Callable[[], WriteStrategy]] = {}

    def register(self, mode: str, builder: Callable[[], WriteStrategy]) -> None:
        self._builders[mode] = builder

    def create(self, mode: str) -> WriteStrategy:
        try:
            return self._builders[mode]()
        except KeyError as exc:
            raise UnsupportedWriteModeError(f"unsupported write mode: {mode!r}") from exc

    @classmethod
    def default(
        cls,
        merge_executor: DeltaMergeExecutor | None = None,
        table_creator: TableCreator = delta_table_create_or_replace,
    ) -> WriteStrategyFactory:
        factory = cls()
        factory.register("overwrite", lambda: OverwriteWriteStrategy(table_creator))
        factory.register("append", AppendWriteStrategy)
        factory.register(
            "merge",
            lambda: MergeWriteStrategy(merge_executor or DeltaMergeExecutor()),
        )
        return factory
