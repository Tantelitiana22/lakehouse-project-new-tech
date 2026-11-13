"""Delta Lake merge execution with UniForm Iceberg compatibility.

The merge is built through Delta Lake's ``DeltaTable`` API (no SQL MERGE
string). Tables are written as Delta tables with
``delta.enableIcebergCompatV2`` and ``delta.universalFormat.enabledFormats``
so the same data is exposed in Iceberg format.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ndr_lakehouse.asset_property.schema_validator import CatalogProperties
from ndr_lakehouse.iceberg.delta_api import delta_table_for_name

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

TableResolver = Callable[["SparkSession", str], Any]


class DeltaMergeExecutor:
    """Execute an atomic Delta merge for overwrite/insert semantics."""

    def __init__(self, table_resolver: TableResolver = delta_table_for_name) -> None:
        self._table_resolver = table_resolver

    @staticmethod
    def build_merge_condition(
        primary_keys: list[str],
        target_alias: str = "target",
        source_alias: str = "source",
    ) -> str:
        """Build the composite-key join condition for the Delta merge builder."""

        return " AND ".join(f"{target_alias}.{key} = {source_alias}.{key}" for key in primary_keys)

    def execute(self, spark: SparkSession, df: DataFrame, catalog: CatalogProperties) -> str:
        target = self._table_resolver(spark, catalog.full_table_name).alias("target")
        source = df.alias("source")
        condition = self.build_merge_condition(catalog.primary_keys)

        merge = target.merge(source=source, condition=condition)

        if catalog.merge_delete_condition:
            merge = merge.whenMatchedDelete(condition=catalog.merge_delete_condition)

        update_set = {column: f"source.{column}" for column in df.columns if column not in catalog.primary_keys}
        if update_set:
            update_kwargs: dict[str, Any] = {"set": update_set}
            if catalog.merge_matched_update_condition:
                update_kwargs["condition"] = catalog.merge_matched_update_condition
            merge = merge.whenMatchedUpdate(**update_kwargs)

        insert_values = {column: f"source.{column}" for column in df.columns}
        insert_kwargs: dict[str, Any] = {"values": insert_values}
        if catalog.merge_not_matched_insert_condition:
            insert_kwargs["condition"] = catalog.merge_not_matched_insert_condition
        merge = merge.whenNotMatchedInsert(**insert_kwargs)

        if catalog.merge_not_matched_by_source_delete_condition:
            merge = merge.whenNotMatchedBySourceDelete(condition=catalog.merge_not_matched_by_source_delete_condition)

        merge.execute()
        return condition


# Backwards-compatible name for callers that only care about the abstraction.
MergeExecutor = DeltaMergeExecutor
