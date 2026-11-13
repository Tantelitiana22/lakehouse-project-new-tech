"""Delta Lake API access, isolated behind small callables.

The ``DeltaTable`` import lives at module scope so the rest of the Iceberg
layer depends on plain callables rather than on ``delta-spark`` directly.
"""

from __future__ import annotations

from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

DEFAULT_WAREHOUSE_DIR = "s3://lakehouse/warehouse"


def table_location(spark: Any, full_table_name: str) -> str:
    """Resolve the warehouse directory path for a 2- or 3-part table identifier."""

    parts = full_table_name.split(".")
    if len(parts) >= 2:
        schema_name, table_name = parts[-2], parts[-1]
    else:
        schema_name, table_name = "default", parts[0]

    warehouse = DEFAULT_WAREHOUSE_DIR
    conf = getattr(spark, "conf", None)
    if conf is not None:
        configured = conf.get("spark.sql.warehouse.dir", DEFAULT_WAREHOUSE_DIR)
        if isinstance(configured, str) and configured.strip():
            warehouse = configured
    return f"{warehouse.rstrip('/')}/{schema_name}.db/{table_name}"


def ensure_table_registered(
    spark: Any,
    full_table_name: str,
    is_delta_table: Any | None = None,
) -> bool:
    """Register an existing Delta table location in the session catalog if missing."""

    if bool(spark.catalog.tableExists(full_table_name)):
        return True
    if not isinstance(spark, SparkSession) and is_delta_table is None:
        return False

    checker = DeltaTable.isDeltaTable if is_delta_table is None else is_delta_table
    location = table_location(spark, full_table_name)
    if not bool(checker(spark, location)):
        return False

    parts = full_table_name.split(".")
    if len(parts) == 3:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {parts[0]}.{parts[1]}")
    elif len(parts) == 2:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {parts[0]}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{location}'")
    return True


class _DeltaTableBuilderAdapter:
    """Normalizes 3-part catalog identifiers and ensures the target namespace exists."""

    def __init__(self, spark: Any, builder: Any) -> None:
        self._spark = spark
        self._builder = builder
        self._namespace: str | None = None
        self._full_identifier: str | None = None
        self._location: str | None = None

    def tableName(self, identifier: str) -> _DeltaTableBuilderAdapter:  # pylint: disable=invalid-name
        self._full_identifier = identifier
        parts = identifier.split(".")
        if len(parts) == 3:
            self._namespace = f"{parts[0]}.{parts[1]}"
            self._builder = self._builder.tableName(f"{parts[1]}.{parts[2]}")
        else:
            self._builder = self._builder.tableName(identifier)
        return self

    def location(self, path: str) -> _DeltaTableBuilderAdapter:
        self._location = path
        self._builder = self._builder.location(path)
        return self

    def addColumns(self, cols: Any) -> _DeltaTableBuilderAdapter:  # pylint: disable=invalid-name
        self._builder = self._builder.addColumns(cols)
        return self

    def partitionedBy(self, *cols: Any) -> _DeltaTableBuilderAdapter:  # pylint: disable=invalid-name
        self._builder = self._builder.partitionedBy(*cols)
        return self

    def property(self, key: str, value: str) -> _DeltaTableBuilderAdapter:
        self._builder = self._builder.property(key, value)
        return self

    def execute(self) -> Any:
        if self._namespace is not None:
            self._spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self._namespace}")
        if self._full_identifier is not None and isinstance(self._spark, SparkSession):
            loc = table_location(self._spark, self._full_identifier)
            ensure_table_registered(self._spark, self._full_identifier)
            if self._location is None:
                self._builder = self._builder.location(loc)
        return self._builder.execute()


def delta_table_for_name(spark: Any, table_name: str) -> Any:
    return DeltaTable.forName(spark, table_name)


def delta_table_create_or_replace(spark: Any) -> Any:
    return _DeltaTableBuilderAdapter(spark, DeltaTable.createOrReplace(spark))
