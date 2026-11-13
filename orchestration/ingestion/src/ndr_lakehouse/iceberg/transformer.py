"""Pure DataFrame transformations before Iceberg writes."""

from __future__ import annotations

import re
from typing import Any

from ndr_lakehouse.asset_property.schema_validator import SourceProperties

_CAMEL_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM = re.compile(r"[^a-z0-9]")


def sanitize_column_name(name: str) -> str:
    """Lower-case a column, splitting camelCase and replacing non ``[a-z0-9]`` with ``_``."""

    snake = _CAMEL_BOUNDARY.sub(r"\1_\2", str(name).strip()).lower()
    sanitized = _NON_ALNUM.sub("_", snake)
    if not sanitized:
        return "_unnamed"
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def sanitize_columns(df: Any) -> Any:
    """Rename every DataFrame column, disambiguating collisions deterministically."""

    seen: dict[str, int] = {}
    new_names: list[str] = []
    for column in df.columns:
        base = sanitize_column_name(column)
        occurrence = seen.get(base, 0)
        new_names.append(base if occurrence == 0 else f"{base}_{occurrence}")
        seen[base] = occurrence + 1
    return df.toDF(*new_names)


class DataFrameTransformer:
    """Stateless transformation pipeline for Bronze Iceberg tables."""

    def flatten_records(self, df: Any, source: SourceProperties) -> Any:
        if source.native_format != "json" or not source.records_json_path:
            return df
        from pyspark.sql.functions import col, explode

        flattened = df.select(explode(col(source.records_json_path)).alias("_record"))
        return flattened.select("_record.*")

    def add_metadata(self, df: Any, source_name: str, source_file: str) -> Any:
        from pyspark.sql.functions import current_timestamp, input_file_name, lit

        return (
            df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_name", lit(source_name))
            .withColumn("_source_file", input_file_name())
        )

    def deduplicate(
        self,
        df: Any,
        primary_keys: list[str],
        dedup_order_by: str,
    ) -> Any:
        if not primary_keys:
            return df
        from pyspark.sql.functions import col, row_number
        from pyspark.sql.window import Window

        window = Window.partitionBy(*primary_keys).orderBy(col(dedup_order_by).desc_nulls_last())
        return (
            df.withColumn("_ingest_row_number", row_number().over(window))
            .filter(col("_ingest_row_number") == 1)
            .drop("_ingest_row_number")
        )

    def transform(
        self,
        df: Any,
        source: SourceProperties,
        asset_name: str,
        landing_uri: str,
    ) -> Any:
        df = self.flatten_records(df, source)
        df = sanitize_columns(df)
        df = self.add_metadata(df, asset_name, landing_uri)
        return df
