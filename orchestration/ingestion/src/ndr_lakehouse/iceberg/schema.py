"""Iceberg schema evolution."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class SchemaEvolutionService:
    """Adds missing DataFrame columns to an existing Iceberg table."""

    def evolve(self, spark: Any, df: Any, full_table_name: str) -> list[str]:
        existing = set(spark.table(full_table_name).columns)
        added: list[str] = []
        for field in df.schema.fields:
            if field.name in existing:
                continue
            data_type = field.dataType.simpleString()
            spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMN {field.name} {data_type}")
            added.append(field.name)
        if added:
            logger.info("Evolved %s with columns %s", full_table_name, added)
        return added
