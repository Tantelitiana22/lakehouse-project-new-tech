"""Delta Lake API access, isolated behind small callables.

The ``DeltaTable`` import lives at module scope so the rest of the Iceberg
layer depends on plain callables rather than on ``delta-spark`` directly.
"""

from __future__ import annotations

from typing import Any

from delta.tables import DeltaTable


def delta_table_for_name(spark: Any, table_name: str) -> Any:
    return DeltaTable.forName(spark, table_name)


def delta_table_create_or_replace(spark: Any) -> Any:
    return DeltaTable.createOrReplace(spark)
