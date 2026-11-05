"""Iceberg/Delta UniForm table properties."""

from __future__ import annotations

from typing import Any

UNIFORM_ICEBERG_PROPERTIES: dict[str, str] = {
    "delta.enableIcebergCompatV2": "true",
    "delta.universalFormat.enabledFormats": "iceberg",
}


def with_uniform_iceberg_properties(
    properties: dict[str, str] | None = None,
) -> dict[str, str]:
    """Always enable Delta UniForm Iceberg output for the table."""

    merged = dict(properties or {})
    merged.update(UNIFORM_ICEBERG_PROPERTIES)
    return merged


class TablePropertiesSynchronizer:
    """Apply table properties (including UniForm) to an existing Delta table."""

    def sync(
        self,
        spark: Any,
        full_table_name: str,
        properties: dict[str, str],
    ) -> None:
        merged = with_uniform_iceberg_properties(properties)
        if not merged:
            return
        rendered = ", ".join(
            f"{key}='{str(value).replace(chr(39), chr(39) * 2)}'" for key, value in merged.items()
        )
        spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ({rendered})")
