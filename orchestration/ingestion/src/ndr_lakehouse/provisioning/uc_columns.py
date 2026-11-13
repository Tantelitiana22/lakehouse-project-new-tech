"""Convert Spark or Delta schemas into Unity Catalog REST column payloads."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")

_SIMPLE_TYPE_MAP: dict[str, tuple[str, str]] = {
    "string": ("STRING", "string"),
    "varchar": ("STRING", "string"),
    "char": ("STRING", "string"),
    "bigint": ("LONG", "bigint"),
    "long": ("LONG", "bigint"),
    "int": ("INT", "int"),
    "integer": ("INT", "int"),
    "smallint": ("SHORT", "smallint"),
    "short": ("SHORT", "smallint"),
    "tinyint": ("BYTE", "tinyint"),
    "byte": ("BYTE", "tinyint"),
    "double": ("DOUBLE", "double"),
    "float": ("FLOAT", "float"),
    "real": ("FLOAT", "float"),
    "boolean": ("BOOLEAN", "boolean"),
    "date": ("DATE", "date"),
    "timestamp": ("TIMESTAMP", "timestamp"),
    "timestamp_ntz": ("TIMESTAMP_NTZ", "timestamp_ntz"),
    "binary": ("BINARY", "binary"),
}

_DELTA_PRIMITIVE_TYPE_MAP: dict[str, str] = {
    "int": "integer",
    "integer": "integer",
    "bigint": "long",
    "long": "long",
    "smallint": "short",
    "short": "short",
    "tinyint": "byte",
    "byte": "byte",
    "float": "float",
    "real": "float",
    "double": "double",
    "boolean": "boolean",
    "bool": "boolean",
    "string": "string",
    "varchar": "string",
    "char": "string",
    "text": "string",
    "binary": "binary",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp_ntz",
}


def _delta_type_json_type(type_text: str) -> Any:
    """Return the Delta JSON schema representation for ``type_text``."""

    normalized = type_text.strip().lower()
    base = normalized.split("(")[0].split("<")[0].strip()
    complex_templates: dict[str, Any] = {
        "struct": {"type": "struct", "fields": []},
        "array": {"type": "array", "elementType": "string", "containsNull": True},
        "map": {
            "type": "map",
            "keyType": "string",
            "valueType": "string",
            "valueContainsNull": True,
        },
    }
    if base in complex_templates:
        return complex_templates[base]
    if base == "decimal":
        return normalized if "(" in normalized else "decimal(10,0)"
    return _DELTA_PRIMITIVE_TYPE_MAP.get(base, "string")


def _map_type(raw_type: Any) -> tuple[str, str, int, int]:
    """Return ``(type_name, type_text, precision, scale)`` for a Spark/Delta type."""

    if isinstance(raw_type, Mapping):
        kind = str(raw_type.get("type", "struct")).strip().lower()
        return {
            "array": ("ARRAY", "array", 0, 0),
            "map": ("MAP", "map", 0, 0),
        }.get(kind, ("STRUCT", "struct", 0, 0))

    normalized = str(raw_type).strip().lower()
    if normalized in _SIMPLE_TYPE_MAP:
        type_name, type_text = _SIMPLE_TYPE_MAP[normalized]
        return (type_name, type_text, 0, 0)

    if match := _DECIMAL_RE.match(normalized):
        return ("DECIMAL", normalized, int(match.group(1)), int(match.group(2)))

    result: tuple[str, str, int, int] = next(
        (
            (uc_name, normalized if prefix == "decimal" else prefix, 0, 0)
            for prefix, uc_name in (
                ("array", "ARRAY"),
                ("struct", "STRUCT"),
                ("map", "MAP"),
                ("decimal", "DECIMAL"),
            )
            if normalized.startswith(prefix)
        ),
        (
            ("STRING", "string", 0, 0)
            if normalized.startswith(("varchar(", "char("))
            else ("STRING", normalized or "string", 0, 0)
        ),
    )
    return result


def _column_entry(
    *,
    name: str,
    raw_type: Any,
    nullable: bool,
    position: int,
    type_json: str | None = None,
) -> dict[str, Any]:
    type_name, type_text, precision, scale = _map_type(raw_type)
    resolved_type_field = raw_type if isinstance(raw_type, Mapping) else _delta_type_json_type(type_text)
    resolved_type_field = raw_type if isinstance(raw_type, Mapping) else _delta_type_json_type(type_text)
    resolved_json = type_json or json.dumps(
        {
            "name": name,
            "type": resolved_type_field,
            "nullable": nullable,
            "metadata": {},
        }
    )
    return {
        "name": name,
        "type_text": type_text,
        "type_json": resolved_json,
        "type_name": type_name,
        "type_precision": precision,
        "type_scale": scale,
        "position": position,
        "comment": "",
        "nullable": nullable,
    }


def columns_from_spark_schema(schema: Any) -> list[dict[str, Any]]:
    """Build Unity Catalog column descriptors from a PySpark ``StructType`` (or stub)."""

    json_value_fn = getattr(schema, "jsonValue", None)
    if callable(json_value_fn):
        raw_json = json_value_fn()
        if isinstance(raw_json, Mapping):
            return columns_from_delta_schema_json(raw_json)

    fields = getattr(schema, "fields", None)
    if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)) or not fields:
        return []
    columns: list[dict[str, Any]] = []
    for position, field in enumerate(fields):
        data_type = getattr(field, "dataType", None)
        simple_string = getattr(data_type, "simpleString", None)
        raw_type = simple_string() if callable(simple_string) else str(data_type or "string")
        nullable = bool(getattr(field, "nullable", True))
        columns.append(
            _column_entry(
                name=str(field.name),
                raw_type=raw_type,
                nullable=nullable,
                position=position,
            )
        )
    return columns


def columns_from_delta_schema_json(schema_json: str | Mapping[str, Any]) -> list[dict[str, Any]]:
    """Build Unity Catalog column descriptors from Delta ``metaData.schemaString``."""

    parsed = json.loads(schema_json) if isinstance(schema_json, str) else dict(schema_json)
    fields = parsed.get("fields") or []
    columns: list[dict[str, Any]] = [
        _column_entry(
            name=str(field["name"]),
            raw_type=field.get("type", "string"),
            nullable=bool(field.get("nullable", True)),
            position=position,
            type_json=json.dumps(dict(field)),
        )
        for position, field in enumerate(fields)
        if isinstance(field, Mapping)
    ]
    return columns
