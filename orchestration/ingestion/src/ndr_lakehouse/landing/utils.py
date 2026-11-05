"""JSON path, Link-header and record-array helpers."""

from __future__ import annotations

from typing import Any

from ndr_lakehouse.asset_property.schema_validator import PaginationConfig


def parse_link_header(header: str | None) -> str | None:
    """Extract the ``rel="next"`` target from an RFC 5988 ``Link`` header."""

    if not header:
        return None
    for segment in header.split(","):
        parts = [part.strip() for part in segment.split(";")]
        if not parts or not parts[0].startswith("<") or not parts[0].endswith(">"):
            continue
        url = parts[0][1:-1].strip()
        relations = {
            value.strip().strip('"')
            for part in parts[1:]
            if "=" in part and part.split("=", 1)[0].strip().lower() == "rel"
            for value in [part.split("=", 1)[1]]
        }
        if "next" in relations:
            return url
    return None


def dig_json_path(payload: Any, dotted_path: str | None) -> Any:
    """Read a value from nested JSON using dot notation (list indexes allowed)."""

    if dotted_path is None or dotted_path == "":
        return None
    current = payload
    for part in dotted_path.split("."):
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def looks_like_records(value: Any) -> bool:
    return isinstance(value, (list, tuple))


def should_stop_records(records: Any, pagination: PaginationConfig) -> bool:
    """Stop when no records remain or a short final page was received."""

    if not looks_like_records(records):
        return False
    if len(records) == 0:
        return True
    if pagination.limit_value is not None and len(records) < pagination.limit_value:
        return True
    return False


class RecordExtractor:
    """Resolve the records array from a raw payload."""

    _COMMON_KEYS = ("records", "data", "items", "results")

    def extract(self, payload: Any, path: str | None) -> Any:
        if path:
            return dig_json_path(payload, path)
        if looks_like_records(payload):
            return payload
        if isinstance(payload, dict):
            for key in self._COMMON_KEYS:
                if looks_like_records(payload.get(key)):
                    return payload[key]
        return None
