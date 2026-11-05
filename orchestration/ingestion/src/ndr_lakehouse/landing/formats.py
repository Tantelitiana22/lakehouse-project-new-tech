"""Native format metadata (extensions and content types)."""

from __future__ import annotations

CONTENT_TYPES: dict[str, str] = {
    "json": "application/json",
    "csv": "text/csv",
    "csv.gz": "application/gzip",
    "parquet": "application/octet-stream",
}

EXTENSIONS: dict[str, str] = {
    "json": ".json",
    "csv": ".csv",
    "csv.gz": ".csv.gz",
    "parquet": ".parquet",
}


def extension_for(native_format: str) -> str:
    return EXTENSIONS[native_format]


def content_type_for(native_format: str) -> str:
    return CONTENT_TYPES[native_format]
