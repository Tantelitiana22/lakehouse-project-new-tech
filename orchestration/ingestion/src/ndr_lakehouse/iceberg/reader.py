"""Read native RustFS files into Spark DataFrames."""

from __future__ import annotations

from typing import Any

from ndr_lakehouse.asset_property.schema_validator import SourceProperties

_FORMAT_TO_READER = {
    "json": "json",
    "csv": "csv",
    "csv.gz": "csv",
    "parquet": "parquet",
}

_DEFAULT_READ_OPTIONS = {
    "json": {"multiLine": "true"},
    "csv": {"header": "true", "inferSchema": "true"},
    "csv.gz": {"header": "true", "inferSchema": "true", "compression": "gzip"},
    "parquet": {},
}


class NativeDatasetReader:
    """Adapter between native file formats and Spark's DataFrameReader."""

    def read(
        self,
        spark: Any,
        source: SourceProperties,
        landing_uri: str,
        read_options: dict[str, str] | None = None,
    ) -> Any:
        options = dict(_DEFAULT_READ_OPTIONS[source.native_format])
        options.update(read_options or {})
        reader = spark.read.options(**options)
        reader_kind = _FORMAT_TO_READER[source.native_format]
        if reader_kind == "json":
            return reader.json(landing_uri)
        if reader_kind == "csv":
            return reader.csv(landing_uri)
        return reader.parquet(landing_uri)
