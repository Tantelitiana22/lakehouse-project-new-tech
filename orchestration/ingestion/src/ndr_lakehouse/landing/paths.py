"""Deterministic landing-zone key construction."""

from __future__ import annotations

from datetime import date

from ndr_lakehouse.asset_property.schema_validator import SourceProperties
from ndr_lakehouse.landing.formats import extension_for


class LandingPathBuilder:
    """Single responsibility: build landing prefixes, keys and URIs."""

    @staticmethod
    def partition_prefix(source: SourceProperties, ingest_date: date) -> str:
        return f"{source.s3_prefix}/ingest_date={ingest_date.isoformat()}"

    def single_key(self, source: SourceProperties, prefix: str) -> str:
        return f"{prefix}/data{extension_for(source.native_format)}"

    def page_key(
        self,
        source: SourceProperties,
        prefix: str,
        page_number: int,
    ) -> str:
        return f"{prefix}/part_{page_number:05d}{extension_for(source.native_format)}"

    @staticmethod
    def s3a_uri(bucket: str, prefix: str) -> str:
        return f"s3a://{bucket}/{prefix}"
