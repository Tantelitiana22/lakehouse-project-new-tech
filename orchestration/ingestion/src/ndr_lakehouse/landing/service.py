"""Landing-zone use case (Facade over extractors and path building)."""

from __future__ import annotations

from datetime import UTC, date, datetime

from ndr_lakehouse.asset_property.schema_validator import (
    AssetConfig,
    SourceProperties,
)
from ndr_lakehouse.landing.factory import ExtractorFactory
from ndr_lakehouse.landing.paths import LandingPathBuilder


class LandingIngestionService:
    """Orchestrate extraction for one asset config, independent of Dagster."""

    def __init__(
        self,
        extractors: ExtractorFactory,
        paths: LandingPathBuilder,
        default_landing_bucket: str = "landing-zone",
    ) -> None:
        self.extractors = extractors
        self.paths = paths
        self.default_landing_bucket = default_landing_bucket

    def ingest(
        self,
        config: AssetConfig | SourceProperties,
        ingest_date: date | datetime | None = None,
    ) -> str:
        source = config.source_properties if isinstance(config, AssetConfig) else config
        run_date = normalise_date(ingest_date)
        bucket = source.landing_bucket or self.default_landing_bucket
        prefix = self.paths.partition_prefix(source, run_date)
        self.extractors.create(source.type).extract(source, bucket, prefix)
        return self.paths.s3a_uri(bucket, prefix)


def normalise_date(value: date | datetime | None) -> date:
    if value is None:
        return datetime.now(UTC).date()
    if isinstance(value, datetime):
        return value.date()
    return value
