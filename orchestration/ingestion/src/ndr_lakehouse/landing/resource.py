"""Dagster adapter for the landing layer."""

from __future__ import annotations

from dagster import ConfigurableResource

from ndr_lakehouse.asset_property.schema_validator import (
    AssetConfig,
    SourceProperties,
)
from ndr_lakehouse.infrastructure.http import HttpxTransport
from ndr_lakehouse.infrastructure.object_store import Boto3ObjectStore
from ndr_lakehouse.landing.extractors import HttpSourceExtractor, S3SourceExtractor
from ndr_lakehouse.landing.factory import ExtractorFactory
from ndr_lakehouse.landing.http import RetryingHttpClient
from ndr_lakehouse.landing.pagination.factory import PaginationFactory
from ndr_lakehouse.landing.paths import LandingPathBuilder
from ndr_lakehouse.landing.retry import RetryPolicy
from ndr_lakehouse.landing.service import LandingIngestionService
from ndr_lakehouse.landing.utils import RecordExtractor
from ndr_lakehouse.landing.writer import LandingWriter


class NativeIngestorResource(ConfigurableResource):
    """Dagster resource that only wires concrete adapters into the use case."""

    endpoint_url: str = "http://rustfs:9000"
    access_key_id: str = "rustfsadmin"
    secret_access_key: str = "rustfssecret"
    region: str = "us-east-1"
    landing_bucket: str = "landing-zone"
    timeout_seconds: float = 60.0
    max_retries: int = 5
    backoff_base_seconds: float = 0.5
    max_backoff_seconds: float = 30.0
    chunk_size: int = 1024 * 1024
    use_path_style: bool = True

    def ingest(
        self,
        config: AssetConfig | SourceProperties,
        ingest_date=None,
    ) -> str:
        object_store = Boto3ObjectStore(
            endpoint_url=self.endpoint_url,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            region=self.region,
            use_path_style=self.use_path_style,
        )
        transport = HttpxTransport(timeout_seconds=self.timeout_seconds)
        http = RetryingHttpClient(
            transport=transport,
            policy=RetryPolicy(
                max_retries=self.max_retries,
                base_seconds=self.backoff_base_seconds,
                max_seconds=self.max_backoff_seconds,
            ),
            chunk_size=self.chunk_size,
        )
        paths = LandingPathBuilder()
        writer = LandingWriter(object_store, paths)
        http_extractor = HttpSourceExtractor(
            http=http,
            writer=writer,
            pagination_factory=PaginationFactory.default(),
            records=RecordExtractor(),
        )
        s3_extractor = S3SourceExtractor(writer)
        extractors = ExtractorFactory.default(http_extractor, s3_extractor)
        service = LandingIngestionService(
            extractors=extractors,
            paths=paths,
            default_landing_bucket=self.landing_bucket,
        )
        try:
            return service.ingest(config, ingest_date=ingest_date)
        finally:
            http.close()
