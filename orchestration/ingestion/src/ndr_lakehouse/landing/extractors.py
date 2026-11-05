"""Source extractors: pull bytes and hand them to the landing writer."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod

from ndr_lakehouse.asset_property.schema_validator import SourceProperties
from ndr_lakehouse.landing.http import RetryingHttpClient
from ndr_lakehouse.landing.pagination.factory import PaginationFactory
from ndr_lakehouse.landing.utils import RecordExtractor
from ndr_lakehouse.landing.writer import LandingWriter


class SourceExtractor(ABC):
    """Port for a source-type extractor."""

    @abstractmethod
    def extract(self, source: SourceProperties, bucket: str, prefix: str) -> None:
        """Extract one source into the landing zone."""


class HttpSourceExtractor(SourceExtractor):
    """HTTP/REST extractor with pagination and byte-preserving writes."""

    def __init__(
        self,
        http: RetryingHttpClient,
        writer: LandingWriter,
        pagination_factory: PaginationFactory,
        records: RecordExtractor,
    ) -> None:
        self.http = http
        self.writer = writer
        self.pagination_factory = pagination_factory
        self.records = records

    def extract(self, source: SourceProperties, bucket: str, prefix: str) -> None:
        if source.pagination.strategy == "none":
            self._extract_single(source, bucket, prefix)
        else:
            self._extract_paginated(source, bucket, prefix)

    def _extract_single(
        self,
        source: SourceProperties,
        bucket: str,
        prefix: str,
    ) -> None:
        if source.native_format == "json":
            response = self.http.request(
                source.method,
                source.url,
                params=source.params,
                headers=source.headers,
                json_body=source.json_body,
            )
            body = response.content
        else:
            body = self.http.stream_bytes(source)
        self.writer.write_single(bucket, prefix, source, body)

    def _extract_paginated(
        self,
        source: SourceProperties,
        bucket: str,
        prefix: str,
    ) -> None:
        pagination = source.pagination
        strategy = self.pagination_factory.create(pagination)
        request = strategy.first_request(source)
        page_number = 1

        while request is not None and page_number <= pagination.max_pages:
            response, payload = self.http.get_json(source, request.url, request.params)
            self.writer.write_page(bucket, prefix, page_number, source, response, payload)
            records = self.records.extract(payload, pagination.records_json_path)
            if strategy.should_stop(records):
                break

            next_request = strategy.next_request(
                request,
                page_number,
                payload,
                response.headers,
                records,
            )
            if next_request is None:
                break
            self._rate_limit_sleep(pagination, page_number)
            request = next_request
            page_number += 1

    @staticmethod
    def _rate_limit_sleep(pagination, page_number: int) -> None:
        if pagination.rate_limit_sleep_sec > 0 and page_number < pagination.max_pages:
            time.sleep(pagination.rate_limit_sleep_sec)


class S3SourceExtractor(SourceExtractor):
    """Copy an existing S3 object without content transformation."""

    def __init__(self, writer: LandingWriter) -> None:
        self.writer = writer

    def extract(self, source: SourceProperties, bucket: str, prefix: str) -> None:
        self.writer.copy_source(bucket, prefix, source)
