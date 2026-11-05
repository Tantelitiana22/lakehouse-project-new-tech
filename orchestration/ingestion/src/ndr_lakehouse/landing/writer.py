"""Write native payloads to the landing-zone object store."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

from ndr_lakehouse.asset_property.schema_validator import SourceProperties
from ndr_lakehouse.domain.ports import HttpResponse, ObjectStore
from ndr_lakehouse.landing.formats import content_type_for, extension_for
from ndr_lakehouse.landing.paths import LandingPathBuilder


class LandingWriter:
    """Persist raw bytes only; never decodes or reshapes the payload."""

    def __init__(self, object_store: ObjectStore, paths: LandingPathBuilder) -> None:
        self.object_store = object_store
        self.paths = paths

    def write_single(
        self,
        bucket: str,
        prefix: str,
        source: SourceProperties,
        body: bytes,
    ) -> None:
        self.object_store.put_object(
            bucket=bucket,
            key=self.paths.single_key(source, prefix),
            body=body,
            content_type=content_type_for(source.native_format),
        )

    def write_page(
        self,
        bucket: str,
        prefix: str,
        page_number: int,
        source: SourceProperties,
        response: HttpResponse,
        payload: Any,
    ) -> None:
        body = response.content
        if not isinstance(body, (bytes, bytearray)) or not body:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.object_store.put_object(
            bucket=bucket,
            key=self.paths.page_key(source, prefix, page_number),
            body=bytes(body),
            content_type=content_type_for(source.native_format),
        )

    def copy_source(
        self,
        bucket: str,
        prefix: str,
        source: SourceProperties,
    ) -> None:
        parsed = urlparse(source.url)
        source_bucket = parsed.netloc
        source_key = parsed.path.lstrip("/")
        filename = source_key.rsplit("/", 1)[-1] or f"data{extension_for(source.native_format)}"
        target_key = f"{prefix}/{filename}"
        self.object_store.copy_object(
            target_bucket=bucket,
            target_key=target_key,
            source_bucket=source_bucket,
            source_key=source_key,
        )
