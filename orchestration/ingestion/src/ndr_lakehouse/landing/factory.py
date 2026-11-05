"""Extractor registry (Factory pattern)."""

from __future__ import annotations

from ndr_lakehouse.domain.errors import UnsupportedSourceTypeError
from ndr_lakehouse.landing.extractors import SourceExtractor


class ExtractorFactory:
    """Resolve a source type to its extractor without conditionals in services."""

    def __init__(self) -> None:
        self._extractors: dict[str, SourceExtractor] = {}

    def register(self, source_type: str, extractor: SourceExtractor) -> None:
        self._extractors[source_type] = extractor

    def create(self, source_type: str) -> SourceExtractor:
        try:
            return self._extractors[source_type]
        except KeyError as exc:
            raise UnsupportedSourceTypeError(f"unsupported source type: {source_type!r}") from exc

    @classmethod
    def default(
        cls,
        http_extractor: SourceExtractor,
        s3_extractor: SourceExtractor,
    ) -> ExtractorFactory:
        factory = cls()
        factory.register("http", http_extractor)
        factory.register("s3", s3_extractor)
        return factory
