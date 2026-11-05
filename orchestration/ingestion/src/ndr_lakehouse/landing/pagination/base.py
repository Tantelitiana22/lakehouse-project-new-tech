"""Pagination strategy contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

from ndr_lakehouse.asset_property.schema_validator import (
    PaginationConfig,
    SourceProperties,
)
from ndr_lakehouse.landing.pagination.models import PageRequest
from ndr_lakehouse.landing.utils import should_stop_records


class PaginationStrategy(ABC):
    """Open/Closed extension point for new pagination algorithms.

    Strategies are stateful only through the immutable ``PageRequest`` passed
    back into ``next_request``; the extractor owns the loop.
    """

    def __init__(self, config: PaginationConfig) -> None:
        self.config = config

    def first_request(self, source: SourceProperties) -> PageRequest:
        return PageRequest(url=source.url, params=dict(source.params))

    def should_stop(self, records: Any) -> bool:
        return should_stop_records(records, self.config)

    @abstractmethod
    def next_request(
        self,
        current: PageRequest,
        page_number: int,
        payload: Any,
        headers: Mapping[str, str],
        records: Any,
    ) -> PageRequest | None:
        """Return the next request, or ``None`` when pagination is complete."""
