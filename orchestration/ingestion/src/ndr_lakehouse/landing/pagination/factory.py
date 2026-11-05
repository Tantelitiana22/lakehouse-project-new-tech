"""Pagination strategy registry (Factory + Open/Closed Principle)."""

from __future__ import annotations

from collections.abc import Callable

from ndr_lakehouse.asset_property.schema_validator import PaginationConfig
from ndr_lakehouse.domain.errors import UnsupportedPaginationStrategyError
from ndr_lakehouse.landing.pagination.base import PaginationStrategy
from ndr_lakehouse.landing.pagination.strategies import (
    CursorPagination,
    LinkHeaderPagination,
    NextUrlPagination,
    OffsetPagination,
    PageNumberPagination,
)

StrategyBuilder = Callable[[PaginationConfig], PaginationStrategy]


class PaginationFactory:
    """Create strategies by name; new strategies can be registered externally."""

    def __init__(self) -> None:
        self._builders: dict[str, StrategyBuilder] = {}

    def register(self, name: str, builder: StrategyBuilder) -> None:
        self._builders[name] = builder

    def create(self, config: PaginationConfig) -> PaginationStrategy:
        try:
            builder = self._builders[config.strategy]
        except KeyError as exc:
            raise UnsupportedPaginationStrategyError(
                f"unsupported pagination strategy: {config.strategy!r}"
            ) from exc
        return builder(config)

    @classmethod
    def default(cls) -> PaginationFactory:
        factory = cls()
        factory.register("offset", OffsetPagination)
        factory.register("page_number", PageNumberPagination)
        factory.register("cursor", CursorPagination)
        factory.register("next_url", NextUrlPagination)
        factory.register("link_header", LinkHeaderPagination)
        return factory
