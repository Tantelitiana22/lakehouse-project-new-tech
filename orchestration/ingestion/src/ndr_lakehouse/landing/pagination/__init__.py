"""Pagination strategy pattern for generic APIs."""

from ndr_lakehouse.landing.pagination.base import PaginationStrategy
from ndr_lakehouse.landing.pagination.factory import PaginationFactory
from ndr_lakehouse.landing.pagination.models import PageRequest
from ndr_lakehouse.landing.pagination.strategies import (
    CursorPagination,
    LinkHeaderPagination,
    NextUrlPagination,
    OffsetPagination,
    PageNumberPagination,
)

__all__ = [
    "CursorPagination",
    "LinkHeaderPagination",
    "NextUrlPagination",
    "OffsetPagination",
    "PageNumberPagination",
    "PageRequest",
    "PaginationFactory",
    "PaginationStrategy",
]
