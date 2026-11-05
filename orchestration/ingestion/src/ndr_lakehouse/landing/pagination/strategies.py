"""Concrete pagination strategies."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urljoin

from ndr_lakehouse.asset_property.schema_validator import SourceProperties
from ndr_lakehouse.landing.pagination.base import PaginationStrategy
from ndr_lakehouse.landing.pagination.models import PageRequest
from ndr_lakehouse.landing.utils import dig_json_path, parse_link_header


def _record_count(records: Any) -> int:
    return len(records) if isinstance(records, (list, tuple)) else 0


class OffsetPagination(PaginationStrategy):
    def first_request(self, source: SourceProperties) -> PageRequest:
        params = dict(source.params)
        params[self.config.offset_param] = str(self.config.offset_start)
        params[self.config.limit_param] = str(self.config.limit_value)
        return PageRequest(url=source.url, params=params)

    def next_request(
        self,
        current: PageRequest,
        page_number: int,
        payload: Any,
        headers: Mapping[str, str],
        records: Any,
    ) -> PageRequest | None:
        if self.should_stop(records):
            return None
        params = dict(current.params or {})
        current_offset = int(params.get(self.config.offset_param, self.config.offset_start))
        increment = self.config.limit_value or _record_count(records) or 1
        params[self.config.offset_param] = str(current_offset + increment)
        params[self.config.limit_param] = str(self.config.limit_value)
        return PageRequest(url=current.url, params=params)


class PageNumberPagination(PaginationStrategy):
    def first_request(self, source: SourceProperties) -> PageRequest:
        params = dict(source.params)
        params[self.config.page_param] = str(self.config.page_start)
        params[self.config.limit_param] = str(self.config.limit_value)
        return PageRequest(url=source.url, params=params)

    def next_request(
        self,
        current: PageRequest,
        page_number: int,
        payload: Any,
        headers: Mapping[str, str],
        records: Any,
    ) -> PageRequest | None:
        if self.should_stop(records):
            return None
        params = dict(current.params or {})
        params[self.config.page_param] = str(self.config.page_start + page_number)
        params[self.config.limit_param] = str(self.config.limit_value)
        return PageRequest(url=current.url, params=params)


class CursorPagination(PaginationStrategy):
    def first_request(self, source: SourceProperties) -> PageRequest:
        params = dict(source.params)
        if self.config.initial_cursor is not None:
            params[self.config.cursor_param] = self.config.initial_cursor
        return PageRequest(url=source.url, params=params)

    def next_request(
        self,
        current: PageRequest,
        page_number: int,
        payload: Any,
        headers: Mapping[str, str],
        records: Any,
    ) -> PageRequest | None:
        if self.should_stop(records):
            return None
        next_cursor = dig_json_path(payload, self.config.next_cursor_json_path)
        if not next_cursor:
            return None
        params = dict(current.params or {})
        if next_cursor == params.get(self.config.cursor_param):
            return None
        params[self.config.cursor_param] = str(next_cursor)
        return PageRequest(url=current.url, params=params)


class NextUrlPagination(PaginationStrategy):
    def next_request(
        self,
        current: PageRequest,
        page_number: int,
        payload: Any,
        headers: Mapping[str, str],
        records: Any,
    ) -> PageRequest | None:
        if self.should_stop(records):
            return None
        next_url = dig_json_path(payload, self.config.next_url_json_path)
        if not next_url:
            return None
        return PageRequest(url=urljoin(current.url, str(next_url)), params=None)


class LinkHeaderPagination(PaginationStrategy):
    def next_request(
        self,
        current: PageRequest,
        page_number: int,
        payload: Any,
        headers: Mapping[str, str],
        records: Any,
    ) -> PageRequest | None:
        if self.should_stop(records):
            return None
        next_url = parse_link_header(headers.get("Link"))
        if not next_url:
            return None
        return PageRequest(url=next_url, params=None)
