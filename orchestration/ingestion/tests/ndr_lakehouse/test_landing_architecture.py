"""Tests for the landing strategy/factory architecture and provisioning use case."""

from __future__ import annotations

from datetime import datetime

import pytest

from ndr_lakehouse.asset_property.schema_validator import parse_asset_config
from ndr_lakehouse.domain.errors import (
    UnsupportedPaginationStrategyError,
    UnsupportedSourceTypeError,
)
from ndr_lakehouse.landing.factory import ExtractorFactory
from ndr_lakehouse.landing.pagination import (
    CursorPagination,
    LinkHeaderPagination,
    OffsetPagination,
    PageNumberPagination,
    PageRequest,
    PaginationFactory,
)
from ndr_lakehouse.landing.retry import RetryPolicy
from ndr_lakehouse.landing.service import normalise_date
from ndr_lakehouse.provisioning.buckets import BucketProvisioner
from ndr_lakehouse.provisioning.service import ProvisioningService
from ndr_lakehouse.provisioning.unity_catalog import NamespaceProvisioner


def _source(asset_payload_factory, pagination):
    payload = asset_payload_factory(pagination=pagination)
    return parse_asset_config(payload)[0].source_properties


@pytest.mark.parametrize(
    ("strategy", "pagination"),
    [
        (
            "offset",
            {
                "strategy": "offset",
                "offset_param": "offset",
                "limit_param": "limit",
                "limit_value": 10,
            },
        ),
        (
            "page_number",
            {
                "strategy": "page_number",
                "page_param": "page",
                "limit_param": "limit",
                "limit_value": 10,
            },
        ),
        (
            "cursor",
            {
                "strategy": "cursor",
                "cursor_param": "cursor",
                "next_cursor_json_path": "meta.next",
            },
        ),
        ("link_header", {"strategy": "link_header"}),
    ],
)
def test_pagination_factory_creates_each_strategy(asset_payload_factory, strategy: str, pagination: dict):
    # Given
    factory = PaginationFactory.default()
    source = _source(asset_payload_factory, pagination)

    # When
    created = factory.create(source.pagination)

    # Then
    assert created.__class__.__name__.lower().startswith(strategy.split("_")[0])


def test_pagination_factory_rejects_unknown_strategy(asset_payload_factory):
    # Given
    source = _source(asset_payload_factory, {"strategy": "none"})

    # When / # Then
    with pytest.raises(UnsupportedPaginationStrategyError):
        PaginationFactory.default().create(source.pagination)


def test_offset_strategy_builds_next_request(asset_payload_factory):
    # Given
    source = _source(
        asset_payload_factory,
        {
            "strategy": "offset",
            "offset_param": "offset",
            "limit_param": "limit",
            "limit_value": 2,
        },
    )
    strategy = OffsetPagination(source.pagination)

    # When
    first = strategy.first_request(source)
    second = strategy.next_request(first, 1, {}, {}, [{"id": 1}, {"id": 2}])
    empty_next = strategy.next_request(first, 1, {}, {}, [])

    # Then
    assert first.params == {"limit": "2", "offset": "0"}
    assert second == PageRequest(url=source.url, params={"limit": "2", "offset": "2"})
    assert empty_next is None


def test_page_number_strategy_increments_page(asset_payload_factory):
    # Given
    source = _source(
        asset_payload_factory,
        {
            "strategy": "page_number",
            "page_param": "page",
            "limit_param": "limit",
            "limit_value": 5,
        },
    )
    strategy = PageNumberPagination(source.pagination)

    # When
    first = strategy.first_request(source)
    second = strategy.next_request(first, 1, {}, {}, [{}] * 5)

    # Then
    assert first.params["page"] == "1"
    assert second.params["page"] == "2"


def test_cursor_strategy_stops_without_next_cursor(asset_payload_factory):
    # Given
    source = _source(
        asset_payload_factory,
        {
            "strategy": "cursor",
            "cursor_param": "cursor",
            "next_cursor_json_path": "meta.next",
        },
    )
    strategy = CursorPagination(source.pagination)

    # When
    first = strategy.first_request(source)
    stop_request = strategy.next_request(first, 1, {"meta": {}}, {}, [{}])
    next_request = strategy.next_request(first, 1, {"meta": {"next": "abc"}}, {}, [{}])

    # Then
    assert "cursor" not in first.params
    assert stop_request is None
    assert next_request.params["cursor"] == "abc"


def test_link_header_strategy_extracts_next_url(asset_payload_factory):
    # Given
    source = _source(asset_payload_factory, {"strategy": "link_header"})
    strategy = LinkHeaderPagination(source.pagination)
    first = strategy.first_request(source)

    # When
    request = strategy.next_request(
        first,
        1,
        {},
        {"Link": '<https://api.test/page2>; rel="next"'},
        [{}],
    )

    # Then
    assert request.url == "https://api.test/page2"
    assert request.params is None


def test_retry_policy_respects_retry_after():
    # Given
    sleeps: list[float] = []
    response = type("Response", (), {"headers": {"Retry-After": "7"}})()
    policy = RetryPolicy(max_seconds=30, sleep=sleeps.append)

    # When
    policy.sleep_for(0, response)

    # Then
    assert sleeps == [7.0]
    assert policy.delay(3) == 4.0


def test_extractor_factory_rejects_unknown_source_type():
    # Given
    factory = ExtractorFactory()

    # When / # Then
    with pytest.raises(UnsupportedSourceTypeError):
        factory.create("ftp")


def test_provisioning_service_composes_storage_and_namespaces():
    # Given
    class FakeStore:
        def __init__(self):
            self.ensured = []

        def ensure_bucket(self, bucket):
            self.ensured.append(bucket)
            return True

    class FakeGateway:
        def __init__(self):
            self.calls = []

        def catalog_exists(self, catalog_name):
            return False

        def create_catalog(self, catalog_name, storage_root, comment):
            self.calls.append(("catalog", catalog_name, storage_root))
            return True

        def schema_exists(self, catalog_name, schema_name):
            return False

        def create_schema(self, catalog_name, schema_name, comment):
            self.calls.append(("schema", catalog_name, schema_name))
            return True

        def table_exists(self, catalog_name, schema_name, table_name):
            return False

        def delete_table(self, catalog_name, schema_name, table_name):
            return False

        def create_table(
            self,
            catalog_name,
            schema_name,
            table_name,
            storage_location,
            columns,
            comment="",
            properties=None,
        ):
            self.calls.append(("table", catalog_name, schema_name, table_name))
            return True

    store = FakeStore()
    gateway = FakeGateway()
    service = ProvisioningService(
        buckets=BucketProvisioner(store, ["landing-zone", "lakehouse"]),
        namespaces=NamespaceProvisioner(gateway, "s3://lakehouse/warehouse"),
    )

    # When
    result = service.provision("lakehouse", "bronze")

    # Then
    assert result == {
        "buckets": {"landing-zone": True, "lakehouse": True},
        "catalog_created": True,
        "schema_created": True,
    }
    assert gateway.calls == [
        ("catalog", "lakehouse", "s3://lakehouse/warehouse/lakehouse"),
        ("schema", "lakehouse", "bronze"),
    ]


def test_normalise_date_accepts_datetime():
    # Given
    value = datetime(2026, 1, 1, 12, 0)

    # When
    result = normalise_date(value).isoformat()

    # Then
    assert result == "2026-01-01"
