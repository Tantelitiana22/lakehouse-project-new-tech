"""Tests for the generic native ingestor: pagination, links and retries."""

from __future__ import annotations

import json

import pytest

from ndr_lakehouse.asset_property.schema_validator import parse_asset_config
from ndr_lakehouse.landing import (
    NativeIngestorResource,
    dig_json_path,
    parse_link_header,
)


def _resource() -> NativeIngestorResource:
    return NativeIngestorResource(
        endpoint_url="http://rustfs:9000",
        access_key_id="rustfsadmin",
        secret_access_key="rustfssecret",
        region="us-east-1",
        landing_bucket="landing-zone",
        max_retries=2,
        backoff_base_seconds=0.1,
    )


def _response(mocker, payload, *, status_code=200, headers=None):
    response = mocker.Mock(name="http-response")
    response.status_code = status_code
    response.headers = headers or {}
    response.content = json.dumps(payload).encode("utf-8")
    response.json.return_value = payload
    response.raise_for_status = mocker.Mock()
    return response


def _patch_http(mocker, responses):
    client = mocker.MagicMock(name="httpx-client")
    client.request.side_effect = list(responses)
    mocker.patch("httpx.Client", return_value=client)
    return client


def _config(asset_payload_factory, pagination, *, records_path=None):
    source_overrides = {}
    if records_path:
        source_overrides["records_json_path"] = records_path
    payload = asset_payload_factory(
        pagination=pagination,
        source_overrides=source_overrides,
        catalog_overrides={"write_mode": "overwrite"},
    )
    return parse_asset_config(payload)[0]


def _keys(mock_s3):
    return [call.kwargs["Key"] for call in mock_s3.put_object.call_args_list]


def test_parse_link_header_rfc5988():
    header = '<https://api.test/records?page=2>; rel="next", ' '<https://api.test/records?page=9>; rel="last"'
    assert parse_link_header(header) == "https://api.test/records?page=2"
    assert parse_link_header(None) is None
    assert parse_link_header('<https://api.test/records?page=1>; rel="prev"') is None


def test_dig_json_path_supports_dots_and_indexes():
    payload = {"data": {"items": [{"id": 1}, {"id": 2}]}}
    assert dig_json_path(payload, "data.items.1.id") == 2
    assert dig_json_path(payload, "missing.path") is None


def test_offset_pagination_writes_part_files(mocker, mock_s3, asset_payload_factory, ingest_date):
    config = _config(
        asset_payload_factory,
        {
            "strategy": "offset",
            "offset_param": "offset",
            "limit_param": "limit",
            "limit_value": 2,
            "records_json_path": "records",
        },
        records_path="records",
    )
    client = _patch_http(
        mocker,
        [
            _response(mocker, {"records": [{"id": 1}, {"id": 2}]}),
            _response(mocker, {"records": [{"id": 3}]}),
        ],
    )

    uri = _resource().ingest(config, ingest_date=ingest_date)

    assert uri == "s3a://landing-zone/raw/demo/ingest_date=2026-01-01"
    assert client.request.call_count == 2
    assert client.request.call_args_list[0].kwargs["params"]["offset"] == "0"
    assert client.request.call_args_list[1].kwargs["params"]["offset"] == "2"
    assert _keys(mock_s3) == [
        "raw/demo/ingest_date=2026-01-01/part_00001.json",
        "raw/demo/ingest_date=2026-01-01/part_00002.json",
    ]


def test_cursor_pagination_follows_next_cursor(mocker, mock_s3, asset_payload_factory, ingest_date):
    config = _config(
        asset_payload_factory,
        {
            "strategy": "cursor",
            "cursor_param": "cursor",
            "next_cursor_json_path": "meta.next_cursor",
            "records_json_path": "items",
        },
        records_path="items",
    )
    client = _patch_http(
        mocker,
        [
            _response(
                mocker,
                {"items": [{"id": 1}], "meta": {"next_cursor": "cursor-2"}},
            ),
            _response(
                mocker,
                {"items": [{"id": 2}], "meta": {"next_cursor": None}},
            ),
        ],
    )

    _resource().ingest(config, ingest_date=ingest_date)

    assert client.request.call_count == 2
    assert "cursor" not in client.request.call_args_list[0].kwargs["params"]
    assert client.request.call_args_list[1].kwargs["params"]["cursor"] == "cursor-2"
    assert _keys(mock_s3) == [
        "raw/demo/ingest_date=2026-01-01/part_00001.json",
        "raw/demo/ingest_date=2026-01-01/part_00002.json",
    ]


def test_link_header_pagination_follows_next_link(mocker, mock_s3, asset_payload_factory, ingest_date):
    config = _config(
        asset_payload_factory,
        {"strategy": "link_header", "records_json_path": "records"},
        records_path="records",
    )
    client = _patch_http(
        mocker,
        [
            _response(
                mocker,
                {"records": [{"id": 1}]},
                headers={"Link": '<https://api.example.test/records?page=2>; rel="next"'},
            ),
            _response(mocker, {"records": [{"id": 2}]}),
        ],
    )

    _resource().ingest(config, ingest_date=ingest_date)

    assert client.request.call_count == 2
    assert client.request.call_args_list[1].args[1] == "https://api.example.test/records?page=2"
    assert len(_keys(mock_s3)) == 2


def test_429_triggers_exponential_backoff(mocker, mock_s3, asset_payload_factory, ingest_date):
    config = _config(
        asset_payload_factory,
        {
            "strategy": "offset",
            "offset_param": "offset",
            "limit_param": "limit",
            "limit_value": 10,
            "records_json_path": "records",
        },
        records_path="records",
    )
    _patch_http(
        mocker,
        [
            _response(mocker, {"records": []}, status_code=429),
            _response(mocker, {"records": [{"id": 1}]}),
        ],
    )
    sleep = mocker.patch("time.sleep")

    _resource().ingest(config, ingest_date=ingest_date)

    assert sleep.call_count == 1
    assert sleep.call_args.args[0] == pytest.approx(0.1)
    assert len(_keys(mock_s3)) == 1


def test_pagination_stops_at_max_pages(mocker, mock_s3, asset_payload_factory, ingest_date):
    config = _config(
        asset_payload_factory,
        {
            "strategy": "page_number",
            "page_param": "page",
            "limit_param": "limit",
            "limit_value": 1,
            "records_json_path": "records",
            "max_pages": 2,
        },
        records_path="records",
    )
    _patch_http(
        mocker,
        [
            _response(mocker, {"records": [{"id": 1}]}),
            _response(mocker, {"records": [{"id": 2}]}),
            _response(mocker, {"records": [{"id": 3}]}),
        ],
    )

    _resource().ingest(config, ingest_date=ingest_date)

    assert len(_keys(mock_s3)) == 2


def test_single_stream_preserves_native_csv_gz_bytes(mocker, mock_s3, asset_payload_factory, ingest_date):
    payload = asset_payload_factory(
        source_overrides={
            "url": "https://api.example.test/data.csv.gz",
            "native_format": "csv.gz",
            "pagination": {"strategy": "none"},
        },
        catalog_overrides={"write_mode": "overwrite"},
    )
    config = parse_asset_config(payload)[0]

    response = mocker.MagicMock(name="stream-response")
    response.status_code = 200
    response.headers = {}
    response.iter_bytes.return_value = iter([b"a,b\n", b"1,2\n"])
    response.raise_for_status = mocker.Mock()
    context_manager = mocker.MagicMock()
    context_manager.__enter__.return_value = response
    context_manager.__exit__.return_value = False
    client = mocker.MagicMock(name="httpx-client")
    client.stream.return_value = context_manager
    mocker.patch("httpx.Client", return_value=client)

    _resource().ingest(config, ingest_date=ingest_date)

    mock_s3.put_object.assert_called_once()
    call = mock_s3.put_object.call_args.kwargs
    assert call["Key"] == "raw/demo/ingest_date=2026-01-01/data.csv.gz"
    assert call["Body"] == b"a,b\n1,2\n"
    assert call["ContentType"] == "application/gzip"


def test_s3_source_is_copied_without_content_transformation(mock_s3, asset_payload_factory, ingest_date):
    payload = asset_payload_factory(
        source_overrides={
            "type": "s3",
            "url": "s3://upstream-bucket/path/source.parquet",
            "native_format": "parquet",
            "pagination": {"strategy": "none"},
        },
        catalog_overrides={"write_mode": "overwrite"},
    )
    config = parse_asset_config(payload)[0]

    uri = _resource().ingest(config, ingest_date=ingest_date)

    assert uri == "s3a://landing-zone/raw/demo/ingest_date=2026-01-01"
    mock_s3.copy_object.assert_called_once_with(
        Bucket="landing-zone",
        Key="raw/demo/ingest_date=2026-01-01/source.parquet",
        CopySource={"Bucket": "upstream-bucket", "Key": "path/source.parquet"},
    )


def test_next_url_pagination_follows_payload_link(mocker, mock_s3, asset_payload_factory, ingest_date):
    config = _config(
        asset_payload_factory,
        {
            "strategy": "next_url",
            "next_url_json_path": "links.next",
            "records_json_path": "records",
        },
        records_path="records",
    )
    client = _patch_http(
        mocker,
        [
            _response(
                mocker,
                {"records": [{"id": 1}], "links": {"next": "/records?page=2"}},
            ),
            _response(mocker, {"records": [{"id": 2}]}),
        ],
    )

    _resource().ingest(config, ingest_date=ingest_date)

    assert client.request.call_count == 2
    assert client.request.call_args_list[1].args[1] == "https://api.example.test/records?page=2"
