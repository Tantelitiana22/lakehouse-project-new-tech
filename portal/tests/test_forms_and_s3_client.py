"""Tests for session tokens, stdlib form parsing, and S3 storage service."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest
from botocore.exceptions import ClientError
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from lakehouse_portal.auth import create_session_token, is_safe_redirect_url, verify_session_token
from lakehouse_portal.forms import parse_request_form
from lakehouse_portal.s3_client import Boto3S3Service, InMemoryS3Service, S3StorageError


def test_session_and_handoff_tokens():
    secret = "super-secret"
    token = create_session_token(42, "alice", secret, ttl_seconds=60, purpose="session")
    payload = verify_session_token(token, secret, expected_purpose="session")
    assert payload is not None
    assert payload["uid"] == 42
    assert payload["sub"] == "alice"

    # Wrong purpose
    assert verify_session_token(token, secret, expected_purpose="handoff") is None
    # Tampered signature
    assert verify_session_token(token + "x", secret) is None
    # Wrong secret
    assert verify_session_token(token, "other") is None
    # Expired token
    expired = create_session_token(42, "alice", secret, ttl_seconds=-10)
    assert verify_session_token(expired, secret) is None
    # Malformed token
    assert verify_session_token(None, secret) is None
    assert verify_session_token("not-a-token", secret) is None


def test_safe_redirect_url():
    assert is_safe_redirect_url("/buckets", "localhost", 8088) is True
    assert is_safe_redirect_url("http://dagster.localhost:8088/runs", "localhost", 8088) is True
    assert is_safe_redirect_url("http://localhost:8088/", "localhost", 8088) is True
    assert is_safe_redirect_url("//evil.com/phish", "localhost", 8088) is False
    assert is_safe_redirect_url("https://evil.com/phish", "localhost", 8088) is False
    assert is_safe_redirect_url("http://dagster.localhost:9999/", "localhost", 8088) is False
    assert is_safe_redirect_url("javascript:alert(1)", "localhost", 8088) is False


@pytest.mark.anyio
async def test_parse_request_form_urlencoded_and_multipart():
    async def endpoint(request):
        parsed = await parse_request_form(request)
        return JSONResponse(
            {
                "fields": parsed.fields,
                "files": {k: [v[0], v[1].decode("utf-8"), v[2]] for k, v in parsed.files.items()},
            }
        )

    app = Starlette(routes=[Route("/submit", endpoint, methods=["POST"])])
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp_url = await client.post("/submit", data={"username": "alice", "role": "read"})
        assert resp_url.status_code == 200
        assert resp_url.json()["fields"] == {"username": "alice", "role": "read"}

        resp_mp = await client.post(
            "/submit",
            data={"prefix": "raw/"},
            files={"file": ("sample.csv", b"col1,col2\n1,2\n", "text/csv")},
        )
        assert resp_mp.status_code == 200
        body = resp_mp.json()
        assert body["fields"] == {"prefix": "raw/"}
        assert body["files"]["file"] == ["sample.csv", "col1,col2\n1,2\n", "text/csv"]

        resp_empty = await client.post("/submit", content=b"{}", headers={"content-type": "application/json"})
        assert resp_empty.json() == {"fields": {}, "files": {}}


def test_in_memory_s3_service_operations():
    s3 = InMemoryS3Service(["lakehouse"])
    assert s3.list_buckets() == ["lakehouse"]
    assert s3.bucket_exists("lakehouse") is True
    assert s3.bucket_exists("missing") is False

    s3.create_bucket("landing-zone")
    s3.put_object("landing-zone", "raw/2026/data.csv", b"id,val\n1,a\n", "text/csv")
    s3.put_object("landing-zone", "readme.txt", b"hello", "text/plain")

    root_list = s3.list_objects("landing-zone", prefix="", delimiter="/")
    assert root_list.prefixes == ["raw/"]
    assert [o.key for o in root_list.objects] == ["readme.txt"]

    nested_list = s3.list_objects("landing-zone", prefix="raw/2026/", delimiter="/")
    assert [o.key for o in nested_list.objects] == ["raw/2026/data.csv"]

    data, ct = s3.get_object("landing-zone", "readme.txt")
    assert data == b"hello"
    assert ct == "text/plain"
    size, head_ct, etag = s3.head_object("landing-zone", "readme.txt")
    assert size == 5
    assert head_ct == "text/plain"
    assert etag

    s3.delete_object("landing-zone", "readme.txt")
    with pytest.raises(S3StorageError) as exc_info:
        s3.get_object("landing-zone", "readme.txt")
    assert exc_info.value.code == "NoSuchKey"

    with pytest.raises(S3StorageError) as exc_bucket:
        s3.list_objects("nope")
    assert exc_bucket.value.code == "NoSuchBucket"


def test_boto3_s3_service_wrapper(monkeypatch):
    mock_client = MagicMock()
    mock_client.list_buckets.return_value = {"Buckets": [{"Name": "b2"}, {"Name": "b1"}]}
    mock_client.list_objects_v2.return_value = {
        "CommonPrefixes": [{"Prefix": "sub/"}],
        "Contents": [],
    }
    monkeypatch.setattr("lakehouse_portal.s3_client.boto3.client", lambda *a, **kw: mock_client)

    svc = Boto3S3Service("http://rustfs:9000", "ak", "sk")
    assert svc.list_buckets() == ["b1", "b2"]
    assert svc.bucket_exists("b1") is True
    assert svc.list_objects("b1").prefixes == ["sub/"]

    mock_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
        "HeadBucket",
    )
    assert svc.bucket_exists("missing") is False
