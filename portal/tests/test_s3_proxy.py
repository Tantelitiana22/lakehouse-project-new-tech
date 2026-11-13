"""Tests for the scoped S3 SigV4 proxy using real botocore SigV4 signatures."""

from __future__ import annotations

import asyncio
import io
import xml.etree.ElementTree as ET
from functools import singledispatch
from urllib.parse import urlparse

import boto3
import httpx
import pytest
from botocore.auth import S3SigV4Auth, S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest, AWSResponse
from botocore.config import Config as BotoConfig
from botocore.credentials import Credentials
from botocore.exceptions import ClientError
from sqlalchemy.orm import Session
from starlette.applications import Starlette
from starlette.routing import Route

from lakehouse_portal.db import (
    create_portal_engine,
    create_s3_key,
    create_user,
    get_user_by_username,
    init_db,
    set_bucket_permission,
)
from lakehouse_portal.s3_client import InMemoryS3Service
from lakehouse_portal.s3_proxy import handle_s3_proxy_request


def _make_proxy_app(engine, s3_service):
    async def endpoint(request):
        return await handle_s3_proxy_request(request, engine, s3_service)

    return Starlette(
        routes=[
            Route("/s3", endpoint, methods=["GET", "POST", "PUT", "DELETE", "HEAD"]),
            Route("/s3/{path:path}", endpoint, methods=["GET", "POST", "PUT", "DELETE", "HEAD"]),
        ]
    )


@singledispatch
def _decode_header_value(value: object) -> str:
    return str(value)


@_decode_header_value.register
def _(value: bytes) -> str:
    return value.decode("utf-8")


@singledispatch
def _read_request_body(_body: object) -> bytes:
    return b""


@_read_request_body.register
def _(body: bytes) -> bytes:
    return body


@_read_request_body.register
def _(body: io.IOBase) -> bytes:
    return bytes(body.read() or b"")


def _sign_request(
    method: str,
    url: str,
    access_key: str,
    secret_key: str,
    body: bytes = b"",
    *,
    query_auth: bool = False,
) -> tuple[str, dict[str, str]]:
    aws_req = AWSRequest(method=method, url=url, data=body)
    creds = Credentials(access_key, secret_key)
    signer = {
        True: lambda: S3SigV4QueryAuth(creds, "s3", "us-east-1", expires=60),
        False: lambda: S3SigV4Auth(creds, "s3", "us-east-1"),
    }[query_auth]()
    signer.add_auth(aws_req)
    return aws_req.url, {k: _decode_header_value(v) for k, v in aws_req.headers.items()}


def _make_boto3_client(app, access_key: str, secret_key: str):
    """Create a real boto3 S3 client wired synchronously to our ASGI S3 proxy app."""
    transport = httpx.ASGITransport(app=app)

    client = boto3.client(
        "s3",
        endpoint_url="http://s3.localhost:8088",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=BotoConfig(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )

    async def _send_async(method: str, path: str, headers: dict[str, str], body: bytes) -> httpx.Response:
        async with httpx.AsyncClient(transport=transport, base_url="http://s3.localhost:8088") as async_client:
            return await async_client.request(method, path, headers=headers, content=body)

    class _RawBytes(io.BytesIO):
        def stream(self, amt=None, decode_content=None):
            yield self.read()

    def _custom_send(request, **kwargs):
        parsed = urlparse(request.url)
        normalized_path = "/" + parsed.path.lstrip("/")
        base_path = f"/s3{normalized_path}"
        path = (parsed.query and f"{base_path}?{parsed.query}") or base_path
        headers = {k: _decode_header_value(v) for k, v in request.headers.items()}
        body_bytes = _read_request_body(request.body)
        resp = asyncio.run(_send_async(request.method, path, headers, body_bytes))
        return AWSResponse(request.url, resp.status_code, resp.headers, _RawBytes(resp.content))

    client._endpoint.http_session.send = _custom_send  # noqa: SLF001
    return client


@pytest.mark.anyio
async def test_s3_proxy_auth_failures_and_bucket_scoping():
    # Given
    engine = create_portal_engine("sqlite:///:memory:")
    init_db(engine, "admin", "adminpass")
    s3 = InMemoryS3Service(["lakehouse", "landing-zone", "secret-bucket"])
    s3.put_object("lakehouse", "bronze/test.csv", b"a,b\n1,2\n", "text/csv")

    with Session(engine, expire_on_commit=False) as session:
        admin = get_user_by_username(session, "admin")
        assert admin is not None
        admin_key = create_s3_key(session, admin.id, "admin-cli")
        admin_ak, admin_sk = admin_key.access_key_id, admin_key.secret_access_key

        alice = create_user(session, "alice", "pw")
        set_bucket_permission(session, alice.id, "lakehouse", "read")
        set_bucket_permission(session, alice.id, "landing-zone", "read_write")
        alice_key = create_s3_key(session, alice.id, "alice-cli")
        alice_ak, alice_sk = alice_key.access_key_id, alice_key.secret_access_key

    app = _make_proxy_app(engine, s3)
    transport = httpx.ASGITransport(app=app)

    # When / # Then
    async with httpx.AsyncClient(transport=transport, base_url="http://s3.localhost:8088") as client:
        # 1. Missing auth
        r_no_auth = await client.get("/s3/")
        assert r_no_auth.status_code == 403
        assert "<Code>AccessDenied</Code>" in r_no_auth.text

        # 2. Unknown access key
        _, headers = _sign_request("GET", "http://s3.localhost:8088/", "LHAKUNKNOWN", "badsecret")
        r_bad_key = await client.get("/s3/", headers=headers)
        assert r_bad_key.status_code == 403
        assert "<Code>InvalidAccessKeyId</Code>" in r_bad_key.text

        # 3. Bad signature
        _, bad_sig_headers = _sign_request("GET", "http://s3.localhost:8088/", alice_key.access_key_id, "wrong-secret")
        r_bad_sig = await client.get("/s3/", headers=bad_sig_headers)
        assert r_bad_sig.status_code == 403
        assert "<Code>SignatureDoesNotMatch</Code>" in r_bad_sig.text

        # 4. ListBuckets filtered to alice's permitted buckets (excludes secret-bucket)
        _, ok_headers = _sign_request(
            "GET", "http://s3.localhost:8088/", alice_key.access_key_id, alice_key.secret_access_key
        )
        r_list = await client.get("/s3/", headers=ok_headers)
        assert r_list.status_code == 200
        root = ET.fromstring(r_list.text)
        names = [el.text for el in root.findall(".//{http://s3.amazonaws.com/doc/2006-03-01/}Name")]
        assert names == ["lakehouse", "landing-zone"]

        # 5. Read-only bucket operations on lakehouse
        _, h_list_obj = _sign_request(
            "GET",
            "http://s3.localhost:8088/lakehouse?list-type=2&prefix=bronze%2F",
            alice_key.access_key_id,
            alice_key.secret_access_key,
        )
        r_objs = await client.get("/s3/lakehouse?list-type=2&prefix=bronze%2F", headers=h_list_obj)
        assert r_objs.status_code == 200
        assert "bronze/test.csv" in r_objs.text

        _, h_get = _sign_request(
            "GET",
            "http://s3.localhost:8088/lakehouse/bronze/test.csv",
            alice_key.access_key_id,
            alice_key.secret_access_key,
        )
        r_get = await client.get("/s3/lakehouse/bronze/test.csv", headers=h_get)
        assert r_get.status_code == 200
        assert r_get.content == b"a,b\n1,2\n"

        _, h_head = _sign_request(
            "HEAD",
            "http://s3.localhost:8088/lakehouse/bronze/test.csv",
            alice_key.access_key_id,
            alice_key.secret_access_key,
        )
        r_head = await client.head("/s3/lakehouse/bronze/test.csv", headers=h_head)
        assert r_head.status_code == 200
        assert r_head.headers["content-length"] == "8"

        # Write denied on read-only bucket
        _, h_put_ro = _sign_request(
            "PUT",
            "http://s3.localhost:8088/lakehouse/bronze/blocked.csv",
            alice_key.access_key_id,
            alice_key.secret_access_key,
            body=b"nope",
        )
        r_put_ro = await client.put("/s3/lakehouse/bronze/blocked.csv", content=b"nope", headers=h_put_ro)
        assert r_put_ro.status_code == 403
        assert "<Code>AccessDenied</Code>" in r_put_ro.text

        # Unpermitted bucket denied
        _, h_secret = _sign_request(
            "GET",
            "http://s3.localhost:8088/secret-bucket?list-type=2",
            alice_key.access_key_id,
            alice_key.secret_access_key,
        )
        r_secret = await client.get("/s3/secret-bucket?list-type=2", headers=h_secret)
        assert r_secret.status_code == 403

        # 6. Read-write bucket operations on landing-zone
        _, h_put_rw = _sign_request(
            "PUT",
            "http://s3.localhost:8088/landing-zone/inbox/hello.txt",
            alice_key.access_key_id,
            alice_key.secret_access_key,
            body=b"hello world",
        )
        r_put_rw = await client.put("/s3/landing-zone/inbox/hello.txt", content=b"hello world", headers=h_put_rw)
        assert r_put_rw.status_code == 200
        assert s3.get_object("landing-zone", "inbox/hello.txt")[0] == b"hello world"

        _, h_del = _sign_request(
            "DELETE",
            "http://s3.localhost:8088/landing-zone/inbox/hello.txt",
            alice_key.access_key_id,
            alice_key.secret_access_key,
        )
        r_del = await client.delete("/s3/landing-zone/inbox/hello.txt", headers=h_del)
        assert r_del.status_code == 204

        # Batch delete
        s3.put_object("landing-zone", "a.txt", b"1")
        s3.put_object("landing-zone", "b.txt", b"2")
        batch_xml = b"<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object></Delete>"
        _, h_batch = _sign_request(
            "POST",
            "http://s3.localhost:8088/landing-zone?delete=",
            alice_key.access_key_id,
            alice_key.secret_access_key,
            body=batch_xml,
        )
        r_batch = await client.post("/s3/landing-zone?delete=", content=batch_xml, headers=h_batch)
        assert r_batch.status_code == 200
        assert "<Deleted><Key>a.txt</Key></Deleted>" in r_batch.text

        # Non-admin cannot create bucket
        _, h_mk_user = _sign_request(
            "PUT",
            "http://s3.localhost:8088/new-bucket",
            alice_key.access_key_id,
            alice_key.secret_access_key,
        )
        r_mk_user = await client.put("/s3/new-bucket", headers=h_mk_user)
        assert r_mk_user.status_code == 403

        # 7. Admin can create bucket
        _, h_mk_admin = _sign_request(
            "PUT",
            "http://s3.localhost:8088/new-bucket",
            admin_ak,
            admin_sk,
        )
        r_mk_admin = await client.put("/s3/new-bucket", headers=h_mk_admin)
        assert r_mk_admin.status_code == 200
        assert s3.bucket_exists("new-bucket")

        # 8. Presigned query auth
        presigned_url, h_presigned = _sign_request(
            "GET",
            "http://s3.localhost:8088/lakehouse/bronze/test.csv",
            alice_ak,
            alice_sk,
            query_auth=True,
        )
        qs = presigned_url.split("?", 1)[1]
        r_presigned = await client.get(f"/s3/lakehouse/bronze/test.csv?{qs}", headers=h_presigned)
        assert r_presigned.status_code == 200
        assert r_presigned.content == b"a,b\n1,2\n"


def test_real_boto3_client_against_s3_proxy():
    # Given
    engine = create_portal_engine("sqlite:///:memory:")
    init_db(engine, "admin", "adminpass")
    s3 = InMemoryS3Service(["lakehouse", "landing-zone", "private"])
    s3.put_object("lakehouse", "gold/kpi.parquet", b"PAR1", "application/octet-stream")

    with Session(engine) as session:
        bob = create_user(session, "bob", "pw")
        set_bucket_permission(session, bob.id, "lakehouse", "read")
        set_bucket_permission(session, bob.id, "landing-zone", "read_write")
        bob_key = create_s3_key(session, bob.id, "boto3")

    app = _make_proxy_app(engine, s3)
    boto = _make_boto3_client(app, bob_key.access_key_id, bob_key.secret_access_key)

    # When
    buckets = [b["Name"] for b in boto.list_buckets()["Buckets"]]
    objs = [o["Key"] for o in boto.list_objects_v2(Bucket="lakehouse", Prefix="gold/")["Contents"]]
    fetched = boto.get_object(Bucket="lakehouse", Key="gold/kpi.parquet")["Body"].read()
    boto.put_object(Bucket="landing-zone", Key="raw/item.json", Body=b'{"ok":true}')
    uploaded_obj = s3.get_object("landing-zone", "raw/item.json")[0]
    boto.delete_objects(Bucket="landing-zone", Delete={"Objects": [{"Key": "raw/item.json"}]})

    # Then
    assert buckets == ["lakehouse", "landing-zone"]
    assert objs == ["gold/kpi.parquet"]
    assert fetched == b"PAR1"
    assert uploaded_obj == b'{"ok":true}'
    with pytest.raises(ClientError) as exc_ro:
        boto.put_object(Bucket="lakehouse", Key="gold/forbidden.txt", Body=b"x")
    assert exc_ro.value.response["Error"]["Code"] == "AccessDenied"
