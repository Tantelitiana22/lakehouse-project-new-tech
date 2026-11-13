"""Integration tests for the Lakehouse Access Management Portal ASGI app."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from lakehouse_portal.app import create_app
from lakehouse_portal.config import PortalConfig
from lakehouse_portal.db import create_portal_engine, get_user_by_username
from lakehouse_portal.s3_client import InMemoryS3Service


def _test_config() -> PortalConfig:
    return PortalConfig(
        database_url="sqlite:///:memory:",
        secret_key="test-portal-secret",
        admin_user="admin",
        admin_password="admin-password",
        gateway_port=8088,
        base_domain="localhost",
        s3_endpoint_url="http://rustfs:9000",
        s3_access_key_id="rustfsadmin",
        s3_secret_access_key="rustfssecret",
        s3_region="us-east-1",
    )


def _make_test_app():
    from sqlalchemy import create_engine

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    s3 = InMemoryS3Service(["lakehouse", "landing-zone"])
    s3.put_object("lakehouse", "bronze/sample.csv", b"col\n1\n", "text/csv")

    async def fake_probe(url: str) -> bool:
        return "dagster" in url or "unity-catalog" in url

    app = create_app(
        config=_test_config(),
        engine=engine,
        s3_service=s3,
        health_checker=fake_probe,
    )
    return app, engine, s3


@pytest.mark.anyio
async def test_portal_auth_sso_admin_and_bucket_rbac():
    app, engine, s3 = _make_test_app()
    transport = httpx.ASGITransport(app=app)

    # Admin client
    async with httpx.AsyncClient(
        transport=transport, base_url="http://portal.localhost:8088", follow_redirects=False
    ) as admin_client:
        # Healthz
        r_hz = await admin_client.get("/healthz")
        assert r_hz.status_code == 200
        assert r_hz.json() == {"status": "ok"}

        # Unauthenticated redirects & verify=401
        r_home_anon = await admin_client.get("/")
        assert r_home_anon.status_code == 303
        assert r_home_anon.headers["location"].startswith("/login")

        r_verify_anon = await admin_client.get("/internal/verify?service=dagster")
        assert r_verify_anon.status_code == 401

        # Bad login
        r_bad = await admin_client.post("/login", data={"username": "admin", "password": "wrong"})
        assert r_bad.status_code == 401
        assert "Invalid username or password" in r_bad.text

        # Valid admin login
        r_login = await admin_client.post(
            "/login", data={"username": "admin", "password": "admin-password", "next": "/"}
        )
        assert r_login.status_code == 303
        assert r_login.headers["location"] == "/"

        # Launchpad shows Online/Offline badges
        r_pad = await admin_client.get("/")
        assert r_pad.status_code == 200
        assert "Dagster" in r_pad.text
        assert "Online" in r_pad.text
        assert "Offline" in r_pad.text

        # SSO handoff to dagster.localhost
        r_handoff = await admin_client.get("/auth/handoff?next=http://dagster.localhost:8088/runs")
        assert r_handoff.status_code == 303
        cb_url = urlparse(r_handoff.headers["location"])
        assert cb_url.netloc == "dagster.localhost:8088"
        assert cb_url.path == "/__auth_callback"
        qs = parse_qs(cb_url.query)
        token = qs["token"][0]

        # Simulate callback on dagster.localhost subdomain
        async with httpx.AsyncClient(
            transport=transport, base_url="http://dagster.localhost:8088", follow_redirects=False
        ) as sub_client:
            r_cb = await sub_client.get(f"/__auth_callback?token={token}&next=/runs")
            assert r_cb.status_code == 303
            assert r_cb.headers["location"] == "/runs"
            r_sub_verify = await sub_client.get("/internal/verify?service=dagster")
            assert r_sub_verify.status_code == 200
            assert r_sub_verify.headers["X-Auth-User"] == "admin"
            assert r_sub_verify.headers["X-Auth-Admin"] == "true"

        # Admin creates bucket
        r_mk_bucket = await admin_client.post("/buckets", data={"bucket_name": "analytics"})
        assert r_mk_bucket.status_code == 303
        assert s3.bucket_exists("analytics")

        # Admin creates user alice and assigns permissions
        r_admin = await admin_client.get("/admin")
        assert r_admin.status_code == 200
        r_add_user = await admin_client.post("/admin/users", data={"username": "alice", "password": "alice-password"})
        assert r_add_user.status_code == 303

        with Session(engine) as session:
            alice = get_user_by_username(session, "alice")
            assert alice is not None
            alice_id = alice.id

        await admin_client.post(f"/admin/users/{alice_id}/service", data={"service_slug": "dagster", "enabled": "true"})
        await admin_client.post(f"/admin/users/{alice_id}/bucket", data={"bucket_name": "lakehouse", "level": "read"})
        await admin_client.post(
            f"/admin/users/{alice_id}/bucket", data={"bucket_name": "landing-zone", "level": "read_write"}
        )
        # Toggle active off and back on, and guard self-disable
        r_toggle_off = await admin_client.post(f"/admin/users/{alice_id}/toggle-active")
        assert r_toggle_off.status_code == 303
        r_toggle_on = await admin_client.post(f"/admin/users/{alice_id}/toggle-active")
        assert r_toggle_on.status_code == 303
        admin_user = None
        with Session(engine) as session:
            admin_user = get_user_by_username(session, "admin")
            assert admin_user is not None
            admin_id = admin_user.id
        r_self = await admin_client.post(f"/admin/users/{admin_id}/toggle-active")
        assert "Cannot+disable" in r_self.headers["location"]

    # Alice client
    async with httpx.AsyncClient(
        transport=transport, base_url="http://portal.localhost:8088", follow_redirects=False
    ) as alice_client:
        r_alice_login = await alice_client.post(
            "/login", data={"username": "alice", "password": "alice-password", "next": "/"}
        )
        assert r_alice_login.status_code == 303

        # Service verify: dagster=200, rustfs-console=403
        r_v_ok = await alice_client.get("/internal/verify?service=dagster")
        assert r_v_ok.status_code == 200
        assert r_v_ok.headers["X-Auth-User"] == "alice"
        assert r_v_ok.headers["X-Auth-Admin"] == "false"

        r_v_denied = await alice_client.get("/internal/verify?service=rustfs-console")
        assert r_v_denied.status_code == 403

        r_forbidden = await alice_client.get("/internal/forbidden?service=rustfs-console")
        assert r_forbidden.status_code == 403
        assert "RustFS Console" in r_forbidden.text

        # Admin page forbidden for alice
        r_admin_403 = await alice_client.get("/admin")
        assert r_admin_403.status_code == 403

        # Bucket explorer listing
        r_buckets = await alice_client.get("/buckets")
        assert r_buckets.status_code == 200
        assert "lakehouse" in r_buckets.text
        assert "landing-zone" in r_buckets.text
        assert "analytics" not in r_buckets.text

        # Unpermitted bucket & create bucket forbidden
        assert (await alice_client.get("/buckets/analytics")).status_code == 403
        assert (await alice_client.post("/buckets", data={"bucket_name": "hacked"})).status_code == 403

        # Read-only lakehouse: view + download OK, upload/delete forbidden
        r_lh = await alice_client.get("/buckets/lakehouse?prefix=bronze/")
        assert r_lh.status_code == 200
        assert "sample.csv" in r_lh.text

        r_dl = await alice_client.get("/buckets/lakehouse/download/bronze/sample.csv")
        assert r_dl.status_code == 200
        assert r_dl.content == b"col\n1\n"

        r_ro_up = await alice_client.post(
            "/buckets/lakehouse/upload",
            data={"prefix": ""},
            files={"file": ("bad.txt", b"x", "text/plain")},
        )
        assert r_ro_up.status_code == 403
        assert (
            await alice_client.post("/buckets/lakehouse/delete", data={"key": "bronze/sample.csv"})
        ).status_code == 403

        # Read-write landing-zone: upload + delete OK
        r_rw_up = await alice_client.post(
            "/buckets/landing-zone/upload",
            data={"prefix": "inbox/"},
            files={"file": ("notes.txt", b"hello", "text/plain")},
        )
        assert r_rw_up.status_code == 303
        assert s3.get_object("landing-zone", "inbox/notes.txt")[0] == b"hello"

        r_rw_del = await alice_client.post(
            "/buckets/landing-zone/delete",
            data={"key": "inbox/notes.txt", "prefix": "inbox/"},
        )
        assert r_rw_del.status_code == 303
        assert s3.list_objects("landing-zone", prefix="inbox/").objects == []

        # Personal S3 keys create + revoke
        r_keys = await alice_client.get("/keys")
        assert r_keys.status_code == 200
        r_key_create = await alice_client.post("/keys", data={"label": "laptop"})
        assert r_key_create.status_code == 201
        assert "LHAK" in r_key_create.text
        assert "AWS_SECRET_ACCESS_KEY=" in r_key_create.text

        with Session(engine) as session:
            alice_ref = get_user_by_username(session, "alice")
            assert alice_ref is not None
            key_id = alice_ref.s3_keys[0].id

        r_revoke = await alice_client.post(f"/keys/{key_id}/revoke")
        assert r_revoke.status_code == 303

        # Logout clears session
        r_logout = await alice_client.post("/logout")
        assert r_logout.status_code == 303
        assert (await alice_client.get("/internal/verify?service=dagster")).status_code == 401


@pytest.mark.anyio
async def test_login_page_and_cross_domain_redirect_after_login():
    app, _engine, _s3 = _make_test_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://portal.localhost:8088", follow_redirects=False
    ) as client:
        r_page = await client.get("/login?next=http://uc.localhost:8088/data")
        assert r_page.status_code == 200
        assert "http://uc.localhost:8088/data" in r_page.text

        # Open redirect blocked -> falls back to "/"
        r_evil = await client.get("/login?next=https://evil.example.com/")
        assert r_evil.status_code == 200
        assert "evil.example.com" not in r_evil.text

        # Login with valid cross-subdomain next -> issues handoff redirect
        r_login = await client.post(
            "/login",
            data={
                "username": "admin",
                "password": "admin-password",
                "next": "http://uc.localhost:8088/data",
            },
        )
        assert r_login.status_code == 303
        assert r_login.headers["location"].startswith("http://uc.localhost:8088/__auth_callback?token=")

        # Invalid handoff callback redirects to portal login
        r_bad_cb = await client.get("/__auth_callback?token=bogus&next=/")
        assert r_bad_cb.status_code == 303
        assert r_bad_cb.headers["location"] == "http://portal.localhost:8088/login"

        # create_portal_engine sqlite file url sanity check
        assert create_portal_engine("sqlite:///:memory:") is not None
