"""Tests for portal config, password hashing, and database RBAC repository."""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from lakehouse_portal.config import SERVICE_BY_SLUG, PortalConfig
from lakehouse_portal.db import (
    create_portal_engine,
    create_s3_key,
    create_user,
    get_active_s3_key,
    get_user_by_id,
    get_user_by_username,
    init_db,
    list_user_s3_keys,
    list_users,
    revoke_s3_key,
    set_bucket_permission,
    set_service_permission,
    toggle_user_active,
    user_bucket_level,
    user_has_service,
)
from lakehouse_portal.passwords import hash_password, verify_password


def test_portal_config_defaults_and_urls(monkeypatch):
    # Given
    monkeypatch.setenv("PORTAL_GATEWAY_PORT", "9099")
    monkeypatch.setenv("PORTAL_BASE_DOMAIN", "localhost")

    # When
    cfg = PortalConfig.from_env()

    # Then
    assert cfg.portal_origin == "http://portal.localhost:9099"
    assert cfg.s3_gateway_url == "http://s3.localhost:9099"
    assert cfg.service_url(SERVICE_BY_SLUG["dagster"]) == "http://dagster.localhost:9099/"


@pytest.mark.parametrize(
    ("candidate", "stored_override", "expected"),
    [
        ("s3cr3t-pass", None, True),
        ("wrong", None, False),
        ("x", "invalid-format", False),
        ("x", "scrypt$nothex$nothex", False),
    ],
)
def test_password_hashing_and_verification(candidate: str, stored_override: str | None, expected: bool):
    # Given
    hashed = hash_password("s3cr3t-pass")
    stored = stored_override or hashed

    # When
    verified = verify_password(candidate, stored)

    # Then
    assert hashed.startswith("scrypt$")
    assert verified is expected


def test_init_db_seeds_admin_and_rbac_crud():
    # Given
    engine = create_portal_engine("sqlite:///:memory:")

    # When
    init_db(engine, "admin", "adminpass")
    # Calling init_db twice is idempotent
    init_db(engine, "admin", "different")

    # Then
    with Session(engine) as session:
        _extracted_from_test_init_db_seeds_admin_and_rbac_crud_12(session)


# TODO Rename this here and in `test_init_db_seeds_admin_and_rbac_crud`
def _extracted_from_test_init_db_seeds_admin_and_rbac_crud_12(session):
    admin = get_user_by_username(session, "admin")
    assert admin is not None
    assert admin.is_admin is True
    assert verify_password("adminpass", admin.password_hash)
    assert user_has_service(admin, "dagster") is True
    assert user_has_service(admin, "unknown") is False
    assert user_bucket_level(admin, "lakehouse") == "read_write"

    alice = create_user(session, "alice", "alicepass")
    assert get_user_by_id(session, alice.id) is not None
    assert [u.username for u in list_users(session)] == ["admin", "alice"]

    assert user_has_service(alice, "dagster") is False
    _extracted_from_test_init_db_seeds_admin_and_rbac_crud_25(session, alice, True)
    _extracted_from_test_init_db_seeds_admin_and_rbac_crud_25(session, alice, False)
    assert user_bucket_level(alice, "landing-zone") is None
    set_bucket_permission(session, alice.id, "landing-zone", "read")
    session.refresh(alice)
    _extracted_from_test_init_db_seeds_admin_and_rbac_crud_35(alice, "read", session, "read_write")
    _extracted_from_test_init_db_seeds_admin_and_rbac_crud_35(alice, "read_write", session, "none")
    assert user_bucket_level(alice, "landing-zone") is None

    key = create_s3_key(session, alice.id, label="cli")
    assert key.access_key_id.startswith("LHAK")
    assert len(list_user_s3_keys(session, alice.id)) == 1
    assert get_active_s3_key(session, key.access_key_id) is not None

    toggle_user_active(session, alice.id)
    session.refresh(alice)
    assert alice.is_active is False
    assert user_has_service(alice, "dagster") is False
    assert user_bucket_level(alice, "landing-zone") is None
    assert get_active_s3_key(session, key.access_key_id) is None

    toggle_user_active(session, alice.id)
    revoke_s3_key(session, alice.id, key.id)
    assert get_active_s3_key(session, key.access_key_id) is None


# TODO Rename this here and in `test_init_db_seeds_admin_and_rbac_crud`
def _extracted_from_test_init_db_seeds_admin_and_rbac_crud_35(alice, arg1, session, arg3):
    assert user_bucket_level(alice, "landing-zone") == arg1
    set_bucket_permission(session, alice.id, "landing-zone", arg3)
    session.refresh(alice)


# TODO Rename this here and in `test_init_db_seeds_admin_and_rbac_crud`
def _extracted_from_test_init_db_seeds_admin_and_rbac_crud_25(session, alice, arg2):
    set_service_permission(session, alice.id, "dagster", arg2)
    session.refresh(alice)
    assert user_has_service(alice, "dagster") is arg2


def test_db_validation_errors():
    # Given
    engine = create_portal_engine("sqlite:///:memory:")
    init_db(engine, "admin", "admin")

    # When / # Then
    with Session(engine) as session:
        with pytest.raises(ValueError, match="empty"):
            create_user(session, "   ", "pw")
        with pytest.raises(ValueError, match="already exists"):
            create_user(session, "admin", "pw")
        with pytest.raises(ValueError, match="Unknown user"):
            toggle_user_active(session, 9999)
        admin = get_user_by_username(session, "admin")
        assert admin is not None
        with pytest.raises(ValueError, match="Unknown service"):
            set_service_permission(session, admin.id, "nope", True)
        with pytest.raises(ValueError, match="Invalid bucket"):
            set_bucket_permission(session, admin.id, "lakehouse", "admin")
