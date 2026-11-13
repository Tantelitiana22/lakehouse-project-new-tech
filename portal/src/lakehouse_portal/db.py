"""SQLAlchemy models, Postgres database bootstrap, and RBAC persistence helpers."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Engine, ForeignKey, String, UniqueConstraint, create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

from lakehouse_portal.config import SERVICE_BY_SLUG
from lakehouse_portal.passwords import hash_password

VALID_BUCKET_LEVELS = frozenset({"read", "read_write"})


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """Declarative base for all portal SQLAlchemy models."""


class User(Base):
    """Portal user account."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    service_permissions: Mapped[list[ServicePermission]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    bucket_permissions: Mapped[list[BucketPermission]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    s3_keys: Mapped[list[S3AccessKey]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class ServicePermission(Base):
    """Explicit service access grant for a non-admin user."""

    __tablename__ = "service_permissions"
    __table_args__ = (UniqueConstraint("user_id", "service_slug", name="uq_user_service"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    service_slug: Mapped[str] = mapped_column(String(64), nullable=False)

    user: Mapped[User] = relationship(back_populates="service_permissions")


class BucketPermission(Base):
    """Per-bucket read or read_write access grant."""

    __tablename__ = "bucket_permissions"
    __table_args__ = (UniqueConstraint("user_id", "bucket_name", name="uq_user_bucket"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    bucket_name: Mapped[str] = mapped_column(String(128), nullable=False)
    permission_level: Mapped[str] = mapped_column(String(16), nullable=False)

    user: Mapped[User] = relationship(back_populates="bucket_permissions")


class S3AccessKey(Base):
    """User-owned S3 access keypair validated by the s3.localhost SigV4 proxy."""

    __tablename__ = "s3_access_keys"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    access_key_id: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)
    secret_access_key: Mapped[str] = mapped_column(String(128), nullable=False)
    label: Mapped[str] = mapped_column(String(128), default="default", nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    user: Mapped[User] = relationship(back_populates="s3_keys")


def ensure_postgres_database(database_url: str) -> None:
    """Create the target PostgreSQL database if it does not yet exist."""
    if not database_url.startswith("postgresql"):
        return
    url = make_url(database_url)
    target_db = url.database
    if not target_db:
        return
    admin_db = url.username or "postgres"
    admin_url = url.set(database=admin_db)
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": target_db},
            ).scalar()
            if not exists:
                safe_name = target_db.replace('"', '""')
                conn.execute(text(f'CREATE DATABASE "{safe_name}"'))
    finally:
        admin_engine.dispose()


def create_portal_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine suitable for SQLite (tests) or PostgreSQL."""
    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(database_url, pool_pre_ping=True, connect_args=connect_args)


def init_db(engine: Engine, admin_user: str, admin_password: str) -> None:
    """Create all schema tables and seed the initial admin account if absent."""
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        existing = session.scalar(select(User).where(User.username == admin_user))
        if existing is None:
            session.add(
                User(
                    username=admin_user,
                    password_hash=hash_password(admin_password),
                    is_admin=True,
                    is_active=True,
                )
            )
            session.commit()


def get_user_by_id(session: Session, user_id: int) -> User | None:
    """Fetch a user by primary key."""
    return session.scalar(select(User).where(User.id == user_id))


def get_user_by_username(session: Session, username: str) -> User | None:
    """Fetch a user by unique username."""
    return session.scalar(select(User).where(User.username == username))


def list_users(session: Session) -> list[User]:
    """Return all portal users ordered by username."""
    return list(session.scalars(select(User).order_by(User.username)).all())


def create_user(
    session: Session,
    username: str,
    password: str,
    *,
    is_admin: bool = False,
) -> User:
    """Create a new user account and commit it."""
    clean_name = username.strip()
    if not clean_name:
        raise ValueError("Username cannot be empty")
    if get_user_by_username(session, clean_name) is not None:
        raise ValueError(f"User {clean_name!r} already exists")
    user = User(
        username=clean_name,
        password_hash=hash_password(password),
        is_admin=is_admin,
        is_active=True,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def toggle_user_active(session: Session, user_id: int) -> User:
    """Flip ``is_active`` for a user and return the updated record."""
    user = get_user_by_id(session, user_id)
    if user is None:
        raise ValueError(f"Unknown user id: {user_id}")
    user.is_active = not user.is_active
    session.commit()
    session.refresh(user)
    return user


def user_has_service(user: User, service_slug: str) -> bool:
    """Return True when an active user is allowed to access ``service_slug``."""
    if not user.is_active:
        return False
    if user.is_admin:
        return service_slug in SERVICE_BY_SLUG
    return any(perm.service_slug == service_slug for perm in user.service_permissions)


def set_service_permission(session: Session, user_id: int, service_slug: str, enabled: bool) -> None:
    """Grant or revoke a service permission for a user."""
    if service_slug not in SERVICE_BY_SLUG:
        raise ValueError(f"Unknown service slug: {service_slug}")
    existing = session.scalar(
        select(ServicePermission).where(
            ServicePermission.user_id == user_id,
            ServicePermission.service_slug == service_slug,
        )
    )
    if enabled and existing is None:
        session.add(ServicePermission(user_id=user_id, service_slug=service_slug))
        session.commit()
    elif not enabled and existing is not None:
        session.delete(existing)
        session.commit()


def user_bucket_level(user: User, bucket_name: str) -> str | None:
    """Return ``'read'``, ``'read_write'``, or ``None`` for a user on a bucket."""
    if not user.is_active:
        return None
    if user.is_admin:
        return "read_write"
    for perm in user.bucket_permissions:
        if perm.bucket_name == bucket_name:
            return perm.permission_level
    return None


def set_bucket_permission(session: Session, user_id: int, bucket_name: str, level: str) -> None:
    """Assign ``'none'``, ``'read'``, or ``'read_write'`` for a user on a bucket."""
    existing = session.scalar(
        select(BucketPermission).where(
            BucketPermission.user_id == user_id,
            BucketPermission.bucket_name == bucket_name,
        )
    )
    if level == "none":
        if existing is not None:
            session.delete(existing)
            session.commit()
        return
    if level not in VALID_BUCKET_LEVELS:
        raise ValueError(f"Invalid bucket permission level: {level!r}")
    if existing is None:
        session.add(BucketPermission(user_id=user_id, bucket_name=bucket_name, permission_level=level))
    else:
        existing.permission_level = level
    session.commit()


def create_s3_key(session: Session, user_id: int, label: str = "default") -> S3AccessKey:
    """Generate and persist a new personal S3 access keypair."""
    access_key_id = f"LHAK{secrets.token_hex(8).upper()}"
    secret_access_key = secrets.token_urlsafe(30)
    key = S3AccessKey(
        user_id=user_id,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        label=label.strip() or "default",
        is_active=True,
    )
    session.add(key)
    session.commit()
    session.refresh(key)
    return key


def list_user_s3_keys(session: Session, user_id: int) -> list[S3AccessKey]:
    """List S3 access keys owned by ``user_id``."""
    return list(
        session.scalars(
            select(S3AccessKey).where(S3AccessKey.user_id == user_id).order_by(S3AccessKey.created_at.desc())
        ).all()
    )


def revoke_s3_key(session: Session, user_id: int, key_id: int, *, allow_any: bool = False) -> None:
    """Deactivate an S3 access key owned by ``user_id`` (or any user if ``allow_any``)."""
    stmt = select(S3AccessKey).where(S3AccessKey.id == key_id)
    if not allow_any:
        stmt = stmt.where(S3AccessKey.user_id == user_id)
    key = session.scalar(stmt)
    if key is not None:
        key.is_active = False
        session.commit()


def get_active_s3_key(session: Session, access_key_id: str) -> S3AccessKey | None:
    """Look up an active S3 key along with its active owner."""
    key = session.scalar(
        select(S3AccessKey).where(
            S3AccessKey.access_key_id == access_key_id,
            S3AccessKey.is_active.is_(True),
        )
    )
    if key is None or not key.user.is_active:
        return None
    return key
