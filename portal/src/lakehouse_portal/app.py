"""Starlette web application for the Lakehouse Access Management Portal."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import httpx
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from lakehouse_portal.auth import (
    SESSION_COOKIE_NAME,
    create_session_token,
    is_safe_redirect_url,
    verify_session_token,
)
from lakehouse_portal.config import SERVICE_BY_SLUG, SERVICES, PortalConfig, ServiceDefinition
from lakehouse_portal.db import (
    User,
    create_portal_engine,
    create_s3_key,
    create_user,
    ensure_postgres_database,
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
from lakehouse_portal.forms import parse_request_form
from lakehouse_portal.passwords import verify_password
from lakehouse_portal.s3_client import Boto3S3Service, S3StorageError, S3StorageService
from lakehouse_portal.s3_proxy import handle_s3_proxy_request

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"


@dataclass(frozen=True)
class ServiceCardView:
    """View model for a service card on the Launchpad."""

    service: ServiceDefinition
    url: str
    allowed: bool
    online: bool


@dataclass(frozen=True)
class BucketGrantView:
    """View model for a permitted RustFS bucket."""

    name: str
    level: str


@dataclass(frozen=True)
class AdminUserView:
    """Detached view model for rendering a user row in the Admin matrix."""

    id: int
    username: str
    is_admin: bool
    is_active: bool
    granted_services: frozenset[str]
    bucket_levels: dict[str, str]


@dataclass(frozen=True)
class _PortalContext:
    cfg: PortalConfig
    engine: Engine
    s3: S3StorageService
    probe: Callable[[str], Awaitable[bool]]
    templates: Jinja2Templates

    def render(
        self,
        request: Request,
        template_name: str,
        context: dict[str, Any],
        status_code: int = 200,
    ) -> Response:
        return self.templates.TemplateResponse(
            request,
            template_name,
            {"config": self.cfg, **context},
            status_code=status_code,
        )

    def current_user(self, request: Request) -> User | None:
        token = request.cookies.get(SESSION_COOKIE_NAME)
        payload = verify_session_token(token, self.cfg.secret_key, expected_purpose="session")
        if payload is None or not isinstance(payload.get("uid"), int):
            return None
        with Session(self.engine, expire_on_commit=False) as session:
            user = get_user_by_id(session, payload["uid"])
            if user is None or not user.is_active:
                return None
            _ = list(user.service_permissions)
            _ = list(user.bucket_permissions)
            return user

    def safe_list_buckets(self) -> list[str]:
        try:
            return self.s3.list_buckets()
        except Exception:
            return []

    def permitted_buckets(self, user: User) -> list[BucketGrantView]:
        grants: list[BucketGrantView] = []
        for bucket in self.safe_list_buckets():
            level = user_bucket_level(user, bucket)
            if level is not None:
                grants.append(BucketGrantView(name=bucket, level=level))
        return grants


async def _default_health_checker(url: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=0.75, follow_redirects=False) as client:
            resp = await client.get(url)
            return resp.status_code < 500
    except Exception:
        return False


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        path="/",
        max_age=28800,
    )


def _login_redirect(request: Request, next_url: str | None = None) -> RedirectResponse:
    target = next_url or str(request.url.path)
    return RedirectResponse(url=f"/login?next={quote(target, safe='')}", status_code=303)


def _parent_prefix(prefix: str) -> str | None:
    cleaned = prefix.strip("/")
    if not cleaned:
        return None
    if "/" not in cleaned:
        return ""
    return cleaned.rsplit("/", 1)[0] + "/"


class _AuthHandlers:
    """Authentication, SSO handoff, and Nginx auth_request handlers."""

    def __init__(self, ctx: _PortalContext) -> None:
        self.ctx = ctx

    def _handoff_redirect_url(self, user: User, target_url: str) -> str:
        parsed = urlparse(target_url)
        if not parsed.netloc:
            return target_url
        handoff_token = create_session_token(
            user.id,
            user.username,
            self.ctx.cfg.secret_key,
            ttl_seconds=60,
            purpose="handoff",
        )
        rel_path = parsed.path or "/"
        if parsed.query:
            rel_path = f"{rel_path}?{parsed.query}"
        return f"{parsed.scheme}://{parsed.netloc}/__auth_callback?token={quote(handoff_token)}&next={quote(rel_path)}"

    async def healthz(self, _request: Request) -> Response:
        return JSONResponse({"status": "ok"})

    async def login_page(self, request: Request) -> Response:
        next_url = request.query_params.get("next", "/")
        if not is_safe_redirect_url(next_url, self.ctx.cfg.base_domain, self.ctx.cfg.gateway_port):
            next_url = "/"
        return self.ctx.render(request, "login.html", {"current_user": None, "next_url": next_url, "error": None})

    async def login_submit(self, request: Request) -> Response:
        form = await parse_request_form(request)
        username = form.fields.get("username", "").strip()
        password = form.fields.get("password", "")
        next_url = form.fields.get("next", "/")
        if not is_safe_redirect_url(next_url, self.ctx.cfg.base_domain, self.ctx.cfg.gateway_port):
            next_url = "/"

        with Session(self.ctx.engine, expire_on_commit=False) as session:
            user = get_user_by_username(session, username)
            if user is None or not user.is_active or not verify_password(password, user.password_hash):
                return self.ctx.render(
                    request,
                    "login.html",
                    {"current_user": None, "next_url": next_url, "error": "Invalid username or password."},
                    status_code=401,
                )

        session_token = create_session_token(user.id, user.username, self.ctx.cfg.secret_key, purpose="session")
        destination = self._handoff_redirect_url(user, next_url) if urlparse(next_url).netloc else next_url
        resp = RedirectResponse(url=destination, status_code=303)
        _set_session_cookie(resp, session_token)
        return resp

    async def logout(self, _request: Request) -> Response:
        resp = RedirectResponse(url="/login", status_code=303)
        resp.delete_cookie(SESSION_COOKIE_NAME, path="/")
        return resp

    async def auth_handoff(self, request: Request) -> Response:
        next_url = request.query_params.get("next", "/")
        if not is_safe_redirect_url(next_url, self.ctx.cfg.base_domain, self.ctx.cfg.gateway_port):
            next_url = "/"
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, next_url)
        return RedirectResponse(url=self._handoff_redirect_url(user, next_url), status_code=303)

    async def auth_callback(self, request: Request) -> Response:
        token = request.query_params.get("token", "")
        next_path = request.query_params.get("next", "/")
        if not next_path.startswith("/") or next_path.startswith("//"):
            next_path = "/"
        payload = verify_session_token(token, self.ctx.cfg.secret_key, expected_purpose="handoff")
        if payload is None or not isinstance(payload.get("uid"), int):
            return RedirectResponse(url=f"{self.ctx.cfg.portal_origin}/login", status_code=303)
        with Session(self.ctx.engine, expire_on_commit=False) as session:
            user = get_user_by_id(session, payload["uid"])
            if user is None or not user.is_active:
                return RedirectResponse(url=f"{self.ctx.cfg.portal_origin}/login", status_code=303)
        session_token = create_session_token(user.id, user.username, self.ctx.cfg.secret_key, purpose="session")
        resp = RedirectResponse(url=next_path, status_code=303)
        _set_session_cookie(resp, session_token)
        return resp

    async def internal_verify(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return Response(status_code=401)
        service_slug = request.query_params.get("service", "").strip()
        if service_slug and not user_has_service(user, service_slug):
            return Response(status_code=403)
        return Response(
            status_code=200,
            headers={
                "X-Auth-User": user.username,
                "X-Auth-Admin": "true" if user.is_admin else "false",
            },
        )

    async def internal_forbidden(self, request: Request) -> Response:
        service_slug = request.query_params.get("service", "").strip()
        svc = SERVICE_BY_SLUG.get(service_slug)
        user = self.ctx.current_user(request)
        return self.ctx.render(
            request,
            "forbidden.html",
            {
                "current_user": user,
                "active_tab": "",
                "service_name": svc.name if svc else service_slug,
                "portal_origin": self.ctx.cfg.portal_origin,
            },
            status_code=403,
        )


class _ExplorerHandlers:
    """Launchpad, Bucket Explorer, S3 Key management, and S3 proxy handlers."""

    def __init__(self, ctx: _PortalContext) -> None:
        self.ctx = ctx

    async def launchpad(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, "/")
        statuses = await asyncio.gather(*(self.ctx.probe(svc.internal_url) for svc in SERVICES))
        cards = [
            ServiceCardView(
                service=svc,
                url=self.ctx.cfg.service_url(svc),
                allowed=user_has_service(user, svc.slug),
                online=online,
            )
            for svc, online in zip(SERVICES, statuses, strict=True)
        ]
        return self.ctx.render(
            request,
            "launchpad.html",
            {
                "current_user": user,
                "active_tab": "launchpad",
                "service_cards": cards,
                "bucket_grants": self.ctx.permitted_buckets(user),
            },
        )

    async def buckets_index(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, "/buckets")
        return self.ctx.render(
            request,
            "buckets.html",
            {
                "current_user": user,
                "active_tab": "buckets",
                "buckets": self.ctx.permitted_buckets(user),
                "error": request.query_params.get("error"),
            },
        )

    async def buckets_create(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, "/buckets")
        if not user.is_admin:
            return self.ctx.render(
                request,
                "forbidden.html",
                {"current_user": user, "active_tab": "buckets", "service_name": "", "portal_origin": "/"},
                status_code=403,
            )
        form = await parse_request_form(request)
        bucket_name = form.fields.get("bucket_name", "").strip()
        if not bucket_name:
            return RedirectResponse(url="/buckets?error=Bucket+name+is+required", status_code=303)
        try:
            self.ctx.s3.create_bucket(bucket_name)
        except S3StorageError as exc:
            return RedirectResponse(url=f"/buckets?error={quote(exc.message)}", status_code=303)
        return RedirectResponse(url="/buckets", status_code=303)

    async def bucket_detail(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, str(request.url.path))
        bucket = request.path_params["bucket"]
        level = user_bucket_level(user, bucket)
        if level is None:
            return self.ctx.render(
                request,
                "forbidden.html",
                {
                    "current_user": user,
                    "active_tab": "buckets",
                    "service_name": f"bucket:{bucket}",
                    "portal_origin": "/",
                },
                status_code=403,
            )
        prefix = request.query_params.get("prefix", "")
        try:
            listing = self.ctx.s3.list_objects(bucket, prefix=prefix, delimiter="/")
        except S3StorageError as exc:
            return RedirectResponse(url=f"/buckets?error={quote(exc.message)}", status_code=303)
        return self.ctx.render(
            request,
            "bucket_detail.html",
            {
                "current_user": user,
                "active_tab": "buckets",
                "bucket": bucket,
                "prefix": prefix,
                "parent_prefix": _parent_prefix(prefix),
                "level": level,
                "listing": listing,
            },
        )

    async def bucket_upload(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, "/buckets")
        bucket = request.path_params["bucket"]
        if user_bucket_level(user, bucket) != "read_write":
            return Response("Write permission required", status_code=403)
        form = await parse_request_form(request)
        prefix = form.fields.get("prefix", "")
        uploaded = form.files.get("file")
        if uploaded is not None:
            filename, data, content_type = uploaded
            clean_name = Path(filename).name
            if clean_name:
                key = f"{prefix.rstrip('/')}/{clean_name}" if prefix else clean_name
                self.ctx.s3.put_object(bucket, key, data, content_type=content_type)
        return RedirectResponse(url=f"/buckets/{bucket}?prefix={quote(prefix)}", status_code=303)

    async def bucket_download(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, str(request.url.path))
        bucket = request.path_params["bucket"]
        key = request.path_params["key"]
        if user_bucket_level(user, bucket) is None:
            return Response("Read permission required", status_code=403)
        try:
            data, content_type = self.ctx.s3.get_object(bucket, key)
        except S3StorageError as exc:
            return Response(exc.message, status_code=exc.status_code)
        filename = Path(key).name or "object"
        return Response(
            content=data,
            media_type=content_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    async def bucket_delete(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, "/buckets")
        bucket = request.path_params["bucket"]
        if user_bucket_level(user, bucket) != "read_write":
            return Response("Write permission required", status_code=403)
        form = await parse_request_form(request)
        key = form.fields.get("key", "")
        prefix = form.fields.get("prefix", "")
        if key:
            self.ctx.s3.delete_object(bucket, key)
        return RedirectResponse(url=f"/buckets/{bucket}?prefix={quote(prefix)}", status_code=303)

    async def keys_index(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, "/keys")
        with Session(self.ctx.engine, expire_on_commit=False) as session:
            keys = list_user_s3_keys(session, user.id)
        return self.ctx.render(
            request,
            "keys.html",
            {"current_user": user, "active_tab": "keys", "keys": keys, "created_key": None},
        )

    async def keys_create(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, "/keys")
        form = await parse_request_form(request)
        label = form.fields.get("label", "default")
        with Session(self.ctx.engine, expire_on_commit=False) as session:
            created = create_s3_key(session, user.id, label=label)
            keys = list_user_s3_keys(session, user.id)
        return self.ctx.render(
            request,
            "keys.html",
            {"current_user": user, "active_tab": "keys", "keys": keys, "created_key": created},
            status_code=201,
        )

    async def keys_revoke(self, request: Request) -> Response:
        user = self.ctx.current_user(request)
        if user is None:
            return _login_redirect(request, "/keys")
        key_id = int(request.path_params["key_id"])
        with Session(self.ctx.engine) as session:
            revoke_s3_key(session, user.id, key_id, allow_any=user.is_admin)
        return RedirectResponse(url="/keys", status_code=303)

    async def s3_proxy(self, request: Request) -> Response:
        return await handle_s3_proxy_request(request, self.ctx.engine, self.ctx.s3)


class _AdminHandlers:
    """Admin user creation and RBAC permission management handlers."""

    def __init__(self, ctx: _PortalContext) -> None:
        self.ctx = ctx

    def _require_admin(self, request: Request) -> tuple[User | None, Response | None]:
        user = self.ctx.current_user(request)
        if user is None:
            return None, _login_redirect(request, "/admin")
        if not user.is_admin:
            return None, self.ctx.render(
                request,
                "forbidden.html",
                {"current_user": user, "active_tab": "", "service_name": "Admin Console", "portal_origin": "/"},
                status_code=403,
            )
        return user, None

    async def admin_index(self, request: Request) -> Response:
        user, denial = self._require_admin(request)
        if denial is not None or user is None:
            return denial or Response(status_code=403)
        with Session(self.ctx.engine, expire_on_commit=False) as session:
            user_views = [
                AdminUserView(
                    id=u.id,
                    username=u.username,
                    is_admin=u.is_admin,
                    is_active=u.is_active,
                    granted_services=frozenset(p.service_slug for p in u.service_permissions),
                    bucket_levels={p.bucket_name: p.permission_level for p in u.bucket_permissions},
                )
                for u in list_users(session)
            ]
        return self.ctx.render(
            request,
            "admin.html",
            {
                "current_user": user,
                "active_tab": "admin",
                "users": user_views,
                "services": SERVICES,
                "all_buckets": self.ctx.safe_list_buckets(),
                "error": request.query_params.get("error"),
            },
        )

    async def admin_create_user(self, request: Request) -> Response:
        _user, denial = self._require_admin(request)
        if denial is not None:
            return denial
        form = await parse_request_form(request)
        username = form.fields.get("username", "").strip()
        password = form.fields.get("password", "")
        is_admin = form.fields.get("is_admin") == "true"
        if not username or not password:
            return RedirectResponse(url="/admin?error=Username+and+password+are+required", status_code=303)
        with Session(self.ctx.engine) as session:
            try:
                create_user(session, username, password, is_admin=is_admin)
            except ValueError as exc:
                return RedirectResponse(url=f"/admin?error={quote(str(exc))}", status_code=303)
        return RedirectResponse(url="/admin", status_code=303)

    async def admin_toggle_active(self, request: Request) -> Response:
        current, denial = self._require_admin(request)
        if denial is not None or current is None:
            return denial or Response(status_code=403)
        target_id = int(request.path_params["user_id"])
        if target_id == current.id:
            return RedirectResponse(url="/admin?error=Cannot+disable+your+own+account", status_code=303)
        with Session(self.ctx.engine) as session:
            try:
                toggle_user_active(session, target_id)
            except ValueError as exc:
                return RedirectResponse(url=f"/admin?error={quote(str(exc))}", status_code=303)
        return RedirectResponse(url="/admin", status_code=303)

    async def admin_set_service(self, request: Request) -> Response:
        _user, denial = self._require_admin(request)
        if denial is not None:
            return denial
        target_id = int(request.path_params["user_id"])
        form = await parse_request_form(request)
        service_slug = form.fields.get("service_slug", "")
        enabled = form.fields.get("enabled") == "true"
        with Session(self.ctx.engine) as session:
            try:
                set_service_permission(session, target_id, service_slug, enabled)
            except ValueError as exc:
                return RedirectResponse(url=f"/admin?error={quote(str(exc))}", status_code=303)
        return RedirectResponse(url="/admin", status_code=303)

    async def admin_set_bucket(self, request: Request) -> Response:
        _user, denial = self._require_admin(request)
        if denial is not None:
            return denial
        target_id = int(request.path_params["user_id"])
        form = await parse_request_form(request)
        bucket_name = form.fields.get("bucket_name", "")
        level = form.fields.get("level", "none")
        with Session(self.ctx.engine) as session:
            try:
                set_bucket_permission(session, target_id, bucket_name, level)
            except ValueError as exc:
                return RedirectResponse(url=f"/admin?error={quote(str(exc))}", status_code=303)
        return RedirectResponse(url="/admin", status_code=303)


def create_app(
    config: PortalConfig | None = None,
    engine: Engine | None = None,
    s3_service: S3StorageService | None = None,
    health_checker: Callable[[str], Awaitable[bool]] | None = None,
) -> Starlette:
    """Create and configure the Starlette ASGI application."""
    cfg = config or PortalConfig.from_env()
    if engine is None:
        ensure_postgres_database(cfg.database_url)
        db_engine = create_portal_engine(cfg.database_url)
    else:
        db_engine = engine
    init_db(db_engine, cfg.admin_user, cfg.admin_password)

    s3: S3StorageService = s3_service or Boto3S3Service(
        endpoint_url=cfg.s3_endpoint_url,
        access_key_id=cfg.s3_access_key_id,
        secret_access_key=cfg.s3_secret_access_key,
        region=cfg.s3_region,
    )
    ctx = _PortalContext(
        cfg=cfg,
        engine=db_engine,
        s3=s3,
        probe=health_checker or _default_health_checker,
        templates=Jinja2Templates(directory=str(TEMPLATES_DIR)),
    )
    auth_h = _AuthHandlers(ctx)
    explorer_h = _ExplorerHandlers(ctx)
    admin_h = _AdminHandlers(ctx)

    @asynccontextmanager
    async def lifespan(_app: Starlette):
        yield
        if engine is None:
            db_engine.dispose()

    routes = [
        Route("/healthz", auth_h.healthz, methods=["GET"]),
        Route("/login", auth_h.login_page, methods=["GET"]),
        Route("/login", auth_h.login_submit, methods=["POST"]),
        Route("/logout", auth_h.logout, methods=["POST"]),
        Route("/auth/handoff", auth_h.auth_handoff, methods=["GET"]),
        Route("/__auth_callback", auth_h.auth_callback, methods=["GET"]),
        Route("/internal/verify", auth_h.internal_verify, methods=["GET"]),
        Route("/internal/forbidden", auth_h.internal_forbidden, methods=["GET"]),
        Route("/", explorer_h.launchpad, methods=["GET"]),
        Route("/buckets", explorer_h.buckets_index, methods=["GET"]),
        Route("/buckets", explorer_h.buckets_create, methods=["POST"]),
        Route("/buckets/{bucket}", explorer_h.bucket_detail, methods=["GET"]),
        Route("/buckets/{bucket}/upload", explorer_h.bucket_upload, methods=["POST"]),
        Route("/buckets/{bucket}/download/{key:path}", explorer_h.bucket_download, methods=["GET"]),
        Route("/buckets/{bucket}/delete", explorer_h.bucket_delete, methods=["POST"]),
        Route("/keys", explorer_h.keys_index, methods=["GET"]),
        Route("/keys", explorer_h.keys_create, methods=["POST"]),
        Route("/keys/{key_id:int}/revoke", explorer_h.keys_revoke, methods=["POST"]),
        Route("/admin", admin_h.admin_index, methods=["GET"]),
        Route("/admin/users", admin_h.admin_create_user, methods=["POST"]),
        Route("/admin/users/{user_id:int}/toggle-active", admin_h.admin_toggle_active, methods=["POST"]),
        Route("/admin/users/{user_id:int}/service", admin_h.admin_set_service, methods=["POST"]),
        Route("/admin/users/{user_id:int}/bucket", admin_h.admin_set_bucket, methods=["POST"]),
        Route("/s3", explorer_h.s3_proxy, methods=["GET", "POST", "PUT", "DELETE", "HEAD"]),
        Route("/s3/{path:path}", explorer_h.s3_proxy, methods=["GET", "POST", "PUT", "DELETE", "HEAD"]),
    ]
    return Starlette(routes=routes, lifespan=lifespan)
