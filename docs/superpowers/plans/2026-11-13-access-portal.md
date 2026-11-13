# Lakehouse Access Management Portal & Nginx Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained Access Management & Service Launchpad Portal (`portal`) behind an Nginx Reverse Proxy Gateway (`gateway` on port `8088`) with `.localhost` SSO, Bucket Explorer, Scoped S3 SigV4 Proxy, and Admin RBAC UI.

**Architecture:** `gateway` (`nginx:1.27-alpine`) routes `*.localhost:8088` traffic and enforces per-service access via `auth_request` to `portal` (Starlette + Jinja2 + SQLAlchemy + Postgres). Cross-subdomain cookies are established via `/auth/handoff` -> `/__auth_callback`. `s3.localhost:8088` proxies AWS SigV4 requests through `portal`, verifying per-user credentials and bucket permissions (`read` / `read_write`) before forwarding to RustFS.

**Tech Stack:** Python 3.11/3.12, Starlette 1.10, Uvicorn, Jinja2, SQLAlchemy 2, psycopg2, boto3/botocore, Nginx 1.27, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-11-13-access-portal-design.md`

---

## Global Constraints

- Use existing locked dependencies only (`starlette`, `uvicorn`, `jinja2`, `sqlalchemy`, `psycopg2`, `httpx`, `boto3`, `botocore`, `pydantic`). Do not add PyPI dependencies.
- `python-multipart` is not installed; use stdlib `urllib.parse.parse_qs` + `email.parser.BytesParser` in `lakehouse_portal.forms`.
- Line length `120`; all modules start with `from __future__ import annotations`; `make check` and `pytest` (>=80% coverage) must pass.

---

### Task 1: Configure Package & Quality Gates
- [ ] Create `portal/src/lakehouse_portal/__init__.py` and `portal/tests/__init__.py`.
- [ ] Update `pyproject.toml`, `Makefile`, and `.pre-commit-config.yaml` to include `lakehouse_portal` and `portal/tests`.

### Task 2: Configuration, Passwords, Database Models & RBAC Repository
- [ ] Create `portal/src/lakehouse_portal/config.py` (`SERVICES`, `ServiceDefinition`, `PortalConfig`).
- [ ] Create `portal/src/lakehouse_portal/passwords.py` (`hash_password`, `verify_password` using `hashlib.scrypt`).
- [ ] Create `portal/src/lakehouse_portal/db.py` (SQLAlchemy models `User`, `ServicePermission`, `BucketPermission`, `S3AccessKey`, `ensure_postgres_database`, `init_db`, and RBAC CRUD helpers).
- [ ] Add tests in `portal/tests/test_db_and_auth.py` and verify passing.

### Task 3: Session Tokens, Form Parser & S3 Storage Client
- [ ] Create `portal/src/lakehouse_portal/auth.py` (`create_session_token`, `verify_session_token`).
- [ ] Create `portal/src/lakehouse_portal/forms.py` (`ParsedForm`, `parse_request_form`).
- [ ] Create `portal/src/lakehouse_portal/s3_client.py` (`S3StorageService`, `Boto3S3Service`, `InMemoryS3Service`).
- [ ] Add tests in `portal/tests/test_forms_and_s3_client.py` and verify passing.

### Task 4: Scoped S3 SigV4 Proxy
- [ ] Create `portal/src/lakehouse_portal/s3_proxy.py` (SigV4 signature verification + bucket permission enforcement + S3 XML serialization).
- [ ] Add tests in `portal/tests/test_s3_proxy.py` testing real `botocore.auth.SigV4Auth` requests and verify passing.

### Task 5: Portal Starlette Application & Templates
- [ ] Create Jinja2 templates under `portal/src/lakehouse_portal/templates/` (`base.html`, `login.html`, `launchpad.html`, `buckets.html`, `bucket_detail.html`, `keys.html`, `admin.html`, `forbidden.html`).
- [ ] Create `portal/src/lakehouse_portal/app.py` implementing all portal routes, `/auth/handoff`, `/__auth_callback`, `/internal/verify`, `/internal/forbidden`, `/buckets`, `/keys`, `/admin`, and `/s3` mount.
- [ ] Add integration tests in `portal/tests/test_portal_app.py` and verify passing.

### Task 6: Nginx Gateway, Dockerfiles, Compose Wiring & Docs
- [ ] Create `gateway/Dockerfile` and `gateway/nginx.conf.template`.
- [ ] Create `Dockerfile.portal`.
- [ ] Update `docker-compose.yml` with `portal` and `gateway` services, Spark UI flags, and internal-only UI ports.
- [ ] Update `README.md` with Portal & Gateway documentation.
- [ ] Run `.venv/bin/python -m pytest`, `make check`, and `docker compose config -q`.
