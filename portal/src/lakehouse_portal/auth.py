"""Signed session and cross-subdomain SSO handoff tokens."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any
from urllib.parse import urlparse

SESSION_COOKIE_NAME = "lh_session"


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def _decode_signed_payload(encoded_payload: str, encoded_sig: str, secret_key: str) -> dict[str, Any] | None:
    expected_sig = hmac.new(
        secret_key.encode("utf-8"),
        encoded_payload.encode("ascii"),
        hashlib.sha256,
    ).digest()
    try:
        actual_sig = _b64decode(encoded_sig)
        if not hmac.compare_digest(actual_sig, expected_sig):
            return None
        payload = json.loads(_b64decode(encoded_payload).decode("utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def create_session_token(
    user_id: int,
    username: str,
    secret_key: str,
    *,
    ttl_seconds: int = 28800,
    purpose: str = "session",
) -> str:
    """Mint an HMAC-SHA256 signed token with an expiration timestamp and purpose."""
    payload = {
        "uid": user_id,
        "sub": username,
        "exp": int(time.time()) + ttl_seconds,
        "p": purpose,
    }
    encoded_payload = _b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(
        secret_key.encode("utf-8"),
        encoded_payload.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{encoded_payload}.{_b64encode(signature)}"


def verify_session_token(
    token: str | None,
    secret_key: str,
    *,
    expected_purpose: str = "session",
) -> dict[str, Any] | None:
    """Verify signature, expiration, and purpose of a signed token."""
    if not token or "." not in token:
        return None
    encoded_payload, encoded_sig = token.split(".", 1)
    payload = _decode_signed_payload(encoded_payload, encoded_sig, secret_key)
    if payload is None or payload.get("p") != expected_purpose:
        return None
    exp = payload.get("exp")
    if not isinstance(exp, int) or exp < int(time.time()):
        return None
    return payload


def is_safe_redirect_url(target: str, base_domain: str, gateway_port: int) -> bool:
    """Allow relative paths or gateway subdomain URLs only, blocking open redirects."""
    if not target:
        return False
    if target.startswith("/") and not target.startswith("//"):
        return True
    parsed = urlparse(target)
    if parsed.scheme not in {"http", "https"}:
        return False
    hostname = parsed.hostname or ""
    if hostname != base_domain and not hostname.endswith(f".{base_domain}"):
        return False
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return port == gateway_port
