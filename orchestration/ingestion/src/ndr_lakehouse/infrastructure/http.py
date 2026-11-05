"""httpx adapter for the HTTP transport port."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any

import httpx

from ndr_lakehouse.domain.ports import HttpResponse


class HttpxTransport:
    """Thin adapter around ``httpx.Client`` with injectable construction."""

    def __init__(
        self,
        timeout_seconds: float = 60.0,
        follow_redirects: bool = True,
        client: httpx.Client | None = None,
    ) -> None:
        self._client = client or httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=follow_redirects,
        )

    @property
    def client(self) -> httpx.Client:
        return self._client

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> HttpResponse:
        return self._client.request(
            method,
            url,
            params=params,
            headers=headers,
            json=json_body,
        )

    def stream(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> AbstractContextManager[HttpResponse]:
        return self._client.stream(
            method,
            url,
            params=params,
            headers=headers,
            json=json_body,
        )

    def close(self) -> None:
        self._client.close()
