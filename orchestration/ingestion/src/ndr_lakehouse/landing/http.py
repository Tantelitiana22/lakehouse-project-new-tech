"""Retrying HTTP client composed from a transport port and retry policy."""

from __future__ import annotations

from typing import Any

import httpx

from ndr_lakehouse.asset_property.schema_validator import SourceProperties
from ndr_lakehouse.domain.errors import RetryExhaustedError
from ndr_lakehouse.domain.ports import HttpResponse, HttpTransport
from ndr_lakehouse.landing.retry import RetryPolicy


class RetryingHttpClient:
    """Adds retry/backoff semantics to any ``HttpTransport`` adapter."""

    def __init__(
        self,
        transport: HttpTransport,
        policy: RetryPolicy,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        self.transport = transport
        self.policy = policy
        self.chunk_size = chunk_size

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> HttpResponse:
        last_response: HttpResponse | None = None
        for attempt in range(self.policy.max_retries + 1):
            try:
                response = self.transport.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    json_body=json_body,
                )
            except httpx.TransportError:
                if attempt >= self.policy.max_retries:
                    raise
                self.policy.sleep_for(attempt)
                continue

            last_response = response
            if self.policy.is_retryable(response.status_code) and attempt < self.policy.max_retries:
                self.policy.sleep_for(attempt, response)
                continue
            response.raise_for_status()
            return response

        if last_response is not None:
            last_response.raise_for_status()
        raise RetryExhaustedError(f"request failed after retries: {url}")

    def get_json(
        self,
        source: SourceProperties,
        url: str,
        params: dict[str, Any] | None,
    ) -> tuple[HttpResponse, Any]:
        response = self.request(
            source.method,
            url,
            params=params,
            headers=source.headers,
            json_body=source.json_body,
        )
        try:
            return response, response.json()
        except ValueError as exc:
            raise ValueError(f"expected JSON response from {url}") from exc

    def stream_bytes(self, source: SourceProperties) -> bytes:
        last_response: HttpResponse | None = None
        for attempt in range(self.policy.max_retries + 1):
            try:
                with self.transport.stream(
                    source.method,
                    source.url,
                    params=source.params,
                    headers=source.headers,
                    json_body=source.json_body,
                ) as response:
                    last_response = response
                    if (
                        self.policy.is_retryable(response.status_code)
                        and attempt < self.policy.max_retries
                    ):
                        response.read()
                        self.policy.sleep_for(attempt, response)
                        continue
                    response.raise_for_status()
                    return b"".join(
                        chunk for chunk in response.iter_bytes(chunk_size=self.chunk_size) if chunk
                    )
            except httpx.TransportError:
                if attempt >= self.policy.max_retries:
                    raise
                self.policy.sleep_for(attempt)

        if last_response is not None:
            last_response.raise_for_status()
        raise RetryExhaustedError(f"stream failed after retries: {source.url}")

    def close(self) -> None:
        self.transport.close()
