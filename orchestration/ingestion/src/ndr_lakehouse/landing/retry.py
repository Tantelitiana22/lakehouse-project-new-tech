"""Retry policy for transient HTTP failures."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from ndr_lakehouse.domain.ports import HttpResponse


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with optional ``Retry-After`` support.

    ``sleep`` is resolved at call time so tests can patch ``time.sleep``.
    """

    max_retries: int = 5
    base_seconds: float = 0.5
    max_seconds: float = 30.0
    retryable_statuses: frozenset[int] = field(default_factory=lambda: frozenset({429}))
    sleep: Callable[[float], None] | None = None

    def is_retryable(self, status_code: int) -> bool:
        return status_code in self.retryable_statuses or 500 <= status_code <= 599

    def delay(self, attempt: int, retry_after: float | None = None) -> float:
        value = retry_after if retry_after is not None else self.base_seconds * (2**attempt)
        return min(value, self.max_seconds)

    def sleep_for(self, attempt: int, response: HttpResponse | None = None) -> None:
        retry_after: float | None = None
        if response is not None:
            raw_retry_after = response.headers.get("Retry-After")
            if raw_retry_after and str(raw_retry_after).isdigit():
                retry_after = float(raw_retry_after)
        delay = self.delay(attempt, retry_after)
        (self.sleep or time.sleep)(delay)
