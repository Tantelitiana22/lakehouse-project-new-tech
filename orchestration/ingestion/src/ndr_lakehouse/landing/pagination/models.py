"""Pagination request value object."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PageRequest:
    """Next HTTP request to execute for a paginated source."""

    url: str
    params: dict[str, Any] | None = field(default_factory=dict)
