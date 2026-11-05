"""Domain-level exceptions."""

from __future__ import annotations


class LakehouseError(Exception):
    """Base class for all lakehouse errors."""


class UnsupportedSourceTypeError(LakehouseError):
    """No extractor is registered for the configured source type."""


class UnsupportedPaginationStrategyError(LakehouseError):
    """No pagination strategy is registered for the configured strategy."""


class UnsupportedWriteModeError(LakehouseError):
    """No Iceberg write strategy is registered for the configured write mode."""


class RetryExhaustedError(LakehouseError):
    """A retryable HTTP operation exhausted its retry budget."""
