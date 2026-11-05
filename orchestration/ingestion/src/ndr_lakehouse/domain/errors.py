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


class UseCaseNotFoundError(LakehouseError):
    """No use-case manifest matched the requested name."""


class AmbiguousDefaultUseCaseError(LakehouseError):
    """More than one use-case manifest is marked as the default."""


class DuplicateAssetNameError(LakehouseError, ValueError):
    """Two asset files declare the same asset name."""
