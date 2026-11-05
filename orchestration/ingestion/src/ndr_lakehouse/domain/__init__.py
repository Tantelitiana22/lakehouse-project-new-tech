"""Domain ports and errors."""

from ndr_lakehouse.domain.errors import (
    AmbiguousDefaultUseCaseError,
    LakehouseError,
    RetryExhaustedError,
    UnsupportedPaginationStrategyError,
    UnsupportedSourceTypeError,
    UnsupportedWriteModeError,
    UseCaseNotFoundError,
)
from ndr_lakehouse.domain.ports import (
    CatalogGateway,
    HttpResponse,
    HttpTransport,
    ObjectStore,
    SparkSessionProvider,
)

__all__ = [
    "AmbiguousDefaultUseCaseError",
    "CatalogGateway",
    "HttpResponse",
    "HttpTransport",
    "LakehouseError",
    "ObjectStore",
    "RetryExhaustedError",
    "SparkSessionProvider",
    "UnsupportedPaginationStrategyError",
    "UnsupportedSourceTypeError",
    "UnsupportedWriteModeError",
    "UseCaseNotFoundError",
]
