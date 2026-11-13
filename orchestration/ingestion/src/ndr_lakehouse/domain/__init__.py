"""Domain ports and errors."""

from ndr_lakehouse.domain.errors import (
    AmbiguousDefaultUseCaseError,
    DuplicateAssetNameError,
    InvalidHandlerReferenceError,
    LakehouseError,
    RetryExhaustedError,
    UnsupportedPaginationStrategyError,
    UnsupportedSourceTypeError,
    UnsupportedWriteModeError,
    UseCaseNotFoundError,
)
from ndr_lakehouse.domain.ports import (
    CatalogGateway,
    HandlerContext,
    HttpResponse,
    HttpTransport,
    IcebergPort,
    LandingPort,
    ObjectStore,
    SparkSessionProvider,
    UseCaseHandler,
)

__all__ = [
    "AmbiguousDefaultUseCaseError",
    "CatalogGateway",
    "DuplicateAssetNameError",
    "HandlerContext",
    "HttpResponse",
    "HttpTransport",
    "IcebergPort",
    "InvalidHandlerReferenceError",
    "LakehouseError",
    "LandingPort",
    "ObjectStore",
    "RetryExhaustedError",
    "SparkSessionProvider",
    "UnsupportedPaginationStrategyError",
    "UnsupportedSourceTypeError",
    "UnsupportedWriteModeError",
    "UseCaseHandler",
    "UseCaseNotFoundError",
]
