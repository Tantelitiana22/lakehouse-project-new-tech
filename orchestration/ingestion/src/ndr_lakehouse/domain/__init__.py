"""Domain ports and errors."""

from ndr_lakehouse.domain.errors import (
    LakehouseError,
    RetryExhaustedError,
    UnsupportedPaginationStrategyError,
    UnsupportedSourceTypeError,
    UnsupportedWriteModeError,
)
from ndr_lakehouse.domain.ports import (
    CatalogGateway,
    HttpResponse,
    HttpTransport,
    ObjectStore,
    SparkSessionProvider,
)

__all__ = [
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
]
