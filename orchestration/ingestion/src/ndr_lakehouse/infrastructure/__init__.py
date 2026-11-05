"""Concrete adapters for external systems (boto3, httpx, Unity Catalog REST)."""

from ndr_lakehouse.infrastructure.http import HttpxTransport
from ndr_lakehouse.infrastructure.object_store import Boto3ObjectStore
from ndr_lakehouse.infrastructure.unity_catalog import UnityCatalogRestClient

__all__ = ["Boto3ObjectStore", "HttpxTransport", "UnityCatalogRestClient"]
