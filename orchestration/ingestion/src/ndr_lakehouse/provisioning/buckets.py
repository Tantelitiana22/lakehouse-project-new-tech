"""Idempotent S3 bucket provisioning."""

from __future__ import annotations

from collections.abc import Iterable

from ndr_lakehouse.domain.ports import ObjectStore


class BucketProvisioner:
    def __init__(self, object_store: ObjectStore, buckets: Iterable[str]) -> None:
        self.object_store = object_store
        self.buckets = tuple(dict.fromkeys(buckets))

    def ensure(self, bucket: str) -> bool:
        return self.object_store.ensure_bucket(bucket)

    def ensure_all(self) -> dict[str, bool]:
        return {bucket: self.ensure(bucket) for bucket in self.buckets}
