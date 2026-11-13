"""S3 storage abstraction for RustFS (boto3 implementation + in-memory test double)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


class S3StorageError(Exception):
    """Typed S3 storage error carrying an S3-compatible error code and HTTP status."""

    def __init__(self, code: str, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class S3ObjectItem:
    """Single object entry inside a bucket."""

    key: str
    size: int
    last_modified: str
    etag: str = ""


@dataclass(frozen=True)
class S3ListResult:
    """Result of listing a bucket with optional prefix and delimiter."""

    prefixes: list[str]
    objects: list[S3ObjectItem]


class S3StorageService(Protocol):
    """Interface used by the Bucket Explorer and the scoped S3 SigV4 proxy."""

    def list_buckets(self) -> list[str]: ...

    def create_bucket(self, bucket: str) -> None: ...

    def bucket_exists(self, bucket: str) -> bool: ...

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
    ) -> S3ListResult: ...

    def get_object(self, bucket: str, key: str) -> tuple[bytes, str]: ...

    def head_object(self, bucket: str, key: str) -> tuple[int, str, str]: ...

    def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str: ...

    def delete_object(self, bucket: str, key: str) -> None: ...


class Boto3S3Service:
    """Production S3 storage backend talking to RustFS over path-style S3."""

    def __init__(
        self,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
            config=BotoConfig(s3={"addressing_style": "path"}, signature_version="s3v4"),
        )

    def list_buckets(self) -> list[str]:
        resp = self._client.list_buckets()
        return sorted(bucket["Name"] for bucket in resp.get("Buckets", []))

    def create_bucket(self, bucket: str) -> None:
        try:
            self._client.create_bucket(Bucket=bucket)
        except ClientError as exc:
            error = exc.response.get("Error", {})
            raise S3StorageError(
                error.get("Code", "InternalError"),
                error.get("Message", str(exc)),
                int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 400)),
            ) from exc

    def bucket_exists(self, bucket: str) -> bool:
        try:
            self._client.head_bucket(Bucket=bucket)
            return True
        except ClientError:
            return False

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
    ) -> S3ListResult:
        try:
            kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": max_keys}
            if delimiter:
                kwargs["Delimiter"] = delimiter
            resp = self._client.list_objects_v2(**kwargs)
        except ClientError as exc:
            error = exc.response.get("Error", {})
            raise S3StorageError(
                error.get("Code", "NoSuchBucket"),
                error.get("Message", str(exc)),
                int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 404)),
            ) from exc

        prefixes = [item["Prefix"] for item in resp.get("CommonPrefixes", [])]
        objects = [
            S3ObjectItem(
                key=obj["Key"],
                size=int(obj.get("Size", 0)),
                last_modified=obj["LastModified"].astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                etag=str(obj.get("ETag", "")).strip('"'),
            )
            for obj in resp.get("Contents", [])
            if obj["Key"] != prefix
        ]
        return S3ListResult(prefixes=prefixes, objects=objects)

    def get_object(self, bucket: str, key: str) -> tuple[bytes, str]:
        try:
            resp = self._client.get_object(Bucket=bucket, Key=key)
            return resp["Body"].read(), resp.get("ContentType", "application/octet-stream")
        except ClientError as exc:
            error = exc.response.get("Error", {})
            raise S3StorageError(
                error.get("Code", "NoSuchKey"),
                error.get("Message", str(exc)),
                int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 404)),
            ) from exc

    def head_object(self, bucket: str, key: str) -> tuple[int, str, str]:
        try:
            resp = self._client.head_object(Bucket=bucket, Key=key)
            return (
                int(resp.get("ContentLength", 0)),
                resp.get("ContentType", "application/octet-stream"),
                str(resp.get("ETag", "")).strip('"'),
            )
        except ClientError as exc:
            error = exc.response.get("Error", {})
            raise S3StorageError(
                error.get("Code", "NoSuchKey"),
                error.get("Message", str(exc)),
                int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 404)),
            ) from exc

    def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        try:
            resp = self._client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
            return str(resp.get("ETag", "")).strip('"')
        except ClientError as exc:
            error = exc.response.get("Error", {})
            raise S3StorageError(
                error.get("Code", "InternalError"),
                error.get("Message", str(exc)),
                int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 400)),
            ) from exc

    def delete_object(self, bucket: str, key: str) -> None:
        try:
            self._client.delete_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            error = exc.response.get("Error", {})
            raise S3StorageError(
                error.get("Code", "InternalError"),
                error.get("Message", str(exc)),
                int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 400)),
            ) from exc


class InMemoryS3Service:
    """Deterministic in-memory S3 backend used by the test suite."""

    def __init__(self, initial_buckets: Iterable[str] = ()) -> None:
        self._buckets: dict[str, dict[str, tuple[bytes, str, str, str]]] = {name: {} for name in initial_buckets}

    def list_buckets(self) -> list[str]:
        return sorted(self._buckets)

    def create_bucket(self, bucket: str) -> None:
        if bucket in self._buckets:
            return
        self._buckets[bucket] = {}

    def bucket_exists(self, bucket: str) -> bool:
        return bucket in self._buckets

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
    ) -> S3ListResult:
        if bucket not in self._buckets:
            raise S3StorageError("NoSuchBucket", f"The specified bucket does not exist: {bucket}", 404)
        prefixes_set: set[str] = set()
        items: list[S3ObjectItem] = []
        for key in sorted(self._buckets[bucket]):
            if not key.startswith(prefix):
                continue
            remainder = key[len(prefix) :]
            if delimiter and delimiter in remainder:
                sub_prefix = prefix + remainder.split(delimiter, 1)[0] + delimiter
                prefixes_set.add(sub_prefix)
                continue
            data, _ct, last_modified, etag = self._buckets[bucket][key]
            items.append(S3ObjectItem(key=key, size=len(data), last_modified=last_modified, etag=etag))
            if len(items) >= max_keys:
                break
        return S3ListResult(prefixes=sorted(prefixes_set), objects=items)

    def get_object(self, bucket: str, key: str) -> tuple[bytes, str]:
        if bucket not in self._buckets:
            raise S3StorageError("NoSuchBucket", f"The specified bucket does not exist: {bucket}", 404)
        entry = self._buckets[bucket].get(key)
        if entry is None:
            raise S3StorageError("NoSuchKey", f"The specified key does not exist: {key}", 404)
        return entry[0], entry[1]

    def head_object(self, bucket: str, key: str) -> tuple[int, str, str]:
        if bucket not in self._buckets:
            raise S3StorageError("NoSuchBucket", f"The specified bucket does not exist: {bucket}", 404)
        entry = self._buckets[bucket].get(key)
        if entry is None:
            raise S3StorageError("NoSuchKey", f"The specified key does not exist: {key}", 404)
        return len(entry[0]), entry[1], entry[3]

    def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        if bucket not in self._buckets:
            raise S3StorageError("NoSuchBucket", f"The specified bucket does not exist: {bucket}", 404)
        etag = hashlib.md5(data, usedforsecurity=False).hexdigest()
        ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._buckets[bucket][key] = (data, content_type, ts, etag)
        return etag

    def delete_object(self, bucket: str, key: str) -> None:
        if bucket not in self._buckets:
            raise S3StorageError("NoSuchBucket", f"The specified bucket does not exist: {bucket}", 404)
        self._buckets[bucket].pop(key, None)
