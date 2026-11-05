"""boto3 adapter for the object-storage port."""

from __future__ import annotations

import logging
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_MISSING_BUCKET_CODES = {"404", "NoSuchBucket", "NotFound"}


class Boto3ObjectStore:
    """S3-compatible adapter configured for RustFS path-style access."""

    def __init__(
        self,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        region: str,
        use_path_style: bool = True,
        client: Any | None = None,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region
        self.use_path_style = use_path_style
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            addressing_style = "path" if self.use_path_style else "virtual"
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region,
                config=Config(s3={"addressing_style": addressing_style}),
            )
        return self._client

    def ensure_bucket(self, bucket: str) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket)
            logger.debug("S3 bucket already exists: %s", bucket)
            return False
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in _MISSING_BUCKET_CODES or status == 404:
                self.client.create_bucket(Bucket=bucket)
                logger.info("Created S3 bucket: %s", bucket)
                return True
            raise

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str,
    ) -> None:
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )
        logger.info("Landed s3://%s/%s (%d bytes)", bucket, key, len(body))

    def copy_object(
        self,
        target_bucket: str,
        target_key: str,
        source_bucket: str,
        source_key: str,
    ) -> None:
        self.client.copy_object(
            Bucket=target_bucket,
            Key=target_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
        )
        logger.info(
            "Copied s3://%s/%s to s3://%s/%s",
            source_bucket,
            source_key,
            target_bucket,
            target_key,
        )
