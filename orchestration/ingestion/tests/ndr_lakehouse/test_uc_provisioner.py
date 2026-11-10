"""Tests for the idempotent RustFS + Unity Catalog provisioner."""

from __future__ import annotations

from botocore.exceptions import ClientError

from ndr_lakehouse.provisioning.resource import (
    UnityCatalogProvisionerResource,
)


def _resource() -> UnityCatalogProvisionerResource:
    return UnityCatalogProvisionerResource(
        endpoint_url="http://rustfs:9000",
        access_key_id="rustfsadmin",
        secret_access_key="rustfssecret",
        region="us-east-1",
        landing_bucket="landing-zone",
        lakehouse_bucket="lakehouse",
        uc_base_url="http://unity-catalog:8080",
        uc_storage_root="s3://lakehouse/warehouse",
    )


def _missing_bucket_error() -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "404", "Message": "Not Found"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        "HeadBucket",
    )


def test_missing_bucket_is_created(mocker, mock_s3):
    mock_s3.head_bucket.side_effect = _missing_bucket_error()

    created = _resource().ensure_bucket("landing-zone")

    assert created is True
    mock_s3.head_bucket.assert_called_once_with(Bucket="landing-zone")
    mock_s3.create_bucket.assert_called_once_with(Bucket="landing-zone")


def test_existing_bucket_is_not_recreated(mock_s3):
    created = _resource().ensure_bucket("landing-zone")

    assert created is False
    mock_s3.head_bucket.assert_called_once_with(Bucket="landing-zone")
    mock_s3.create_bucket.assert_not_called()


def test_ensure_buckets_provisions_both_buckets(mocker, mock_s3):
    mock_s3.head_bucket.side_effect = [None, _missing_bucket_error()]

    result = _resource().ensure_buckets()

    assert result == {"landing-zone": False, "lakehouse": True}
    assert mock_s3.create_bucket.call_count == 1


def test_uc_catalog_and_schema_are_created_on_404(mocker):
    http_client = mocker.MagicMock(name="httpx-client")
    http_client.get.return_value = mocker.Mock(status_code=404)
    http_client.post.return_value = mocker.Mock(status_code=200)
    mocker.patch("httpx.Client", return_value=http_client)

    resource = _resource()
    result = resource.ensure_catalog_and_schema("lakehouse", "bronze")

    assert result == {"catalog_created": True, "schema_created": True}
    http_client.post.assert_any_call(
        "/api/2.1/unity-catalog/catalogs",
        json={
            "name": "lakehouse",
            "comment": "Managed by ndr_lakehouse: lakehouse",
            "storage_root": "s3://lakehouse/warehouse/lakehouse",
        },
    )
    http_client.post.assert_any_call(
        "/api/2.1/unity-catalog/schemas",
        json={
            "name": "bronze",
            "catalog_name": "lakehouse",
            "comment": "Managed by ndr_lakehouse: lakehouse.bronze",
        },
    )
    http_client.get.assert_any_call("/api/2.1/unity-catalog/catalogs/lakehouse")
    http_client.get.assert_any_call("/api/2.1/unity-catalog/schemas/lakehouse.bronze")
    http_client.close.assert_called()


def test_uc_existing_catalog_and_schema_are_idempotent(mocker):
    http_client = mocker.MagicMock(name="httpx-client")
    http_client.get.return_value = mocker.Mock(status_code=200)
    mocker.patch("httpx.Client", return_value=http_client)

    result = _resource().ensure_catalog_and_schema("lakehouse", "bronze")

    assert result == {"catalog_created": False, "schema_created": False}
    http_client.post.assert_not_called()


def test_uc_409_is_treated_as_success(mocker):
    http_client = mocker.MagicMock(name="httpx-client")
    http_client.get.return_value = mocker.Mock(status_code=404)
    http_client.post.return_value = mocker.Mock(status_code=409)
    mocker.patch("httpx.Client", return_value=http_client)

    result = _resource().ensure_catalog_and_schema("lakehouse", "bronze")

    assert result == {"catalog_created": False, "schema_created": False}
