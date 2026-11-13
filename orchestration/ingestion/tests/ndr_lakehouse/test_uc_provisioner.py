"""Tests for the idempotent RustFS + Unity Catalog provisioner."""

from __future__ import annotations

import pytest
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
    # Given
    mock_s3.head_bucket.side_effect = _missing_bucket_error()

    # When
    created = _resource().ensure_bucket("landing-zone")

    # Then
    assert created is True
    mock_s3.head_bucket.assert_called_once_with(Bucket="landing-zone")
    mock_s3.create_bucket.assert_called_once_with(Bucket="landing-zone")


def test_existing_bucket_is_not_recreated(mock_s3):
    # Given
    resource = _resource()

    # When
    created = resource.ensure_bucket("landing-zone")

    # Then
    assert created is False
    mock_s3.head_bucket.assert_called_once_with(Bucket="landing-zone")
    mock_s3.create_bucket.assert_not_called()


def test_ensure_buckets_provisions_both_buckets(mocker, mock_s3):
    # Given
    mock_s3.head_bucket.side_effect = [None, _missing_bucket_error()]

    # When
    result = _resource().ensure_buckets()

    # Then
    assert result == {"landing-zone": False, "lakehouse": True}
    assert mock_s3.create_bucket.call_count == 1


def test_uc_catalog_and_schema_are_created_on_404(mocker):
    # Given
    http_client = mocker.MagicMock(name="httpx-client")
    http_client.get.return_value = mocker.Mock(status_code=404)
    http_client.post.return_value = mocker.Mock(status_code=200)
    mocker.patch("httpx.Client", return_value=http_client)
    resource = _resource()

    # When
    result = resource.ensure_catalog_and_schema("lakehouse", "bronze")

    # Then
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
    # Given
    http_client = mocker.MagicMock(name="httpx-client")
    http_client.get.return_value = mocker.Mock(status_code=200)
    mocker.patch("httpx.Client", return_value=http_client)

    # When
    result = _resource().ensure_catalog_and_schema("lakehouse", "bronze")

    # Then
    assert result == {"catalog_created": False, "schema_created": False}
    http_client.post.assert_not_called()


def test_uc_409_is_treated_as_success(mocker):
    # Given
    http_client = mocker.MagicMock(name="httpx-client")
    http_client.get.return_value = mocker.Mock(status_code=404)
    http_client.post.return_value = mocker.Mock(status_code=409)
    mocker.patch("httpx.Client", return_value=http_client)

    # When
    result = _resource().ensure_catalog_and_schema("lakehouse", "bronze")

    # Then
    assert result == {"catalog_created": False, "schema_created": False}


def test_uc_columns_from_spark_and_delta_schema():
    # Given
    import json
    from types import SimpleNamespace

    from ndr_lakehouse.provisioning.uc_columns import (
        columns_from_delta_schema_json,
        columns_from_spark_schema,
    )

    spark_schema = SimpleNamespace(
        fields=[
            SimpleNamespace(name="id", dataType=SimpleNamespace(simpleString=lambda: "bigint"), nullable=False),
            SimpleNamespace(
                name="price", dataType=SimpleNamespace(simpleString=lambda: "decimal(10,2)"), nullable=True
            ),
            SimpleNamespace(
                name="coords", dataType=SimpleNamespace(simpleString=lambda: "array<double>"), nullable=True
            ),
        ]
    )
    delta_schema = json.dumps(
        {
            "type": "struct",
            "fields": [
                {"name": "code", "type": "string", "nullable": True, "metadata": {}},
                {"name": "centre", "type": {"type": "struct", "fields": []}, "nullable": True, "metadata": {}},
            ],
        }
    )

    # When
    spark_cols = columns_from_spark_schema(spark_schema)
    delta_cols = columns_from_delta_schema_json(delta_schema)

    # Then
    assert [c["type_name"] for c in spark_cols] == ["LONG", "DECIMAL", "ARRAY"]
    assert spark_cols[1]["type_precision"] == 10
    assert spark_cols[1]["type_scale"] == 2
    assert spark_cols[0]["nullable"] is False
    assert [c["type_name"] for c in delta_cols] == ["STRING", "STRUCT"]


@pytest.mark.parametrize(
    ("spark_type", "expected_uc_type", "expected_delta_json_type"),
    [
        ("int", "INT", "integer"),
        ("bigint", "LONG", "long"),
        ("smallint", "SHORT", "short"),
        ("tinyint", "BYTE", "byte"),
        ("struct<x:double,y:double>", "STRUCT", {"type": "struct", "fields": []}),
        ("array<string>", "ARRAY", {"type": "array", "elementType": "string", "containsNull": True}),
    ],
)
def test_uc_columns_maps_delta_type_json_for_uc_oss(
    spark_type: str,
    expected_uc_type: str,
    expected_delta_json_type: object,
):
    # Given
    import json
    from types import SimpleNamespace

    from ndr_lakehouse.provisioning.uc_columns import columns_from_spark_schema

    schema = SimpleNamespace(
        fields=[SimpleNamespace(name="col", dataType=SimpleNamespace(simpleString=lambda: spark_type), nullable=True)]
    )

    # When
    [column] = columns_from_spark_schema(schema)
    parsed_type_json = json.loads(column["type_json"])

    # Then
    assert column["type_name"] == expected_uc_type
    assert parsed_type_json["type"] == expected_delta_json_type


def test_uc_columns_prefers_spark_json_value_when_available():
    # Given
    import json
    from types import SimpleNamespace

    from ndr_lakehouse.provisioning.uc_columns import columns_from_spark_schema

    native_struct = {
        "name": "centre",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "coordinates",
                    "type": {"type": "array", "elementType": "double", "containsNull": True},
                    "nullable": True,
                    "metadata": {},
                },
            ],
        },
        "nullable": True,
        "metadata": {},
    }
    schema = SimpleNamespace(jsonValue=lambda: {"type": "struct", "fields": [native_struct]})

    # When
    [column] = columns_from_spark_schema(schema)

    # Then
    assert column["name"] == "centre"
    assert column["type_name"] == "STRUCT"
    assert json.loads(column["type_json"]) == native_struct


def test_uc_ensure_table_refreshes_existing_table_and_registers_from_storage(mocker, mock_s3):
    # Given
    import json

    http_client = mocker.MagicMock(name="httpx-client")
    # catalog exists, schema exists, table exists -> delete + create
    http_client.get.return_value = mocker.Mock(status_code=200)
    http_client.delete.return_value = mocker.Mock(status_code=200)
    http_client.post.return_value = mocker.Mock(status_code=200)
    mocker.patch("httpx.Client", return_value=http_client)

    schema_json = json.dumps(
        {
            "type": "struct",
            "fields": [{"name": "commune_code", "type": "string", "nullable": True, "metadata": {}}],
        }
    )
    commit_body = json.dumps({"metaData": {"schemaString": schema_json}}).encode("utf-8")
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "warehouse/gold.db/dim_communes/_delta_log/00000000000000000001.json"}],
        "IsTruncated": False,
    }
    mock_s3.get_object.return_value = {"Body": mocker.Mock(read=lambda: commit_body)}

    # When
    created = _resource().register_delta_table_from_storage(
        "real_estate_77",
        "gold",
        "dim_communes",
        comment="Gold communes",
    )

    # Then
    assert created is True
    http_client.delete.assert_called_once_with("/api/2.1/unity-catalog/tables/real_estate_77.gold.dim_communes")
    http_client.post.assert_called_once()
    posted = http_client.post.call_args.kwargs["json"]
    assert posted["catalog_name"] == "real_estate_77"
    assert posted["schema_name"] == "gold"
    assert posted["name"] == "dim_communes"
    assert posted["table_type"] == "EXTERNAL"
    assert posted["data_source_format"] == "DELTA"
    assert posted["storage_location"] == "s3://lakehouse/warehouse/gold.db/dim_communes"
    assert posted["columns"][0]["name"] == "commune_code"
