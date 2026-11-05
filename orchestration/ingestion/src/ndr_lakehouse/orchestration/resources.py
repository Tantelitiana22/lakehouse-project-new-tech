"""Dagster resource bundle construction."""

from __future__ import annotations

import logging
import shutil

from ndr_lakehouse.asset_property.schema_validator import (
    PlatformConfig,
    load_platform_config,
)
from ndr_lakehouse.iceberg.resource import SparkIcebergResource
from ndr_lakehouse.landing.resource import NativeIngestorResource
from ndr_lakehouse.orchestration.paths import ASSET_PROPERTY_DIR, DBT_PROJECT_DIR
from ndr_lakehouse.provisioning.resource import UnityCatalogProvisionerResource

logger = logging.getLogger(__name__)

DEFAULT_PLATFORM = load_platform_config(ASSET_PROPERTY_DIR / "platform.yaml")


def build_resource_bundle(
    platform: PlatformConfig | None = None,
) -> dict[str, object]:
    """Instantiate one shared resource bundle for the whole graph."""

    platform = platform or DEFAULT_PLATFORM
    storage = platform.storage
    unity = platform.unity_catalog
    spark = platform.spark

    provisioner = UnityCatalogProvisionerResource(
        endpoint_url=storage.endpoint_url,
        access_key_id=storage.access_key_id,
        secret_access_key=storage.secret_access_key,
        region=storage.region,
        landing_bucket=storage.landing_bucket,
        lakehouse_bucket=storage.lakehouse_bucket,
        uc_base_url=unity.base_url,
        uc_api_version=unity.api_version,
        uc_storage_root=unity.storage_root,
    )
    ingestor = NativeIngestorResource(
        endpoint_url=storage.endpoint_url,
        access_key_id=storage.access_key_id,
        secret_access_key=storage.secret_access_key,
        region=storage.region,
        landing_bucket=storage.landing_bucket,
    )
    spark_resource = SparkIcebergResource(
        endpoint_url=storage.endpoint_url,
        access_key_id=storage.access_key_id,
        secret_access_key=storage.secret_access_key,
        region=storage.region,
        uc_iceberg_uri=unity.iceberg_rest_uri,
        warehouse=spark.warehouse,
        master=spark.master,
        app_name=spark.app_name,
        catalog_name=unity.catalog_name,
    )

    resources: dict[str, object] = {
        "uc_provisioner": provisioner,
        "native_ingestor": ingestor,
        "spark_iceberg": spark_resource,
    }

    try:
        from dagster_dbt import DbtCliResource

        if shutil.which("dbt"):
            resources["dbt"] = DbtCliResource(project_dir=str(DBT_PROJECT_DIR))
        else:
            logger.info("dbt executable not found; dbt assets will be skipped")
    except Exception as exc:
        logger.warning("DbtCliResource unavailable: %s", exc)

    return resources
