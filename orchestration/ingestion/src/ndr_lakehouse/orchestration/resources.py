"""Dagster resource bundle construction."""

from __future__ import annotations

import logging

from ndr_lakehouse.asset_property.schema_validator import load_platform_config
from ndr_lakehouse.asset_property.use_case import UseCaseConfig
from ndr_lakehouse.iceberg.resource import SparkIcebergResource
from ndr_lakehouse.landing.resource import NativeIngestorResource
from ndr_lakehouse.provisioning.resource import UnityCatalogProvisionerResource

logger = logging.getLogger(__name__)


def build_resource_bundle(
    use_case: UseCaseConfig,
    *,
    register_dbt: bool = True,
) -> dict[str, object]:
    """Instantiate the shared resource bundle for one use case.

    There is no default platform: the platform file always comes from the
    use case manifest, and a missing manifest is a configuration error rather
    than something to paper over with hardcoded defaults.
    """

    platform = load_platform_config(use_case.platform_path)
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

    if not register_dbt:
        return resources

    project_dir = use_case.dbt_project_path
    if not (project_dir / "dbt_project.yml").is_file():
        logger.warning(
            "dbt project not found at %s (missing dbt_project.yml); "
            "the dbt resource will be skipped",
            project_dir,
        )
        return resources

    try:
        from dagster_dbt import DbtCliResource
    except Exception as exc:
        logger.warning("DbtCliResource unavailable: %s", exc)
        return resources

    resources["dbt"] = DbtCliResource(project_dir=str(project_dir))
    return resources
