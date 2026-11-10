"""SparkSession factory configured for Delta UniForm + Iceberg REST access."""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass
from typing import Any

from ndr_lakehouse.domain.ports import SparkSessionProvider


@dataclass(frozen=True)
class SparkSettings:
    endpoint_url: str = "http://rustfs:9000"
    access_key_id: str = "rustfsadmin"
    secret_access_key: str = "rustfssecret"
    region: str = "us-east-1"
    uc_iceberg_uri: str = "http://unity-catalog:8080/api/2.1/unity-catalog/iceberg"
    warehouse: str = "s3://lakehouse/warehouse"
    master: str = "local[*]"
    app_name: str = "lakehouse-ingestion"
    use_path_style: bool = True
    spark_jars_dir: str = "/opt/spark-jars"


class SparkSessionFactory(SparkSessionProvider):
    """Factory that owns Spark configuration and session construction."""

    def __init__(self, settings: SparkSettings) -> None:
        self.settings = settings

    def configs(self, catalog_name: str) -> dict[str, str]:
        settings = self.settings
        catalog = f"spark.sql.catalog.{catalog_name}"
        iceberg_catalog = "spark.sql.catalog.iceberg_rest"
        configs = {
            # Delta is the physical format; UniForm emits Iceberg metadata.
            "spark.sql.extensions": (
                "io.delta.sql.DeltaSparkSessionExtension,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            "spark.sql.defaultCatalog": catalog_name,
            catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            f"{catalog}.warehouse": settings.warehouse,
            "spark.sql.catalog.spark_catalog": ("org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            # Optional read-only Unity Catalog Iceberg REST catalog.
            iceberg_catalog: "org.apache.iceberg.spark.SparkCatalog",
            f"{iceberg_catalog}.type": "rest",
            f"{iceberg_catalog}.uri": settings.uc_iceberg_uri,
            f"{iceberg_catalog}.warehouse": settings.warehouse,
            f"{iceberg_catalog}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"{iceberg_catalog}.s3.endpoint": settings.endpoint_url,
            f"{iceberg_catalog}.s3.path-style-access": str(settings.use_path_style).lower(),
            f"{iceberg_catalog}.s3.access-key-id": settings.access_key_id,
            f"{iceberg_catalog}.s3.secret-access-key": settings.secret_access_key,
            f"{iceberg_catalog}.s3.region": settings.region,
            # RustFS S3A access for Delta/Spark file IO.
            "spark.hadoop.fs.s3a.endpoint": settings.endpoint_url,
            "spark.hadoop.fs.s3a.access.key": settings.access_key_id,
            "spark.hadoop.fs.s3a.secret.key": settings.secret_access_key,
            "spark.hadoop.fs.s3a.path.style.access": str(settings.use_path_style).lower(),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": ("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
            "spark.sql.warehouse.dir": settings.warehouse,
        }
        jars = sorted(glob.glob(os.path.join(settings.spark_jars_dir, "*.jar")))
        if jars:
            configs["spark.jars"] = ",".join(jars)
        return configs

    def create(self, catalog_name: str | None = None) -> Any:
        from pyspark.sql import SparkSession

        selected = catalog_name or "lakehouse"
        builder = SparkSession.builder.appName(self.settings.app_name).master(self.settings.master)
        for key, value in self.configs(selected).items():
            builder = builder.config(key, value)
        return builder.getOrCreate()
