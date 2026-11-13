"""Portal configuration and static service registry."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceDefinition:
    """Metadata describing a lakehouse web service routed through the gateway."""

    slug: str
    name: str
    subtitle: str
    description: str
    subdomain: str
    internal_url: str


SERVICES: tuple[ServiceDefinition, ...] = (
    ServiceDefinition(
        slug="dagster",
        name="Dagster",
        subtitle="Orchestration & Assets",
        description="Materialize ingestion, dbt, and semantic assets and inspect scheduled runs.",
        subdomain="dagster",
        internal_url="http://dagster-webserver:3000/server_info",
    ),
    ServiceDefinition(
        slug="unity-catalog",
        name="Unity Catalog",
        subtitle="Data Catalog & Governance",
        description="Browse catalogs, schemas, Iceberg/Delta tables, and storage volumes.",
        subdomain="uc",
        internal_url="http://unity-catalog-ui:3000/",
    ),
    ServiceDefinition(
        slug="spark-ui",
        name="Spark Thrift UI",
        subtitle="Compute & SQL Queries",
        description="Inspect active Spark SQL queries, stages, executors, and job timelines.",
        subdomain="spark",
        internal_url="http://spark-thrift:4040/",
    ),
    ServiceDefinition(
        slug="rustfs-console",
        name="RustFS Console",
        subtitle="Storage Admin Console",
        description="Full administrative object-storage console for RustFS (admins only by default).",
        subdomain="rustfs",
        internal_url="http://rustfs:9001/",
    ),
)

SERVICE_BY_SLUG: dict[str, ServiceDefinition] = {svc.slug: svc for svc in SERVICES}


@dataclass(frozen=True)
class PortalConfig:
    """Runtime settings for the Lakehouse Access Management Portal."""

    database_url: str
    secret_key: str
    admin_user: str
    admin_password: str
    gateway_port: int
    base_domain: str
    s3_endpoint_url: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region: str

    @classmethod
    def from_env(cls) -> PortalConfig:
        """Build configuration from environment variables with local-dev defaults."""
        return cls(
            database_url=os.environ.get(
                "PORTAL_DATABASE_URL",
                "postgresql+psycopg2://dagster:dagster@postgres:5432/portal",
            ),
            secret_key=os.environ.get("PORTAL_SECRET_KEY", "ndr-lakehouse-dev-portal-secret-change-me"),
            admin_user=os.environ.get("PORTAL_ADMIN_USER", "admin"),
            admin_password=os.environ.get("PORTAL_ADMIN_PASSWORD", "admin"),
            gateway_port=int(os.environ.get("PORTAL_GATEWAY_PORT", "8088")),
            base_domain=os.environ.get("PORTAL_BASE_DOMAIN", "localhost"),
            s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://rustfs:9000"),
            s3_access_key_id=os.environ.get("S3_ACCESS_KEY_ID", "rustfsadmin"),
            s3_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY", "rustfssecret"),
            s3_region=os.environ.get("S3_REGION", "us-east-1"),
        )

    def service_url(self, service: ServiceDefinition) -> str:
        """Return the external browser URL for a service through the Nginx gateway."""
        return f"http://{service.subdomain}.{self.base_domain}:{self.gateway_port}/"

    @property
    def portal_origin(self) -> str:
        """Return the primary origin for the portal web interface."""
        return f"http://portal.{self.base_domain}:{self.gateway_port}"

    @property
    def s3_gateway_url(self) -> str:
        """Return the external S3 gateway endpoint URL for CLI/SDK clients."""
        return f"http://s3.{self.base_domain}:{self.gateway_port}"
