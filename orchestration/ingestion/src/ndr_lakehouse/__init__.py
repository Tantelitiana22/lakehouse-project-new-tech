"""ndr_lakehouse: layered, metadata-driven Open Data Lakehouse package.

Layers:
- ``asset_property``  declarative contracts and YAML
- ``landing``         extraction + native-format persistence
- ``iceberg``         conversion, deduplication and Iceberg writes
- ``provisioning``    RustFS buckets + Unity Catalog namespaces
- ``orchestration``   Dagster assets, resources and dbt/MetricFlow glue
- ``domain`` / ``infrastructure``  ports and concrete adapters
"""

from ndr_lakehouse.orchestration.definitions import build_definitions, defs

__all__ = ["build_definitions", "defs"]
__version__ = "0.2.0"
