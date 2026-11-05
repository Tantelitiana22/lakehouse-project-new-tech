# NDR Open Data Lakehouse

A 100% metadata-driven, asset-orchestrated Open Data Lakehouse. Ingestion,
landing-zone storage, Unity Catalog namespaces, Iceberg write behaviour and
Dagster assets are all generated from declarative YAML in
`orchestration/ingestion/src/ndr_lakehouse/asset_property/`.

## Architecture

```
HTTP / S3 sources
      │  (declarative YAML)
      ▼
landing/  ───────────────────────────► s3://landing-zone/<prefix>/ingest_date=YYYY-MM-DD/
  RetryingHttpClient + pagination strategies          │
  ExtractorFactory + LandingWriter                    ▼
                                             iceberg/
                                               NativeDatasetReader
                                               DataFrameTransformer (sanitize + dedup)
                                               WriteStrategyFactory (overwrite/append/merge)
                                               SchemaEvolutionService
                                             │
                        ┌────────────────────┴───────────────────┐
                        ▼                                        ▼
             Unity Catalog OSS (REST)                  Delta + UniForm Iceberg metadata
             catalog.schema.table                     on RustFS (s3://lakehouse/warehouse)
                        │
                        ▼
                  dbt (Spark Thrift)  ──►  lakehouse.gold.*
                        │
                        ▼
             MetricFlow semantic layer (prix_moyen_m2)
```

There is **no Hive Metastore**. Tables are written as Delta Lake with UniForm
enabled, which emits Iceberg metadata. Unity Catalog OSS remains available as a
read-only Iceberg REST catalog:

```
http://unity-catalog:8080/api/2.1/unity-catalog/iceberg
```

## Layering and SOLID boundaries

| Layer | Responsibility | Key patterns |
|---|---|---|
| `asset_property/` | YAML contracts and validation | Pydantic v2, declarative config |
| `landing/` | Pull bytes and persist native formats | Strategy (pagination), Factory (extractors), Adapter, Facade |
| `iceberg/` | Convert, sanitize, deduplicate and write Delta UniForm (Iceberg-compatible) | Strategy (write modes), Factory, Fluent Delta merge |
| `provisioning/` | Create RustFS buckets and UC namespaces idempotently | Service + Gateway |
| `orchestration/` | Dagster assets, resources, dbt glue | Factory, Dependency Injection |
| `domain/` + `infrastructure/` | Ports and concrete adapters | Dependency Inversion (Protocols) |

High-level use cases (`LandingIngestionService`, `IcebergLoadService`,
`ProvisioningService`) depend only on ports. `boto3`, `httpx` and Spark are
isolated in adapters, so the business layers can be tested without external
services.

## Repository layout

```
orchestration/ingestion/
├── src/ndr_lakehouse/
│   ├── __init__.py                 # re-exports defs from orchestration/definitions.py
│   ├── asset_property/             # platform.yaml + source YAMLs + Pydantic v2 contracts
│   ├── domain/                     # ports (Protocols) and domain errors
│   ├── infrastructure/             # boto3, httpx and Unity Catalog REST adapters
│   ├── landing/                    # extraction + native-format landing zone
│   │   ├── pagination/             # one strategy per pagination algorithm
│   │   ├── extractors.py
│   │   ├── writer.py
│   │   └── service.py
│   ├── iceberg/                    # conversion, dedup and Iceberg writes
│   │   ├── transformer.py
│   │   ├── writers.py
│   │   ├── schema.py
│   │   └── service.py
│   ├── provisioning/               # RustFS buckets + UC catalogs/schemas
│   └── orchestration/              # Dagster-only: assets, resources, definitions, dbt glue
└── tests/ndr_lakehouse/            # pytest + pytest-mock + coverage suite
dbt_lakehouse/                      # dedicated dbt + MetricFlow project
```

## Declarative contract

Every file in `asset_property/` (except `platform.yaml`) declares one or more
assets with two blocks:

* `source_properties` — URL, method, params, headers, native format, RustFS
  landing bucket/prefix and pagination strategy (`none`, `page_number`,
  `offset`, `cursor`, `next_url`, `link_header`).
* `catalog_properties` — Unity Catalog `catalog_name.schema_name.table_name`,
  `write_mode` (`overwrite`, `append`, `merge`), `primary_keys` (mandatory for
  merge), `dedup_order_by`, `auto_evolve_schema`, `partition_by`,
  `read_options` and `iceberg_tblproperties`.

Adding a source is a configuration-only change: the asset factory, Spark
loader and dbt `_sources.yml` sync discover it automatically. The generated
Dagster keys are always:

* landing: `AssetKey(["landing", "<asset_name>"])`
* Bronze:  `AssetKey(["<catalog>", "<schema>", "<table>"])`

## Quick start

```bash
cp .env.example .env
make up                 # RustFS, bucket init, Unity Catalog OSS, Spark Thrift, Dagster
open http://localhost:3000
make ingest             # materialize all generated landing + Bronze assets
make dbt-build          # build Gold Iceberg models
make mf-query           # query the prix_moyen_m2 ratio metric
```

Useful endpoints:

| Service | URL |
|---|---|
| Dagster | http://localhost:3000 |
| RustFS console | http://localhost:9001 |
| RustFS S3 API | http://localhost:9000 |
| Unity Catalog OSS | http://localhost:8080 |
| Iceberg REST API | http://localhost:8080/api/2.1/unity-catalog/iceberg |
| Spark Thrift | thrift://localhost:10000 |

## Tests

```bash
uv sync
uv run pytest
```

The suite uses `pytest`, `pytest-mock` (`mocker`) and `pytest-cov`. `httpx`,
`boto3` and Spark APIs are mocked, so **no network, Docker, RustFS, Unity
Catalog or Spark runtime is required**. Test paths, imports and the coverage
gate are enforced in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["orchestration/ingestion/tests/ndr_lakehouse"]
pythonpath = ["orchestration/ingestion/src"]
addopts = [
    "--cov=ndr_lakehouse",
    "--cov-report=term-missing",
    "--cov-fail-under=80",
]
```

`uv run pytest` fails if total coverage drops below **80%** (branch coverage is
enabled).

## Write semantics

* **overwrite / initial creation** — `DeltaTable.createOrReplace(...)` with all
  table properties, partitions, and UniForm enabled.
* **append** — `format("delta").mode("append").option("mergeSchema", "true")`.
* **merge** — the fluent Delta `DeltaTable.merge(...)` API on the configured
  composite primary key (no SQL MERGE string), with optional configured
  matched/not-matched-by-source delete conditions.
* **Iceberg compatibility** — every table receives
  `delta.enableIcebergCompatV2=true` and
  `delta.universalFormat.enabledFormats=iceberg`.
* **schema evolution** — `ALTER TABLE ... ADD COLUMN` for new sanitized columns.
* **deduplication** — `Window.partitionBy(*primary_keys).orderBy(dedup_order_by desc)`
  keeping `row_number() == 1`.
* **column hygiene** — `snake_case` sanitization plus `_ingested_at`,
  `_source_name`, `_source_file`.

## Semantic layer

`dbt_lakehouse/models/semantic/semantic_models.yml` defines the Gold semantic
models and the ratio metric:

```yaml
metrics:
  - name: prix_moyen_m2
    type: ratio
    type_params:
      numerator: volume_ventes_eur
      denominator: surface_vendue_m2
```

`fct_lots_batis.sql` pre-aggregates mutation totals in a deduplicated CTE
before the semantic layer consumes it, preventing fan-out of the ratio
denominator. The `metricflow_semantic_validation` Dagster asset runs
`mf validate-configs --skip-dw` after the Gold dbt assets materialize. The
`--skip-dw` flag is required because dbt-spark is not a MetricFlow warehouse
adapter; semantic graph validation (entities, dimensions, measures, metrics)
still runs in full.
