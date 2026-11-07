# NDR Open Data Lakehouse

A 100% metadata-driven, asset-orchestrated Open Data Lakehouse. Ingestion,
landing-zone storage, Unity Catalog namespaces, Iceberg write behaviour and
Dagster assets are all generated from declarative YAML.

Platform code lives in `orchestration/ingestion/src/ndr_lakehouse/`; the YAML
that describes *what* to ingest lives in `use_case/`, one directory per use case.
Adding a data source or a new business domain means adding a directory under
`use_case/` — the library does not change.

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
| `asset_property/` | Pydantic v2 contracts that validate the YAML in `use_case/` | Pydantic v2, declarative config |
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
use_case/                           # Dagster entrypoint + situated configuration
├── __init__.py                     # module-level defs = build_definitions()
└── real_estate_77/
    ├── use_case.yaml               # manifest: assets dir, platform, dbt, gold models
    └── assets/                     # platform.yaml + source YAMLs for this use case
orchestration/ingestion/
├── src/ndr_lakehouse/
│   ├── __init__.py                 # generic library package
│   ├── asset_property/             # Pydantic v2 contracts and YAML validation
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

Adding a source to an existing use case is a configuration-only change: the
asset factory, Spark loader and dbt `_sources.yml` sync discover it
automatically. The generated Dagster keys are always:

* landing: `AssetKey(["landing", "<asset_name>"])`
* Bronze:  `AssetKey(["<catalog>", "<schema>", "<table>"])`

## Use cases

A use case is a directory under `use_case/` holding everything that is specific
to one business situation — which sources to pull, which dbt models are Gold,
which metric is the headline number. The generic machinery stays in
`ndr_lakehouse/` and is reused as-is.

```
use_case/
├── __init__.py                 # defs = build_definitions()
└── real_estate_77/
    ├── use_case.yaml           # manifest
    └── assets/
        ├── platform.yaml
        ├── dvf_mutations_77.yaml
        └── ...
```

The manifest keys (`ndr_lakehouse.asset_property.use_case.UseCaseConfig`):

| Key | Meaning |
|---|---|
| `name` | Use-case identifier; also the Dagster code-location name. |
| `description` | Free text, shown in tooling. |
| `assets_dir` | Directory holding the per-asset YAML. Relative to the manifest. |
| `platform_file` | Shared platform/Spark settings YAML. Relative to the manifest. |
| `dbt_project_dir` | The dbt project this use case builds its Gold layer in. |
| `dbt_sources_path` | Where `_sources.yml` is regenerated inside that project. |
| `gold_models` | Gold model names to register as Dagster dbt assets. |
| `semantic_metric` | MetricFlow metric to expose as an asset. |
| `default` | Marks the use case loaded when none is named explicitly. |

`use_case/__init__.py` calls `build_definitions()` at import time, which resolves
the default use case, builds the ingestion assets from its YAML, provisions the
Spark/ingestion/UC resources and — if `dbt_lakehouse/target/manifest.json` exists
— registers the dbt and semantic assets too.

### Escape hatch: per-use-case Python

When a source needs behaviour the YAML cannot express, do not fork the library.
Give the asset a `handler` — a *top-level* key next to `name` and
`source_properties`, taking a `module:Class` dotted path:

```yaml
name: dvf_mutations_77
handler: real_estate_77.handlers.MutationsHandler
source_properties:
  ...
catalog_properties:
  ...
```

The named class is imported and instantiated with no arguments, and must satisfy
`ndr_lakehouse.domain.ports.UseCaseHandler` — `land(context, config)` returning a
landing URI or `None`, and `load(context, config, landing_uri)` returning a
summary dict or `None`. The two halves behave differently: a `None` from `land`
falls back to the generic ingestor, while `load` still runs the generic Spark
load and merges any non-empty dict you return over its summary. `HandlerContext`
carries the `landing` and `iceberg` ports plus the platform settings.

The library still owns the contract; the use case owns only the exotic part. Use
cases are plain Python, so Dagster runs them in-process; they are deliberately
*not* a wheel package (`use_case/` is on `PYTHONPATH`, not installed by
`uv sync`), which keeps the library free of business vocabulary.

New use case checklist: create `use_case/<name>/use_case.yaml` with
`default: false`, drop the asset YAML in its `assets/` directory, and serve it
from its own code location — `dagster dev -m use_case` loads the `default: true`
one.

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
