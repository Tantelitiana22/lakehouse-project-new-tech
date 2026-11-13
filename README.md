# NDR Open Data Lakehouse

[![CI](https://github.com/Tantelitiana22/lakehouse-project-new-tech/actions/workflows/ci.yml/badge.svg)](https://github.com/Tantelitiana22/lakehouse-project-new-tech/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12-3776AB?logo=python&logoColor=white)
![Dagster](https://img.shields.io/badge/Dagster-1.13-4F43DD?logo=dagster&logoColor=white)
![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?logo=pydantic&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark-4.1.1-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-4.3.1%20%2B%20UniForm-00ADD4)
![Apache Iceberg](https://img.shields.io/badge/Iceberg-1.12.0-3B7DDD)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-OSS-1B3139)
![dbt](https://img.shields.io/badge/dbt-2.0.11-FF694B?logo=dbt&logoColor=white)
![MetricFlow](https://img.shields.io/badge/Semantic%20Layer-MetricFlow-FF694B)
![RustFS](https://img.shields.io/badge/Storage-RustFS%20(S3)-DEA584)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

A 100% metadata-driven, asset-orchestrated Open Data Lakehouse. Ingestion,
landing-zone storage, Unity Catalog namespaces, Iceberg write behaviour and
Dagster assets are all generated from declarative YAML.

Platform code lives in `orchestration/ingestion/src/ndr_lakehouse/`; the YAML
that describes *what* to ingest lives in `use_case/`, one directory per use case.
Adding a data source or a new business domain means adding a directory under
`use_case/` — the library does not change.

## Tech stack

| Layer | Technology | Notes |
|---|---|---|
| Language | Python 3.11 / 3.12 | pinned in `pyproject.toml` |
| Orchestration | Dagster 1.13 (`dagster-webserver`, `dagster-daemon`, `dagster-postgres`) | assets, jobs, schedules, UI |
| Declarative contracts | Pydantic v2 + PyYAML | validates every YAML file at load time |
| Ingestion | `httpx` + `boto3` | retries, pagination strategies, S3/HTTP extractors |
| Object storage | RustFS | S3-compatible landing zone and warehouse |
| Table format | Delta Lake 4.3.1 with UniForm | writes Iceberg metadata in the same commit |
| Compute | Apache Spark 4.1.1 (Scala 2.13, Java 17) | in-process writer + Thrift server for dbt |
| Catalog | Unity Catalog OSS (REST) + UI | `catalog.schema.table`, Iceberg REST reads |
| Transformation | dbt 2.0.11 (Spark adapter) | staging, Gold and semantic models |
| Semantic layer | dbt Semantic Layer / MetricFlow (`dbt sl`) | `prix_moyen_m2` and simple metrics |
| State | PostgreSQL 16 | Dagster run history, event log, schedule ticks |
| Runtime | Docker Compose | one `make up` for the whole stack |

## Architecture

```mermaid
flowchart LR
    subgraph sources["Data sources"]
        HTTP["HTTP APIs"]
        S3SRC["S3 objects"]
    end

    subgraph dagster["Dagster orchestration<br/>dagster-webserver + dagster-daemon"]
        LANDING["landing asset<br/>retrying client + pagination"]
        BRONZE["bronze asset<br/>Spark normalize / dedup / write"]
    end

    subgraph storage["RustFS (S3-compatible)"]
        LZ[("landing-zone<br/>native payloads")]
        WH[("lakehouse<br/>Delta + UniForm")]
    end

    UC["Unity Catalog OSS<br/>catalog.schema.table"]
    SPARK["Spark Thrift Server<br/>:10000"]
    DBT["dbt 2.0<br/>staging → Gold"]
    SEM["Semantic Layer<br/>prix_moyen_m2"]
    PG[("Postgres<br/>Dagster state")]

    HTTP --> LANDING
    S3SRC --> LANDING
    LANDING -->|"part files"| LZ
    LANDING --> BRONZE
    BRONZE -->|"Delta + UniForm"| WH
    BRONZE -->|"create namespace"| UC
    UC -.->|"read-only Iceberg REST"| WH
    WH --> SPARK
    SPARK --> DBT
    DBT -->|"Gold Delta tables"| WH
    DBT --> SEM
    dagster -.->|"run history / schedule ticks"| PG
```

The service map behind that flow:

| Service | Role |
|---|---|
| `dagster-webserver` | UI/API on `:3000`; loads the `use_case` code location and launches runs |
| `dagster-daemon` | fires schedules and sensors, queues runs; required for automation |
| `postgres` | Dagster's own bookkeeping — run history, event log, schedule state |
| `rustfs` | S3-compatible store: `landing-zone` for raw payloads, `lakehouse` for table data |
| `init-buckets` | one-shot job that creates both buckets |
| `unity-catalog` | Unity Catalog OSS REST API; exposes the Iceberg REST read endpoint |
| `unity-catalog-ui` | Unity Catalog web UI on `:3001` |
| `spark-thrift` | Spark Thrift server on `:10000`; the endpoint dbt connects to |

Ingestion assets run inside the Dagster container: Python downloads the payload
to `landing-zone`, then an in-process `SparkSession` (same pinned Spark/Delta/Iceberg
jars as the Thrift server) reads it, sanitizes and deduplicates it, and writes the
Bronze table to `lakehouse/warehouse`. Provisioning of buckets and Unity Catalog
namespaces happens before either step.

There is **no Hive Metastore**. Tables are written as Delta Lake with UniForm
enabled, which emits Iceberg metadata. Unity Catalog OSS remains available as a
read-only Iceberg REST catalog:

```
http://unity-catalog:8080/api/2.1/unity-catalog/iceberg
```

## Ingesting a new source

Ingestion is configuration-only. Do not edit Python unless the source needs
behaviour YAML cannot express (see the handler escape hatch below).

1. **Declare the source** in a YAML file under `use_case/<use_case>/assets/`
   (create the file or extend an existing one). The minimum shape is one entry
   with `source_properties` and `catalog_properties`:

   ```yaml
   assets:
     - name: my_new_source
       group_name: bronze
       source_properties:
         type: http                 # or s3
         url: https://api.example.fr/records
         native_format: json        # csv, csv.gz, json, parquet, ...
         landing_bucket: landing-zone
         s3_prefix: raw/my-domain/my_new_source
         pagination:
           strategy: none           # none, page_number, offset, cursor, next_url, link_header
       catalog_properties:
         catalog_name: lakehouse
         schema_name: bronze
         table_name: my_new_source
         write_mode: overwrite      # overwrite, append, merge
         primary_keys: [id]         # mandatory for merge
         dedup_order_by: _ingested_at
         partition_by: [code_commune]
   ```

   The generated Dagster keys are always
   `AssetKey(["landing", "<name>"])` for the raw payload and
   `AssetKey(["<catalog>", "<schema>", "<table>"])` for Bronze.

2. **Start the stack** with `make up`. Both Dagster processes load the
   `use_case` code location from `workspace.yaml`; the asset factory discovers
   the new YAML, and `sync_dbt_sources` regenerates
   `dbt_lakehouse/models/staging/_sources.yml` so dbt can see the table.

3. **Materialize it** — all assets with `make ingest`, only the new source from
   the Dagster UI (select the landing and Bronze assets), or by adding the
   selection to `use_case/<use_case>/schedule.yaml` so it runs on a cron.

What happens during a run:

| Step | Where | What |
|---|---|---|
| 1 | `dagster-webserver` (run worker) | provisions the RustFS buckets and the `catalog.schema` namespace (S3 + Unity Catalog) |
| 2 | same process, `landing/` | downloads every page/part with retries and writes native bytes to `s3://landing-zone/<prefix>/ingest_date=.../` |
| 3 | same process, `iceberg/` | an in-process Spark session reads the landed files, sanitizes columns, deduplicates on the primary key, evolves the schema and writes Bronze |
| 4 | `rustfs` | stores the Delta table with UniForm enabled, so Iceberg metadata exists in the same commit |
| 5 | `unity-catalog` | holds the `catalog.schema.table` registration; `unity-catalog-ui` shows it on `:3001` |

Verify the result in Dagster's asset metadata (`row_count`, `write_mode`,
`table_properties`, `snapshot`) and in the Unity Catalog UI. For a source that
needs custom Python, add `handler: my_package.module:MyHandler` next to `name`
instead of forking the library; the contract is described under
[Escape hatch: per-use-case Python](#escape-hatch-per-use-case-python).

## Transforming data and building the semantic layer

Transformation is a separate dbt project, `dbt_lakehouse/`, that reads Bronze
through the Spark Thrift server (`thrift://spark-thrift:10000`) and writes Gold
tables back to the same RustFS warehouse.

| dbt layer | Location | What it does |
|---|---|---|
| staging | `models/staging/_sources.yml` | generated by the ingestion graph; declares Bronze tables as dbt sources |
| Gold | `models/gold/*.sql` | `dim_*` and `fct_*` models, materialized as Delta tables with UniForm enabled |
| semantic | `models/semantic/` | the time spine plus `semantic_models.yml` (entities, dimensions, simple metrics, ratio metric) |

Build everything with:

```bash
make dbt-build      # dbt build: Gold + semantic models (sources resolved from Bronze)
make dbt-parse      # compile and validate the project/semantic graph
make dbt-lint       # dbt's native SQL linter
```

When a compiled `dbt_lakehouse/target/manifest.json` exists, Dagster registers
the models named in `gold_models` (from `use_case.yaml`) as dbt assets, plus the
`metricflow_semantic_validation` asset that runs `dbt parse` after them. A
scheduled or UI materialization then runs ingestion and dbt in dependency order.

To add a Gold model:

1. Create the SQL under `dbt_lakehouse/models/gold/`.
2. Add its name to `gold_models` in `use_case/<use_case>/use_case.yaml`.
3. Run `make dbt-parse` (or `make dbt-build`), then reload the Dagster code
   location so the new dbt asset appears.

The semantic layer uses the current dbt spec: entities and dimensions live on
the semantic model's columns, simple metrics declare `agg` and `expr`, and
`prix_moyen_m2` is a top-level ratio metric
(`volume_ventes_eur / surface_vendue_m2`). Edit
`dbt_lakehouse/models/semantic/semantic_models.yml` to add dimensions or
metrics, then run `make dbt-parse` to validate the graph and query it with
`dbt sl query --metrics prix_moyen_m2`. The caveats of self-hosted MetricFlow
validation are covered under [Semantic layer](#semantic-layer).

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

## The Spark stack is version-locked

Spark, Delta and Iceberg are upgraded together or not at all. The current stack
is **Spark 4.1.1 / Delta 4.3.1 / Iceberg 1.12.0**, on Scala 2.13 and Java 17.

That ceiling is real, not a preference:

| Component | Constraint |
|---|---|
| `delta-spark` 4.3.1 | built against `spark-sql` 4.1.0; requires `pyspark<=4.1.1` |
| `delta-spark` 4.4.x | raises the ceiling to Spark 4.2 |
| Iceberg | publishes `iceberg-spark-runtime` for Spark **4.0 and 4.1 only** |

Iceberg is the binding constraint: there is no `iceberg-spark-runtime-4.2_2.13`
or `-4.3_2.13` artifact. Because this project writes Delta with UniForm (which
produces the Iceberg metadata in the same commit) *and* reads through the
Iceberg REST catalog, bumping Spark past 4.1 would break both directions.

Two consequences worth remembering:

* Scala moved from 2.12 to 2.13 in Spark 4, so every Maven artifact suffix
  changed (`iceberg-spark-runtime-3.5_2.12` → `-4.1_2.13`).
* `hadoop-aws` is pinned to **3.4.2** to match the Hadoop that Spark 4.1.1
  bundles. Mixing Hadoop majors is what produces the classic
  `NoSuchMethodError: S3AFileSystem` at runtime.
* **UniForm ships in its own artifact**, `delta-iceberg_2.13`. Without it,
  every `CREATE TABLE` carrying `delta.enableIcebergCompatV2` compiles fine and
  then fails at runtime with `DELTA_MISSING_ICEBERG_CLASS`
  (`ClassNotFoundException: org.apache.spark.sql.delta.icebergShaded.IcebergConverter`).
  It is not pulled in transitively by `delta-spark`.

The jars are downloaded in *two* places — `Dockerfile.spark` (the Thrift server)
and `Dockerfile.dagster` (the local `SparkSession` Dagster uses to write Delta
tables). They must stay in lockstep.

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
dbt_lakehouse/                      # dedicated dbt project (models + Semantic Layer)
```

## Declarative contract

Every YAML file in a use case's `assets/` directory (except `platform.yaml`)
declares one or more assets with two blocks:

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
        └── dvf_and_geo_77.yaml # declares communes_geo_77, dvf_mutations_77, dvf_lots_batis_77
```

The manifest keys (`ndr_lakehouse.asset_property.use_case.UseCaseConfig`):

| Key | Meaning |
|---|---|
| `name` | Use-case identifier, used by `resolve_use_case` to select a manifest. |
| `description` | Free text, shown in tooling. |
| `assets_dir` | Directory holding the per-asset YAML. Relative to the manifest. |
| `platform_file` | Shared platform/Spark settings YAML. Relative to the manifest. |
| `dbt_project_dir` | The dbt project this use case builds its Gold layer in. |
| `dbt_sources_path` | Where `_sources.yml` is regenerated inside that project. |
| `gold_models` | Gold model names to register as Dagster dbt assets. |
| `semantic_metric` | Semantic Layer metric to expose as an asset. |
| `default` | Marks the manifest loaded when none is named explicitly. |

The Dagster code location is *not* named after the use case — Dagster names it
after the module, so it appears as `use_case` unless `workspace.yaml` sets an
explicit `location_name`.

`use_case/__init__.py` calls `build_definitions()` at import time, which resolves
the default use case, builds the ingestion assets from its YAML, provisions the
Spark/ingestion/UC resources and — when the dbt project and its compiled
`target/manifest.json` are both present — registers the dbt and semantic assets
too.

### Escape hatch: per-use-case Python

When a source needs behaviour the YAML cannot express, do not fork the library.
Give the asset a `handler` — a *top-level* key next to `name` and
`source_properties` in an entry of the file's `assets:` list — taking a
`module:Class` reference (a **colon**, and a class, not a function):

```yaml
assets:
  - name: dvf_mutations_77
    handler: use_case.real_estate_77.handlers:MutationsHandler
    source_properties:
      ...
    catalog_properties:
      ...
```

The module path must be importable from the repo root, hence the `use_case.`
prefix. The class is imported and instantiated with no arguments, and must
satisfy `ndr_lakehouse.domain.ports.UseCaseHandler` — `land(context, config)`
returning a landing URI or `None`, and `load(context, config, landing_uri)`
returning a summary dict or `None`. The two halves behave differently: a `None`
from `land` falls back to the generic ingestor, while `load` still runs the
generic Spark load and merges any non-empty dict you return over its summary.
`HandlerContext` carries the `landing` and `iceberg` ports plus a read-only
mapping of this asset's `asset_name` and `full_table_name` — nothing else.

The library still owns the contract; the use case owns only the exotic part. Use
cases are plain Python, so Dagster runs them in-process; they are deliberately
*not* a wheel package (the wheel ships `ndr_lakehouse` only, so `use_case/` is
reachable via `PYTHONPATH`, not installed by `uv sync`), which keeps the library
free of business vocabulary.

Adding a use case: this process loads the manifest marked `default: true`, or the
one named explicitly through `build_definitions(use_case=...)`. To run two side
by side, give each its own entrypoint module and a `workspace.yaml` entry with a
distinct `location_name` — a second location that also targets `-m use_case`
collides on the location name.

## Quick start

```bash
cp .env.example .env
make up                 # RustFS, Unity Catalog (+ UI), Spark Thrift, Dagster, Portal, Nginx Gateway
open http://localhost:8088
make ingest             # materialize all generated landing + Bronze assets
make dbt-build          # build Gold Iceberg models
make dbt-lint           # lint the SQL with dbt's native linter
```

Useful endpoints (all web UIs are routed and gated through the Nginx gateway on port `8088`):

| Service | URL |
|---|---|
| Access Portal & Launchpad | http://localhost:8088 (or http://portal.localhost:8088) |
| Dagster (gated) | http://dagster.localhost:8088 |
| Unity Catalog UI (gated) | http://uc.localhost:8088 |
| Spark Thrift UI (gated) | http://spark.localhost:8088 |
| RustFS Console (gated, admin) | http://rustfs.localhost:8088 |
| Scoped S3 Proxy (SigV4) | http://s3.localhost:8088 |
| RustFS S3 API (direct internal/dev) | http://localhost:9000 |
| Unity Catalog OSS API | http://localhost:8080 |
| Unity Catalog API browser | http://localhost:8080/docs/ |
| Iceberg REST API | http://localhost:8080/api/2.1/unity-catalog/iceberg |
| Spark Thrift JDBC | thrift://localhost:10000 |
| Postgres (Dagster & Portal state) | postgresql://dagster:dagster@localhost:5432/dagster |

Note that Unity Catalog's `/docs/` path **needs the trailing slash**. Without it
the page loads but renders blank: the HTML fetches `main.<hash>.js` relatively,
which resolves to `/main.<hash>.js` and 404s.

### Unity Catalog UI

`unitycatalog/unitycatalog` ships no frontend -- the image is API only, and
`/docs/` is an Armeria API browser rather than a data catalog. The
`unity-catalog-ui` service builds the React app from the upstream `ui/` tree and
serves it behind nginx on port 3001 (3000 is Dagster's).

The upstream `unitycatalog/unitycatalog-ui` image is deliberately **not** used.
It runs the CRA dev server with the API host baked into `package.json` at build
time -- pointing at a service literally named `server` -- and `react-scripts`
rejects any other Host header with `Invalid Host header`, so it 502s in this
stack. See `unity-catalog-ui/Dockerfile` for the full rationale.

Because the client's axios baseURL is relative (`/api/2.1/unity-catalog`),
nginx reverse-proxies `/api/` and the API target stays a **runtime** setting:

```bash
UC_SERVER=http://unity-catalog:8080   # substituted into nginx.conf at startup
```

Rebuild the UI against a different Unity Catalog release by bumping the
`UC_UI_VERSION` build arg (`v0.7.0` currently, matching the server tag):

```bash
docker compose build --build-arg UC_UI_VERSION=v0.7.0 unity-catalog-ui
```

### Access Portal & Nginx Reverse Proxy Gateway

The `portal` and `gateway` (`nginx:1.27-alpine`) services provide unified authentication, service discovery, and access management for the stack:

- **Default admin login**: `admin` / `admin` (override via `PORTAL_ADMIN_USER`, `PORTAL_ADMIN_PASSWORD`, and `PORTAL_SECRET_KEY`).
- **Nginx `auth_request` gating**: Requests to `dagster.localhost:8088`, `uc.localhost:8088`, `spark.localhost:8088`, and `rustfs.localhost:8088` are verified by `portal:8000/internal/verify` before proxying.
- **Cross-subdomain SSO**: Clicking **Launch** on the portal routes through `/auth/handoff` and `/__auth_callback` so the subdomain receives a signed `lh_session` cookie without requiring a second login.
- **In-Portal Bucket Explorer (`/buckets`)**: Standard users see only buckets granted to them (`read` or `read_write`) and can browse/download or upload/delete objects accordingly.
- **Scoped S3 Proxy (`http://s3.localhost:8088`)**: Users generate personal `LHAK...` S3 access keys under `/keys`. Requests signed with AWS SigV4 against `http://s3.localhost:8088` are verified by the portal and enforced per-bucket (`read` vs `read_write`) before executing against RustFS:

```bash
aws --endpoint-url http://s3.localhost:8088 s3 ls
```

- **Admin Matrix (`/admin`)**: Admins create/disable users, toggle service entitlements (`dagster`, `unity-catalog`, `spark-ui`, `rustfs-console`), and set per-bucket roles (`None`, `Read`, `Read / Write`).

### Where Dagster keeps its state

Dagster needs somewhere to record run history, the event log and sensor/schedule
ticks. `docker-compose.yml` runs a small `postgres` service for exactly that, and
`dagster-instance.yaml` points the instance at it. The file is bind-mounted to
`$DAGSTER_HOME/dagster.yaml` so the **webserver and the daemon share one
instance** — without it each process would silently fall back to its own SQLite
file and the UI would not show daemon-written runs.

This database is Dagster's own bookkeeping. It holds no lakehouse data and no
metadata about lakehouse tables; those stay in RustFS and Unity Catalog.

Every value in `dagster-instance.yaml` is an `env:` reference, so credentials are
never committed. The `scheme: postgresql+psycopg2` line is required rather than
cosmetic: SQLAlchemy 2.1 maps a bare `postgresql://` URL to the **psycopg v3**
driver, while `dagster-postgres` ships psycopg2, so omitting it fails at startup
with `ModuleNotFoundError: No module named 'psycopg'`.

`dagster dev` (outside Docker) ignores this file entirely and keeps using a local
SQLite instance under `.dagster/`.

## Schedules

Automation is declared in `use_case/<use_case>/schedule.yaml` (override the path
with `schedule_file:` in `use_case.yaml`). Each entry becomes one asset job and
one Dagster schedule. The shipped example runs daily:

```yaml
schedules:
  - name: daily_lakehouse_refresh
    cron: "0 6 * * *"          # five-field cron: minute hour day month weekday
    timezone: Europe/Paris
    enabled: true
```

`enabled: true` registers the schedule as running; `false` registers it as
stopped, so it stays visible in **Automation** and can be started from the UI.
This is the **initial** status only: once Dagster has stored a state for the
schedule (after its first tick, or after someone toggles it in the UI), that
persisted state wins. Editing `enabled` afterwards has no effect until the
stored state is cleared; use the toggle in **Automation** instead. Omit
`selection` to materialize every asset in dependency order, or narrow it with
`selection: {groups: [landing, bronze, dbt, semantic]}` or
`selection: {assets: [["lakehouse", "bronze", "dvf_mutations_77"]]}`. A
selection naming an unknown group or asset, or matching no asset at all, fails
at load instead of ticking a partial or empty job.

`./use_case` is bind-mounted into both Dagster containers, so applying an edit
does not need an image rebuild — only a restart, because both processes load the
workspace at startup:

```bash
docker compose restart dagster-webserver dagster-daemon
```

The `dagster-daemon` service must be running for ticks to fire; it is part of
`make up`.

## Tests

```bash
uv sync --extra spark      # the suite imports delta.tables at module level
uv run pytest
```

The suite uses `pytest`, `pytest-mock` (`mocker`) and `pytest-cov`. `httpx`,
`boto3` and Spark APIs are mocked, so **no network, Docker, RustFS, Unity
Catalog or Spark cluster is required**. `pyspark` itself must still be
importable: `ndr_lakehouse.iceberg` imports `delta.tables` at module scope, so a
bare `uv sync` (without `--extra spark`) fails at collection time with
`ModuleNotFoundError: No module named 'delta'`. Test paths, imports and the
coverage gate are enforced in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["orchestration/ingestion/tests/ndr_lakehouse"]
pythonpath = ["orchestration/ingestion/src", "."]
addopts = [
    "--cov=ndr_lakehouse",
    "--cov-report=term-missing",
    "--cov-fail-under=80",
]
```

`uv run pytest` fails if total coverage drops below **80%** (branch coverage is
enabled).

## Continuous integration

`.github/workflows/ci.yml` runs three independent jobs on every push to `main`
and every pull request:

| Job | What it runs |
| --- | --- |
| **Python quality** | `isort`, `black`, `flake8`, `ruff`, `mypy`, `pylint` |
| **SQL quality** | `dbt parse`, `dbt format --check`, `dbt lint` |
| **Unit tests** | `pytest` (with the coverage gate) and `dagster definitions validate`, on Python 3.11 **and** 3.12 |

`make check` runs the same sequence locally, so a red build should never be a
surprise:

```bash
uv sync --extra spark --extra dbt
make check
```

Three details explain the shape of the workflow:

* **The jobs are split, not merged.** A SQL lint error must not hide a test
  regression, and the tests job pays a 455 MB `pyspark` sdist download — there is
  no wheel for 4.1.1 — which should not delay the cheap static checks.
* **`ruff format` is not part of CI.** `ruff check` runs (it adds the
  pyupgrade and bugbear rule sets that isort/black/flake8 lack), but
  `ruff format` disagrees with `black` on a few string-concatenation lines, and
  two formatters demanding different output is worse than either alone. Only
  `black` decides formatting.
* **`dbt lint --fix` is never run.** It exits 0 unconditionally, so it cannot
  gate anything; plain `dbt lint` exits 1 on remaining violations. Use
  `make dbt-format` to fix locally.

The tools in the `lint` dependency group are pinned to the exact `rev:` values in
`.pre-commit-config.yaml`. They are installed into the project venv because
pre-commit builds each hook in its own throwaway environment, which is why
`uv run isort` used to fail with *Failed to spawn* — nothing ever landed in
`.venv`. Bump both files together.

## Pre-commit hooks

Linting and formatting run through [pre-commit](https://pre-commit.com). Install
the git hook once per clone:

```bash
uv sync --extra spark --extra dbt
uv run pre-commit install          # on every `git commit`
uv run pre-commit run --all-files  # to check the whole tree on demand
```

Eight hooks are configured in `.pre-commit-config.yaml`:

| Hook | Config | Scope |
| --- | --- | --- |
| `isort` | `[tool.isort]` in `pyproject.toml` | import order (black profile) |
| `black` | `--line-length=120` | Python formatting |
| `flake8` | `.flake8` | pyflakes/pycodestyle |
| `ruff` | `[tool.ruff]` in `pyproject.toml` | pyupgrade + bugbear + import order |
| `mypy` | `[tool.mypy]` in `pyproject.toml` | type checking |
| `pylint` | `.pylintrc` | design and code smells |
| `dbt format (check)` | `.sqlfluff`-ported rules | SQL layout, via `dbt format --check` |
| `dbt lint` | same rules | SQL linting, via `dbt lint` |

A few project-specific details are easy to get wrong:

* The line length is **120** everywhere — `pyproject.toml`, `.flake8`, `.pylintrc`
  and `.sqlfluff` all agree, so the tools cannot fight each other.
* `ruff` and `isort` both order imports, so `[tool.ruff.lint.isort]` repeats
  isort's `known_first_party` list. Neither tool can discover the first-party
  packages on its own: the importable root is `orchestration/ingestion/src`, not
  `orchestration/`, so `ndr_lakehouse` looks third-party from the repo root and
  the two tools would otherwise emit contradictory orderings.
* SQL is linted by **dbt's own linter** (dbt v2), not by `sqlfluff`. The rules were
  ported from the previous `.sqlfluff` file — `capitalisation.keywords = upper`,
  `capitalisation.identifiers = upper` (all), `aliasing.table/column = explicit`,
  `min_alias_length = 3`, `indented_using_on = true`, `indented_joins = false` —
  which is why every model is written in upper case.
* The dialect is **`sparksql`**, not `trino`. `dbt_lakehouse/profiles.yml` targets
  Spark over thrift, and the models use `explode`, `sequence` and
  `interval 1 day`, which a Trino parser cannot read.
* Both hooks need `DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true`. dbt v2 bundles the Spark
  adapter but keeps it behind that flag, and it gates `lint`/`format` too, not
  just `run`. The Docker images and `docker-compose.yml` set it as well.
* The hooks pass `pass_filenames: false`: `dbt lint` resolves paths relative to
  the dbt project, so feeding it repo-relative paths fails with `dbt1007`.

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

`dbt_lakehouse/models/semantic/semantic_models.yml` declares one semantic model
per Gold model (`lots_batis`, `mutations`) and the ratio metric. The layout
follows the current dbt spec (v1.12+): entities and dimensions live on the
*semantic model's columns*, and each simple metric names its `agg` and `expr`.
The ratio metric is the only top-level entry:

```yaml
metrics:
  - name: prix_moyen_m2
    type: ratio
    numerator: volume_ventes_eur
    denominator: surface_vendue_m2
```

`fct_lots_batis.sql` pre-aggregates mutation totals in a deduplicated CTE
before the semantic layer consumes it, preventing fan-out of the ratio
denominator.

The `metricflow_semantic_validation` Dagster asset runs `dbt parse` after the
Gold dbt assets materialize, which compiles and validates the semantic graph
(entities, dimensions, simple metrics, ratio inputs). Two caveats are worth
knowing:

* `dbt sl validate` — the direct replacement for `mf validate-configs` — is a
  **dbt Cloud feature**. On a self-hosted lakehouse it fails with
  `Missing project in dbt_cloud.yaml`, so `dbt parse` is the strongest offline
  check available.
* `dbt parse` rejects malformed YAML (an unknown `agg` is an error), but it does
  **not** resolve cross-metric references: a `numerator:` pointing at a
  non-existent metric parses silently. Keep those names honest by hand.
