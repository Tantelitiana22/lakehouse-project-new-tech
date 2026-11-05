# Use-case package split — design

Date: 2026-11-05
Status: approved approach (A), design pending sign-off

## Intent and why

Today the repository mixes two kinds of code in one package:

- **Platform machinery** in `orchestration/ingestion/src/ndr_lakehouse/` — landing,
  Delta/Iceberg conversion, provisioning, Dagster glue. Reusable, domain-agnostic.
- **Situated configuration** embedded in that same package — `asset_property/platform.yaml`,
  `asset_property/dvf_and_geo_77.yaml`, plus two hardcoded names in generic modules.

The goal is to move everything situated into a sibling package `use_case/` next to
`dbt_lakehouse/`, so `ndr_lakehouse` becomes a library you install and call, and a
use case becomes a folder you own. Generic ingestion stays pure YAML/JSON that calls
the existing pipeline. A use case with special behaviour ships a Python module that
implements a contract declared in `ndr_lakehouse.domain`. Dagster is only one driver:
it may import the use-case package or launch a container later.

## Goals

- One folder per use case holding its declarative assets; no asset YAML inside `ndr_lakehouse`.
- A use case can override landing, conversion, or both via a Python class, without
  forking the platform.
- `ndr_lakehouse` exposes a parameterised factory; no default asset directory, no
  import-time `defs` construction inside the library.
- Generic code contains zero use-case names (`communes`, `dvf`, `77`, ...).
- Existing 66 tests keep passing; the current pipeline behaviour is unchanged.

## Non-goals

- Moving `dbt_lakehouse/` inside `use_case/`. It stays where it is for now; only the
  path that points at it becomes configurable.
- Multi-tenant isolation (separate images, separate warehouses) in this change.
- Renaming or restructuring `landing/`, `iceberg/`, `provisioning/`.
- Building a container launcher. It remains a future launcher option (approach C).

## Anti-goals

- A discovery mechanism that silently ignores a malformed use case. Unknown handler
  references, duplicate asset names and missing platform files must fail loudly at
  load time.
- An abstraction that forces use cases to subclass internals or import `boto3`/`pyspark`.
- Two sources of truth for the same setting (e.g. platform endpoints in both YAML and
  a hardcoded default in `resources.py`).

## Constraints

1. `ndr_lakehouse/__init__.py` currently executes `build_definitions()` at import time,
   and `workspace.yaml` loads `python_module: ndr_lakehouse`. This must move to a
   parameterised entrypoint.
2. `orchestration/paths.py` hardcodes `ASSET_PROPERTY_DIR = PACKAGE_ROOT / "asset_property"`.
   `DEFAULT_PLATFORM` in `resources.py` is evaluated at import time from that path.
3. `discover_asset_configs()` globs `*.yaml` non-recursively and excludes `platform.yaml`
   by filename. A per-use-case layout requires recursive discovery plus a platform file
   located per use case.
4. `GOLD_MODEL_NAMES` in `orchestration/dbt_assets.py` is a use-case fact in generic code.
5. `dbt_source_sync` writes to `dbt_lakehouse/models/staging/_sources.yml` and stamps the
   absolute location into the generated header.
6. `pyspark`/`delta-spark` are an optional extra (`uv sync --extra spark`). Use-case
   Python modules must not force Spark imports at package import time.

## Approaches considered

**A. Manifest per use case, dotted-path handlers, one code location.** (chosen)
`use_case/<name>/use_case.yaml` declares the use case; `assets/*.yaml` holds sources;
optional `handlers/*.py` implements `domain.ports.UseCaseHandler`. A parameterised
factory scans or loads one use case and builds `Definitions`.
Trade-off: one process holds every use case, so a bad handler import can affect others.
Mitigated because handlers are validated and imported lazily, per asset.

**B. Each use case is its own Dagster code location.**
`use_case/<name>/definitions.py` builds `Definitions` itself; `workspace.yaml` lists one
module per use case. Maximum isolation.
Rejected for now: a hand-written `definitions.py` per use case and no single view of the
graph. Approach A can migrate to B without changing handlers, because the factory already
accepts a use-case name.

**C. Use case as a container image.**
Dagster only submits runs; use-case code is never imported into the Dagster process.
Rejected: requires independent packaging and image builds, and loses asset-level lineage
in the Dagster UI. Remains the natural next step if a use case needs its own dependencies.

## Design

### Layout

```
use_case/
├── __init__.py                      # exports build_definitions(use_case=...)
├── dvf_geo_77/
│   ├── use_case.yaml                # manifest: name, layout paths, dbt project, dataset metadata
│   ├── assets/
│   │   ├── platform.yaml            # moved from ndr_lakehouse/asset_property/
│   │   └── dvf_and_geo_77.yaml      # moved from ndr_lakehouse/asset_property/
│   └── handlers/                    # only when generic behaviour is not enough
│       └── __init__.py
└── _shared/                         # optional; only if a second use case needs it
```

`ndr_lakehouse/` keeps every module it has today. `asset_property/` loses its YAML files
and keeps only the Pydantic contracts and discovery functions.

### Manifest

`use_case/dvf_geo_77/use_case.yaml`:

```yaml
use_case:
  name: dvf_geo_77
  description: Seine-et-Marne (77) real-estate and commune referential
  assets_dir: assets                       # relative to this manifest
  platform_file: assets/platform.yaml      # relative to this manifest
  dbt_project_dir: ../../dbt_lakehouse     # relative to this manifest
  dbt_sources_path: models/staging/_sources.yml   # relative to dbt_project_dir
  gold_models: [dim_communes, fct_mutations, fct_lots_batis]
  semantic_metric: prix_moyen_m2
```

The manifest is validated by a new `UseCaseConfig` Pydantic model alongside the existing
contracts. **Path resolution rule:** `assets_dir` and `platform_file` resolve against the
manifest's directory; `dbt_sources_path` resolves against `dbt_project_dir`. This asymmetry
is deliberate — it matches how dbt itself resolves paths, and resolving
`models/staging/_sources.yml` against the manifest would silently write to the wrong place
and leave the real sources file stale.

### Domain contract for special behaviour

New protocol in `ndr_lakehouse/domain/ports.py`:

```python
class UseCaseHandler(Protocol):
    """Optional override points for a situated ingestion asset."""

    def land(self, context: HandlerContext, config: AssetConfig) -> str | None: ...
    def load(self, context: HandlerContext, config: AssetConfig, landing_uri: str) -> dict[str, Any] | None: ...
```

Both methods are optional; returning `None` means "use the platform implementation".

`HandlerContext` is a small frozen dataclass defined in `domain/ports.py`, carrying what a
handler legitimately needs without exposing Dagster or `boto3`:

```python
@dataclass(frozen=True)
class HandlerContext:
    landing: LandingPort      # Protocol: ingest(config) -> str
    iceberg: IcebergPort      # Protocol: load(config, landing_uri) -> dict[str, Any]
    settings: Mapping[str, str]  # platform settings, read-only
```

The two ports are satisfied by thin adapters over `NativeIngestorResource` and
`SparkIcebergResource` in the Dagster layer. This keeps the anti-goal intact: a handler can
delegate to the platform (`return ctx.landing.ingest(config)`) or do its own work using
`s3fs`/`requests`/`pyspark` that the use case declares — but it never receives a Dagster
resource object and never needs to construct adapters itself.

**Who runs what.** Provisioning (`ensure_buckets`, `ensure_catalog_and_schema`) stays in the
platform landing asset and **always runs**, before either the handler or the generic path.
A handler overrides extraction and/or conversion, never infrastructure setup.

**Lifecycle.** Handlers are instantiated once per asset build (one instance shared by all
materialisations of that asset). They take no constructor arguments; anything they need
comes through `HandlerContext`, which keeps them trivially testable.

**Return contract.** `load` returning `None` means the platform fully handled it. Returning
a dict is **merged over** the platform's metadata dict, so a handler may add keys or override
`row_count` while inheriting `table`, `write_mode`, `snapshot`. A handler is not required to
reproduce the platform's key set.

An asset YAML opts in with a single string field:

```yaml
use_case:
  handler: use_case.dvf_geo_77.handlers.custom_x:CustomX
```

The factory resolves the dotted path via `importlib` only for assets that declare one, so
Spark or heavy third-party imports never happen for pure-config assets. Resolution failures
raise a new `InvalidHandlerReferenceError(LakehouseError)` listing the module, attribute and
the asset name.

### Factory API in `ndr_lakehouse`

`orchestration/definitions.py` becomes parameterised and side-effect free:

```python
def build_definitions(
    use_case: str | Path | None = None,
    *,
    register_dbt: bool = True,
    sync_sources: bool = True,
) -> Definitions: ...

def build_resource_bundle(use_case: UseCaseConfig | None = None) -> dict[str, object]: ...
```

- No module-level `defs = build_definitions()` inside the library.
- No `DEFAULT_PLATFORM` import-time evaluation; the platform file comes from the use case.
  `build_resource_bundle(use_case=None)` is an error, not a defaults path: the rule is
  "a platform file always comes from a use case, fail loudly if absent". The Pydantic
  field defaults on `StorageConfig`/`UnityCatalogConfig`/`SparkConfig` stay as they are,
  since they serve library-only construction and test fixtures; the *runtime* never invents
  a platform.
- `GOLD_MODEL_NAMES` moves to the manifest (`gold_models`), and `metricflow_semantic_validation`
  becomes a factory parameterised by `dbt_project_dir` and `semantic_metric`, since all three
  facts are use-case-specific. The platform-default constant is deleted, not kept.
- `dbt_source_sync` receives `output_path` and a relative label for its header, so no
  absolute path leaks into the generated file.
- `sync_sources=False` disables the write; `register_dbt=False` additionally skips dbt assets.

### Compatibility with existing tests

"Tests keep passing" means **behaviour is preserved, not that every assertion is untouched.**
These three tests are rewritten as part of the change, and that is expected:

- `test_dbt_semantic_assets.py` imports `GOLD_ASSET_KEYS` from a module constant. It is
  rewritten to build the semantic asset through the factory with an explicit `gold_models`
  list, asserting the same dependency keys. Coverage intent is unchanged.
- `test_schema_validator.py` asserts `pytest.raises(ValueError, match="duplicate asset name")`.
  `DuplicateAssetNameError` therefore **subclasses both `LakehouseError` and `ValueError`**,
  so the existing assertion and error taxonomy both hold.
- `test_dagster_defs.py` calls `build_ingestion_assets_from_yaml(configs=[...])`, which keeps
  its signature; only call sites that relied on the implicit default directory change.

### Entrypoint and packaging

`use_case/` is a **repo-local package, not a wheel package**. It is added to:
- `[tool.pytest.ini_options].pythonpath` in `pyproject.toml`,
- `PYTHONPATH` in `Dockerfile.dagster` (and the `use_case` directory added to its `COPY` set),
- `docker-compose.yml` volumes if the Dagster service mounts the repo.

`[tool.hatch.build.targets.wheel].packages` stays `["orchestration/ingestion/src/ndr_lakehouse"]`,
so the library installs alone; `use_case` ships with the deployment, not with the library.
`build_definitions(use_case=None)` resolves the default use case by importing the `use_case`
package and reading its manifest's `default: true`. A library consumer that wants its own
layout passes an explicit path and never touches the `use_case` package.

`ndr_lakehouse.defs` is **removed outright**, not kept as a shim, because a shim reintroduces
exactly the default-directory coupling this design removes.

`use_case/__init__.py` owns `defs`:

```python
from ndr_lakehouse.orchestration.definitions import build_definitions

defs = build_definitions()
```

`workspace.yaml` changes to `python_module: use_case`. The `Makefile` targets change from
`-m ndr_lakehouse` to `-m use_case`.

### One use case per process

A Dagster code location loads exactly one use case: `defs` builds a single `Definitions` for
the manifest marked `default: true` (or named explicitly). Consequently:

- Duplicate asset names across use cases are not a concern — they never load together.
- The single dbt asset name `lakehouse_dbt_assets` and the single shared `_sources.yml` are
  per-process, so no clobbering occurs.
- Running two use cases means two code locations, which is approach B. Nothing in this
  design prevents it; the factory already takes an explicit name.

### Errors

Add to `domain/errors.py`:

- `UseCaseNotFoundError` — no manifest matched the requested name.
- `InvalidHandlerReferenceError` — dotted path does not resolve or violates the contract.
- `DuplicateAssetNameError` — promoted from a bare `ValueError` in `discover_asset_configs`.

## Behaviour compatibility

- The 6 asset definitions built from `dvf_and_geo_77.yaml` keep the same asset keys,
  groups, tags and resource names.
- `dbt_lakehouse/models/staging/_sources.yml` is regenerated from the moved YAML with the
  same content, differing only in the header comment.
- `make ingest` and `make dagster-dev` continue to materialise the same graph.

## What is left to the builder

- Exact wording of the manifest docstring and README sections.
- Whether `_shared/` is introduced now or only when a second use case appears.
- Test layout for the new modules (`tests/test_use_case_loading.py` versus extending
  `test_dagster_defs.py`).
- Whether `sync_dbt_sources` should remain a `build_definitions()` side effect or become an
  explicit Make target. Kept as a side effect for now (behaviour preserved), but it writes
  into the checkout on every build, which is undesirable in a read-only container.

## Calls made during the builder check

Settled without asking, because the code or the stated constraints already answer them:

- **Path resolution asymmetry** — `dbt_sources_path` is relative to `dbt_project_dir`,
  matching dbt conventions. The other paths are relative to the manifest.
- **`DuplicateAssetNameError` subclasses `ValueError`** as well as `LakehouseError`, so the
  existing assertion keeps working.
- **`metricflow_semantic_validation` becomes factory-built** — it hardcodes a project dir
  and the `prix_moyen_m2` metric, both use-case facts.
- **Provisioning always runs**, before either the handler or the generic path.
- **Handlers are constructor-argument-free** and get everything through `HandlerContext`.
- **`load` results merge over platform metadata** rather than replacing it.
- **`ndr_lakehouse.defs` is removed**, not shimmed.
- **`use_case/` is repo-local**, not a wheel package.
- **One use case per process**, so cross-use-case asset-name and sources-file collisions
  cannot occur.
