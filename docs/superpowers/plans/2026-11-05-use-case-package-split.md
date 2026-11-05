# Use-Case Package Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every situated configuration out of `ndr_lakehouse` into a repo-root `use_case/` package, leaving the library generic and parameterised.

**Architecture:** `use_case/<name>/use_case.yaml` declares one use case (assets directory, platform file, dbt paths, gold models). A new `UseCaseConfig` Pydantic model validates it. `ndr_lakehouse.orchestration.definitions.build_definitions(use_case=...)` builds `Definitions` from that manifest, and an optional dotted-path handler in each asset's YAML overrides landing and/or conversion through a `UseCaseHandler` protocol in `ndr_lakehouse.domain`. Dagster loads `python_module: use_case`, which owns `defs`.

**Tech Stack:** Python 3.12, Pydantic v2, Dagster 1.9, pytest, uv

**Spec:** `docs/superpowers/specs/2026-11-05-use-case-package-design.md`

## Global Constraints

- Python `>=3.11,<3.13`; `ruff` line length 100; lint rules `E, F, I, UP, B`.
- `pyspark` / `delta-spark` are an optional extra — never import them at module import time outside `try`-guarded or handler-lazy paths.
- Generic code in `ndr_lakehouse/` must contain **zero** use-case names (`communes`, `dvf`, `77`, `prix_moyen_m2`).
- The default use case directory is named `real_estate_77`.
- Exactly one use case loads per Dagster process.
- `ndr_lakehouse.defs` is deleted, not shimmed.
- `use_case/` is repo-local: added to pytest `pythonpath`, `Dockerfile.dagster` `COPY`/`PYTHONPATH`, and compose `PYTHONPATH` — but **not** to `[tool.hatch.build.targets.wheel].packages`.
- All work happens on a branch; commit at the end of every task.

**Running tests in this repo:**

```bash
# single test / module (the coverage gate would fail on a subset)
.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/<file>::<test> -q --no-cov

# full suite with the project's coverage gate (run before every commit from Task 5 on,
# because each task adds new modules whose uncovered lines push total coverage down)
.venv/bin/python -m pytest -q
```

Use `.venv/bin/python -m pytest`, not `uv run` — `uv run` from a subdirectory fails to spawn entry points in this workspace.

## Review Focus

These are the conditions the spec implies but no single test covers today. Each gets an explicit test in the task that owns the code.

1. **Two manifests both marked `default: true`** — a reasonable user expects a loud error naming both files, not a silently arbitrary winner.
2. **A handler dotted path that does not resolve** (`typo:Class`, missing attribute, module raising on import) — expect a `InvalidHandlerReferenceError` naming the asset and the path, not a bare `ImportError` traceback.
3. **Requesting a use case name that does not exist** — expect `UseCaseNotFoundError` listing the available names.
4. **`dbt_project_dir` pointing somewhere that is not a dbt project** — dbt lineage silently breaks rather than failing; expect an explicit failure or warning at build time.
5. **Underlying `_sources.yml` write failing (read-only checkout / container)** — expect a logged warning and a still-usable graph, preserving today's behaviour.

---

### Task 1: `UseCaseConfig` manifest model

**Files:**
- Create: `orchestration/ingestion/src/ndr_lakehouse/asset_property/use_case.py`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/asset_property/__init__.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_use_case_config.py`

**Interfaces:**
- Produces:
  - `class UseCaseConfig(BaseModel)` with fields `name: str`, `description: str = ""`, `assets_dir: Path`, `platform_file: Path`, `dbt_project_dir: Path`, `dbt_sources_path: Path`, `gold_models: list[str]`, `semantic_metric: str`, `default: bool = False`. Subclass the existing `_StrictModel` in `schema_validator.py` so unknown keys are rejected.
  - `UseCaseConfig.assets_path -> Path` (absolute)
  - `UseCaseConfig.platform_path -> Path` (absolute)
  - `UseCaseConfig.dbt_project_path -> Path` (absolute)
  - `UseCaseConfig.dbt_sources_file -> Path` (absolute; `dbt_project_path / dbt_sources_path`)
  - `UseCaseConfig.dbt_manifest_path -> Path` (absolute; `dbt_project_path / "target" / "manifest.json"`)
  - `load_use_case_config(path: str | Path) -> UseCaseConfig`
  - `discover_use_case_configs(root: str | Path) -> list[UseCaseConfig]`
  - `resolve_use_case(root: str | Path, name: str | None = None) -> UseCaseConfig`
- Consumes: `ndr_lakehouse.domain.errors.UseCaseNotFoundError`, `AmbiguousDefaultUseCaseError` (created in this task).

- [ ] **Step 1: Add a shared helper to `conftest.py`**

Later tasks (6, 8) also need throwaway use-case trees, so the helper goes in `orchestration/ingestion/tests/ndr_lakehouse/conftest.py`, not in one test module. Add `from pathlib import Path` to the imports there (`conftest.py` currently imports only `copy`, `date`, `Any` and `pytest`).

```python
MINIMAL_PLATFORM_YAML = """
platform:
  storage:
    endpoint: http://localhost:9000
    access_key: test
    secret_key: test
    landing_bucket: landing-zone
    lakehouse_bucket: lakehouse
  unity_catalog:
    base_url: http://localhost:8080
    catalog_name: lakehouse
  spark:
    master: local[*]
"""


@pytest.fixture
def write_use_case(tmp_path):
    """Create ``<root>/<name>/use_case.yaml`` plus a minimal platform file."""

    def _write(name: str, *, default: bool = False, root: Path | None = None) -> Path:
        base = root or tmp_path
        directory = base / name
        assets = directory / "assets"
        assets.mkdir(parents=True, exist_ok=True)
        (assets / "platform.yaml").write_text(MINIMAL_PLATFORM_YAML, encoding="utf-8")
        manifest = directory / "use_case.yaml"
        manifest.write_text(
            f"""
use_case:
  name: {name}
  assets_dir: assets
  platform_file: assets/platform.yaml
  dbt_project_dir: ../../dbt_lakehouse
  dbt_sources_path: models/staging/_sources.yml
  gold_models: [dim_a]
  semantic_metric: m
  default: {str(default).lower()}
""",
            encoding="utf-8",
        )
        return manifest

    return _write
```

- [ ] **Step 2: Write the failing tests**

```python
# test_use_case_config.py
import pytest
from ndr_lakehouse.asset_property.use_case import (
    discover_use_case_configs,
    load_use_case_config,
    resolve_use_case,
)
from ndr_lakehouse.domain.errors import AmbiguousDefaultUseCaseError, UseCaseNotFoundError


def test_manifest_paths_resolve_against_manifest_directory(tmp_path, write_use_case):
    manifest = write_use_case("demo")
    config = load_use_case_config(manifest)
    assert config.assets_path == (tmp_path / "demo" / "assets").resolve()
    assert config.platform_path == (tmp_path / "demo" / "assets" / "platform.yaml").resolve()


def test_dbt_sources_path_resolves_against_dbt_project_dir(tmp_path, write_use_case):
    manifest = write_use_case("demo")
    config = load_use_case_config(manifest)
    # dbt_project_dir is manifest-relative, dbt_sources_path is dbt_project_dir-relative
    expected = (tmp_path / "demo" / "../../dbt_lakehouse/models/staging/_sources.yml").resolve()
    assert config.dbt_sources_file == expected


def test_default_use_case_is_selected(tmp_path, write_use_case):
    write_use_case("one")
    write_use_case("two", default=True)
    assert resolve_use_case(tmp_path).name == "two"


def test_two_default_use_cases_are_rejected(tmp_path, write_use_case):
    write_use_case("one", default=True)
    write_use_case("two", default=True)
    with pytest.raises(AmbiguousDefaultUseCaseError, match="one.*two|two.*one"):
        resolve_use_case(tmp_path)


def test_unknown_use_case_name_lists_available(tmp_path, write_use_case):
    write_use_case("one")
    with pytest.raises(UseCaseNotFoundError, match="one"):
        resolve_use_case(tmp_path, "nope")


def test_unknown_manifest_keys_are_rejected(tmp_path, write_use_case):
    manifest = write_use_case("demo")
    manifest.write_text(
        manifest.read_text(encoding="utf-8") + "  bogus_key: 1\n", encoding="utf-8"
    )
    with pytest.raises(ValidationError):
        load_use_case_config(manifest)


def test_discover_returns_every_manifest_sorted(tmp_path, write_use_case):
    write_use_case("beta")
    write_use_case("alpha")
    assert [c.name for c in discover_use_case_configs(tmp_path)] == ["alpha", "beta"]
```

Import `ValidationError` from `pydantic` in the test module.

- [ ] **Step 3: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_use_case_config.py -q --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'ndr_lakehouse.asset_property.use_case'`

- [ ] **Step 4: Add the two errors to `domain/errors.py`**

Append:

```python
class UseCaseNotFoundError(LakehouseError):
    """No use-case manifest matched the requested name."""


class AmbiguousDefaultUseCaseError(LakehouseError):
    """More than one use-case manifest is marked as the default."""
```

- [ ] **Step 5: Implement `asset_property/use_case.py`**

`UseCaseConfig` extends the existing `_StrictModel`. Store the three raw path fields plus the resolved base directory in a private attribute set by a `model_validator(mode="after")`, following the pattern already used for `SourceProperties._normalise_records_path`. `load_use_case_config` reads YAML and validates the `use_case:` block. `resolve_use_case(root, name=None)` loads every manifest under `root/*/use_case.yaml`; with no name it requires exactly one `default=True` and raises `AmbiguousDefaultUseCaseError` naming all conflicting paths, or `UseCaseNotFoundError` (with the discovered names in the message) when zero exist.

- [ ] **Step 6: Export the new names**

Add `UseCaseConfig`, `discover_use_case_configs`, `load_use_case_config`, `resolve_use_case` to `asset_property/__init__.py` imports and `__all__`. Add `UseCaseNotFoundError`, `AmbiguousDefaultUseCaseError` to `domain/__init__.py` imports and `__all__`.

- [ ] **Step 7: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_use_case_config.py -q --no-cov`
Expected: PASS (7 tests)

- [ ] **Step 8: Commit**

```bash
git add orchestration/ingestion/src/ndr_lakehouse/asset_property/use_case.py \
        orchestration/ingestion/src/ndr_lakehouse/asset_property/__init__.py \
        orchestration/ingestion/src/ndr_lakehouse/domain/errors.py \
        orchestration/ingestion/src/ndr_lakehouse/domain/__init__.py \
        orchestration/ingestion/tests/ndr_lakehouse/test_use_case_config.py
git commit -m "feat(asset_property): add use-case manifest model"
```

---

### Task 2: Recursive asset discovery and typed duplicate error

**Files:**
- Modify: `orchestration/ingestion/src/ndr_lakehouse/asset_property/schema_validator.py:406-425`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/domain/errors.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_schema_validator.py:123-129`

**Interfaces:**
- Produces: `DuplicateAssetNameError(LakehouseError, ValueError)`; `discover_asset_configs(asset_dir, *, recursive: bool = False) -> list[AssetConfig]`.
- Consumes: nothing new.

- [ ] **Step 1: Write the failing test**

Add to `test_schema_validator.py`:

```python
def test_duplicate_asset_name_error_is_a_value_error(tmp_path: Path, asset_payload):
    import yaml

    (tmp_path / "a.yaml").write_text(yaml.safe_dump(asset_payload), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate asset name"):
        discover_asset_configs(tmp_path)


def test_recursive_discovery_finds_nested_assets(tmp_path: Path, asset_payload):
    import yaml

    nested = tmp_path / "sub"
    nested.mkdir()
    (nested / "a.yaml").write_text(yaml.safe_dump(asset_payload), encoding="utf-8")
    assert [c.name for c in discover_asset_configs(tmp_path, recursive=True)] == ["demo_source"]
    assert discover_asset_configs(tmp_path, recursive=False) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_schema_validator.py -q --no-cov -k "duplicate or recursive"`
Expected: FAIL — `TypeError: discover_asset_configs() got an unexpected keyword argument 'recursive'`

- [ ] **Step 3: Implement**

Add `DuplicateAssetNameError(LakehouseError, ValueError)` to `domain/errors.py`. In `discover_asset_configs`, replace the bare `ValueError` with it and add the `recursive` keyword: `directory.glob("**/*.yaml")` when recursive, keeping the existing `platform.yaml` exclusion and `sorted()` ordering.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_schema_validator.py -q --no-cov`
Expected: PASS (existing count + 2)

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(asset_property): typed duplicate error and recursive discovery"
```

---

### Task 3: Handler protocol and context in `domain`

**Files:**
- Modify: `orchestration/ingestion/src/ndr_lakehouse/domain/ports.py`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/domain/errors.py`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/domain/__init__.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_use_case_handler.py`

**Interfaces:**
- Produces:
  - `class LandingPort(Protocol): def ingest(self, config: AssetConfig) -> str: ...`
  - `class IcebergPort(Protocol): def load(self, config: AssetConfig, landing_uri: str) -> dict[str, Any]: ...`
  - `@dataclass(frozen=True) class HandlerContext: landing: LandingPort; iceberg: IcebergPort; settings: Mapping[str, str]`
  - `class UseCaseHandler(Protocol): def land(self, context: HandlerContext, config: AssetConfig) -> str | None: ...; def load(self, context: HandlerContext, config: AssetConfig, landing_uri: str) -> dict[str, Any] | None: ...`
  - `InvalidHandlerReferenceError(LakehouseError)`
- Consumes: `AssetConfig` from `asset_property.schema_validator`. `ports.py` currently imports only stdlib, so import `AssetConfig` under `if TYPE_CHECKING:` and quote the annotations — this keeps `domain` free of a runtime dependency on `asset_property`.

- [ ] **Step 1: Write the failing test**

```python
# test_use_case_handler.py
from dataclasses import FrozenInstanceError

import pytest
from ndr_lakehouse.domain.ports import HandlerContext, UseCaseHandler


class _Stub:
    def __init__(self):
        self.landed = []
        self.loaded = []

    def ingest(self, config):
        self.landed.append(config)
        return "s3a://landing/x"

    def load(self, config, landing_uri):
        self.loaded.append((config, landing_uri))
        return {"table": "t", "row_count": 1}


def test_handler_context_is_frozen(sample_asset_config):
    stub = _Stub()
    ctx = HandlerContext(landing=stub, iceberg=stub, settings={"a": "b"})
    with pytest.raises(FrozenInstanceError):
        ctx.settings = {}
    assert ctx.landing.ingest(sample_asset_config) == "s3a://landing/x"


def test_handler_protocol_declares_land_and_load():
    # ``__protocol_attrs__`` is 3.12+, so assert on the declared members instead.
    members = set(dir(UseCaseHandler))
    assert {"land", "load"} <= members
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_use_case_handler.py -q --no-cov`
Expected: FAIL — `ImportError: cannot import name 'HandlerContext'`

- [ ] **Step 3: Implement the protocol, context and error**

Add the three ports, the dataclass and the error. `UseCaseHandler` is a `Protocol` (no `runtime_checkable` — conformance is checked by `resolve_handler` via attribute lookup, not `isinstance`). Annotate `AssetConfig` with a quoted string under `if TYPE_CHECKING:` so `domain` keeps no runtime import edge into `asset_property`.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_use_case_handler.py -q --no-cov`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(domain): add use-case handler contract"
```

---

### Task 4: Handler resolution

**Files:**
- Create: `orchestration/ingestion/src/ndr_lakehouse/orchestration/handlers.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_handler_resolution.py`

**Interfaces:**
- Produces:
  - `resolve_handler(dotted: str, *, asset_name: str) -> UseCaseHandler`
  - `class NullHandler` implementing both methods returning `None`.
- Consumes: `InvalidHandlerReferenceError` (Task 3).

- [ ] **Step 1: Write the failing tests**

```python
import pytest
from ndr_lakehouse.domain.errors import InvalidHandlerReferenceError
from ndr_lakehouse.orchestration.handlers import NullHandler, resolve_handler


def test_resolves_a_module_attribute():
    handler = resolve_handler(
        "ndr_lakehouse.orchestration.handlers:NullHandler", asset_name="demo"
    )
    assert isinstance(handler, NullHandler)


def test_missing_attribute_names_asset_and_path():
    with pytest.raises(InvalidHandlerReferenceError, match="demo.*Missing|Missing.*demo"):
        resolve_handler("ndr_lakehouse.orchestration.handlers:Missing", asset_name="demo")


def test_malformed_reference_is_rejected():
    with pytest.raises(InvalidHandlerReferenceError, match="colon|':'"):
        resolve_handler("ndr_lakehouse.orchestration.handlers.NullHandler", asset_name="demo")


def test_missing_module_is_wrapped():
    with pytest.raises(InvalidHandlerReferenceError, match="no_such_module"):
        resolve_handler("no_such_module:X", asset_name="demo")


def test_null_handler_returns_none(sample_asset_config):
    handler = NullHandler()
    ctx = None  # never touched
    assert handler.land(ctx, sample_asset_config) is None
    assert handler.load(ctx, sample_asset_config, "uri") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_handler_resolution.py -q --no-cov`
Expected: FAIL — module not found

- [ ] **Step 3: Implement**

Split on `:` once, require exactly two non-empty parts, `importlib.import_module`, `getattr`. Wrap `ImportError`, `AttributeError` and `ValueError` into `InvalidHandlerReferenceError` with a message containing the asset name and the dotted path. Do not check `isinstance` against the protocol; instead verify `callable(getattr(cls(), "land", None))` and the same for `load`, raising the same error when absent.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_handler_resolution.py -q --no-cov`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(orchestration): resolve use-case handlers from dotted paths"
```

---

### Task 5: `AssetConfig.handler` field and factory wiring

**Files:**
- Modify: `orchestration/ingestion/src/ndr_lakehouse/asset_property/schema_validator.py` (`AssetConfig`)
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/assets.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py`

**Interfaces:**
- Produces: `AssetConfig.handler: str | None = None`; `build_ingestion_assets_from_yaml(asset_dir=None, configs=None, *, recursive=False)` unchanged otherwise.
- Consumes: `resolve_handler`, `NullHandler` (Task 4); `HandlerContext` (Task 3).

- [ ] **Step 1: Write the failing test**

Handlers are resolved by importing a module attribute, so the test handler must live at **module level** in the test file, not inside the test function.

```python
# module level, in test_dagster_defs.py
HANDLER_CALLS: list[str] = []


class _RecordingHandler:
    def land(self, context, config):
        HANDLER_CALLS.append("land")
        return "s3a://custom/output"

    def load(self, context, config, landing_uri):
        HANDLER_CALLS.append("load")
        return {"row_count": 7}


def test_custom_handler_overrides_landing_and_merges_load(mocker, sample_asset_config):
    HANDLER_CALLS.clear()
    mocker.patch.object(UnityCatalogProvisionerResource, "ensure_buckets", return_value={})
    mocker.patch.object(
        UnityCatalogProvisionerResource, "ensure_catalog_and_schema", return_value={}
    )
    ingestor = NativeIngestorResource()
    mocker.patch.object(NativeIngestorResource, "ingest", return_value="s3a://platform/x")
    spark = SparkIcebergResource()
    mocker.patch.object(
        SparkIcebergResource,
        "load",
        return_value={"table": "lakehouse.bronze.demo_table", "row_count": 42},
    )

    config = sample_asset_config.model_copy(
        update={"handler": f"{__name__}:_RecordingHandler"}
    )
    assets = build_ingestion_assets_from_yaml(configs=[config])
    result = materialize(
        assets,
        resources={
            "uc_provisioner": UnityCatalogProvisionerResource(),
            "native_ingestor": ingestor,
            "spark_iceberg": spark,
        },
    )

    assert result.success
    assert HANDLER_CALLS == ["land", "load"]
    NativeIngestorResource.ingest.assert_not_called()  # handler returned non-None
    SparkIcebergResource.load.assert_not_called()
    metadata = result.asset_materializations_for_node("lakehouse__bronze__demo_table")[0].metadata
    assert metadata["row_count"].value == 7  # handler dict wins
    assert metadata["table"].value == "lakehouse.bronze.demo_table"


def test_asset_without_handler_uses_platform_path(mocker, sample_asset_config):
    mocker.patch.object(UnityCatalogProvisionerResource, "ensure_buckets", return_value={})
    mocker.patch.object(
        UnityCatalogProvisionerResource, "ensure_catalog_and_schema", return_value={}
    )
    ingestor = NativeIngestorResource()
    mocker.patch.object(
        NativeIngestorResource, "ingest", return_value="s3a://platform/output"
    )

    assets = build_ingestion_assets_from_yaml(configs=[sample_asset_config])
    landing_asset = next(
        a for a in assets if AssetKey(["landing", "demo_source"]) in a.keys
    )

    result = materialize(
        [landing_asset],
        resources={
            "uc_provisioner": UnityCatalogProvisionerResource(),
            "native_ingestor": ingestor,
        },
    )
    assert result.success
    NativeIngestorResource.ingest.assert_called_once_with(sample_asset_config)
```

The `HANDLER_CALLS` module-level list is cleared at the top of each test that uses it, so ordering between tests cannot leak state. `resolve_handler` uses `importlib.import_module(__name__)`, which resolves straight out of `sys.modules` for an already-collected test module — so this works even though the test module's dotted name is not importable from a clean interpreter.

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py -q --no-cov -k handler`
Expected: FAIL — `handler` is not a valid field for `AssetConfig`

- [ ] **Step 3: Implement**

Add `handler: str | None = None` to `AssetConfig`. In `_landing_asset`, resolve the handler **once at asset-build time** (not per materialisation) when `config.handler` is set, else use `NullHandler()`. Build a `HandlerContext` inside the op from the Dagster resources via two thin adapters defined in `orchestration/assets.py` (`_LandingAdapter` wrapping `NativeIngestorResource`, `_IcebergAdapter` wrapping `SparkIcebergResource`) implementing `LandingPort` / `IcebergPort`. `settings` maps the catalog `full_table_name` and asset name so a handler needs no other context.

Provisioning (`ensure_buckets`, `ensure_catalog_and_schema`) runs first, **unconditionally**, before either branch. Then:

```python
result = handler.land(ctx, config)
s3a_uri = result if result is not None else native_ingestor.ingest(config)
```

and for load, merge rather than replace:

```python
override = handler.load(ctx, config, landing_uri)
result = spark_iceberg.load(config, landing_uri)
if override:
    result = {**result, **override}
```

Keep the existing `context.add_output_metadata` blocks unchanged so metadata keys stay stable.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py -q --no-cov`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(orchestration): let asset configs override landing and load"
```

---

### Task 6: Parameterise resources and remove import-time defaults

**Files:**
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/resources.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py`

**Interfaces:**
- Produces: `build_resource_bundle(use_case: UseCaseConfig, *, register_dbt: bool = True) -> dict[str, object]`. There is **no default-platform path**: the function cannot be called without a `UseCaseConfig`.
- Removes: `DEFAULT_PLATFORM` only. The `paths.py` constants (`ASSET_PROPERTY_DIR`, `DBT_PROJECT_DIR`, `DBT_SOURCES_PATH`, `DBT_MANIFEST_PATH`) **stay for now** — `assets.py`, `dbt_assets.py` and `definitions.py` still import them, so deleting them here would leave the tree non-importable. Task 8 deletes them once the last consumer is gone.
- Consumes: `UseCaseConfig` (Task 1).

- [ ] **Step 1: Write the failing test**

```python
def test_resource_bundle_requires_a_use_case():
    from ndr_lakehouse.orchestration.resources import build_resource_bundle

    assert "use_case" in inspect.signature(build_resource_bundle).parameters
    with pytest.raises(TypeError):
        build_resource_bundle()


def test_resource_bundle_uses_manifest_platform(write_use_case):
    from ndr_lakehouse.asset_property.use_case import load_use_case_config
    from ndr_lakehouse.orchestration.resources import build_resource_bundle

    config = load_use_case_config(write_use_case("demo"))
    bundle = build_resource_bundle(config, register_dbt=False)
    assert {"uc_provisioner", "native_ingestor", "spark_iceberg"} <= bundle.keys()
    assert "dbt" not in bundle


def test_resource_bundle_warns_when_dbt_project_is_missing(write_use_case, caplog):
    from ndr_lakehouse.asset_property.use_case import load_use_case_config
    from ndr_lakehouse.orchestration.resources import build_resource_bundle

    config = load_use_case_config(write_use_case("demo"))
    with caplog.at_level("WARNING"):
        bundle = build_resource_bundle(config, register_dbt=True)
    assert "dbt" not in bundle
    assert any("dbt_project.yml" in record.message for record in caplog.records)
```

Import `inspect` at the top of the test module.

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py -q --no-cov -k resource_bundle`
Expected: FAIL — `build_resource_bundle() takes 0 positional arguments`

- [ ] **Step 3: Implement**

Delete `DEFAULT_PLATFORM` and the `platform or DEFAULT_PLATFORM` fallback. `build_resource_bundle` takes the config, loads `use_case.platform_path` with `load_platform_config`, and builds the same three resources. When `register_dbt` is true, the `dbt` resource is registered only if `(dbt_project_dir / "dbt_project.yml").is_file()`; otherwise log a warning naming the resolved path (Review Focus #4). Leave `orchestration/__init__.py` alone in this task — it still re-exports `metricflow_semantic_validation`, which Task 7 removes.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py -q --no-cov`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git commit -am "refactor(orchestration): build resources from a use-case manifest"
```

---

### Task 7: Parameterise the dbt and MetricFlow assets

**Files:**
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/dbt_assets.py`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/dbt_source_sync.py`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/__init__.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_dbt_semantic_assets.py`

**Interfaces:**
- Produces:
  - `build_metricflow_asset(*, gold_models: Sequence[str], dbt_project_dir: Path, metric: str) -> AssetsDefinition`
  - `build_dbt_assets(manifest_path=None, project_dir=None) -> list[Any]` (unchanged signature)
  - `sync_dbt_sources(asset_dir, output_path, *, source_label: str) -> Path`
- Removes: `GOLD_MODEL_NAMES`, `GOLD_ASSET_KEYS`, the module-level `metricflow_semantic_validation` asset — and their re-exports in `orchestration/__init__.py` (`__all__` and imports), which would otherwise raise `ImportError` at package import.

- [ ] **Step 1: Rewrite the failing test**

Replace the two constant-based tests:

```python
def test_metricflow_asset_depends_on_configured_gold_models():
    from ndr_lakehouse.orchestration.dbt_assets import build_metricflow_asset

    asset = build_metricflow_asset(
        gold_models=["dim_a", "fct_b"],
        dbt_project_dir=Path("/tmp/dbt"),
        metric="prix",
    )
    key = next(iter(asset.keys))
    assert set(asset.asset_deps[key]) == {AssetKey("dim_a"), AssetKey("fct_b")}


def test_generic_module_has_no_use_case_constants():
    import ndr_lakehouse.orchestration.dbt_assets as mod

    assert not hasattr(mod, "GOLD_MODEL_NAMES")
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_dbt_semantic_assets.py -q --no-cov`
Expected: FAIL — `ImportError: cannot import name 'GOLD_ASSET_KEYS'` / no `build_metricflow_asset`

- [ ] **Step 3: Implement**

Turn the asset into a factory returning an `@asset(...)`-decorated inner function named `metricflow_semantic_validation`, using `metric` in the description and `dbt_project_dir` in the subprocess `cwd`/env. Add a keyword-only `source_label` to `sync_dbt_sources` and thread it into the generated header in place of the hardcoded `orchestration/ingestion/src/ndr_lakehouse/asset_property/*.yaml` path. Keep the `mf validate-configs --skip-dw` invocation and `RuntimeError` on non-zero exit. Update `orchestration/__init__.py`: drop the `GOLD_ASSET_KEYS`, `metricflow_semantic_validation` and `defs` names from the imports and `__all__`, add `build_metricflow_asset`.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_dbt_semantic_assets.py -q --no-cov`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git commit -am "refactor(orchestration): build dbt and semantic assets from config"
```

---

### Task 8: `build_definitions` from a use case

**Files:**
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/definitions.py`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/paths.py`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/assets.py`
- Modify: `orchestration/ingestion/src/ndr_lakehouse/__init__.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py`

**Interfaces:**
- Produces: `build_definitions(use_case: str | Path | None = None, *, register_dbt: bool = True, sync_sources: bool = True, root: Path | None = None) -> Definitions`.
- Removes: module-level `defs` from `ndr_lakehouse` entirely; the `ASSET_PROPERTY_DIR`, `DBT_PROJECT_DIR`, `DBT_SOURCES_PATH`, `DBT_MANIFEST_PATH` constants from `paths.py` (only now, once `assets.py`/`definitions.py` stop importing them); `DEFAULT_ASSET_PROPERTY_DIR` from `assets.py`. `paths.py` ends up holding just `PACKAGE_ROOT`, `REPO_ROOT` and `DEFAULT_USE_CASE_ROOT = REPO_ROOT / "use_case"`.

- [ ] **Step 1: Write the failing tests**

```python
def test_build_definitions_requires_an_explicit_or_default_use_case(tmp_path):
    from ndr_lakehouse.domain.errors import UseCaseNotFoundError
    from ndr_lakehouse.orchestration.definitions import build_definitions

    with pytest.raises(UseCaseNotFoundError):
        build_definitions(root=tmp_path)


def test_build_definitions_builds_assets_from_the_manifest(write_use_case, asset_payload):
    from ndr_lakehouse.orchestration.definitions import build_definitions

    manifest = write_use_case("demo")
    (manifest.parent / "assets" / "demo.yaml").write_text(
        yaml.safe_dump(asset_payload), encoding="utf-8"
    )

    definitions = build_definitions(
        "demo", root=manifest.parent.parent, register_dbt=False, sync_sources=False
    )
    assert AssetKey(["landing", "demo_source"]) in definitions.get_all_asset_keys()


def test_library_no_longer_exposes_defs():
    import ndr_lakehouse

    assert not hasattr(ndr_lakehouse, "defs")
```

`yaml` is already imported at the top of `test_dagster_defs.py`; add `import inspect` and `import pytest` there too.

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py -q --no-cov -k "definitions or exposes_defs"`
Expected: FAIL — `build_definitions() got an unexpected keyword argument 'root'`

- [ ] **Step 3: Implement**

Resolve the manifest with `resolve_use_case(root or DEFAULT_USE_CASE_ROOT, use_case)`. Discover assets with `recursive=False` from `use_case.assets_path` (pass `asset_dir=` explicitly; `discover_ingestion_configs` loses its default). When `sync_sources`, call `sync_dbt_sources(asset_dir=..., output_path=use_case.dbt_sources_file, source_label=...)` in a `try/except OSError` that logs a warning (Review Focus #5). When `register_dbt` **and** the dbt project exists, append `build_dbt_assets(use_case.dbt_manifest_path, use_case.dbt_project_path)` and `build_metricflow_asset(gold_models=use_case.gold_models, dbt_project_dir=use_case.dbt_project_path, metric=use_case.semantic_metric)`. Call `build_resource_bundle(use_case, register_dbt=register_dbt)` so the `dbt` resource and the dbt assets appear together or not at all. Delete `defs = build_definitions()` and the `defs` export from `ndr_lakehouse/__init__.py`.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/ -q --no-cov`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git commit -am "refactor(orchestration): parameterise build_definitions by use case"
```

---

### Task 9: Create the `use_case/` package and move the YAML

**Files:**
- Create: `use_case/__init__.py`, `use_case/real_estate_77/use_case.yaml`, `use_case/real_estate_77/handlers/__init__.py`
- Move: `ndr_lakehouse/asset_property/platform.yaml` → `use_case/real_estate_77/assets/platform.yaml`
- Move: `ndr_lakehouse/asset_property/dvf_and_geo_77.yaml` → `use_case/real_estate_77/assets/dvf_and_geo_77.yaml`
- Modify: `pyproject.toml` (`pythonpath`), `workspace.yaml`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_use_case_package.py`

**Interfaces:**
- Produces: `use_case/__init__.py` exposing `defs`.
- Consumes: `build_definitions` (Task 8).

- [ ] **Step 1: Write the failing test**

```python
def test_packaged_use_case_builds_the_full_graph():
    from use_case import defs

    keys = defs.get_all_asset_keys()
    assert AssetKey(["landing", "communes_geo_77"]) in keys
    assert AssetKey(["lakehouse", "bronze", "dvf_mutations_77"]) in keys


def test_no_asset_yaml_remains_in_the_library():
    from pathlib import Path
    import ndr_lakehouse

    asset_property = Path(ndr_lakehouse.__file__).parent / "asset_property"
    assert list(asset_property.glob("*.yaml")) == []
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_use_case_package.py -q --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'use_case'`. This stays failing until both the package exists (step 3) **and** `"."` is on `pythonpath`, so step 3 must do both before step 4 can pass.

- [ ] **Step 3: Create the package and move files**

Write `use_case/real_estate_77/use_case.yaml`:

```yaml
use_case:
  name: real_estate_77
  description: Seine-et-Marne (77) real-estate and commune referential
  assets_dir: assets
  platform_file: assets/platform.yaml
  dbt_project_dir: ../../dbt_lakehouse
  dbt_sources_path: models/staging/_sources.yml
  gold_models: [dim_communes, fct_mutations, fct_lots_batis]
  semantic_metric: prix_moyen_m2
  default: true
```

`use_case/__init__.py`:

```python
from ndr_lakehouse.orchestration.definitions import build_definitions

defs = build_definitions()
```

Use `git mv` for the two YAML files. Update `pyproject.toml` `pythonpath = ["orchestration/ingestion/src", "."]` and `workspace.yaml` to `python_module: use_case`.

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest orchestration/ingestion/tests/ndr_lakehouse/test_use_case_package.py -q --no-cov`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add -A use_case pyproject.toml workspace.yaml
git commit -m "feat(use_case): move situated configuration into a use-case package"
```

---

### Task 10: Deployment wiring and docs

**Files:**
- Modify: `Dockerfile.dagster` (`COPY use_case ./use_case`, `PYTHONPATH`), `docker-compose.yml` (both Dagster services), `Makefile` (`-m use_case`), `README.md`
- Test: none (verified by commands below)

**Interfaces:** none.

- [ ] **Step 1: Update the image**

Add `COPY use_case ./use_case` next to the existing `COPY` lines and set `PYTHONPATH=/app/orchestration/ingestion/src:/app`.

- [ ] **Step 2: Update compose**

Change `PYTHONPATH` to `/app/orchestration/ingestion/src:/app` in **both** `dagster-webserver` and `dagster-daemon`.

- [ ] **Step 3: Update the Makefile**

Replace `-m ndr_lakehouse` with `-m use_case` in `dagster-dev` and `ingest`.

- [ ] **Step 4: Update the README**

Document the `use_case/` layout, the manifest keys, and the handler escape hatch. Add `use_case/` to the repository-layout tree and remove `platform.yaml` from the `asset_property/` description.

- [ ] **Step 5: Verify**

```bash
.venv/bin/python -c "from use_case import defs; print(sorted(str(k) for k in defs.get_all_asset_keys()))"
.venv/bin/python -m pytest -q
```

Expected: the asset keys include the six ingestion assets plus the dbt/semantic ones; full suite passes with the 80% coverage gate.

- [ ] **Step 6: Commit**

```bash
git commit -am "chore: load the Dagster workspace from the use-case package"
```
