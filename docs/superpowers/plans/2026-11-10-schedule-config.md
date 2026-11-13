# YAML-driven Dagster schedules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a per-use-case `schedule.yaml` declare cron-triggered asset jobs, wired into the existing metadata-driven `build_definitions()`.

**Architecture:** `UseCaseConfig` gains a manifest-relative `schedule_file` path. A new `orchestration/schedules.py` validates that file with Pydantic and builds one `define_asset_job` + `ScheduleDefinition` per entry. `build_definitions()` merges the schedules into `Definitions`, and rejects a schedule whose selection matches no asset. The use-case directory is bind-mounted into both Dagster containers so cron edits need a restart, not an image rebuild.

**Tech Stack:** Python 3.11/3.12, Dagster 1.13, Pydantic v2, PyYAML, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-11-10-schedule-config-design.md`

## Global Constraints

- Dagster `>=1.13.29,<2`; do not add a cron library — use `dagster._utils.schedules.is_valid_cron_string`.
- Line length 120; `black`, `isort`, `flake8`, `ruff`, `mypy`, `pylint` all run in CI.
- Tests run on Python 3.11 and 3.12; coverage gate is 80%.
- The YAML file is the only source of the cron; no cron literal in production Python.
- All new modules start with `from __future__ import annotations`.

## Review Focus

- Unknown key in `schedule.yaml` → load fails loudly (`extra="forbid"`), never ignored.
- Two schedules with the same name → load fails, never silently drops one.
- Selection matching zero assets → `build_definitions()` fails; no empty job ticks.
- Explicit `schedule_file` path that does not exist → `FileNotFoundError`, not a silent no-op.
- `enabled` omitted → schedule is registered STOPPED, not running.

---

### Task 1: Manifest schedule path

**Files:**
- Modify: `orchestration/ingestion/src/ndr_lakehouse/asset_property/use_case.py`
- Test: `orchestration/ingestion/tests/ndr_lakehouse/test_use_case_config.py`

**Interfaces:**
- Consumes: existing `UseCaseConfig._base_dir`.
- Produces: `UseCaseConfig.schedule_file: Path` (default `Path("schedule.yaml")`) and `UseCaseConfig.schedule_path -> Path` (absolute, resolved against the manifest directory). Task 2's `load_schedule_specs(config)` uses `config.schedule_path` and `config.model_fields_set`.

- [ ] **Step 1: Write the failing tests**

Append to `test_use_case_config.py`:

```python
def test_schedule_path_defaults_to_schedule_yaml_next_to_the_manifest(tmp_path, write_use_case):
    manifest = write_use_case("demo")
    config = load_use_case_config(manifest)
    assert config.schedule_path == (tmp_path / "demo" / "schedule.yaml").resolve()


def test_schedule_file_is_manifest_relative(tmp_path, write_use_case):
    manifest = write_use_case("demo")
    manifest.write_text(
        manifest.read_text(encoding="utf-8") + "  schedule_file: configs/cron.yaml\n",
        encoding="utf-8",
    )
    config = load_use_case_config(manifest)
    assert config.schedule_path == (tmp_path / "demo" / "configs" / "cron.yaml").resolve()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest orchestration/ingestion/tests/ndr_lakehouse/test_use_case_config.py -k schedule -v`
Expected: FAIL with `AttributeError: 'UseCaseConfig' object has no attribute 'schedule_path'`.

- [ ] **Step 3: Implement the field and property**

In `UseCaseConfig`, after `semantic_metric: str`:

```python
    schedule_file: Path = Path("schedule.yaml")
```

After the `dbt_manifest_path` property:

```python
    @property
    def schedule_path(self) -> Path:
        """Absolute path of the schedule YAML, next to the manifest by default."""
        return (self._base_dir / self.schedule_file).resolve()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest orchestration/ingestion/tests/ndr_lakehouse/test_use_case_config.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add orchestration/ingestion/src/ndr_lakehouse/asset_property/use_case.py \
        orchestration/ingestion/tests/ndr_lakehouse/test_use_case_config.py
git commit -m "feat: resolve a per-use-case schedule file from the manifest"
```

---

### Task 2: Schedule models, loader and builder

**Files:**
- Create: `orchestration/ingestion/src/ndr_lakehouse/orchestration/schedules.py`
- Create: `orchestration/ingestion/tests/ndr_lakehouse/test_schedules.py`

**Interfaces:**
- Consumes: `UseCaseConfig` (Task 1); `_StrictModel` from `ndr_lakehouse.asset_property.schema_validator`.
- Produces:
  - `class ScheduleSelection(_StrictModel)` with `groups: list[str] | None`, `assets: list[list[str]] | None`, exactly one non-empty.
  - `class ScheduleSpec(_StrictModel)` with `name: str`, `description: str = ""`, `cron: str`, `timezone: str = "UTC"`, `enabled: bool = False`, `selection: ScheduleSelection | None = None`.
  - `class SchedulesFile(_StrictModel)` with `schedules: list[ScheduleSpec] = []`.
  - `def load_schedule_specs(config: UseCaseConfig) -> list[ScheduleSpec]`.
  - `def build_schedules(specs: Sequence[ScheduleSpec]) -> list[ScheduleDefinition]`.
  - `def validate_schedule_targets(definitions: Definitions) -> None`.

- [ ] **Step 1: Write the failing tests**

Create `test_schedules.py`:

```python
"""Tests for the YAML schedule contract and Dagster schedule builder."""

from __future__ import annotations

import pytest
import yaml
from dagster import (
    AssetKey,
    AssetSelection,
    DagsterInvalidDefinitionError,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import ValidationError

from ndr_lakehouse.asset_property.use_case import load_use_case_config
from ndr_lakehouse.orchestration.schedules import (
    ScheduleSelection,
    ScheduleSpec,
    build_schedules,
    load_schedule_specs,
    validate_schedule_targets,
)

VALID_SCHEDULE = {
    "schedules": [
        {
            "name": "daily",
            "cron": "0 6 * * *",
            "timezone": "Europe/Paris",
            "enabled": True,
            "selection": {"groups": ["landing"]},
        }
    ]
}


def _write_schedule(manifest, payload):
    path = manifest.parent / "schedule.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


def test_missing_default_schedule_file_yields_no_specs(write_use_case):
    config = load_use_case_config(write_use_case("demo"))
    assert load_schedule_specs(config) == []


def test_explicit_missing_schedule_file_raises(write_use_case):
    manifest = write_use_case("demo")
    manifest.write_text(
        manifest.read_text(encoding="utf-8") + "  schedule_file: nope.yaml\n",
        encoding="utf-8",
    )
    with pytest.raises(FileNotFoundError, match="nope.yaml"):
        load_schedule_specs(load_use_case_config(manifest))


def test_valid_schedule_file_is_parsed(write_use_case):
    manifest = write_use_case("demo")
    _write_schedule(manifest, VALID_SCHEDULE)
    [spec] = load_schedule_specs(load_use_case_config(manifest))
    assert spec.name == "daily"
    assert spec.cron == "0 6 * * *"
    assert spec.timezone == "Europe/Paris"
    assert spec.enabled is True
    assert spec.selection == ScheduleSelection(groups=["landing"])


def test_unknown_keys_are_rejected(write_use_case):
    manifest = write_use_case("demo")
    _write_schedule(manifest, {"schedules": [{"name": "daily", "cron": "0 6 * * *", "bogus": 1}]})
    with pytest.raises(ValidationError):
        load_schedule_specs(load_use_case_config(manifest))


def test_invalid_cron_is_rejected(write_use_case):
    manifest = write_use_case("demo")
    _write_schedule(manifest, {"schedules": [{"name": "daily", "cron": "99 6 * * *"}]})
    with pytest.raises(ValidationError, match="invalid cron"):
        load_schedule_specs(load_use_case_config(manifest))


def test_invalid_timezone_is_rejected():
    with pytest.raises(ValidationError, match="unknown timezone"):
        ScheduleSpec(name="daily", cron="0 6 * * *", timezone="Not/AZone")


def test_selection_cannot_set_groups_and_assets():
    with pytest.raises(ValidationError, match="only one of"):
        ScheduleSelection(groups=["landing"], assets=[["lakehouse", "bronze", "t"]])


def test_duplicate_schedule_names_are_rejected(write_use_case):
    manifest = write_use_case("demo")
    payload = {"schedules": [{"name": "daily", "cron": "0 6 * * *"}] * 2}
    _write_schedule(manifest, payload)
    with pytest.raises(ValidationError, match="duplicate schedule names"):
        load_schedule_specs(load_use_case_config(manifest))


def test_enabled_maps_to_schedule_status():
    running = ScheduleSpec(name="on", cron="0 6 * * *", enabled=True)
    stopped = ScheduleSpec(name="off", cron="0 6 * * *")
    [running_schedule, stopped_schedule] = build_schedules([running, stopped])
    assert running_schedule.default_status == DefaultScheduleStatus.RUNNING
    assert stopped_schedule.default_status == DefaultScheduleStatus.STOPPED


def test_selection_maps_to_the_job_selection():
    by_group = ScheduleSpec(name="g", cron="0 6 * * *", selection=ScheduleSelection(groups=["landing"]))
    by_asset = ScheduleSpec(
        name="a", cron="0 6 * * *", selection=ScheduleSelection(assets=[["lakehouse", "bronze", "t"]])
    )
    group_schedule, asset_schedule = build_schedules([by_group, by_asset])
    assert group_schedule.job.selection == AssetSelection.groups("landing")
    assert asset_schedule.job.selection == AssetSelection.assets(AssetKey(["lakehouse", "bronze", "t"]))


def test_schedule_selecting_no_assets_is_rejected():
    @asset(group_name="landing")
    def _a():
        ...

    schedule = ScheduleDefinition(
        name="empty",
        job=define_asset_job(name="empty_job", selection=AssetSelection.groups("nope")),
        cron_schedule="0 6 * * *",
    )
    with pytest.raises(DagsterInvalidDefinitionError, match="selects no assets"):
        validate_schedule_targets(Definitions(assets=[_a], schedules=[schedule]))


def test_schedule_selecting_assets_passes_validation():
    @asset(group_name="landing")
    def _a():
        ...

    schedule = ScheduleDefinition(
        name="ok",
        job=define_asset_job(name="ok_job", selection=AssetSelection.groups("landing")),
        cron_schedule="0 6 * * *",
    )
    validate_schedule_targets(Definitions(assets=[_a], schedules=[schedule]))
```

Note: `AssetSelection.assets("lakehouse", "bronze", "t")` accepts path components and yields `AssetKey(["lakehouse", "bronze", "t"])`; the production code below must build the same selection from a path list, i.e. `AssetSelection.assets(*selection.assets)`, so the equality assertion is meaningful.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest orchestration/ingestion/tests/ndr_lakehouse/test_schedules.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ndr_lakehouse.orchestration.schedules'`.

- [ ] **Step 3: Implement `schedules.py`**

```python
"""YAML-declared Dagster schedules for one use case."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from dagster import (
    AssetKey,
    AssetSelection,
    DagsterInvalidDefinitionError,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster._utils.schedules import is_valid_cron_string
from pydantic import Field, field_validator, model_validator

from ndr_lakehouse.asset_property.schema_validator import _StrictModel
from ndr_lakehouse.asset_property.use_case import UseCaseConfig


class ScheduleSelection(_StrictModel):
    """Narrow a schedule to asset groups or explicit asset key paths."""

    groups: list[str] | None = Field(default=None, min_length=1)
    assets: list[list[str]] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _exactly_one_form(self) -> ScheduleSelection:
        if self.groups is None and self.assets is None:
            raise ValueError("selection must set 'groups' or 'assets'")
        if self.groups is not None and self.assets is not None:
            raise ValueError("selection must set only one of 'groups' or 'assets'")
        return self


class ScheduleSpec(_StrictModel):
    """One cron entry in ``schedule.yaml``."""

    name: str = Field(min_length=1)
    description: str = ""
    cron: str
    timezone: str = "UTC"
    enabled: bool = False
    selection: ScheduleSelection | None = None

    @field_validator("cron")
    @classmethod
    def _validate_cron(cls, value: str) -> str:
        # Dagster's own five-field validator; private, so it is asserted by the
        # unit tests rather than left to chance on upgrade.
        if not is_valid_cron_string(value):
            raise ValueError(f"invalid cron expression: {value!r}")
        return value

    @field_validator("timezone")
    @classmethod
    def _validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except Exception as exc:  # ZoneInfoNotFoundError, ValueError
            raise ValueError(f"unknown timezone: {value!r}") from exc
        return value


class SchedulesFile(_StrictModel):
    """Top-level shape of ``schedule.yaml``."""

    schedules: list[ScheduleSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_names(self) -> SchedulesFile:
        names = [spec.name for spec in self.schedules]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate schedule names: {', '.join(duplicates)}")
        return self


def load_schedule_specs(config: UseCaseConfig) -> list[ScheduleSpec]:
    """Read the use case's schedule file; a missing default file means no schedules."""
    path = config.schedule_path
    if not path.is_file():
        if "schedule_file" in config.model_fields_set:
            raise FileNotFoundError(f"schedule file not found: {path}")
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"schedule file must contain a 'schedules' mapping: {path}")
    return SchedulesFile.model_validate(payload).schedules


def _asset_selection(selection: ScheduleSelection | None) -> AssetSelection:
    if selection is None:
        return AssetSelection.all()
    groups, assets = selection.groups, selection.assets
    if groups is not None:
        return AssetSelection.groups(*groups)
    assert assets is not None  # guaranteed by ScheduleSelection's validator
    return AssetSelection.assets(*assets)


def build_schedules(specs: Sequence[ScheduleSpec]) -> list[ScheduleDefinition]:
    """Turn validated specs into Dagster schedules, one asset job each."""
    schedules: list[ScheduleDefinition] = []
    for spec in specs:
        job = define_asset_job(name=f"{spec.name}_job", selection=_asset_selection(spec.selection))
        schedules.append(
            ScheduleDefinition(
                name=spec.name,
                job=job,
                cron_schedule=spec.cron,
                execution_timezone=spec.timezone,
                default_status=(
                    DefaultScheduleStatus.RUNNING if spec.enabled else DefaultScheduleStatus.STOPPED
                ),
                description=spec.description or None,
            )
        )
    return schedules


def validate_schedule_targets(definitions: Definitions) -> None:
    """Fail loudly when a schedule would run an empty selection."""
    graph = definitions.resolve_asset_graph()
    for schedule in definitions.schedules or []:
        selection = getattr(schedule.job, "selection", None)
        if selection is not None and not selection.resolve(graph):
            raise DagsterInvalidDefinitionError(
                f"schedule {schedule.name!r} selects no assets"
            )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest orchestration/ingestion/tests/ndr_lakehouse/test_schedules.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add orchestration/ingestion/src/ndr_lakehouse/orchestration/schedules.py \
        orchestration/ingestion/tests/ndr_lakehouse/test_schedules.py
git commit -m "feat: validate and build Dagster schedules from YAML"
```

---

### Task 3: Wire schedules into definitions, ship the example, document it

**Files:**
- Modify: `orchestration/ingestion/src/ndr_lakehouse/orchestration/definitions.py`
- Modify: `orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py`
- Create: `use_case/real_estate_77/schedule.yaml`
- Modify: `docker-compose.yml` (both Dagster services)
- Modify: `README.md`

**Interfaces:**
- Consumes: `load_schedule_specs`, `build_schedules`, `validate_schedule_targets` (Task 2); `config.schedule_path` (Task 1).
- Produces: `Definitions(..., schedules=[...])` from `build_definitions()`; no new public function.

- [ ] **Step 1: Write the failing integration tests**

Append to `test_dagster_defs.py`:

```python
def test_build_definitions_registers_a_schedule_from_yaml(write_use_case, asset_payload):
    from dagster import DefaultScheduleStatus

    from ndr_lakehouse.orchestration.definitions import build_definitions

    manifest = write_use_case("demo")
    (manifest.parent / "assets" / "demo.yaml").write_text(yaml.safe_dump(asset_payload), encoding="utf-8")
    (manifest.parent / "schedule.yaml").write_text(
        yaml.safe_dump(
            {
                "schedules": [
                    {
                        "name": "daily",
                        "cron": "0 6 * * *",
                        "timezone": "Europe/Paris",
                        "enabled": True,
                        "selection": {"groups": ["landing"]},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    definitions = build_definitions("demo", root=manifest.parent.parent, register_dbt=False, sync_sources=False)

    [schedule] = definitions.schedules
    assert schedule.name == "daily"
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "Europe/Paris"
    assert schedule.default_status == DefaultScheduleStatus.RUNNING


def test_build_definitions_has_no_schedules_without_a_file(write_use_case, asset_payload):
    from ndr_lakehouse.orchestration.definitions import build_definitions

    manifest = write_use_case("demo")
    (manifest.parent / "assets" / "demo.yaml").write_text(yaml.safe_dump(asset_payload), encoding="utf-8")

    definitions = build_definitions("demo", root=manifest.parent.parent, register_dbt=False, sync_sources=False)

    assert not definitions.schedules
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py -k schedule -v`
Expected: FAIL because `definitions.schedules` is empty.

- [ ] **Step 3: Wire schedules into `build_definitions`**

Add the import:

```python
from ndr_lakehouse.orchestration.schedules import (
    build_schedules,
    load_schedule_specs,
    validate_schedule_targets,
)
```

Replace the final `return` with:

```python
    definitions = Definitions(
        assets=assets,
        resources=resources,
        schedules=build_schedules(load_schedule_specs(config)),
    )
    validate_schedule_targets(definitions)
    return definitions
```

Update the function docstring with one sentence: "Cron-triggered asset jobs come from the use case's optional `schedule.yaml`; a selection that matches no asset fails at load."

- [ ] **Step 4: Add the example schedule**

Create `use_case/real_estate_77/schedule.yaml`:

```yaml
# ---------------------------------------------------------------------------
# Automation for the real-estate use case.
#
# Edit the cron below, then reload/restart the Dagster processes. With the
# compose bind mount, no image rebuild is needed. Standard five-field cron:
#   minute hour day-of-month month day-of-week
# Set enabled: false to register the schedule as stopped.
# ---------------------------------------------------------------------------
schedules:
  - name: daily_lakehouse_refresh
    description: Landing -> Bronze -> dbt Gold -> semantic validation
    cron: "0 6 * * *"          # 06:00 every day
    timezone: Europe/Paris
    enabled: true
    # No `selection` means every asset in dependency order. Narrow it with:
    # selection:
    #   groups: [landing, bronze, dbt, semantic]
```

- [ ] **Step 5: Bind-mount the use-case directory**

In `docker-compose.yml`, add this line to the `volumes:` list of **both** `dagster-webserver` and `dagster-daemon`, beside `./dagster-instance.yaml`:

```yaml
      - ./use_case:/app/use_case:ro
```

- [ ] **Step 6: Document it in the README**

Add a `## Schedules` section immediately after `### Where Dagster keeps its state`, covering: the file location, the fields (`cron`, `timezone`, `enabled`, optional `selection`), that the example runs daily at 06:00 Europe/Paris, and how to apply an edit:

```bash
docker compose restart dagster-webserver dagster-daemon
```

State that the daemon must be running for ticks, and that `enabled: false` registers a stopped schedule rather than hiding it.

- [ ] **Step 7: Run the tests and the definitions check**

Run: `uv run pytest orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py -k schedule -v`
Expected: PASS.

Run: `uv run dagster definitions validate -w workspace.yaml`
Expected: exit 0 with the schedule registered.

- [ ] **Step 8: Commit**

```bash
git add orchestration/ingestion/src/ndr_lakehouse/orchestration/definitions.py \
        orchestration/ingestion/tests/ndr_lakehouse/test_dagster_defs.py \
        use_case/real_estate_77/schedule.yaml docker-compose.yml README.md
git commit -m "feat: register YAML-declared schedules in Dagster"
```

---

## Final verification

- [ ] `uv run pytest` — full suite passes at the 80% coverage gate.
- [ ] `make check` — lint/type/SQL gates pass.
- [ ] `uv run dagster definitions validate -w workspace.yaml` — the shipped example loads.
- [ ] Manual: edit `use_case/real_estate_77/schedule.yaml`, restart both Dagster services, confirm the new cron appears in **Automation**.
