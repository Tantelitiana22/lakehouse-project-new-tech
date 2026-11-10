"""YAML-declared Dagster schedules for one use case."""

from __future__ import annotations

from collections.abc import Sequence
from zoneinfo import ZoneInfo

import yaml
from dagster import (
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
        # Dagster types the decorator as returning None; it returns an unresolved job.
        job = define_asset_job(  # pylint: disable=assignment-from-no-return
            name=f"{spec.name}_job", selection=_asset_selection(spec.selection)
        )
        schedules.append(
            ScheduleDefinition(
                name=spec.name,
                job=job,
                cron_schedule=spec.cron,
                execution_timezone=spec.timezone,
                default_status=(DefaultScheduleStatus.RUNNING if spec.enabled else DefaultScheduleStatus.STOPPED),
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
            raise DagsterInvalidDefinitionError(f"schedule {schedule.name!r} selects no assets")
