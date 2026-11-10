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
    asset,
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
    spec = ScheduleSpec(name="empty", cron="0 6 * * *")
    with pytest.raises(DagsterInvalidDefinitionError, match="selects no assets"):
        validate_schedule_targets(Definitions(assets=[]), [spec])


def test_schedule_with_unknown_group_in_a_partial_selection_is_rejected():
    @asset(group_name="landing")
    def _a(): ...

    spec = ScheduleSpec(name="partial", cron="0 6 * * *", selection=ScheduleSelection(groups=["landing", "nope"]))
    with pytest.raises(DagsterInvalidDefinitionError, match="unknown group.*nope"):
        validate_schedule_targets(Definitions(assets=[_a]), [spec])


def test_schedule_with_unknown_asset_key_names_the_schedule():
    @asset(group_name="landing")
    def _a(): ...

    spec = ScheduleSpec(
        name="badkey", cron="0 6 * * *", selection=ScheduleSelection(assets=[["lakehouse", "bronze", "nope"]])
    )
    with pytest.raises(DagsterInvalidDefinitionError, match="'badkey'.*unknown assets"):
        validate_schedule_targets(Definitions(assets=[_a]), [spec])


def test_schedule_selecting_assets_passes_validation():
    @asset(group_name="landing")
    def _a(): ...

    spec = ScheduleSpec(name="ok", cron="0 6 * * *", selection=ScheduleSelection(groups=["landing"]))
    validate_schedule_targets(Definitions(assets=[_a]), [spec])
