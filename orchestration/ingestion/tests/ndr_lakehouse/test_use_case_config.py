"""Tests for the use-case manifest model and discovery helpers."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

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
    # dbt_project_dir is manifest-relative, dbt_sources_path is dbt_project_dir-
    # relative. The fixture keeps both inside tmp_path, so this asserts the
    # asymmetry without reaching into the repository.
    project = tmp_path / "demo" / "dbt_project"
    expected = (project / "models" / "staging" / "_sources.yml").resolve()
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


def test_no_default_use_case_lists_available(tmp_path, write_use_case):
    write_use_case("one")
    write_use_case("two")
    with pytest.raises(UseCaseNotFoundError, match="one, two"):
        resolve_use_case(tmp_path)


def test_empty_root_reports_no_available_use_cases(tmp_path):
    with pytest.raises(UseCaseNotFoundError, match="available: none"):
        resolve_use_case(tmp_path)


def test_unknown_manifest_keys_are_rejected(tmp_path, write_use_case):
    manifest = write_use_case("demo")
    manifest.write_text(manifest.read_text(encoding="utf-8") + "  bogus_key: 1\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        load_use_case_config(manifest)


def test_discover_returns_every_manifest_sorted(tmp_path, write_use_case):
    write_use_case("beta")
    write_use_case("alpha")
    assert [c.name for c in discover_use_case_configs(tmp_path)] == ["alpha", "beta"]


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
