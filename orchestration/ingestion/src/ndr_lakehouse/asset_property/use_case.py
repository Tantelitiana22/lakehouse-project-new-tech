"""Use-case manifest: the situated configuration that binds the library to one project.

The generic ``ndr_lakehouse`` library knows nothing about a particular use case.
A single ``use_case.yaml`` names one, points at its asset directory, its platform
file, its dbt project and the gold models that project exposes. Everything else
(the assets and platform files themselves) stays declarative YAML.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import PrivateAttr, ValidationInfo, model_validator

from ndr_lakehouse.asset_property.schema_validator import _StrictModel
from ndr_lakehouse.domain.errors import AmbiguousDefaultUseCaseError, UseCaseNotFoundError

MANIFEST_FILENAME = "use_case.yaml"
_DEFAULT_MANIFEST_GLOB = f"*/{MANIFEST_FILENAME}"


class UseCaseConfig(_StrictModel):
    """Validated content of one ``use_case.yaml`` manifest.

    Relative paths are interpreted against the manifest's own directory, except
    ``dbt_sources_path`` which is relative to ``dbt_project_dir``. The ``*_path``
    and ``*_file`` properties return absolute, resolved paths.

    A config loaded by :func:`load_use_case_config` remembers the directory it
    was loaded from. A config constructed directly (e.g. in a unit test) has no
    such directory and falls back to the current working directory.
    """

    name: str
    description: str = ""
    assets_dir: Path
    platform_file: Path
    dbt_project_dir: Path
    dbt_sources_path: Path
    gold_models: list[str]
    semantic_metric: str
    default: bool = False

    _base_dir: Path = PrivateAttr(default_factory=Path.cwd)

    @model_validator(mode="after")
    def _record_base_dir(self, info: ValidationInfo) -> UseCaseConfig:
        """Remember the manifest directory so relative paths can be resolved."""
        context = info.context or {}
        base_dir = context.get("base_dir")
        if base_dir is not None:
            self._base_dir = Path(base_dir).resolve()
        return self

    @property
    def assets_path(self) -> Path:
        """Absolute directory holding this use case's asset YAML files."""
        return (self._base_dir / self.assets_dir).resolve()

    @property
    def platform_path(self) -> Path:
        """Absolute path of the platform YAML used by this use case."""
        return (self._base_dir / self.platform_file).resolve()

    @property
    def dbt_project_path(self) -> Path:
        """Absolute dbt project directory."""
        return (self._base_dir / self.dbt_project_dir).resolve()

    @property
    def dbt_sources_file(self) -> Path:
        """Absolute dbt sources file, relative to the dbt project directory."""
        return (self.dbt_project_path / self.dbt_sources_path).resolve()

    @property
    def dbt_manifest_path(self) -> Path:
        """Absolute path of the compiled dbt manifest."""
        return (self.dbt_project_path / "target" / "manifest.json").resolve()


def _read_manifest(manifest: Path) -> dict[str, Any]:
    """Read one manifest file and return its ``use_case`` block."""
    if not manifest.is_file():
        raise FileNotFoundError(f"use-case manifest not found: {manifest}")
    with manifest.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict) or "use_case" not in payload:
        raise ValueError(f"use-case manifest must contain a 'use_case' block: {manifest}")
    block = payload["use_case"]
    if not isinstance(block, dict):
        raise ValueError(f"'use_case' must be a mapping in {manifest}")
    return block


def load_use_case_config(path: str | Path) -> UseCaseConfig:
    """Load and validate one ``use_case.yaml`` manifest."""
    manifest = Path(path).resolve()
    return UseCaseConfig.model_validate(
        _read_manifest(manifest),
        context={"base_dir": manifest.parent},
    )


def _load_manifests(root: str | Path) -> list[tuple[Path, UseCaseConfig]]:
    """Load every ``<root>/*/use_case.yaml`` paired with the file it came from."""
    directory = Path(root)
    if not directory.is_dir():
        raise FileNotFoundError(f"use-case root directory not found: {directory}")
    manifests = [
        (manifest, load_use_case_config(manifest)) for manifest in sorted(directory.glob(_DEFAULT_MANIFEST_GLOB))
    ]
    return sorted(manifests, key=lambda item: item[1].name)


def discover_use_case_configs(root: str | Path) -> list[UseCaseConfig]:
    """Load every use-case manifest directly under ``root``, sorted by name."""
    return [config for _, config in _load_manifests(root)]


def resolve_use_case(root: str | Path, name: str | None = None) -> UseCaseConfig:
    """Select one use case from ``root`` by name, or the single marked default."""
    manifests = _load_manifests(root)
    available = ", ".join(sorted(config.name for _, config in manifests)) or "none"

    if name is not None:
        for _, config in manifests:
            if config.name == name:
                return config
        raise UseCaseNotFoundError(f"no use case named {name!r}; available: {available}")

    defaults = [(manifest, config) for manifest, config in manifests if config.default]
    if len(defaults) > 1:
        paths = " and ".join(str(manifest) for manifest, _ in defaults)
        raise AmbiguousDefaultUseCaseError(f"multiple default use cases: {paths}")
    if not defaults:
        raise UseCaseNotFoundError(f"no default use case under {Path(root)}; available: {available}")
    return defaults[0][1]
