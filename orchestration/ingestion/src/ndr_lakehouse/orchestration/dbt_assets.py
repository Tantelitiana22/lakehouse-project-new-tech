"""dbt ``@dbt_assets`` integration and the MetricFlow validation asset."""

import json
import logging
import os
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dagster import AssetKey, AssetsDefinition, MetadataValue, asset

logger = logging.getLogger(__name__)


def build_dbt_assets(
    manifest_path: str | Path | None = None,
    project_dir: str | Path | None = None,
) -> list[Any]:
    """Build dbt assets when a manifest exists; otherwise return ``[]``."""

    if manifest_path is None or project_dir is None:
        logger.info("dbt assets need both a manifest path and a project dir; skipping")
        return []

    resolved_manifest = Path(manifest_path)
    resolved_project = Path(project_dir)
    if not resolved_manifest.exists():
        logger.info(
            "dbt manifest not found at %s; dbt assets are not registered yet",
            resolved_manifest,
        )
        return []

    from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

    project = DbtProject(project_dir=resolved_project)
    manifest = json.loads(resolved_manifest.read_text(encoding="utf-8"))

    @dbt_assets(manifest=manifest, project=project, name="lakehouse_dbt_assets")
    def lakehouse_dbt_assets(context, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    return [lakehouse_dbt_assets]


def build_metricflow_asset(
    *,
    gold_models: Sequence[str],
    dbt_project_dir: Path,
    metric: str,
) -> AssetsDefinition:
    """Build the MetricFlow validation asset for one use case's Gold models."""

    project_dir = Path(dbt_project_dir)
    deps = [AssetKey(name) for name in gold_models]

    @asset(
        name="metricflow_semantic_validation",
        deps=deps,
        group_name="semantic",
        compute_kind="metricflow",
        description=(
            "Validate the MetricFlow semantic graph (entities, dimensions, measures "
            f"and the {metric} ratio metric) after Gold models are materialised."
        ),
        tags={"layer": "semantic", "owner": "data-platform"},
    )
    def metricflow_semantic_validation(context) -> dict[str, Any]:
        """Run ``mf validate-configs --skip-dw`` against the dbt project."""

        command = ["mf", "validate-configs", "--skip-dw"]
        try:
            completed = subprocess.run(
                command,
                cwd=project_dir,
                capture_output=True,
                text=True,
                check=False,
                env={**os.environ, "DBT_PROFILES_DIR": str(project_dir)},
            )
        except FileNotFoundError as exc:
            raise RuntimeError("MetricFlow CLI 'mf' is not installed in this runtime") from exc

        stdout = (completed.stdout or "").strip()
        stderr = (completed.stderr or "").strip()
        context.add_output_metadata(
            {
                "command": MetadataValue.text(" ".join(command)),
                "project_dir": MetadataValue.text(str(project_dir)),
                "return_code": MetadataValue.int(completed.returncode),
                "stdout_tail": MetadataValue.text(stdout[-4000:]),
                "stderr_tail": MetadataValue.text(stderr[-4000:]),
            }
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"MetricFlow semantic validation failed ({completed.returncode}): {stderr}"
            )
        return {
            "return_code": completed.returncode,
            "stdout": stdout,
            "stderr": stderr,
        }

    return metricflow_semantic_validation
