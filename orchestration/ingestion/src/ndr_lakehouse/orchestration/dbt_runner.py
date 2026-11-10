"""Dagster resource that drives the dbt v2 CLI as a subprocess.

dbt v2 ships as a bundled binary whose Python package exposes no importable API:
``dbt.adapters``, ``dbt.config``, ``dbt.flags`` and ``dbt.graph`` no longer exist.
``dagster-dbt`` imports all of them, so it cannot coexist with dbt v2 in one
environment. This module replaces that integration with the CLI contract dbt v2
does guarantee, which also keeps dbt an optional extra: nothing here imports
``dbt``.

Two v2 details are load-bearing:

* The Spark adapter is built into the binary but gated as experimental, so
  ``DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true`` is required or every command fails
  with ``dbt1005``.
* ``--project-dir`` / ``--profiles-dir`` are passed explicitly and the working
  directory is the project, so a use case never depends on where the Dagster
  process happened to start.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dagster import ConfigurableResource, MetadataValue

from ndr_lakehouse.domain.errors import DbtCommandError

logger = logging.getLogger(__name__)

#: Environment variable dbt v2 requires before it will load the Spark adapter.
EXPERIMENTAL_ADAPTERS_ENV = "DBT_ALLOW_EXPERIMENTAL_ADAPTERS"

#: Tail length kept when surfacing a failed command in Dagster metadata.
_OUTPUT_TAIL = 4000


@dataclass(frozen=True)
class DbtCommandResult:
    """Outcome of one ``dbt`` invocation."""

    args: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    @property
    def command(self) -> str:
        """The invocation as a shell-quotable string, for logs and metadata."""
        return shlex.join(self.args)


def dbt_environment(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return the environment dbt v2 needs, layered over ``base``.

    The Spark adapter is bundled but experimental, so it is only registered when
    this flag is set. Doing it here means no caller has to remember.
    """

    environment = dict(os.environ if base is None else base)
    environment[EXPERIMENTAL_ADAPTERS_ENV] = "true"
    return environment


class DbtCliResource(ConfigurableResource):
    """Run ``dbt`` against one project directory and expose its artefacts.

    Commands are executed eagerly by :meth:`invoke`, which returns the captured
    result instead of raising, so a validation asset can report a failure as
    metadata rather than as a Dagster exception.
    """

    project_dir: str
    profiles_dir: str | None = None
    executable: str = "dbt"
    target: str | None = None
    timeout_seconds: float = 900.0

    def _resolved_paths(self) -> tuple[Path, Path]:
        project = Path(self.project_dir).resolve()
        profiles = Path(self.profiles_dir).resolve() if self.profiles_dir else project
        return project, profiles

    def is_available(self) -> bool:
        """Return whether the ``dbt`` executable can be found and started.

        ``dbt`` is an optional extra of this project, so the resource bundle
        probes this before registering itself: a use case that never runs dbt
        must still load in an environment without the binary.
        """

        executable = shutil.which(self.executable)
        return executable is not None

    def command(self, args: Sequence[str]) -> list[str]:
        """Build the full ``dbt`` argv for ``args``, keeping caller order."""

        project, profiles = self._resolved_paths()
        argv = [self.executable, *args, "--project-dir", str(project), "--profiles-dir", str(profiles)]
        if self.target:
            argv.extend(["--target", self.target])
        return argv

    def invoke(self, args: Sequence[str]) -> DbtCommandResult:
        """Run ``dbt <args>`` and capture its output.

        Raises:
            DbtCommandError: the executable is missing or the run timed out.
        """

        project, _ = self._resolved_paths()
        argv = self.command(args)
        try:
            completed = subprocess.run(  # noqa: S603 - argv is not shell-expanded
                argv,
                cwd=project,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout_seconds,
                env=dbt_environment(),
            )
        except FileNotFoundError as exc:
            raise DbtCommandError(f"dbt executable not found: {self.executable}") from exc
        except subprocess.TimeoutExpired as exc:
            raise DbtCommandError(f"dbt {args[0] if args else ''} timed out after {self.timeout_seconds}s") from exc

        return DbtCommandResult(
            args=tuple(argv),
            returncode=completed.returncode,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )

    def run(self, args: Sequence[str]) -> DbtCommandResult:
        """Run ``dbt <args>`` and raise when it fails."""

        result = self.invoke(args)
        if result.returncode != 0:
            raise DbtCommandError(f"dbt failed ({result.returncode}): {result.command}\n{result.stderr.strip()}")
        return result

    def read_manifest(self) -> dict[str, Any]:
        """Return the parsed ``target/manifest.json`` as written by the engine."""

        project, _ = self._resolved_paths()
        manifest_path = project / "target" / "manifest.json"
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def metadata(self, result: DbtCommandResult) -> dict[str, MetadataValue]:
        """Summarise a result for Dagster's materialisation metadata."""

        return {
            "command": MetadataValue.text(result.command),
            "return_code": MetadataValue.int(result.returncode),
            "stdout_tail": MetadataValue.text(result.stdout[-_OUTPUT_TAIL:]),
            "stderr_tail": MetadataValue.text(result.stderr[-_OUTPUT_TAIL:]),
        }
