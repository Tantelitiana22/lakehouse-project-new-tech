"""Repo-local Dagster entrypoint: one use case per process.

``use_case/`` is deliberately not a wheel package — it is the composition
root that binds the generic ``ndr_lakehouse`` library to one situated
configuration.
"""

from ndr_lakehouse.orchestration.definitions import build_definitions

defs = build_definitions()
