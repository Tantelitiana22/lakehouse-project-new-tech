"""Contract tests for the use-case handler protocol and its context."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from ndr_lakehouse.domain.ports import HandlerContext, UseCaseHandler


class _Stub:
    """Satisfies both ``LandingPort`` and ``IcebergPort`` structurally."""

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
