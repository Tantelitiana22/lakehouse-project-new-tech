"""Contract tests for resolving use-case handlers from dotted paths."""

from __future__ import annotations

import pytest

from ndr_lakehouse.domain.errors import InvalidHandlerReferenceError
from ndr_lakehouse.orchestration.handlers import NullHandler, resolve_handler


def test_resolves_a_module_attribute():
    # Given
    reference = "ndr_lakehouse.orchestration.handlers:NullHandler"

    # When
    handler = resolve_handler(reference, asset_name="demo")

    # Then
    assert isinstance(handler, NullHandler)


def test_missing_attribute_names_asset_and_path():
    # Given
    reference = "ndr_lakehouse.orchestration.handlers:Missing"

    # When / # Then
    with pytest.raises(InvalidHandlerReferenceError, match="demo.*Missing|Missing.*demo"):
        resolve_handler(reference, asset_name="demo")


def test_malformed_reference_is_rejected():
    # Given
    reference = "ndr_lakehouse.orchestration.handlers.NullHandler"

    # When / # Then
    with pytest.raises(InvalidHandlerReferenceError, match="demo.*colon"):
        resolve_handler(reference, asset_name="demo")


def test_missing_module_is_wrapped():
    # Given
    reference = "no_such_module:X"

    # When / # Then
    with pytest.raises(InvalidHandlerReferenceError, match="no_such_module"):
        resolve_handler(reference, asset_name="demo")


def test_class_without_land_or_load_is_rejected():
    # Given
    reference = "ndr_lakehouse.domain.errors:InvalidHandlerReferenceError"

    # When / # Then
    with pytest.raises(InvalidHandlerReferenceError, match="demo.*no callable land"):
        resolve_handler(reference, asset_name="demo")


def test_handler_without_load_is_rejected():
    # Given
    reference = f"{__name__}:LandOnlyHandler"

    # When / # Then
    with pytest.raises(InvalidHandlerReferenceError, match="demo.*no callable load"):
        resolve_handler(reference, asset_name="demo")


def test_instantiation_failure_is_wrapped():
    # Given
    reference = "ndr_lakehouse.asset_property.schema_validator:AssetConfig"

    # When / # Then
    with pytest.raises(InvalidHandlerReferenceError, match="Cannot instantiate handler.*demo"):
        resolve_handler(reference, asset_name="demo")


def test_null_handler_returns_none(sample_asset_config):
    # Given
    handler = NullHandler()
    ctx = None  # never touched

    # When
    land_result = handler.land(ctx, sample_asset_config)
    load_result = handler.load(ctx, sample_asset_config, "uri")

    # Then
    assert land_result is None
    assert load_result is None


class LandOnlyHandler:
    """Non-conforming handler with ``land()`` but no ``load()``."""

    def land(self, context, config):
        return None
