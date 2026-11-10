"""Resolve use-case handler classes from dotted ``module:attribute`` references."""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Any

from ndr_lakehouse.domain.errors import InvalidHandlerReferenceError

if TYPE_CHECKING:
    from ndr_lakehouse.asset_property.schema_validator import AssetConfig
    from ndr_lakehouse.domain.ports import HandlerContext, UseCaseHandler

logger = logging.getLogger(__name__)


def resolve_handler(dotted: str, *, asset_name: str) -> UseCaseHandler:
    """Import, instantiate and validate the handler named by ``dotted``."""

    module_path, separator, attribute = dotted.partition(":")
    if not separator or not module_path or not attribute or ":" in attribute:
        raise InvalidHandlerReferenceError(
            f"Handler for asset {asset_name!r} must use the 'module:attribute' form "
            f"with a single colon, got {dotted!r}"
        )

    try:
        module = importlib.import_module(module_path)
        cls = getattr(module, attribute)
    except (ImportError, AttributeError, ValueError) as exc:
        raise InvalidHandlerReferenceError(
            f"Cannot resolve handler {dotted!r} for asset {asset_name!r}: {exc}"
        ) from exc

    try:
        instance = cls()
    except Exception as exc:
        raise InvalidHandlerReferenceError(
            f"Cannot instantiate handler {dotted!r} for asset {asset_name!r}: {exc}"
        ) from exc

    for method in ("land", "load"):
        if not callable(getattr(instance, method, None)):
            raise InvalidHandlerReferenceError(
                f"Handler {dotted!r} for asset {asset_name!r} has no callable {method}()"
            )

    logger.debug("Resolved handler %s for asset %s", dotted, asset_name)
    return instance


class NullHandler:
    """Handler that keeps the default landing and loading behaviour."""

    def land(self, context: HandlerContext, config: AssetConfig) -> str | None:
        """Return ``None`` to let the default landing flow run."""

        return None

    def load(self, context: HandlerContext, config: AssetConfig, landing_uri: str) -> dict[str, Any] | None:
        """Return ``None`` to let the default loading flow run."""

        return None
