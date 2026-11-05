"""Registry integration: adapter, rule key checks and the legacy client."""

from metacrafter.registry.adapter import (  # noqa: F401
    DatatypeRef,
    RegistryAdapter,
    RegistryProvenance,
    Resolution,
    get_adapter,
)

__all__ = ["DatatypeRef", "RegistryAdapter", "RegistryProvenance", "Resolution", "get_adapter"]
