"""Checksum validators backed by metacrafter-registry (``validator: checksum:<algo>``)."""
from __future__ import annotations

from typing import Callable, Dict, Optional

CHECKSUM_PREFIX = "checksum:"


def registry_algorithms() -> Dict[str, Callable[[str], bool]]:
    """Checksum algorithms from metacrafter-registry, or an empty dict if not installed."""
    try:
        from metacrafter_registry.checksums import ALGORITHMS
    except Exception:  # pragma: no cover - depends on the environment
        return {}
    return dict(ALGORITHMS)


def checksum_validator(spec: str) -> Optional[Callable[[str], bool]]:
    """Return a validator for ``checksum:<algorithm>`` or None when unavailable."""
    if not spec.startswith(CHECKSUM_PREFIX):
        raise ValueError(f"Not a checksum validator: {spec}")
    name = spec[len(CHECKSUM_PREFIX):]
    algorithm = registry_algorithms().get(name)
    if algorithm is None:
        return None

    def validate(value: str) -> bool:
        try:
            return bool(algorithm(str(value)))
        except Exception:  # noqa: BLE001 - a malformed value fails the check
            return False

    validate.__name__ = f"checksum_{name}"
    return validate
