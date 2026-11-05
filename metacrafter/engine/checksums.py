"""Checksum validators (``validator: checksum:<algorithm>``).

The algorithms are vendored from metacrafter-registry (``checksum_algorithms.py``) so
registry ``checksum`` names work without the registry package.
"""
from __future__ import annotations

from typing import Callable, Dict, Optional

from metacrafter.engine.checksum_algorithms import ALGORITHMS

CHECKSUM_PREFIX = "checksum:"


def registry_algorithms() -> Dict[str, Callable[[str], bool]]:
    """Checksum algorithms by registry name."""
    return dict(ALGORITHMS)


def checksum_function(name: str) -> Optional[Callable[[str], bool]]:
    """A safe validator for a registry checksum name, or None when unknown."""
    algorithm = ALGORITHMS.get(name)
    if algorithm is None:
        return None

    def validate(value: str) -> bool:
        try:
            return bool(algorithm(str(value)))
        except Exception:  # noqa: BLE001 - a malformed value fails the check
            return False

    validate.__name__ = f"checksum_{name}"
    return validate


def checksum_validator(spec: str) -> Optional[Callable[[str], bool]]:
    """Return a validator for ``checksum:<algorithm>`` or None when the algorithm is unknown."""
    if not spec.startswith(CHECKSUM_PREFIX):
        raise ValueError(f"Not a checksum validator: {spec}")
    return checksum_function(spec[len(CHECKSUM_PREFIX):])
