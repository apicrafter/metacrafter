"""Metacrafter - Data classification and field labeling tool."""

from metacrafter.exceptions import (
    MetacrafterError,
    ConfigurationError,
    RuleCompilationError,
    FileProcessingError,
    DatabaseError,
    ValidationError,
)

__all__ = [
    "MetacrafterError",
    "ConfigurationError",
    "RuleCompilationError",
    "FileProcessingError",
    "DatabaseError",
    "ValidationError",
    "CrafterCmd",
]


def __getattr__(name):
    """Lazily expose the public API without eager heavy imports (PEP 562)."""
    if name == "CrafterCmd":
        from metacrafter.core import CrafterCmd

        return CrafterCmd
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

