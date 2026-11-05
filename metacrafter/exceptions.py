# -*- coding: utf-8 -*-
"""Custom exception classes for Metacrafter."""


class MetacrafterError(Exception):
    """Base exception for all Metacrafter errors."""
    pass


class ConfigurationError(MetacrafterError):
    """Raised when configuration is invalid or cannot be loaded."""
    pass


class RuleCompilationError(MetacrafterError):
    """Raised when a rule cannot be compiled."""
    pass


class FileProcessingError(MetacrafterError):
    """Raised when file processing fails."""
    pass


class DatabaseError(MetacrafterError):
    """Raised when database operations fail."""
    pass


class ValidationError(MetacrafterError):
    """Raised when input validation fails."""
    pass



class MissingExtraError(MetacrafterError, ImportError):
    """Raised when a feature needs an optional dependency group that is not installed."""

    def __init__(self, extra: str, feature: str, module: str) -> None:
        self.extra = extra
        self.feature = feature
        self.module = module
        super().__init__(
            f"{feature} requires the '{extra}' extra (missing module '{module}'). "
            f"Install it with: pip install 'metacrafter[{extra}]'"
        )


def require_extra(module: str, extra: str, feature: str):
    """Import ``module`` or raise :class:`MissingExtraError` naming the extra."""
    import importlib

    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise MissingExtraError(extra, feature, module) from exc
