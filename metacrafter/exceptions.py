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

