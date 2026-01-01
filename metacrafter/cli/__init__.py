"""CLI command definitions for Metacrafter.

This module contains all Typer command functions extracted from core.py
as part of Phase 1 refactoring to improve code organization.
"""

from .commands import (
    register_commands,
)

__all__ = ['register_commands']

