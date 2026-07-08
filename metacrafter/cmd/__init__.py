"""Command-layer package for Metacrafter.

This package incrementally absorbs orchestration logic that historically lived
in the monolithic ``metacrafter/core.py``. See
``openspec/changes/refactor-metacrafter-core`` for the full plan.
"""

from .helpers import split_option_list, resolve_output_target

__all__ = ["split_option_list", "resolve_output_target"]
