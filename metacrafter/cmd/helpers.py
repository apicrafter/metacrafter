"""Pure, stateless CLI helper functions extracted from ``core.py``.

These have no dependency on ``CrafterCmd`` state, which makes them the safe
first slice of the core refactor. ``core.py`` re-exports them under their
original private names for backward compatibility.
"""
import sys
from typing import Optional


def split_option_list(value: Optional[str]):
    """Split comma-separated option values preserving empty-string marker."""
    if value is None:
        return None
    entries = []
    for token in value.split(","):
        token = token.strip()
        if token == "":
            continue
        if token in {'""', "''"}:
            entries.append("")
        elif token.lower() in ("none", "null"):
            entries.append(None)
        else:
            entries.append(token)
    return entries or None


def resolve_output_target(output: Optional[str], stdout_flag: bool):
    """Convert output arguments into file-like objects when needed."""
    if stdout_flag:
        return sys.stdout
    if isinstance(output, str) and output.strip().lower() in ("-", "stdout"):
        return sys.stdout
    return output
