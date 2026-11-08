"""Pure, stateless CLI helper functions extracted from ``core.py``.

These have no dependency on ``CrafterCmd`` state, which makes them the safe
first slice of the core refactor. ``core.py`` re-exports them under their
original private names for backward compatibility.
"""
import sys
from typing import Any, List, Optional

UTF8_BOM = "\ufeff"


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


def strip_bom_from_keys(items: List[Any]) -> List[Any]:
    """Remove a UTF-8 byte order mark left at the start of field names.

    A CSV file that starts with a BOM and otherwise decodes as plain UTF-8 is read
    with the ``utf-8`` codec, so the BOM stays in the first header (``\\ufeffid``)
    and field-name rules do not match it. Records are changed in place.
    """
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        if not any(isinstance(key, str) and key.startswith(UTF8_BOM) for key in item):
            continue
        items[index] = {
            (key.lstrip(UTF8_BOM) if isinstance(key, str) else key): value
            for key, value in item.items()
        }
    return items


def resolve_output_target(output: Optional[str], stdout_flag: bool):
    """Convert output arguments into file-like objects when needed."""
    if stdout_flag:
        return sys.stdout
    if isinstance(output, str) and output.strip().lower() in ("-", "stdout"):
        return sys.stdout
    return output
