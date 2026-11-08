"""Pure, stateless CLI helper functions extracted from ``core.py``.

These have no dependency on ``CrafterCmd`` state, which makes them the safe
first slice of the core refactor. ``core.py`` re-exports them under their
original private names for backward compatibility.
"""
import sys
from typing import Any, Dict, List, Optional

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


def _path_segment(key: Any) -> str:
    """A key as ``dict_generator`` writes it in a field path (dots and backslashes escaped)."""
    return str(key).replace("\\", "\\\\").replace(".", "\\.")


def strip_key_whitespace(items: List[Any]) -> Dict[str, str]:
    """Remove leading and trailing whitespace from field names, at every nesting level.

    Header cells padded with spaces (``wheelchair_boarding   `` in GTFS files, sheet headers
    ending in a space) became field names of their own that name rules do not match (#103).
    Inner runs of spaces stay. A name that would become empty or equal to another key of the
    same record keeps its spaces. Records are changed in place; the result maps each changed
    field path to the path in the source.
    """
    renamed: Dict[str, str] = {}

    def clean(value: Any, path: List[str], source: List[str]) -> Any:
        if isinstance(value, list):
            return [clean(v, path, source) for v in value]
        if not isinstance(value, dict):
            return value
        if not any(isinstance(k, str) and k != k.strip() for k in value) and not any(
            isinstance(v, (dict, list)) for v in value.values()
        ):
            return value
        out = {}
        for key, item in value.items():
            new = key.strip() if isinstance(key, str) else key
            if new != key and (not new or new in value or new in out):
                new = key
            new_path, old_path = path + [_path_segment(new)], source + [_path_segment(key)]
            if new != key:
                renamed[".".join(new_path)] = ".".join(old_path)
            out[new] = clean(item, new_path, old_path)
        return out

    for index, item in enumerate(items):
        items[index] = clean(item, [], [])
    return renamed


def resolve_output_target(output: Optional[str], stdout_flag: bool):
    """Convert output arguments into file-like objects when needed."""
    if stdout_flag:
        return sys.stdout
    if isinstance(output, str) and output.strip().lower() in ("-", "stdout"):
        return sys.stdout
    return output
